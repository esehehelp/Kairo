package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"kairo/internal/id"
)

// EnsurePriorityPreemption asks lower-priority cooperative executions to
// suspend when, and only when, their active GPU leases are the remaining
// obstacle to the highest-priority waiting execution on the physical node
// hosting executorID. The victim may belong to another executor on that node;
// its own executor receives the attempt-scoped command through normal polling.
//
// It returns the priority-preemption command IDs selected for delivery. A
// repeated call returns the same live command IDs; it never creates a second
// suspend command for an attempt. This method only coordinates transfer of
// execution rights. It never requeues an execution or interprets its exit.
func (s *Store) EnsurePriorityPreemption(ctx context.Context, executorID string) ([]string, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var nodeID, executorAttributes string
	var enabled bool
	err = tx.QueryRowContext(ctx, `SELECT node_id,attributes_json,enabled FROM executors WHERE id=?`, executorID).Scan(&nodeID, &executorAttributes, &enabled)
	if errors.Is(err, sql.ErrNoRows) || !enabled {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	candidates, err := preemptionCandidatesTx(ctx, tx, executorAttributes)
	if err != nil || len(candidates) == 0 {
		return nil, err
	}
	highestPriority := candidates[0].execution.Priority
	for _, candidate := range candidates {
		if candidate.execution.Priority != highestPriority {
			break
		}
		victims, viable, planErr := preemptionPlanTx(ctx, tx, nodeID, candidate)
		if planErr != nil {
			return nil, planErr
		}
		if !viable || len(victims) == 0 {
			continue
		}
		commandIDs, enqueueErr := enqueuePrioritySuspendsTx(ctx, tx, candidate.execution, victims)
		if enqueueErr != nil {
			return nil, enqueueErr
		}
		if err = tx.Commit(); err != nil {
			return nil, err
		}
		return commandIDs, nil
	}
	return nil, nil
}

func preemptionCandidatesTx(ctx context.Context, tx *sql.Tx, executorAttributes string) ([]executionCandidate, error) {
	rows, err := tx.QueryContext(ctx, `SELECT e.id,e.project_scope_id,e.queue_scope_id,e.task_scope_id,e.client_request_id,e.spec_digest,e.state,e.argv_json,e.cwd,e.executor_selector_json,e.priority,e.checkpointable,e.preemptible,e.input_continuation_ref,e.terminal_cause,e.submitted_at,e.authorized_at,e.started_at,e.terminal_at
		FROM execution_requests e
		WHERE e.state='waiting'
		AND NOT EXISTS (SELECT 1 FROM leases l WHERE l.execution_id=e.id AND l.state!='released')
		AND NOT EXISTS (
			SELECT 1 FROM admission_gates g
			WHERE g.scope_id IN(e.project_scope_id,e.queue_scope_id,e.task_scope_id) AND g.state!='open'
		)
		ORDER BY e.priority DESC,e.submitted_at,e.id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var candidates []executionCandidate
	for rows.Next() {
		var candidate executionCandidate
		if err = rows.Scan(&candidate.execution.ID, &candidate.execution.ProjectScopeID, &candidate.execution.QueueScopeID, &candidate.execution.TaskScopeID, &candidate.execution.ClientRequestID, &candidate.execution.SpecDigest, &candidate.execution.State, &candidate.argvJSON, &candidate.execution.CWD, &candidate.selectorJSON, &candidate.execution.Priority, &candidate.execution.Checkpointable, &candidate.execution.Preemptible, &candidate.execution.InputContinuationRef, &candidate.execution.TerminalCause, &candidate.execution.SubmittedAt, &candidate.execution.AuthorizedAt, &candidate.execution.StartedAt, &candidate.execution.TerminalAt); err != nil {
			return nil, err
		}
		if selectorMatches(candidate.selectorJSON, executorAttributes) {
			candidates = append(candidates, candidate)
		}
	}
	return candidates, rows.Err()
}

type preemptionVictim struct {
	executionID        string
	projectID          string
	attemptID          string
	leaseID            string
	epoch              int64
	priority           int
	allowsUnattributed bool
}

type preemptionGPU struct {
	id           string
	total        sql.NullInt64
	free         sql.NullInt64
	validUntil   sql.NullString
	external     bool
	unattributed bool
	victim       *preemptionVictim
}

func preemptionPlanTx(ctx context.Context, tx *sql.Tx, nodeID string, candidate executionCandidate) (map[string]preemptionVictim, bool, error) {
	requests, err := loadResourceRequestsTx(ctx, tx, candidate.execution.ID)
	if err != nil {
		return nil, false, err
	}
	var exclusive []requestRow
	var capacity []requestRow
	for _, request := range requests {
		if request.Type == "exclusive" {
			if request.Kind != "gpu" {
				return nil, false, nil
			}
			exclusive = append(exclusive, request)
			continue
		}
		capacity = append(capacity, request)
	}
	if len(exclusive) == 0 {
		return nil, false, nil
	}

	gpus, err := preemptionGPUsTx(ctx, tx, nodeID, candidate.execution.Priority)
	if err != nil {
		return nil, false, err
	}
	selected := make(map[string]bool)
	victims := make(map[string]preemptionVictim)
	stamp := time.Now().UTC()
	for _, request := range exclusive {
		var constraints gpuConstraints
		if json.Unmarshal([]byte(request.Constraints), &constraints) != nil {
			return nil, false, nil
		}
		needed := request.Quantity
		// Prefer GPUs already free. This prevents gratuitous preemption when
		// the request can be admitted without displacing an execution.
		for pass := 0; pass < 2 && needed > 0; pass++ {
			for _, gpu := range gpus {
				if needed == 0 || selected[gpu.id] || !preemptionGPUPhysicallyEligible(gpu, constraints, stamp) {
					continue
				}
				if pass == 0 && gpu.victim != nil || pass == 1 && gpu.victim == nil {
					continue
				}
				if gpu.victim == nil {
					if !gpu.free.Valid || gpu.free.Int64 < constraints.MinFree {
						continue
					}
				} else {
					victims[gpu.victim.attemptID] = *gpu.victim
				}
				selected[gpu.id] = true
				needed--
			}
		}
		if needed != 0 {
			return nil, false, nil
		}
	}
	for _, victim := range victims {
		safe, safeErr := preemptionVictimLeaseSafeTx(ctx, tx, victim.leaseID, nodeID, stamp)
		if safeErr != nil || !safe {
			return nil, false, safeErr
		}
	}
	for _, request := range capacity {
		available, capacityErr := preemptionCapacityAvailableTx(ctx, tx, nodeID, request, victims)
		if capacityErr != nil || !available {
			return nil, false, capacityErr
		}
	}
	return victims, len(victims) != 0, nil
}

func preemptionGPUPhysicallyEligible(gpu preemptionGPU, constraints gpuConstraints, stamp time.Time) bool {
	if gpu.external || gpu.unattributed && (gpu.victim == nil || !gpu.victim.allowsUnattributed) || !gpu.total.Valid || gpu.total.Int64 < constraints.MinTotal || !gpu.validUntil.Valid {
		return false
	}
	deadline, err := time.Parse(time.RFC3339Nano, gpu.validUntil.String)
	return err == nil && deadline.After(stamp)
}

func preemptionGPUsTx(ctx context.Context, tx *sql.Tx, nodeID string, waitingPriority int) ([]preemptionGPU, error) {
	rows, err := tx.QueryContext(ctx, `SELECT ri.id,o.valid_until,o.total_bytes,o.free_bytes,
		EXISTS(SELECT 1 FROM external_claims c WHERE c.resource_id=ri.id AND c.cleared_at IS NULL AND c.claim_kind='external_process'),
		EXISTS(SELECT 1 FROM external_claims c WHERE c.resource_id=ri.id AND c.cleared_at IS NULL AND c.claim_kind='unattributed_activity')
		FROM resource_instances ri
		LEFT JOIN resource_observations o ON o.id=(SELECT id FROM resource_observations WHERE resource_id=ri.id ORDER BY id DESC LIMIT 1)
		WHERE ri.node_id=? AND ri.kind='gpu' AND ri.admin_state='enabled'
		ORDER BY ri.id`, nodeID)
	if err != nil {
		return nil, err
	}
	var gpus []preemptionGPU
	for rows.Next() {
		var gpu preemptionGPU
		if err = rows.Scan(&gpu.id, &gpu.validUntil, &gpu.total, &gpu.free, &gpu.external, &gpu.unattributed); err != nil {
			rows.Close()
			return nil, err
		}
		gpus = append(gpus, gpu)
	}
	if err = rows.Close(); err != nil {
		return nil, err
	}
	for index := range gpus {
		var victim preemptionVictim
		var attemptID sql.NullString
		var leaseState, attemptState, leaseExecutorNode, executionState string
		var checkpointable, preemptible, leaseExecutorEnabled bool
		err = tx.QueryRowContext(ctx, `SELECT l.execution_id,e.project_scope_id,a.id,l.id,l.coordination_epoch,e.priority,e.checkpointable,e.preemptible,l.state,COALESCE(a.state,''),xe.node_id,xe.enabled,e.state,
			COALESCE((SELECT MIN(CASE WHEN COALESCE(json_extract(rr.policy_json,'$.on_unattributed_activity'),'wait')='allow' THEN 1 ELSE 0 END)
				FROM resource_requests rr WHERE rr.execution_id=e.id AND rr.request_type='exclusive' AND rr.kind='gpu'),0)
			FROM lease_items li
			JOIN leases l ON l.id=li.lease_id
			JOIN execution_requests e ON e.id=l.execution_id
			JOIN executors xe ON xe.id=l.executor_id
			LEFT JOIN attempts a ON a.id=l.attempt_id
			WHERE li.resource_id=? AND li.kind='gpu'
			AND l.state IN('reserved','prepared','active','releasing','stale','revocation_requested')
			LIMIT 1`, gpus[index].id).Scan(&victim.executionID, &victim.projectID, &attemptID, &victim.leaseID, &victim.epoch, &victim.priority, &checkpointable, &preemptible, &leaseState, &attemptState, &leaseExecutorNode, &leaseExecutorEnabled, &executionState, &victim.allowsUnattributed)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if attemptID.Valid && leaseState == "active" && attemptState == "running" && executionState == "started" && checkpointable && preemptible && victim.priority < waitingPriority && leaseExecutorEnabled && leaseExecutorNode == nodeID {
			victim.attemptID = attemptID.String
			gpus[index].victim = &victim
		} else {
			// A live but non-preemptible lease makes this GPU unavailable. Mark
			// it ineligible rather than mistaking it for an unleased GPU.
			gpus[index].validUntil = sql.NullString{}
		}
	}
	return gpus, nil
}

func preemptionVictimLeaseSafeTx(ctx context.Context, tx *sql.Tx, leaseID, nodeID string, stamp time.Time) (bool, error) {
	rows, err := tx.QueryContext(ctx, `SELECT ri.node_id,ri.admin_state,o.valid_until,
		EXISTS(SELECT 1 FROM external_claims c WHERE c.resource_id=ri.id AND c.cleared_at IS NULL AND
			(c.claim_kind='external_process' OR c.claim_kind='unattributed_activity' AND COALESCE(
				(SELECT MIN(CASE WHEN COALESCE(json_extract(rr.policy_json,'$.on_unattributed_activity'),'wait')='allow' THEN 1 ELSE 0 END)
				 FROM resource_requests rr WHERE rr.execution_id=l.execution_id AND rr.request_type='exclusive' AND rr.kind=li.kind),
				0)=0))
		FROM lease_items li
		JOIN leases l ON l.id=li.lease_id
		JOIN resource_instances ri ON ri.id=li.resource_id
		LEFT JOIN resource_observations o ON o.id=(SELECT id FROM resource_observations WHERE resource_id=ri.id ORDER BY id DESC LIMIT 1)
		WHERE li.lease_id=? AND li.kind='gpu' AND li.resource_id IS NOT NULL`, leaseID)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
		var resourceNode, adminState string
		var validUntil sql.NullString
		var unsafeClaim bool
		if err = rows.Scan(&resourceNode, &adminState, &validUntil, &unsafeClaim); err != nil {
			return false, err
		}
		deadline, parseErr := time.Parse(time.RFC3339Nano, validUntil.String)
		if resourceNode != nodeID || adminState != "enabled" || unsafeClaim || !validUntil.Valid || parseErr != nil || !deadline.After(stamp) {
			return false, nil
		}
	}
	return count != 0, rows.Err()
}

func preemptionCapacityAvailableTx(ctx context.Context, tx *sql.Tx, nodeID string, request requestRow, victims map[string]preemptionVictim) (bool, error) {
	identity := ""
	if request.Kind == "disk" {
		identity = request.Filesystem
	}
	var total, free sql.NullInt64
	err := tx.QueryRowContext(ctx, `SELECT o.total_bytes,o.free_bytes
		FROM resource_instances ri
		JOIN resource_observations o ON o.id=(SELECT id FROM resource_observations WHERE resource_id=ri.id ORDER BY id DESC LIMIT 1)
		WHERE ri.node_id=? AND ri.kind=? AND ri.admin_state='enabled' AND (?='' OR ri.stable_identity=?) AND julianday(o.valid_until)>julianday(?)
		ORDER BY ri.id LIMIT 1`, nodeID, request.Kind, identity, identity, now()).Scan(&total, &free)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	victimLeases := make(map[string]bool, len(victims))
	for _, victim := range victims {
		victimLeases[victim.leaseID] = true
	}
	rows, err := tx.QueryContext(ctx, `SELECT l.id,li.quantity
		FROM lease_items li JOIN leases l ON l.id=li.lease_id JOIN executors e ON e.id=l.executor_id
		WHERE e.node_id=? AND li.kind=? AND li.filesystem=? AND l.state IN('reserved','prepared','active','releasing','stale','revocation_requested')`, nodeID, request.Kind, request.Filesystem)
	if err != nil {
		return false, err
	}
	var reserved int64
	var reclaimable int64
	for rows.Next() {
		var leaseID string
		var quantity int64
		if err = rows.Scan(&leaseID, &quantity); err != nil {
			rows.Close()
			return false, err
		}
		if victimLeases[leaseID] {
			if request.Kind == "ram" {
				reclaimable += quantity
			}
		} else {
			reserved += quantity
		}
	}
	if err = rows.Close(); err != nil {
		return false, err
	}
	if request.Kind == "cpu" {
		return total.Valid && total.Int64-reserved >= request.Quantity, nil
	}
	// Current free RAM includes the running victims' physical usage. Project
	// their admitted RAM capacity as reclaimable for preemption planning; the
	// normal reservation/prepare path still requires a fresh observation after
	// the victims have quiesced, so this cannot authorize an unsafe launch.
	if request.Kind == "ram" {
		if !total.Valid || !free.Valid || total.Int64 < request.Quantity {
			return false, nil
		}
		projectedFree := free.Int64
		if reclaimable >= total.Int64-projectedFree {
			projectedFree = total.Int64
		} else {
			projectedFree += reclaimable
		}
		return projectedFree-reserved >= request.Quantity, nil
	}
	if !free.Valid || free.Int64-reserved-request.Quantity < 0 {
		return false, nil
	}
	if request.Kind == "disk" {
		var constraints map[string]int64
		_ = json.Unmarshal([]byte(request.Constraints), &constraints)
		return free.Int64-reserved-request.Quantity >= constraints["min_free_after_bytes"], nil
	}
	return true, nil
}

func enqueuePrioritySuspendsTx(ctx context.Context, tx *sql.Tx, waiting ExecutionRequest, victims map[string]preemptionVictim) ([]string, error) {
	attemptIDs := make([]string, 0, len(victims))
	for attemptID := range victims {
		attemptIDs = append(attemptIDs, attemptID)
	}
	sort.Strings(attemptIDs)
	commandIDs := make([]string, 0, len(attemptIDs))
	for _, attemptID := range attemptIDs {
		victim := victims[attemptID]
		var existingID, existingOrigin, existingState string
		err := tx.QueryRowContext(ctx, `SELECT id,origin,state FROM commands WHERE attempt_id=? ORDER BY created_at LIMIT 1`, attemptID).Scan(&existingID, &existingOrigin, &existingState)
		if err == nil {
			if existingOrigin == "priority_preemption" && existingState != "rejected" {
				commandIDs = append(commandIDs, existingID)
			}
			continue
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}
		commandID, stamp := id.New("cmd"), now()
		reason := "higher-priority execution waiting: " + waiting.ID
		if _, err = tx.ExecContext(ctx, `INSERT INTO commands(id,execution_id,attempt_id,lease_id,coordination_epoch,kind,origin,reason,state,created_at,updated_at) VALUES(?,?,?,?,?,'suspend','priority_preemption',?,'pending',?,?)`, commandID, victim.executionID, victim.attemptID, victim.leaseID, victim.epoch, reason, stamp, stamp); err != nil {
			return nil, err
		}
		if err = appendCoordinationEventTx(ctx, tx, "suspend_requested", &victim.projectID, "command", commandID, &victim.epoch, map[string]any{"execution_id": victim.executionID, "attempt_id": victim.attemptID, "origin": "priority_preemption", "reason": reason, "waiting_execution_id": waiting.ID}); err != nil {
			return nil, err
		}
		commandIDs = append(commandIDs, commandID)
	}
	return commandIDs, nil
}
