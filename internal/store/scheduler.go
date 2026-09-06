package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"kairo/internal/id"
)

// Reservation is a tentative, pre-launch allocation for exactly one immutable
// execution request. GateGenerations are captured when the lease is created
// and fenced at every later launch boundary.
type Reservation struct {
	Lease           Lease              `json:"lease"`
	Execution       ExecutionRequest   `json:"execution"`
	Argv            []string           `json:"argv"`
	CWD             string             `json:"cwd"`
	Resources       []ResourceInstance `json:"resources"`
	GateGenerations map[string]int64   `json:"gate_generations"`
}

// Launch is a single-use authorization. It contains coordination facts only;
// it does not describe success, retry, or any project workflow transition.
type Launch struct {
	Execution            ExecutionRequest   `json:"execution"`
	Attempt              Attempt            `json:"attempt"`
	Lease                Lease              `json:"lease"`
	AuthorizationID      string             `json:"authorization_id"`
	AuthorizationToken   string             `json:"authorization_token"`
	Argv                 []string           `json:"argv"`
	CWD                  string             `json:"cwd"`
	Resources            []ResourceInstance `json:"resources"`
	InputContinuationRef *string            `json:"input_continuation_ref,omitempty"`
}

type gpuConstraints struct {
	SameNode bool  `json:"same_node"`
	MinTotal int64 `json:"min_total_memory_bytes"`
	MinFree  int64 `json:"min_observed_free_memory_bytes"`
}

type conflictPolicy struct {
	External     string `json:"on_external_claim"`
	Unattributed string `json:"on_unattributed_activity"`
	Stale        string `json:"on_stale_observation"`
}

type requestRow struct {
	ID, Type, Kind, Filesystem string
	Quantity                   int64
	Constraints, Policy        string
}

type executionCandidate struct {
	execution    ExecutionRequest
	argvJSON     string
	selectorJSON string
}

// ReserveNext chooses only waiting execution requests whose complete scope
// chain is currently open. Priority orders coordination admission; it does not
// imply retry or workflow semantics.
func (s *Store) ReserveNext(ctx context.Context, executorID string) (*Reservation, error) {
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
	var candidates []executionCandidate
	for rows.Next() {
		var c executionCandidate
		if err = rows.Scan(&c.execution.ID, &c.execution.ProjectScopeID, &c.execution.QueueScopeID, &c.execution.TaskScopeID, &c.execution.ClientRequestID, &c.execution.SpecDigest, &c.execution.State, &c.argvJSON, &c.execution.CWD, &c.selectorJSON, &c.execution.Priority, &c.execution.Checkpointable, &c.execution.Preemptible, &c.execution.InputContinuationRef, &c.execution.TerminalCause, &c.execution.SubmittedAt, &c.execution.AuthorizedAt, &c.execution.StartedAt, &c.execution.TerminalAt); err != nil {
			rows.Close()
			return nil, err
		}
		if selectorMatches(c.selectorJSON, executorAttributes) {
			candidates = append(candidates, c)
		}
	}
	if err = rows.Close(); err != nil {
		return nil, err
	}
	if len(candidates) == 0 {
		return nil, nil
	}

	// Strict priority: a resource-starved lower-priority request cannot bypass
	// an eligible request at the highest priority.
	highest := candidates[0].execution.Priority
	for _, candidate := range candidates {
		if candidate.execution.Priority != highest {
			break
		}
		reservation, terminalized, reserveErr := reserveExecutionTx(ctx, tx, executorID, nodeID, candidate)
		if reserveErr != nil {
			return nil, reserveErr
		}
		if terminalized {
			if err = tx.Commit(); err != nil {
				return nil, err
			}
			return nil, nil
		}
		if reservation != nil {
			if err = tx.Commit(); err != nil {
				return nil, err
			}
			return reservation, nil
		}
	}
	return nil, nil
}

func selectorMatches(selectorJSON, executorJSON string) bool {
	var selector struct {
		Labels map[string]string `json:"labels"`
	}
	var attributes map[string]any
	if json.Unmarshal([]byte(selectorJSON), &selector) != nil || json.Unmarshal([]byte(executorJSON), &attributes) != nil {
		return false
	}
	for key, want := range selector.Labels {
		if fmt.Sprint(attributes[key]) != want {
			return false
		}
	}
	return true
}

func reserveExecutionTx(ctx context.Context, tx *sql.Tx, executorID, nodeID string, candidate executionCandidate) (*Reservation, bool, error) {
	requests, err := loadResourceRequestsTx(ctx, tx, candidate.execution.ID)
	if err != nil {
		return nil, false, err
	}
	type leaseItem struct {
		resourceID       *string
		kind, filesystem string
		quantity         int64
	}
	var resources []ResourceInstance
	var items []leaseItem
	selectedResourceIDs := make(map[string]bool)
	capacityGroups := make(map[string]requestRow)
	for _, request := range requests {
		if request.Type == "exclusive" {
			selected, reason, terminal, selectErr := selectExclusive(ctx, tx, nodeID, request, selectedResourceIDs)
			if selectErr != nil {
				return nil, false, selectErr
			}
			if reason != "" {
				if terminal {
					if err = terminalizeAdmissionTx(ctx, tx, candidate.execution, reason); err != nil {
						return nil, false, err
					}
					return nil, true, nil
				}
				return nil, false, nil
			}
			for i := range selected {
				resources = append(resources, selected[i])
				resourceID := selected[i].ID
				selectedResourceIDs[resourceID] = true
				items = append(items, leaseItem{resourceID: &resourceID, kind: request.Kind, quantity: 1})
			}
			continue
		}
		key := request.Kind + "\x00" + request.Filesystem
		if existing, ok := capacityGroups[key]; ok {
			existing.Quantity += request.Quantity
			if request.Kind == "disk" {
				var oldConstraints, newConstraints map[string]int64
				_ = json.Unmarshal([]byte(existing.Constraints), &oldConstraints)
				_ = json.Unmarshal([]byte(request.Constraints), &newConstraints)
				if newConstraints["min_free_after_bytes"] > oldConstraints["min_free_after_bytes"] {
					oldConstraints["min_free_after_bytes"] = newConstraints["min_free_after_bytes"]
				}
				merged, _ := json.Marshal(oldConstraints)
				existing.Constraints = string(merged)
			}
			capacityGroups[key] = existing
		} else {
			capacityGroups[key] = request
		}
	}
	capacityKeys := make([]string, 0, len(capacityGroups))
	for key := range capacityGroups {
		capacityKeys = append(capacityKeys, key)
	}
	sort.Strings(capacityKeys)
	for _, key := range capacityKeys {
		request := capacityGroups[key]
		resourceID, available, capacityErr := capacityAvailable(ctx, tx, nodeID, request)
		if capacityErr != nil {
			return nil, false, capacityErr
		}
		if !available {
			return nil, false, nil
		}
		items = append(items, leaseItem{resourceID: &resourceID, kind: request.Kind, filesystem: request.Filesystem, quantity: request.Quantity})
	}

	gates, err := executionGatesTx(ctx, tx, candidate.execution.ID)
	if err != nil {
		return nil, false, err
	}
	for _, gate := range gates {
		if gate.State != "open" {
			return nil, false, nil
		}
	}
	epoch := int64(1)
	var previous sql.NullInt64
	if err = tx.QueryRowContext(ctx, `SELECT MAX(coordination_epoch) FROM leases WHERE execution_id=?`, candidate.execution.ID).Scan(&previous); err != nil {
		return nil, false, err
	}
	if previous.Valid {
		epoch = previous.Int64 + 1
	}
	leaseID, stamp := id.New("lea"), now()
	expires := time.Now().UTC().Add(30 * time.Second).Format(time.RFC3339Nano)
	if _, err = tx.ExecContext(ctx, `INSERT INTO leases(id,execution_id,executor_id,coordination_epoch,state,expires_at,created_at) VALUES(?,?,?,?,'reserved',?,?)`, leaseID, candidate.execution.ID, executorID, epoch, expires, stamp); err != nil {
		return nil, false, err
	}
	gateGenerations := make(map[string]int64, len(gates))
	for _, gate := range gates {
		gateGenerations[gate.ScopeID] = gate.Generation
		if _, err = tx.ExecContext(ctx, `INSERT INTO lease_gate_snapshots(lease_id,scope_id,generation) VALUES(?,?,?)`, leaseID, gate.ScopeID, gate.Generation); err != nil {
			return nil, false, err
		}
	}
	for _, item := range items {
		var resourceID any
		if item.resourceID != nil {
			resourceID = *item.resourceID
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO lease_items(lease_id,resource_id,kind,quantity,filesystem) VALUES(?,?,?,?,?)`, leaseID, resourceID, item.kind, item.quantity, item.filesystem); err != nil {
			return nil, false, err
		}
	}
	if err = appendCoordinationEventTx(ctx, tx, "lease_reserved", &candidate.execution.ProjectScopeID, "lease", leaseID, &epoch, map[string]any{"execution_id": candidate.execution.ID, "gate_generations": gateGenerations}); err != nil {
		return nil, false, err
	}
	if err = json.Unmarshal([]byte(candidate.argvJSON), &candidate.execution.Argv); err != nil {
		return nil, false, err
	}
	candidate.execution.ExecutorSelector = json.RawMessage(candidate.selectorJSON)
	sortResources(resources)
	lease := Lease{ID: leaseID, ExecutionID: candidate.execution.ID, ExecutorID: executorID, CoordinationEpoch: epoch, State: "reserved", CreatedAt: stamp, ExpiresAt: &expires}
	return &Reservation{Lease: lease, Execution: candidate.execution, Argv: candidate.execution.Argv, CWD: candidate.execution.CWD, Resources: resources, GateGenerations: gateGenerations}, false, nil
}

func loadResourceRequestsTx(ctx context.Context, tx *sql.Tx, executionID string) ([]requestRow, error) {
	rows, err := tx.QueryContext(ctx, `SELECT id,request_type,kind,quantity,filesystem,constraints_json,policy_json FROM resource_requests WHERE execution_id=? ORDER BY request_type DESC,id`, executionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var requests []requestRow
	for rows.Next() {
		var request requestRow
		if err = rows.Scan(&request.ID, &request.Type, &request.Kind, &request.Quantity, &request.Filesystem, &request.Constraints, &request.Policy); err != nil {
			return nil, err
		}
		requests = append(requests, request)
	}
	return requests, rows.Err()
}

func terminalizeAdmissionTx(ctx context.Context, tx *sql.Tx, execution ExecutionRequest, reason string) error {
	stamp := now()
	detail, _ := json.Marshal(map[string]string{"reason": reason})
	if _, err := tx.ExecContext(ctx, `INSERT INTO admission_blocks(id,execution_id,reason_code,detail_json,created_at) VALUES(?,?,?,?,?)`, id.New("blk"), execution.ID, reason, string(detail), stamp); err != nil {
		return err
	}
	cause := "admission_" + reason
	result, err := tx.ExecContext(ctx, `UPDATE execution_requests SET state='terminal',terminal_cause=?,terminal_at=? WHERE id=? AND state='waiting'`, cause, stamp, execution.ID)
	if err != nil {
		return err
	}
	if count, _ := result.RowsAffected(); count != 1 {
		return ErrExecutionStarted
	}
	return appendCoordinationEventTx(ctx, tx, "execution_terminal", &execution.ProjectScopeID, "execution", execution.ID, nil, map[string]string{"cause": cause})
}

func executionGatesTx(ctx context.Context, tx *sql.Tx, executionID string) ([]AdmissionGate, error) {
	rows, err := tx.QueryContext(ctx, `SELECT g.scope_id,g.state,g.generation,g.updated_at FROM execution_requests e JOIN admission_gates g ON g.scope_id IN(e.project_scope_id,e.queue_scope_id,e.task_scope_id) WHERE e.id=? ORDER BY g.scope_id`, executionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var gates []AdmissionGate
	for rows.Next() {
		var gate AdmissionGate
		if err = rows.Scan(&gate.ScopeID, &gate.State, &gate.Generation, &gate.UpdatedAt); err != nil {
			return nil, err
		}
		gates = append(gates, gate)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	if len(gates) == 0 {
		return nil, ErrNotFound
	}
	return gates, nil
}

func validateLeaseGatesTx(ctx context.Context, tx *sql.Tx, leaseID string) error {
	var mismatches int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM lease_gate_snapshots s LEFT JOIN admission_gates g ON g.scope_id=s.scope_id WHERE s.lease_id=? AND (g.scope_id IS NULL OR g.state!='open' OR g.generation!=s.generation)`, leaseID).Scan(&mismatches)
	if err != nil {
		return err
	}
	var captured, expected int
	if err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM lease_gate_snapshots WHERE lease_id=?`, leaseID).Scan(&captured); err != nil {
		return err
	}
	if err = tx.QueryRowContext(ctx, `SELECT 1+(e.queue_scope_id IS NOT NULL)+(e.task_scope_id IS NOT NULL) FROM leases l JOIN execution_requests e ON e.id=l.execution_id WHERE l.id=?`, leaseID).Scan(&expected); errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	} else if err != nil {
		return err
	}
	if mismatches != 0 || captured != expected {
		return fmt.Errorf("%w: admission gate changed after reservation", ErrGateClosed)
	}
	return nil
}

func selectExclusive(ctx context.Context, tx *sql.Tx, nodeID string, request requestRow, excluded map[string]bool) ([]ResourceInstance, string, bool, error) {
	var constraints gpuConstraints
	var policy conflictPolicy
	_ = json.Unmarshal([]byte(request.Constraints), &constraints)
	_ = json.Unmarshal([]byte(request.Policy), &policy)
	rows, err := tx.QueryContext(ctx, `SELECT ri.id,ri.node_id,ri.provider_id,ri.kind,ri.stable_identity,ri.binding_json,ri.attributes_json,ri.admin_state,ri.quarantine_reason,
		o.observed_at,o.valid_until,o.total_bytes,o.free_bytes,
		EXISTS(SELECT 1 FROM external_claims c WHERE c.resource_id=ri.id AND c.cleared_at IS NULL AND c.claim_kind='external_process'),
		EXISTS(SELECT 1 FROM external_claims c WHERE c.resource_id=ri.id AND c.cleared_at IS NULL AND c.claim_kind='unattributed_activity'),
		EXISTS(SELECT 1 FROM lease_items li JOIN leases l ON l.id=li.lease_id WHERE li.resource_id=ri.id AND l.state IN('reserved','prepared','active','releasing','stale','revocation_requested'))
		FROM resource_instances ri
		LEFT JOIN resource_observations o ON o.id=(SELECT id FROM resource_observations WHERE resource_id=ri.id ORDER BY id DESC LIMIT 1)
		WHERE ri.node_id=? AND ri.kind=? AND ri.admin_state='enabled' ORDER BY ri.id`, nodeID, request.Kind)
	if err != nil {
		return nil, "", false, err
	}
	defer rows.Close()
	var available []ResourceInstance
	stale, external, unattributed := false, false, false
	stamp := time.Now().UTC()
	for rows.Next() {
		var resource ResourceInstance
		var binding, attributes string
		var observed, valid sql.NullString
		var total, free sql.NullInt64
		var hasExternal, hasUnattributed, leased bool
		if err = rows.Scan(&resource.ID, &resource.NodeID, &resource.ProviderID, &resource.Kind, &resource.StableIdentity, &binding, &attributes, &resource.AdminState, &resource.QuarantineReason, &observed, &valid, &total, &free, &hasExternal, &hasUnattributed, &leased); err != nil {
			return nil, "", false, err
		}
		resource.Binding = json.RawMessage(binding)
		resource.Attributes = json.RawMessage(attributes)
		if leased || excluded[resource.ID] {
			continue
		}
		if hasExternal {
			external = true
			continue
		}
		if hasUnattributed && policy.Unattributed != "allow" {
			unattributed = true
			continue
		}
		deadline, parseErr := time.Parse(time.RFC3339Nano, valid.String)
		if !valid.Valid || parseErr != nil || !deadline.After(stamp) {
			stale = true
			continue
		}
		if !total.Valid || total.Int64 < constraints.MinTotal || !free.Valid || free.Int64 < constraints.MinFree {
			continue
		}
		available = append(available, resource)
	}
	if err = rows.Err(); err != nil {
		return nil, "", false, err
	}
	if int64(len(available)) >= request.Quantity {
		return available[:request.Quantity], "", false, nil
	}
	if external {
		return nil, "external_claim", policy.External == "fail", nil
	}
	if unattributed {
		return nil, "unattributed_activity", policy.Unattributed == "fail", nil
	}
	if stale {
		return nil, "stale_observation", policy.Stale == "fail", nil
	}
	return nil, "insufficient_" + request.Kind, false, nil
}

func capacityAvailable(ctx context.Context, tx *sql.Tx, nodeID string, request requestRow) (string, bool, error) {
	identity := ""
	if request.Kind == "disk" {
		identity = request.Filesystem
	}
	var resourceID string
	var total, free sql.NullInt64
	err := tx.QueryRowContext(ctx, `SELECT ri.id,o.total_bytes,o.free_bytes
		FROM resource_instances ri
		JOIN resource_observations o ON o.id=(SELECT id FROM resource_observations WHERE resource_id=ri.id ORDER BY id DESC LIMIT 1)
		WHERE ri.node_id=? AND ri.kind=? AND ri.admin_state='enabled' AND (?='' OR ri.stable_identity=?) AND julianday(o.valid_until)>julianday(?)
		ORDER BY ri.id LIMIT 1`, nodeID, request.Kind, identity, identity, now()).Scan(&resourceID, &total, &free)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	var reserved int64
	if err = tx.QueryRowContext(ctx, `SELECT COALESCE(SUM(li.quantity),0)
		FROM lease_items li JOIN leases l ON l.id=li.lease_id JOIN executors e ON e.id=l.executor_id
		WHERE e.node_id=? AND li.kind=? AND li.filesystem=? AND l.state IN('reserved','prepared','active','releasing','stale','revocation_requested')`, nodeID, request.Kind, request.Filesystem).Scan(&reserved); err != nil {
		return "", false, err
	}
	if request.Kind == "cpu" {
		return resourceID, total.Valid && total.Int64-reserved >= request.Quantity, nil
	}
	if !free.Valid || free.Int64-reserved-request.Quantity < 0 {
		return "", false, nil
	}
	if request.Kind == "disk" {
		var constraints map[string]int64
		_ = json.Unmarshal([]byte(request.Constraints), &constraints)
		return resourceID, free.Int64-reserved-request.Quantity >= constraints["min_free_after_bytes"], nil
	}
	return resourceID, true, nil
}

// ValidateReservation re-observes the lease's exclusive resources and fences
// any pause/resume generation change since reservation.
func (s *Store) ValidateReservation(ctx context.Context, leaseID string, epoch int64) error {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var executionID, state string
	if err = tx.QueryRowContext(ctx, `SELECT execution_id,state FROM leases WHERE id=? AND coordination_epoch=?`, leaseID, epoch).Scan(&executionID, &state); errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	} else if err != nil {
		return err
	}
	if state != "reserved" {
		return fmt.Errorf("%w: lease is not reserved", ErrStaleEpoch)
	}
	if err = validateLeaseGatesTx(ctx, tx, leaseID); err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, `SELECT li.resource_id,rr.constraints_json,rr.policy_json,o.valid_until,o.total_bytes,o.free_bytes,
		EXISTS(SELECT 1 FROM external_claims c WHERE c.resource_id=li.resource_id AND c.cleared_at IS NULL AND c.claim_kind='external_process'),
		EXISTS(SELECT 1 FROM external_claims c WHERE c.resource_id=li.resource_id AND c.cleared_at IS NULL AND c.claim_kind='unattributed_activity')
		FROM lease_items li
		JOIN resource_requests rr ON rr.execution_id=? AND rr.request_type='exclusive' AND rr.kind=li.kind
		LEFT JOIN resource_observations o ON o.id=(SELECT id FROM resource_observations WHERE resource_id=li.resource_id ORDER BY id DESC LIMIT 1)
		WHERE li.lease_id=? AND li.resource_id IS NOT NULL AND li.kind='gpu'`, executionID, leaseID)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var resourceID, constraintsJSON, policyJSON string
		var valid sql.NullString
		var total, free sql.NullInt64
		var external, unattributed bool
		if err = rows.Scan(&resourceID, &constraintsJSON, &policyJSON, &valid, &total, &free, &external, &unattributed); err != nil {
			return err
		}
		var constraints gpuConstraints
		var policy conflictPolicy
		_ = json.Unmarshal([]byte(constraintsJSON), &constraints)
		_ = json.Unmarshal([]byte(policyJSON), &policy)
		deadline, parseErr := time.Parse(time.RFC3339Nano, valid.String)
		if !valid.Valid || parseErr != nil || !deadline.After(time.Now().UTC()) {
			return fmt.Errorf("resource %s has stale observation during prepare", resourceID)
		}
		if !total.Valid || total.Int64 < constraints.MinTotal || !free.Valid || free.Int64 < constraints.MinFree {
			return fmt.Errorf("resource %s no longer meets memory constraints", resourceID)
		}
		if external {
			return fmt.Errorf("resource %s acquired an external claim", resourceID)
		}
		if unattributed && policy.Unattributed != "allow" {
			return fmt.Errorf("resource %s has unattributed activity", resourceID)
		}
	}
	return rows.Err()
}

func (s *Store) MarkLeasePrepared(ctx context.Context, leaseID string, epoch int64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = validateLeaseGatesTx(ctx, tx, leaseID); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE leases SET state='prepared',prepared_at=? WHERE id=? AND coordination_epoch=? AND state='reserved'`, now(), leaseID, epoch)
	if err != nil {
		return err
	}
	if count, _ := result.RowsAffected(); count != 1 {
		return fmt.Errorf("%w: lease is not reserved at this epoch", ErrStaleEpoch)
	}
	if _, err = tx.ExecContext(ctx, `UPDATE lease_items SET prepared=1 WHERE lease_id=?`, leaseID); err != nil {
		return err
	}
	var projectID string
	if err = tx.QueryRowContext(ctx, `SELECT e.project_scope_id FROM leases l JOIN execution_requests e ON e.id=l.execution_id WHERE l.id=?`, leaseID).Scan(&projectID); err != nil {
		return err
	}
	if err = appendCoordinationEventTx(ctx, tx, "lease_prepared", &projectID, "lease", leaseID, &epoch, nil); err != nil {
		return err
	}
	return tx.Commit()
}

// ReleaseReservation releases a lease before activation. If authorization has
// already been issued, the one-shot execution becomes terminal; before that,
// it remains waiting and may receive a new reservation epoch.
func (s *Store) ReleaseReservation(ctx context.Context, leaseID string, epoch int64, reason string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var executionID, projectID, state string
	var attemptID sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT l.execution_id,e.project_scope_id,l.state,l.attempt_id FROM leases l JOIN execution_requests e ON e.id=l.execution_id WHERE l.id=? AND l.coordination_epoch=?`, leaseID, epoch).Scan(&executionID, &projectID, &state, &attemptID)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if state != "reserved" && state != "prepared" && state != "revocation_requested" {
		return fmt.Errorf("%w: lease cannot be released from %s", ErrStaleEpoch, state)
	}
	stamp := now()
	// Provider cleanup must be followed by fresh, unclaimed evidence before a
	// pre-activation GPU lease can become allocatable again. This also covers a
	// process spawned just before activation was fenced.
	var unsafeExclusive int
	if err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM lease_items li
		LEFT JOIN resource_observations o ON o.id=(SELECT id FROM resource_observations WHERE resource_id=li.resource_id ORDER BY id DESC LIMIT 1)
		WHERE li.lease_id=? AND li.kind='gpu' AND li.resource_id IS NOT NULL AND
		(o.id IS NULL OR julianday(o.valid_until)<=julianday(?) OR julianday(o.observed_at)<=julianday((SELECT created_at FROM leases WHERE id=?)) OR EXISTS(SELECT 1 FROM external_claims c WHERE c.resource_id=li.resource_id AND c.cleared_at IS NULL))`, leaseID, stamp, leaseID).Scan(&unsafeExclusive); err != nil {
		return err
	}
	if unsafeExclusive != 0 {
		return errors.New("reservation cleanup lacks fresh unclaimed GPU observation")
	}
	revokedByPause := state == "revocation_requested"
	if attemptID.Valid {
		if _, err = tx.ExecContext(ctx, `UPDATE launch_authorizations SET state='revoked',revoked_at=? WHERE attempt_id=? AND state='issued'`, stamp, attemptID.String); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE attempts SET state='quiesced',exited_at=COALESCE(exited_at,?),quiesced_at=? WHERE id=? AND state='authorized'`, stamp, stamp, attemptID.String); err != nil {
			return err
		}
		cause := "launch_aborted"
		if revokedByPause {
			cause = "coordination_revoked"
		} else if reason != "" {
			cause = "launch_aborted: " + reason
		}
		if _, err = tx.ExecContext(ctx, `UPDATE execution_requests SET state='terminal',terminal_cause=?,terminal_at=? WHERE id=? AND state='authorized'`, cause, stamp, executionID); err != nil {
			return err
		}
	}
	result, err := tx.ExecContext(ctx, `UPDATE leases SET state='released',released_at=? WHERE id=? AND coordination_epoch=? AND state IN('reserved','prepared','revocation_requested')`, stamp, leaseID, epoch)
	if err != nil {
		return err
	}
	if count, _ := result.RowsAffected(); count != 1 {
		return fmt.Errorf("%w: lease cannot be released at this epoch", ErrStaleEpoch)
	}
	if revokedByPause {
		if _, err = tx.ExecContext(ctx, `UPDATE pause_targets SET state='quiesced',blocker_reason=NULL,updated_at=? WHERE execution_id=? AND lease_id=? AND state='revoking'`, stamp, executionID, leaseID); err != nil {
			return err
		}
	}
	if err = appendCoordinationEventTx(ctx, tx, "lease_released_before_activation", &projectID, "lease", leaseID, &epoch, map[string]any{"execution_id": executionID, "reason": reason}); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) AuthorizeLaunch(ctx context.Context, reservation *Reservation) (*Launch, error) {
	if reservation == nil {
		return nil, errors.New("reservation is required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var executionID, projectID, leaseState, executionState, executorID string
	var leaseEpoch int64
	err = tx.QueryRowContext(ctx, `SELECT l.execution_id,e.project_scope_id,l.state,e.state,l.executor_id,l.coordination_epoch FROM leases l JOIN execution_requests e ON e.id=l.execution_id WHERE l.id=?`, reservation.Lease.ID).Scan(&executionID, &projectID, &leaseState, &executionState, &executorID, &leaseEpoch)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if executionID != reservation.Execution.ID || leaseEpoch != reservation.Lease.CoordinationEpoch {
		return nil, fmt.Errorf("%w: reservation does not match lease", ErrStaleEpoch)
	}
	if leaseState != "prepared" || executionState != "waiting" {
		return nil, fmt.Errorf("%w: lease=%s execution=%s", ErrExecutionStarted, leaseState, executionState)
	}
	if err = validateLeaseGatesTx(ctx, tx, reservation.Lease.ID); err != nil {
		return nil, err
	}
	attemptID, stamp := id.New("att"), now()
	if _, err = tx.ExecContext(ctx, `INSERT INTO attempts(id,execution_id,state,executor_id,coordination_epoch,authorized_at) VALUES(?,?,'authorized',?,?,?)`, attemptID, executionID, executorID, leaseEpoch, stamp); err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE leases SET attempt_id=? WHERE id=? AND attempt_id IS NULL`, attemptID, reservation.Lease.ID); err != nil {
		return nil, err
	}
	token, tokenHash, err := newLaunchToken()
	if err != nil {
		return nil, err
	}
	authorizationID := id.New("auth")
	if _, err = tx.ExecContext(ctx, `INSERT INTO launch_authorizations(id,lease_id,attempt_id,coordination_epoch,token_hash,state,issued_at) VALUES(?,?,?,?,?,'issued',?)`, authorizationID, reservation.Lease.ID, attemptID, leaseEpoch, tokenHash, stamp); err != nil {
		return nil, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE execution_requests SET state='authorized',authorized_at=? WHERE id=? AND state='waiting'`, stamp, executionID)
	if err != nil {
		return nil, err
	}
	if count, _ := result.RowsAffected(); count != 1 {
		return nil, ErrExecutionStarted
	}
	if err = appendCoordinationEventTx(ctx, tx, "launch_authorized", &projectID, "attempt", attemptID, &leaseEpoch, map[string]string{"execution_id": executionID, "lease_id": reservation.Lease.ID, "authorization_id": authorizationID}); err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}

	reservation.Lease.AttemptID = &attemptID
	attempt := Attempt{ID: attemptID, ExecutionID: executionID, State: "authorized", ExecutorID: executorID, CoordinationEpoch: leaseEpoch}
	launch := &Launch{
		Execution:            reservation.Execution,
		Attempt:              attempt,
		Lease:                reservation.Lease,
		AuthorizationID:      authorizationID,
		AuthorizationToken:   token,
		Argv:                 append([]string(nil), reservation.Argv...),
		CWD:                  reservation.CWD,
		Resources:            append([]ResourceInstance(nil), reservation.Resources...),
		InputContinuationRef: reservation.Execution.InputContinuationRef,
	}
	launch.Execution.State = "authorized"
	launch.Execution.AuthorizedAt = &stamp
	return launch, nil
}

func (s *Store) ActivateLaunch(ctx context.Context, attemptID, leaseID string, epoch int64, token string, pid int, processIdentity string) error {
	if pid <= 0 || processIdentity == "" {
		return errors.New("launcher pid and process identity are required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = validateEpochTx(ctx, tx, attemptID, leaseID, epoch); err != nil {
		return err
	}
	if err = validateLeaseGatesTx(ctx, tx, leaseID); err != nil {
		return err
	}
	var authorizationID, executionID, projectID string
	err = tx.QueryRowContext(ctx, `SELECT a.id,l.execution_id,e.project_scope_id
		FROM launch_authorizations a JOIN leases l ON l.id=a.lease_id JOIN execution_requests e ON e.id=l.execution_id
		WHERE a.attempt_id=? AND a.lease_id=? AND a.coordination_epoch=? AND a.token_hash=? AND a.state='issued'`, attemptID, leaseID, epoch, hashToken(token)).Scan(&authorizationID, &executionID, &projectID)
	if errors.Is(err, sql.ErrNoRows) {
		return errors.New("invalid or consumed launch authorization")
	}
	if err != nil {
		return err
	}
	stamp := now()
	result, err := tx.ExecContext(ctx, `UPDATE launch_authorizations SET state='consumed',consumed_at=? WHERE id=? AND state='issued'`, stamp, authorizationID)
	if err != nil {
		return err
	}
	if count, _ := result.RowsAffected(); count != 1 {
		return errors.New("invalid or consumed launch authorization")
	}
	result, err = tx.ExecContext(ctx, `UPDATE attempts SET state='running',pid=?,process_identity=?,started_at=?,last_heartbeat_at=? WHERE id=? AND state='authorized'`, pid, processIdentity, stamp, stamp, attemptID)
	if err != nil {
		return err
	}
	if count, _ := result.RowsAffected(); count != 1 {
		return ErrExecutionStarted
	}
	result, err = tx.ExecContext(ctx, `UPDATE leases SET state='active',activated_at=? WHERE id=? AND coordination_epoch=? AND state='prepared'`, stamp, leaseID, epoch)
	if err != nil {
		return err
	}
	if count, _ := result.RowsAffected(); count != 1 {
		return fmt.Errorf("%w: lease is not prepared", ErrStaleEpoch)
	}
	result, err = tx.ExecContext(ctx, `UPDATE execution_requests SET state='started',started_at=? WHERE id=? AND state='authorized'`, stamp, executionID)
	if err != nil {
		return err
	}
	if count, _ := result.RowsAffected(); count != 1 {
		return ErrExecutionStarted
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO attempt_processes(attempt_id,role,namespace,rank,pid,process_identity,registered_at,last_seen_at) VALUES(?,'launcher','host',-1,?,?,?,?)`, attemptID, pid, processIdentity, stamp, stamp); err != nil {
		return err
	}
	if err = appendCoordinationEventTx(ctx, tx, "execution_started", &projectID, "attempt", attemptID, &epoch, map[string]any{"execution_id": executionID, "lease_id": leaseID, "pid": pid, "process_identity": processIdentity}); err != nil {
		return err
	}
	return tx.Commit()
}

func newLaunchToken() (string, string, error) {
	value := make([]byte, 32)
	if _, err := rand.Read(value); err != nil {
		return "", "", err
	}
	token := base64.RawURLEncoding.EncodeToString(value)
	return token, hashToken(token), nil
}

func hashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return fmt.Sprintf("%x", sum[:])
}

func sortResources(resources []ResourceInstance) {
	sort.Slice(resources, func(i, j int) bool { return resources[i].ID < resources[j].ID })
}
