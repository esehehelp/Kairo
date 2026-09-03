package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"kairo/internal/id"
)

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

func (s *Store) refreshRunnableTx(ctx context.Context, tx *sql.Tx) error {
	rows, err := tx.QueryContext(ctx, `SELECT tr.task_id,tr.revision FROM task_revisions tr JOIN tasks t ON t.id=tr.task_id JOIN queues q ON q.id=t.queue_id WHERE tr.revision=t.current_revision AND tr.desired_state='active' AND tr.scheduling_state='pending' AND q.desired_state='active' AND NOT EXISTS(SELECT 1 FROM admission_blocks b WHERE b.task_id=tr.task_id AND b.task_revision=tr.revision AND b.cleared_at IS NULL) AND NOT EXISTS(SELECT 1 FROM task_dependencies d JOIN task_revisions dep ON dep.task_id=d.dependency_task_id AND dep.revision=d.dependency_revision WHERE d.task_id=tr.task_id AND d.task_revision=tr.revision AND dep.scheduling_state!='succeeded')`)
	if err != nil {
		return err
	}
	defer rows.Close()
	type ref struct {
		id  string
		rev int
	}
	var refs []ref
	for rows.Next() {
		var r ref
		if err := rows.Scan(&r.id, &r.rev); err != nil {
			return err
		}
		refs = append(refs, r)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	t := now()
	for _, r := range refs {
		if _, err := tx.ExecContext(ctx, `UPDATE task_revisions SET became_runnable_at=COALESCE(became_runnable_at,?) WHERE task_id=? AND revision=?`, t, r.id, r.rev); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE tasks SET became_runnable_at=COALESCE(became_runnable_at,?),scheduling_state=CASE WHEN current_revision=? THEN 'pending' ELSE scheduling_state END,updated_at=? WHERE id=?`, t, r.rev, t, r.id); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) ReserveNext(ctx context.Context, executorID string) (*Reservation, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if err = s.refreshRunnableTx(ctx, tx); err != nil {
		return nil, err
	}
	var executorNode, attrs string
	var executorEnabled bool
	if err = tx.QueryRowContext(ctx, `SELECT node_id,attributes_json,enabled FROM executors WHERE id=?`, executorID).Scan(&executorNode, &attrs, &executorEnabled); errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	if !executorEnabled {
		return nil, nil
	}
	rows, err := tx.QueryContext(ctx, `SELECT t.id,t.queue_id,t.task_key,t.current_revision,t.desired_state,t.scheduling_state,t.effective_priority,t.became_runnable_at,tr.revision,tr.argv_json,tr.cwd,tr.executor_selector_json FROM tasks t JOIN queues q ON q.id=t.queue_id JOIN task_revisions tr ON tr.task_id=t.id AND tr.revision=t.current_revision WHERE q.desired_state='active' AND t.desired_state='active' AND tr.desired_state='active' AND tr.scheduling_state='pending' AND tr.became_runnable_at IS NOT NULL AND (tr.next_retry_at IS NULL OR tr.next_retry_at<=?) AND NOT EXISTS(SELECT 1 FROM admission_blocks b WHERE b.task_id=tr.task_id AND b.task_revision=tr.revision AND b.cleared_at IS NULL) AND NOT EXISTS(SELECT 1 FROM attempts a WHERE a.task_id=t.id AND a.state IN('authorized','running','suspend_requested')) ORDER BY t.effective_priority DESC,tr.became_runnable_at ASC,t.task_key ASC,tr.revision ASC`, now())
	if err != nil {
		return nil, err
	}
	type candidate struct {
		task                    Task
		rev                     int
		argvJSON, cwd, selector string
	}
	var candidates []candidate
	for rows.Next() {
		var c candidate
		if err := rows.Scan(&c.task.ID, &c.task.QueueID, &c.task.Key, &c.task.CurrentRevision, &c.task.DesiredState, &c.task.SchedulingState, &c.task.Priority, &c.task.BecameRunnableAt, &c.rev, &c.argvJSON, &c.cwd, &c.selector); err != nil {
			rows.Close()
			return nil, err
		}
		if selectorMatches(c.selector, attrs) {
			candidates = append(candidates, c)
		}
	}
	rows.Close()
	if len(candidates) == 0 {
		return nil, nil
	}
	// Strict priority: only candidates tied at the highest priority may allocate.
	highest := candidates[0].task.Priority
	for _, c := range candidates {
		if c.task.Priority != highest {
			break
		}
		reservation, blocked, err := reserveCandidate(ctx, tx, executorID, executorNode, c)
		if err != nil {
			return nil, err
		}
		if blocked {
			return nil, tx.Commit()
		}
		if reservation != nil {
			if err := tx.Commit(); err != nil {
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
	var attrs map[string]any
	if json.Unmarshal([]byte(selectorJSON), &selector) != nil || json.Unmarshal([]byte(executorJSON), &attrs) != nil {
		return false
	}
	for k, want := range selector.Labels {
		if fmt.Sprint(attrs[k]) != want {
			return false
		}
	}
	return true
}

func reserveCandidate(ctx context.Context, tx *sql.Tx, executorID, nodeID string, c struct {
	task                    Task
	rev                     int
	argvJSON, cwd, selector string
}) (*Reservation, bool, error) {
	rows, err := tx.QueryContext(ctx, `SELECT id,request_type,kind,quantity,COALESCE(filesystem,''),constraints_json,policy_json FROM resource_requests WHERE task_id=? AND task_revision=? ORDER BY request_type DESC,id`, c.task.ID, c.rev)
	if err != nil {
		return nil, false, err
	}
	var requests []requestRow
	for rows.Next() {
		var r requestRow
		if err := rows.Scan(&r.ID, &r.Type, &r.Kind, &r.Quantity, &r.Filesystem, &r.Constraints, &r.Policy); err != nil {
			rows.Close()
			return nil, false, err
		}
		requests = append(requests, r)
	}
	rows.Close()
	var resources []ResourceInstance
	type item struct {
		resourceID       *string
		kind, filesystem string
		quantity         int64
	}
	var items []item
	for _, r := range requests {
		if r.Type == "exclusive" {
			selected, reason, fail, err := selectExclusive(ctx, tx, nodeID, r)
			if err != nil {
				return nil, false, err
			}
			if reason != "" {
				if fail {
					detail, _ := json.Marshal(map[string]string{"reason": reason})
					if _, err := tx.ExecContext(ctx, `INSERT INTO admission_blocks(id,task_id,task_revision,reason_code,detail_json,created_at) VALUES(?,?,?,?,?,?)`, id.New("blk"), c.task.ID, c.rev, reason, string(detail), now()); err != nil {
						return nil, false, err
					}
					if _, err := tx.ExecContext(ctx, `UPDATE task_revisions SET scheduling_state='failed_admission' WHERE task_id=? AND revision=?`, c.task.ID, c.rev); err != nil {
						return nil, false, err
					}
					if c.rev == c.task.CurrentRevision {
						_, _ = tx.ExecContext(ctx, `UPDATE tasks SET scheduling_state='failed_admission',updated_at=? WHERE id=?`, now(), c.task.ID)
					}
					return nil, true, nil
				}
				return nil, false, nil
			}
			for i := range selected {
				resources = append(resources, selected[i])
				resourceID := selected[i].ID
				items = append(items, item{resourceID: &resourceID, kind: r.Kind, quantity: 1})
			}
		} else {
			resourceID, ok, err := capacityAvailable(ctx, tx, nodeID, r)
			if err != nil {
				return nil, false, err
			}
			if !ok {
				return nil, false, nil
			}
			items = append(items, item{resourceID: &resourceID, kind: r.Kind, filesystem: r.Filesystem, quantity: r.Quantity})
		}
	}
	epoch := int64(1)
	var previous sql.NullInt64
	_ = tx.QueryRowContext(ctx, `SELECT MAX(coordination_epoch) FROM leases WHERE task_id=?`, c.task.ID).Scan(&previous)
	if previous.Valid {
		epoch = previous.Int64 + 1
	}
	leaseID, t := id.New("lea"), now()
	expires := time.Now().UTC().Add(30 * time.Second).Format(time.RFC3339Nano)
	if _, err := tx.ExecContext(ctx, `INSERT INTO leases(id,task_id,task_revision,executor_id,coordination_epoch,state,expires_at,created_at) VALUES(?,?,?,?,?,'reserved',?,?)`, leaseID, c.task.ID, c.rev, executorID, epoch, expires, t); err != nil {
		return nil, false, err
	}
	for _, x := range items {
		var resourceID any
		if x.resourceID != nil {
			resourceID = *x.resourceID
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO lease_items(lease_id,resource_id,kind,quantity,filesystem) VALUES(?,?,?,?,?)`, leaseID, resourceID, x.kind, x.quantity, x.filesystem); err != nil {
			return nil, false, err
		}
	}
	if err := appendEvent(ctx, tx, "lease_reserved", "lease", leaseID, &epoch, map[string]any{"task_id": c.task.ID, "task_revision": c.rev}); err != nil {
		return nil, false, err
	}
	var argv []string
	if err := json.Unmarshal([]byte(c.argvJSON), &argv); err != nil {
		return nil, false, err
	}
	lease := Lease{ID: leaseID, TaskID: &c.task.ID, TaskRevision: &c.rev, ExecutorID: executorID, CoordinationEpoch: epoch, State: "reserved", CreatedAt: t, ExpiresAt: &expires}
	return &Reservation{Lease: lease, Task: c.task, TaskRevision: c.rev, Argv: argv, CWD: c.cwd, Resources: resources}, false, nil
}

func selectExclusive(ctx context.Context, tx *sql.Tx, nodeID string, r requestRow) ([]ResourceInstance, string, bool, error) {
	var constraints gpuConstraints
	var policy conflictPolicy
	_ = json.Unmarshal([]byte(r.Constraints), &constraints)
	_ = json.Unmarshal([]byte(r.Policy), &policy)
	rows, err := tx.QueryContext(ctx, `SELECT ri.id,ri.node_id,ri.provider_id,ri.kind,ri.stable_identity,ri.binding_json,ri.attributes_json,ri.admin_state,ri.quarantine_reason,
	 o.observed_at,o.valid_until,o.total_bytes,o.free_bytes,
	 EXISTS(SELECT 1 FROM external_claims c WHERE c.resource_id=ri.id AND c.cleared_at IS NULL AND c.claim_kind='external_process'),
	 EXISTS(SELECT 1 FROM external_claims c WHERE c.resource_id=ri.id AND c.cleared_at IS NULL AND c.claim_kind='unattributed_activity'),
	 EXISTS(SELECT 1 FROM lease_items li JOIN leases l ON l.id=li.lease_id WHERE li.resource_id=ri.id AND l.state IN('reserved','prepared','active','releasing','stale','revocation_requested'))
	 FROM resource_instances ri LEFT JOIN resource_observations o ON o.id=(SELECT id FROM resource_observations WHERE resource_id=ri.id ORDER BY observed_at DESC LIMIT 1)
	 WHERE ri.node_id=? AND ri.kind=? AND ri.admin_state='enabled' ORDER BY ri.id`, nodeID, r.Kind)
	if err != nil {
		return nil, "", false, err
	}
	defer rows.Close()
	var available []ResourceInstance
	stale, external, unattributed := false, false, false
	nowTime := time.Now().UTC()
	for rows.Next() {
		var x ResourceInstance
		var binding, attrs string
		var observed, valid sql.NullString
		var total, free sql.NullInt64
		var ext, unattr, leased bool
		if err := rows.Scan(&x.ID, &x.NodeID, &x.ProviderID, &x.Kind, &x.StableIdentity, &binding, &attrs, &x.AdminState, &x.QuarantineReason, &observed, &valid, &total, &free, &ext, &unattr, &leased); err != nil {
			return nil, "", false, err
		}
		x.Binding = json.RawMessage(binding)
		x.Attributes = json.RawMessage(attrs)
		if leased {
			continue
		}
		if ext {
			external = true
			continue
		}
		if unattr && policy.Unattributed != "allow" {
			unattributed = true
			continue
		}
		deadline, e := time.Parse(time.RFC3339Nano, valid.String)
		if !valid.Valid || e != nil || !deadline.After(nowTime) {
			stale = true
			continue
		}
		if !total.Valid || total.Int64 < constraints.MinTotal || !free.Valid || free.Int64 < constraints.MinFree {
			continue
		}
		available = append(available, x)
	}
	if int64(len(available)) >= r.Quantity {
		return available[:r.Quantity], "", false, nil
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
	return nil, "insufficient_" + r.Kind, false, nil
}

func capacityAvailable(ctx context.Context, tx *sql.Tx, nodeID string, r requestRow) (string, bool, error) {
	kind := r.Kind
	identity := ""
	if kind == "disk" {
		identity = r.Filesystem
	}
	var resourceID string
	var total, free sql.NullInt64
	err := tx.QueryRowContext(ctx, `SELECT ri.id,o.total_bytes,o.free_bytes FROM resource_instances ri JOIN resource_observations o ON o.id=(SELECT id FROM resource_observations WHERE resource_id=ri.id ORDER BY observed_at DESC LIMIT 1) WHERE ri.node_id=? AND ri.kind=? AND ri.admin_state='enabled' AND (?='' OR ri.stable_identity=?) AND o.valid_until>? ORDER BY ri.id LIMIT 1`, nodeID, kind, identity, identity, now()).Scan(&resourceID, &total, &free)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	var reserved int64
	err = tx.QueryRowContext(ctx, `SELECT COALESCE(SUM(li.quantity),0) FROM lease_items li JOIN leases l ON l.id=li.lease_id JOIN executors e ON e.id=l.executor_id WHERE e.node_id=? AND li.kind=? AND li.filesystem=? AND l.state IN('reserved','prepared','active','releasing','stale','revocation_requested')`, nodeID, kind, r.Filesystem).Scan(&reserved)
	if err != nil {
		return "", false, err
	}
	if kind == "cpu" {
		return resourceID, total.Valid && total.Int64-reserved >= r.Quantity, nil
	}
	if !free.Valid || free.Int64-reserved-r.Quantity < 0 {
		return "", false, nil
	}
	if kind == "disk" {
		var c map[string]int64
		_ = json.Unmarshal([]byte(r.Constraints), &c)
		return resourceID, free.Int64-reserved-r.Quantity >= c["min_free_after_bytes"], nil
	}
	return resourceID, true, nil
}

func (s *Store) MarkLeasePrepared(ctx context.Context, leaseID string, epoch int64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, `UPDATE leases SET state='prepared',prepared_at=? WHERE id=? AND coordination_epoch=? AND state='reserved'`, now(), leaseID, epoch)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return fmt.Errorf("%w: lease is not reserved at this epoch", ErrStaleEpoch)
	}
	if _, err = tx.ExecContext(ctx, `UPDATE lease_items SET prepared=1 WHERE lease_id=?`, leaseID); err != nil {
		return err
	}
	if err = appendEvent(ctx, tx, "lease_prepared", "lease", leaseID, &epoch, nil); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) ValidateReservation(ctx context.Context, leaseID string, epoch int64) error {
	var taskID string
	var revision int
	var state string
	if err := s.db.QueryRowContext(ctx, `SELECT task_id,task_revision,state FROM leases WHERE id=? AND coordination_epoch=?`, leaseID, epoch).Scan(&taskID, &revision, &state); err != nil {
		return err
	}
	if state != "reserved" {
		return fmt.Errorf("%w: lease is not reserved", ErrStaleEpoch)
	}
	rows, err := s.db.QueryContext(ctx, `SELECT li.resource_id,rr.constraints_json,rr.policy_json,o.valid_until,o.total_bytes,o.free_bytes,
	 EXISTS(SELECT 1 FROM external_claims c WHERE c.resource_id=li.resource_id AND c.cleared_at IS NULL AND c.claim_kind='external_process'),
	 EXISTS(SELECT 1 FROM external_claims c WHERE c.resource_id=li.resource_id AND c.cleared_at IS NULL AND c.claim_kind='unattributed_activity')
	 FROM lease_items li JOIN resource_requests rr ON rr.task_id=? AND rr.task_revision=? AND rr.request_type='exclusive' AND rr.kind=li.kind
	 LEFT JOIN resource_observations o ON o.id=(SELECT id FROM resource_observations WHERE resource_id=li.resource_id ORDER BY observed_at DESC LIMIT 1)
	 WHERE li.lease_id=? AND li.resource_id IS NOT NULL`, taskID, revision, leaseID)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var resourceID, constraintsJSON, policyJSON string
		var valid sql.NullString
		var total, free sql.NullInt64
		var external, unattributed bool
		if err := rows.Scan(&resourceID, &constraintsJSON, &policyJSON, &valid, &total, &free, &external, &unattributed); err != nil {
			return err
		}
		var c gpuConstraints
		var p conflictPolicy
		_ = json.Unmarshal([]byte(constraintsJSON), &c)
		_ = json.Unmarshal([]byte(policyJSON), &p)
		deadline, parseErr := time.Parse(time.RFC3339Nano, valid.String)
		if !valid.Valid || parseErr != nil || !deadline.After(time.Now().UTC()) {
			return fmt.Errorf("resource %s has stale observation during prepare", resourceID)
		}
		if !total.Valid || total.Int64 < c.MinTotal || !free.Valid || free.Int64 < c.MinFree {
			return fmt.Errorf("resource %s no longer meets memory constraints", resourceID)
		}
		if external {
			return fmt.Errorf("resource %s acquired an external claim", resourceID)
		}
		if unattributed && p.Unattributed != "allow" {
			return fmt.Errorf("resource %s has unattributed activity", resourceID)
		}
	}
	return rows.Err()
}
func (s *Store) ReleaseReservation(ctx context.Context, leaseID string, epoch int64, reason string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, `UPDATE leases SET state='released',released_at=? WHERE id=? AND coordination_epoch=? AND state IN('reserved','prepared')`, now(), leaseID, epoch)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return fmt.Errorf("%w: lease cannot be released at this epoch", ErrStaleEpoch)
	}
	if err = appendEvent(ctx, tx, "lease_prepare_failed", "lease", leaseID, &epoch, map[string]string{"reason": reason}); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) AuthorizeLaunch(ctx context.Context, reservation *Reservation) (*V1Launch, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var state string
	if err = tx.QueryRowContext(ctx, `SELECT state FROM leases WHERE id=? AND coordination_epoch=?`, reservation.Lease.ID, reservation.Lease.CoordinationEpoch).Scan(&state); err != nil {
		return nil, err
	}
	if state != "prepared" {
		return nil, fmt.Errorf("lease is %s, not prepared", state)
	}
	var ordinal int
	if err = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(ordinal),0)+1 FROM attempts WHERE task_id=? AND task_revision=?`, reservation.Task.ID, reservation.TaskRevision).Scan(&ordinal); err != nil {
		return nil, err
	}
	attemptID, t := id.New("att"), now()
	if _, err = tx.ExecContext(ctx, `INSERT INTO attempts(id,task_id,task_revision,ordinal,state,executor_id,coordination_epoch,authorized_at) VALUES(?,?,?,?,'authorized',?,?,?)`, attemptID, reservation.Task.ID, reservation.TaskRevision, ordinal, reservation.Lease.ExecutorID, reservation.Lease.CoordinationEpoch, t); err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE leases SET attempt_id=? WHERE id=?`, attemptID, reservation.Lease.ID); err != nil {
		return nil, err
	}
	token, hash, err := randomToken()
	if err != nil {
		return nil, err
	}
	authorizationID := id.New("auth")
	if _, err = tx.ExecContext(ctx, `INSERT INTO launch_authorizations(id,lease_id,attempt_id,coordination_epoch,token_hash,state,issued_at) VALUES(?,?,?,?,?,'issued',?)`, authorizationID, reservation.Lease.ID, attemptID, reservation.Lease.CoordinationEpoch, hash, t); err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE task_revisions SET scheduling_state='running' WHERE task_id=? AND revision=?`, reservation.Task.ID, reservation.TaskRevision); err != nil {
		return nil, err
	}
	if reservation.TaskRevision == reservation.Task.CurrentRevision {
		_, err = tx.ExecContext(ctx, `UPDATE tasks SET scheduling_state='running',updated_at=? WHERE id=?`, t, reservation.Task.ID)
		if err != nil {
			return nil, err
		}
	}
	if err = appendEvent(ctx, tx, "launch_authorized", "attempt", attemptID, &reservation.Lease.CoordinationEpoch, map[string]string{"lease_id": reservation.Lease.ID}); err != nil {
		return nil, err
	}
	var continuation sql.NullString
	_ = tx.QueryRowContext(ctx, `SELECT continuation_ref FROM attempts WHERE task_id=? AND task_revision=? AND continuation_ref IS NOT NULL AND id!=? ORDER BY ordinal DESC LIMIT 1`, reservation.Task.ID, reservation.TaskRevision, attemptID).Scan(&continuation)
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	reservation.Lease.AttemptID = &attemptID
	a := Attempt{ID: attemptID, TaskID: &reservation.Task.ID, TaskRevision: &reservation.TaskRevision, Ordinal: ordinal, State: "authorized", ExecutorID: reservation.Lease.ExecutorID, CoordinationEpoch: reservation.Lease.CoordinationEpoch}
	launch := &V1Launch{Task: reservation.Task, TaskRevision: reservation.TaskRevision, Argv: reservation.Argv, CWD: reservation.CWD, Attempt: a, Lease: reservation.Lease, AuthorizationID: authorizationID, AuthorizationToken: token, Resources: reservation.Resources}
	if continuation.Valid {
		launch.ContinuationRef = &continuation.String
	}
	return launch, nil
}

func (s *Store) ActivateLaunch(ctx context.Context, attemptID, leaseID string, epoch int64, token string, pid int, processIdentity string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = validateEpochTx(ctx, tx, attemptID, leaseID, epoch); err != nil {
		return err
	}
	sum := sha256String(token)
	var authID string
	if err = tx.QueryRowContext(ctx, `SELECT id FROM launch_authorizations WHERE attempt_id=? AND coordination_epoch=? AND token_hash=? AND state='issued'`, attemptID, epoch, sum).Scan(&authID); errors.Is(err, sql.ErrNoRows) {
		return errors.New("invalid or consumed launch authorization")
	} else if err != nil {
		return err
	}
	t := now()
	if _, err = tx.ExecContext(ctx, `UPDATE launch_authorizations SET state='consumed',consumed_at=? WHERE id=?`, t, authID); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE attempts SET state='running',pid=?,process_identity=?,started_at=?,last_heartbeat_at=? WHERE id=? AND state='authorized'`, pid, processIdentity, t, t, attemptID); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE leases SET state='active',activated_at=? WHERE id=? AND state='prepared'`, t, leaseID); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO attempt_processes(attempt_id,rank,pid,process_identity,registered_at,last_seen_at) VALUES(?,0,?,?,?,?)`, attemptID, pid, processIdentity, t, t); err != nil {
		return err
	}
	return tx.Commit()
}
func sha256String(value string) string {
	sum := sha256.Sum256([]byte(value))
	return fmt.Sprintf("%x", sum[:])
}

func (s *Store) RegisterProcess(ctx context.Context, attemptID, leaseID string, epoch int64, rank, pid int, identity string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = validateEpochTx(ctx, tx, attemptID, leaseID, epoch); err != nil {
		return err
	}
	t := now()
	if _, err = tx.ExecContext(ctx, `INSERT INTO attempt_processes(attempt_id,rank,pid,process_identity,registered_at,last_seen_at) VALUES(?,?,?,?,?,?) ON CONFLICT(attempt_id,rank) DO UPDATE SET pid=excluded.pid,process_identity=excluded.process_identity,last_seen_at=excluded.last_seen_at`, attemptID, rank, pid, identity, t, t); err != nil {
		return err
	}
	return tx.Commit()
}

func sortResources(resources []ResourceInstance) {
	sort.Slice(resources, func(i, j int) bool { return resources[i].ID < resources[j].ID })
}

// EnsureV1Preemption only acts when the highest-priority runnable GPU request
// cannot fit on currently available GPUs. It chooses the fewest lower-priority
// checkpointable workloads, preferring sets that release more GPUs.
func (s *Store) EnsureV1Preemption(ctx context.Context) ([]string, error) {
	var highPriority int
	var needed int64
	err := s.db.QueryRowContext(ctx, `SELECT t.effective_priority,rr.quantity FROM tasks t JOIN task_revisions tr ON tr.task_id=t.id AND tr.revision=t.current_revision JOIN resource_requests rr ON rr.task_id=tr.task_id AND rr.task_revision=tr.revision AND rr.request_type='exclusive' AND rr.kind='gpu' WHERE t.desired_state='active' AND tr.desired_state='active' AND tr.scheduling_state='pending' AND tr.became_runnable_at IS NOT NULL AND NOT EXISTS(SELECT 1 FROM attempts a WHERE a.task_id=t.id AND a.state IN('authorized','running','suspend_requested')) ORDER BY t.effective_priority DESC,tr.became_runnable_at,t.task_key LIMIT 1`).Scan(&highPriority, &needed)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var free int64
	if err = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM resource_instances r WHERE r.kind='gpu' AND r.admin_state='enabled' AND EXISTS(SELECT 1 FROM resource_observations o WHERE o.resource_id=r.id AND o.valid_until>? ORDER BY o.observed_at DESC LIMIT 1) AND NOT EXISTS(SELECT 1 FROM external_claims c WHERE c.resource_id=r.id AND c.cleared_at IS NULL) AND NOT EXISTS(SELECT 1 FROM lease_items li JOIN leases l ON l.id=li.lease_id WHERE li.resource_id=r.id AND l.state IN('reserved','prepared','active','releasing','stale','revocation_requested'))`, now()).Scan(&free); err != nil {
		return nil, err
	}
	if free >= needed {
		return nil, nil
	}
	type victim struct {
		attempt  string
		priority int
		gpus     int
	}
	rows, err := s.db.QueryContext(ctx, `SELECT a.id,t.effective_priority,COUNT(li.resource_id) FROM attempts a JOIN tasks t ON t.id=a.task_id JOIN task_revisions tr ON tr.task_id=a.task_id AND tr.revision=a.task_revision JOIN leases l ON l.attempt_id=a.id JOIN lease_items li ON li.lease_id=l.id AND li.kind='gpu' WHERE a.state='running' AND l.state='active' AND tr.checkpointable=1 AND t.effective_priority<? GROUP BY a.id,t.effective_priority ORDER BY COUNT(li.resource_id) DESC,t.effective_priority ASC,a.id`, highPriority)
	if err != nil {
		return nil, err
	}
	var victims []victim
	for rows.Next() {
		var v victim
		if err := rows.Scan(&v.attempt, &v.priority, &v.gpus); err != nil {
			rows.Close()
			return nil, err
		}
		victims = append(victims, v)
	}
	rows.Close()
	potential := free
	for _, v := range victims {
		potential += int64(v.gpus)
	}
	if potential < needed {
		return nil, nil
	}
	released := free
	var commands []string
	for _, v := range victims {
		command, err := s.EnqueueCommandV1(ctx, v.attempt, "suspend", "higher_priority_workload")
		if err != nil {
			return commands, err
		}
		commands = append(commands, command)
		released += int64(v.gpus)
		if released >= needed {
			break
		}
	}
	return commands, nil
}
