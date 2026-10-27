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
	// Gang is set for one rank of a placed gang (informational: AuthorizeLaunch
	// reads the placement from the store).
	Gang *GangLaunch `json:"gang,omitempty"`
}

// Launch is a single-use authorization. It contains coordination facts only;
// it does not describe success, retry, or any project workflow transition.
type Launch struct {
	Execution          ExecutionRequest `json:"execution"`
	Attempt            Attempt          `json:"attempt"`
	Lease              Lease            `json:"lease"`
	AuthorizationID    string           `json:"authorization_id"`
	AuthorizationToken string           `json:"authorization_token"`
	// WorkerToken is the credential handed to the attempt's processes
	// (KAIRO_ATTEMPT_TOKEN); only its hash is stored.
	WorkerToken          string             `json:"worker_token"`
	Argv                 []string           `json:"argv"`
	CWD                  string             `json:"cwd"`
	Resources            []ResourceInstance `json:"resources"`
	InputContinuationRef *string            `json:"input_continuation_ref,omitempty"`
	Gang                 *GangLaunch        `json:"gang,omitempty"`
	// Env is the execution's declared environment (a gang rank's already
	// merged with the gang's), read from the store, never from the caller.
	Env map[string]string `json:"env,omitempty"`
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

// ReserveNext runs a planning pass (planner.go) and reserves for executorID
// what the pass places on it: one execution, or, when the pass places a gang
// anywhere, every rank of it (this executor's rank, if any, is handed over).
// Priority orders coordination admission; it does not imply retry or
// workflow semantics. A pass that reserves nothing records every waiting
// execution's reason.
func (s *Store) ReserveNext(ctx context.Context, executorID string) (*Reservation, error) {
	// A gang rank already placed on this executor is handed over first.
	if reservation, err := s.deliverGangLease(ctx, executorID); reservation != nil || err != nil {
		return reservation, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if ok, err := callerMayPlanTx(ctx, tx, executorID); err != nil || !ok {
		return nil, err
	}
	m, err := loadPlanModel(ctx, tx, s.policy, executorID)
	if err != nil {
		return nil, err
	}
	candidates, err := loadPlanCandidates(ctx, tx, m)
	if err != nil {
		return nil, err
	}
	p := newPlanner(m, false)
	caller, _ := m.executor(executorID)
	waits := map[string]planWait{}
	var reservation *Reservation
	gangPlaced := false
	err = p.order(candidates, func(c *planCandidate) (bool, error) {
		placement := p.place(c)
		switch {
		case placement.terminal != "":
			return false, terminalizeAdmissionTx(ctx, tx, c.execution, placement.terminal)
		case len(placement.slots) == 0:
			waits[c.execution.ID] = placement.wait
		case c.gangID != "":
			for i, slot := range placement.slots {
				placement.slots[i].executor = m.onNode(slot, caller, c.ranks[i].candidate.selectorJSON)
			}
			placed, placeErr := placeGangTx(ctx, tx, c.gangID, placement.slots)
			if placeErr != nil || placed {
				gangPlaced = placed
				return true, placeErr
			}
		case m.onNode(placement.slots[0], caller, c.selectorJSON).id != executorID:
			slot := placement.slots[0]
			waits[c.execution.ID] = planWait{reason: "awaiting_executor", node: slot.fit.node, message: "placed on executor " + slot.executor.id + ", which reserves it when it next polls"}
		default:
			r, writeErr := writeReservationTx(ctx, tx, executorID, c.executionCandidate, placement.slots[0].fit)
			reservation = r
			return r != nil || writeErr != nil, writeErr
		}
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	// A pass cut short by a reservation has seen only the executions ahead of
	// it: their reasons are kept, the others' left as they were.
	complete := reservation == nil && !gangPlaced
	if err = saveWaitsTx(ctx, tx, waits, complete); err == nil && complete {
		err = saveNodeAlertsTx(ctx, tx, m)
	}
	if err == nil && reservation != nil {
		_, err = tx.ExecContext(ctx, `DELETE FROM admission_waits WHERE execution_id=?`, reservation.Execution.ID)
	}
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	if gangPlaced {
		return s.deliverGangLease(ctx, executorID)
	}
	return reservation, nil
}

// callerMayPlanTx reports whether executorID may take work: it exists, is
// enabled, and its node is not quarantined (ReconcileNodeQuarantines stops
// what runs there).
func callerMayPlanTx(ctx context.Context, tx *sql.Tx, executorID string) (bool, error) {
	var nodeID string
	var enabled bool
	err := tx.QueryRowContext(ctx, `SELECT node_id,enabled FROM executors WHERE id=?`, executorID).Scan(&nodeID, &enabled)
	if errors.Is(err, sql.ErrNoRows) || err == nil && !enabled {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	quarantined, err := nodeQuarantinedTx(ctx, tx, nodeID)
	return !quarantined, err
}

// writeReservationTx creates the reserved lease of a planned fit, capturing
// the scope gates' generations. It returns nil when a gate has closed.
func writeReservationTx(ctx context.Context, tx *sql.Tx, executorID string, candidate executionCandidate, fit planFit) (*Reservation, error) {
	gates, err := executionGatesTx(ctx, tx, candidate.execution.ID)
	if err != nil {
		return nil, err
	}
	for _, gate := range gates {
		if gate.State != "open" {
			return nil, nil
		}
	}
	epoch := int64(1)
	var previous sql.NullInt64
	if err = tx.QueryRowContext(ctx, `SELECT MAX(coordination_epoch) FROM leases WHERE execution_id=?`, candidate.execution.ID).Scan(&previous); err != nil {
		return nil, err
	}
	if previous.Valid {
		epoch = previous.Int64 + 1
	}
	leaseID, stamp := id.New("lea"), now()
	expires := time.Now().UTC().Add(30 * time.Second).Format(time.RFC3339Nano)
	if _, err = tx.ExecContext(ctx, `INSERT INTO leases(id,execution_id,executor_id,coordination_epoch,state,expires_at,created_at) VALUES(?,?,?,?,'reserved',?,?)`, leaseID, candidate.execution.ID, executorID, epoch, expires, stamp); err != nil {
		return nil, err
	}
	gateGenerations := make(map[string]int64, len(gates))
	for _, gate := range gates {
		gateGenerations[gate.ScopeID] = gate.Generation
		if _, err = tx.ExecContext(ctx, `INSERT INTO lease_gate_snapshots(lease_id,scope_id,generation) VALUES(?,?,?)`, leaseID, gate.ScopeID, gate.Generation); err != nil {
			return nil, err
		}
	}
	resources := make([]ResourceInstance, 0, len(fit.gpus))
	for _, resourceID := range fit.gpus {
		var r ResourceInstance
		var binding, attributes string
		if err = tx.QueryRowContext(ctx, `SELECT id,node_id,provider_id,kind,stable_identity,binding_json,attributes_json,admin_state,quarantine_reason FROM resource_instances WHERE id=?`, resourceID).Scan(&r.ID, &r.NodeID, &r.ProviderID, &r.Kind, &r.StableIdentity, &binding, &attributes, &r.AdminState, &r.QuarantineReason); err != nil {
			return nil, err
		}
		r.Binding, r.Attributes = json.RawMessage(binding), json.RawMessage(attributes)
		resources = append(resources, r)
		if _, err = tx.ExecContext(ctx, `INSERT INTO lease_items(lease_id,resource_id,kind,quantity,filesystem) VALUES(?,?,'gpu',1,'')`, leaseID, resourceID); err != nil {
			return nil, err
		}
	}
	for _, c := range fit.capacity {
		if _, err = tx.ExecContext(ctx, `INSERT INTO lease_items(lease_id,resource_id,kind,quantity,filesystem) VALUES(?,?,?,?,?)`, leaseID, c.resourceID, c.key.kind, c.quantity, c.key.filesystem); err != nil {
			return nil, err
		}
	}
	if err = appendCoordinationEventTx(ctx, tx, "lease_reserved", &candidate.execution.ProjectScopeID, "lease", leaseID, &epoch, map[string]any{"execution_id": candidate.execution.ID, "gate_generations": gateGenerations}); err != nil {
		return nil, err
	}
	if err = json.Unmarshal([]byte(candidate.argvJSON), &candidate.execution.Argv); err != nil {
		return nil, err
	}
	candidate.execution.ExecutorSelector = json.RawMessage(candidate.selectorJSON)
	sortResources(resources)
	lease := Lease{ID: leaseID, ExecutionID: candidate.execution.ID, ExecutorID: executorID, CoordinationEpoch: epoch, State: "reserved", CreatedAt: stamp, ExpiresAt: &expires}
	return &Reservation{Lease: lease, Execution: candidate.execution, Argv: candidate.execution.Argv, CWD: candidate.execution.CWD, Resources: resources, GateGenerations: gateGenerations}, nil
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
		EXISTS(SELECT 1 FROM external_claims c WHERE c.resource_id IN `+sameDevice("li.resource_id")+` AND c.cleared_at IS NULL AND c.claim_kind='external_process'),
		EXISTS(SELECT 1 FROM external_claims c WHERE c.resource_id IN `+sameDevice("li.resource_id")+` AND c.cleared_at IS NULL AND c.claim_kind='unattributed_activity')
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
		(o.id IS NULL OR julianday(o.valid_until)<=julianday(?) OR julianday(o.observed_at)<=julianday((SELECT created_at FROM leases WHERE id=?))
		 OR EXISTS(SELECT 1 FROM external_claims c WHERE c.resource_id IN `+sameDevice("li.resource_id")+` AND c.cleared_at IS NULL
		   AND (c.claim_kind='external_process' OR c.claim_kind='unattributed_activity' AND COALESCE(
		     (SELECT json_extract(rr.policy_json,'$.on_unattributed_activity') FROM resource_requests rr
		      JOIN leases policy_lease ON policy_lease.execution_id=rr.execution_id
		      WHERE policy_lease.id=li.lease_id AND rr.request_type='exclusive' AND rr.kind=li.kind LIMIT 1),
		     'wait')!='allow')))`, leaseID, stamp, leaseID).Scan(&unsafeExclusive); err != nil {
		return err
	}
	if unsafeExclusive != 0 {
		return errors.New("reservation cleanup lacks a fresh GPU observation allowed by its conflict policy")
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
	var envJSON sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT l.execution_id,e.project_scope_id,l.state,e.state,l.executor_id,l.coordination_epoch,e.env_json FROM leases l JOIN execution_requests e ON e.id=l.execution_id WHERE l.id=?`, reservation.Lease.ID).Scan(&executionID, &projectID, &leaseState, &executionState, &executorID, &leaseEpoch, &envJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if executionID != reservation.Execution.ID || leaseEpoch != reservation.Lease.CoordinationEpoch {
		return nil, fmt.Errorf("%w: reservation does not match lease", ErrStaleEpoch)
	}
	env, err := decodeEnv(envJSON)
	if err != nil {
		return nil, err
	}
	if leaseState != "prepared" || executionState != "waiting" {
		return nil, fmt.Errorf("%w: lease=%s execution=%s", ErrExecutionStarted, leaseState, executionState)
	}
	if err = validateLeaseGatesTx(ctx, tx, reservation.Lease.ID); err != nil {
		return nil, err
	}
	// Launch barrier: a gang rank launches only once every rank is prepared.
	gang, err := gangLaunchGateTx(ctx, tx, reservation.Lease.ID)
	if err != nil {
		return nil, err
	}
	attemptID, stamp := id.New("att"), now()
	if _, err = tx.ExecContext(ctx, `INSERT INTO attempts(id,execution_id,state,executor_id,coordination_epoch,authorized_at) VALUES(?,?,'authorized',?,?,?)`, attemptID, executionID, executorID, leaseEpoch, stamp); err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE leases SET attempt_id=? WHERE id=? AND attempt_id IS NULL`, attemptID, reservation.Lease.ID); err != nil {
		return nil, err
	}
	token, tokenHash, err := newSecret("")
	if err != nil {
		return nil, err
	}
	authorizationID := id.New("auth")
	if _, err = tx.ExecContext(ctx, `INSERT INTO launch_authorizations(id,lease_id,attempt_id,coordination_epoch,token_hash,state,issued_at) VALUES(?,?,?,?,?,'issued',?)`, authorizationID, reservation.Lease.ID, attemptID, leaseEpoch, tokenHash, stamp); err != nil {
		return nil, err
	}
	workerToken, workerTokenHash, err := newSecret("kairo_worker_")
	if err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO attempt_tokens(attempt_id,lease_id,coordination_epoch,token_hash,issued_at) VALUES(?,?,?,?,?)`, attemptID, reservation.Lease.ID, leaseEpoch, workerTokenHash, stamp); err != nil {
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
		WorkerToken:          workerToken,
		Argv:                 append([]string(nil), reservation.Argv...),
		CWD:                  reservation.CWD,
		Resources:            append([]ResourceInstance(nil), reservation.Resources...),
		InputContinuationRef: reservation.Execution.InputContinuationRef,
		Gang:                 gang,
		Env:                  env,
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

// newSecret returns prefix followed by 32 random bytes (base64url), and the
// hash under which it is stored.
func newSecret(prefix string) (string, string, error) {
	value := make([]byte, 32)
	if _, err := rand.Read(value); err != nil {
		return "", "", err
	}
	token := prefix + base64.RawURLEncoding.EncodeToString(value)
	return token, hashToken(token), nil
}

func hashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return fmt.Sprintf("%x", sum[:])
}

func sortResources(resources []ResourceInstance) {
	sort.Slice(resources, func(i, j int) bool { return resources[i].ID < resources[j].ID })
}
