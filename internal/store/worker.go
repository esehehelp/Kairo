package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"kairo/internal/id"
)

func (s *Store) Heartbeat(ctx context.Context, attemptID, leaseID string, epoch int64, progress json.RawMessage) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = validateEpochTx(ctx, tx, attemptID, leaseID, epoch); err != nil {
		return err
	}
	if len(progress) == 0 {
		progress = json.RawMessage(`{}`)
	}
	if !json.Valid(progress) {
		return errors.New("progress must be valid JSON")
	}
	t := now()
	res, err := tx.ExecContext(ctx, `UPDATE attempts SET last_heartbeat_at=?,progress_json=? WHERE id=? AND state IN('running','quiescing')`, t, string(progress), attemptID)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return ErrStaleEpoch
	}
	return tx.Commit()
}

func (s *Store) RegisterProcess(ctx context.Context, attemptID, leaseID string, epoch int64, rank, pid int, identity string) error {
	if rank < 0 || pid <= 0 || identity == "" {
		return errors.New("worker rank, PID, and process identity are required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = validateEpochTx(ctx, tx, attemptID, leaseID, epoch); err != nil {
		return err
	}
	var attemptState, leaseState string
	if err = tx.QueryRowContext(ctx, `SELECT a.state,l.state FROM attempts a JOIN leases l ON l.id=? AND l.attempt_id=a.id WHERE a.id=?`, leaseID, attemptID).Scan(&attemptState, &leaseState); err != nil {
		return err
	}
	if (attemptState != "running" && attemptState != "quiescing") || (leaseState != "active" && leaseState != "releasing") {
		return ErrStaleEpoch
	}
	var registeredPID int
	var registeredIdentity string
	var registeredExited sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT pid,process_identity,exited_at FROM attempt_processes WHERE attempt_id=? AND role='worker' AND rank=?`, attemptID, rank).Scan(&registeredPID, &registeredIdentity, &registeredExited)
	if err == nil && (registeredPID != pid || registeredIdentity != identity || registeredExited.Valid) {
		return errors.New("worker rank is already bound to a different or exited process identity")
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	t := now()
	_, err = tx.ExecContext(ctx, `INSERT INTO attempt_processes(attempt_id,role,namespace,rank,pid,process_identity,registered_at,last_seen_at)
	 VALUES(?,'worker','executor',?,?,?,?,?)
	 ON CONFLICT(attempt_id,role,rank) DO UPDATE SET last_seen_at=excluded.last_seen_at`, attemptID, rank, pid, identity, t, t)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) PollCommands(ctx context.Context, attemptID, leaseID string, epoch int64) ([]Command, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if err = validateEpochTx(ctx, tx, attemptID, leaseID, epoch); err != nil {
		return nil, err
	}
	var attemptState, leaseState string
	if err = tx.QueryRowContext(ctx, `SELECT a.state,l.state FROM attempts a JOIN leases l ON l.id=? AND l.attempt_id=a.id WHERE a.id=?`, leaseID, attemptID).Scan(&attemptState, &leaseState); err != nil {
		return nil, err
	}
	if (attemptState != "running" && attemptState != "quiescing") || (leaseState != "active" && leaseState != "releasing") {
		return nil, ErrStaleEpoch
	}
	rows, err := tx.QueryContext(ctx, `SELECT id,execution_id,attempt_id,kind,origin,reason,state,delivery_count,created_at,payload_json
	 FROM commands WHERE attempt_id=? AND state IN('pending','accepted','checkpointing') ORDER BY created_at,id`, attemptID)
	if err != nil {
		return nil, err
	}
	var commands []Command
	for rows.Next() {
		var command Command
		var payload string
		if err = rows.Scan(&command.ID, &command.ExecutionID, &command.AttemptID, &command.Kind, &command.Origin, &command.Reason, &command.State, &command.DeliveryCount, &command.CreatedAt, &payload); err != nil {
			rows.Close()
			return nil, err
		}
		command.Payload = json.RawMessage(payload)
		commands = append(commands, command)
	}
	if err = rows.Close(); err != nil {
		return nil, err
	}
	t := now()
	for i := range commands {
		commands[i].DeliveryCount++
		if _, err = tx.ExecContext(ctx, `UPDATE commands SET delivery_count=delivery_count+1,updated_at=? WHERE id=?`, t, commands[i].ID); err != nil {
			return nil, err
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO command_deliveries(command_id,attempt_id,coordination_epoch,delivered_at) VALUES(?,?,?,?)`, commands[i].ID, attemptID, epoch, t); err != nil {
			return nil, err
		}
	}
	return commands, tx.Commit()
}

func (s *Store) AckCommand(ctx context.Context, attemptID, leaseID string, epoch int64, commandID, phase string, payload json.RawMessage) error {
	if _, ok := ackState(phase); !ok {
		return errors.New("invalid command acknowledgement phase")
	}
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	if !json.Valid(payload) {
		return errors.New("command acknowledgement payload must be valid JSON")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = validateEpochTx(ctx, tx, attemptID, leaseID, epoch); err != nil {
		return err
	}
	var executionID, state string
	if err = tx.QueryRowContext(ctx, `SELECT execution_id,state FROM commands WHERE id=? AND attempt_id=? AND lease_id=? AND coordination_epoch=? AND kind='suspend'`, commandID, attemptID, leaseID, epoch).Scan(&executionID, &state); errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	} else if err != nil {
		return err
	}
	t := now()
	if _, err = tx.ExecContext(ctx, `INSERT INTO command_acks(command_id,attempt_id,coordination_epoch,phase,payload_json,created_at) VALUES(?,?,?,?,?,?)`, commandID, attemptID, epoch, phase, string(payload), t); err != nil {
		return err
	}
	advance := false
	if state != "rejected" {
		if phase == "rejected" {
			advance = state != "checkpointed"
		} else {
			advance = commandStateRank(phase) > commandStateRank(state)
		}
	}
	if advance {
		if _, err = tx.ExecContext(ctx, `UPDATE commands SET state=?,payload_json=?,updated_at=? WHERE id=?`, phase, string(payload), t, commandID); err != nil {
			return err
		}
	}
	if phase == "rejected" && advance {
		result, updateErr := tx.ExecContext(ctx, `UPDATE attempts SET state='running' WHERE id=? AND state='quiescing'`, attemptID)
		if updateErr != nil {
			return updateErr
		}
		restored, _ := result.RowsAffected()
		if restored == 1 {
			if _, err = tx.ExecContext(ctx, `UPDATE leases SET state='active',releasing_at=NULL WHERE id=? AND state='releasing'`, leaseID); err != nil {
				return err
			}
		}
		if _, err = tx.ExecContext(ctx, `UPDATE pause_targets SET state='blocked',blocker_reason='checkpoint_rejected',updated_at=? WHERE command_id=? AND state!='quiesced'`, t, commandID); err != nil {
			return err
		}
	}
	if (phase == "accepted" || phase == "checkpointing") && state != "checkpointed" && state != "rejected" {
		if _, err = tx.ExecContext(ctx, `UPDATE attempts SET state='quiescing' WHERE id=? AND state='running'`, attemptID); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE leases SET state='releasing',releasing_at=COALESCE(releasing_at,?) WHERE id=? AND state='active'`, t, leaseID); err != nil {
			return err
		}
	}
	if phase == "checkpointed" && advance {
		var body struct {
			ContinuationRef string `json:"continuation_ref"`
		}
		if err = json.Unmarshal(payload, &body); err != nil || body.ContinuationRef == "" {
			return errors.New("checkpointed acknowledgement requires continuation_ref")
		}
		if _, err = tx.ExecContext(ctx, `UPDATE attempts SET state=CASE WHEN state IN('running','quiescing') THEN 'quiescing' ELSE state END,checkpointed_at=?,continuation_ref=? WHERE id=?`, t, body.ContinuationRef, attemptID); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE leases SET state='releasing',releasing_at=COALESCE(releasing_at,?) WHERE id=? AND state='active'`, t, leaseID); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE pause_targets SET state='quiescing',updated_at=? WHERE command_id=? AND state!='quiesced'`, t, commandID); err != nil {
			return err
		}
		projectID, projectErr := executionProjectIDTx(ctx, tx, executionID)
		if projectErr != nil {
			return projectErr
		}
		if err = appendCoordinationEventTx(ctx, tx, "checkpoint_published", &projectID, "attempt", attemptID, &epoch, map[string]any{"execution_id": executionID, "command_id": commandID, "continuation_ref": body.ContinuationRef}); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) EnqueueSuspend(ctx context.Context, attemptID, origin, reason string) (string, error) {
	if origin != "scope_pause" && origin != "priority_preemption" {
		return "", errors.New("invalid suspend origin")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	var executionID, leaseID, attemptState, leaseState string
	var epoch int64
	if err = tx.QueryRowContext(ctx, `SELECT a.execution_id,l.id,a.coordination_epoch,a.state,l.state FROM attempts a JOIN leases l ON l.attempt_id=a.id WHERE a.id=?`, attemptID).Scan(&executionID, &leaseID, &epoch, &attemptState, &leaseState); errors.Is(err, sql.ErrNoRows) {
		return "", ErrNotFound
	} else if err != nil {
		return "", err
	}
	if attemptState != "running" && attemptState != "quiescing" {
		return "", fmt.Errorf("attempt is not suspendable from state %s", attemptState)
	}
	if leaseState != "active" && leaseState != "releasing" {
		return "", fmt.Errorf("lease is not suspendable from state %s", leaseState)
	}
	var existing string
	if err = tx.QueryRowContext(ctx, `SELECT id FROM commands WHERE attempt_id=? AND state!='rejected' LIMIT 1`, attemptID).Scan(&existing); err == nil {
		return existing, tx.Commit()
	} else if !errors.Is(err, sql.ErrNoRows) {
		return "", err
	}
	commandID, t := id.New("cmd"), now()
	if _, err = tx.ExecContext(ctx, `INSERT INTO commands(id,execution_id,attempt_id,lease_id,coordination_epoch,kind,origin,reason,state,created_at,updated_at) VALUES(?,?,?,?,?,'suspend',?,?,'pending',?,?)`, commandID, executionID, attemptID, leaseID, epoch, origin, reason, t, t); err != nil {
		return "", err
	}
	projectID, err := executionProjectIDTx(ctx, tx, executionID)
	if err != nil {
		return "", err
	}
	if err = appendCoordinationEventTx(ctx, tx, "suspend_requested", &projectID, "command", commandID, &epoch, map[string]any{"execution_id": executionID, "attempt_id": attemptID, "origin": origin, "reason": reason}); err != nil {
		return "", err
	}
	return commandID, tx.Commit()
}

func executionProjectIDTx(ctx context.Context, tx *sql.Tx, executionID string) (string, error) {
	var projectID string
	err := tx.QueryRowContext(ctx, `SELECT project_scope_id FROM execution_requests WHERE id=?`, executionID).Scan(&projectID)
	return projectID, err
}

// RecordTerminal stores raw process termination and deliberately does not
// classify success, failure, or retryability. Process absence is finalized
// separately by FinalizeQuiescence.
func (s *Store) RecordTerminal(ctx context.Context, attemptID, leaseID string, epoch int64, exitCode int, exitSignal string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = validateEpochTx(ctx, tx, attemptID, leaseID, epoch); err != nil {
		return err
	}
	var executionID, state string
	if err = tx.QueryRowContext(ctx, `SELECT execution_id,state FROM attempts WHERE id=?`, attemptID).Scan(&executionID, &state); err != nil {
		return err
	}
	if state == "quiesced" || state == "exited" {
		return tx.Commit()
	}
	if state == "lost" {
		return errors.New("lost attempt requires reconciliation")
	}
	t := now()
	if _, err = tx.ExecContext(ctx, `UPDATE attempts SET state='exited',exit_code=?,exit_signal=?,exited_at=? WHERE id=?`, exitCode, nullable(exitSignal), t, attemptID); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE attempt_processes SET exited_at=?,last_seen_at=? WHERE attempt_id=? AND role='launcher' AND exited_at IS NULL`, t, t, attemptID); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE leases SET state='releasing',releasing_at=COALESCE(releasing_at,?) WHERE id=? AND state IN('prepared','active')`, t, leaseID); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE execution_requests SET state='terminal',terminal_cause='process_exit',terminal_at=? WHERE id=? AND state IN('authorized','started')`, t, executionID); err != nil {
		return err
	}
	projectID, err := executionProjectIDTx(ctx, tx, executionID)
	if err != nil {
		return err
	}
	if err = appendCoordinationEventTx(ctx, tx, "execution_terminal", &projectID, "execution", executionID, &epoch, map[string]any{"attempt_id": attemptID, "exit_code": exitCode, "exit_signal": exitSignal}); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) SetAttemptLogPaths(ctx context.Context, attemptID string, epoch int64, stdoutPath, stderrPath string) error {
	res, err := s.db.ExecContext(ctx, `UPDATE attempts SET stdout_path=?,stderr_path=? WHERE id=? AND coordination_epoch=?`, stdoutPath, stderrPath, attemptID, epoch)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return ErrStaleEpoch
	}
	return nil
}

type QuiescenceCandidate struct {
	Attempt   Attempt
	Lease     Lease
	Processes []AttemptProcess
	Resources []ResourceInstance
}

func (s *Store) ListQuiescenceCandidates(ctx context.Context, executorID string) ([]QuiescenceCandidate, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT a.id,a.execution_id,a.state,a.executor_id,a.coordination_epoch,a.pid,a.process_identity,a.exit_code,a.exit_signal,a.continuation_ref,a.progress_json,a.started_at,a.last_heartbeat_at,a.checkpointed_at,a.exited_at,a.quiesced_at,
	 l.id,l.execution_id,l.attempt_id,l.executor_id,l.coordination_epoch,l.state,l.created_at,l.expires_at
	 FROM attempts a JOIN leases l ON l.attempt_id=a.id WHERE a.executor_id=? AND a.state='exited' AND l.state='releasing' ORDER BY a.exited_at`, executorID)
	if err != nil {
		return nil, err
	}
	var out []QuiescenceCandidate
	for rows.Next() {
		var c QuiescenceCandidate
		var progress sql.NullString
		if err = rows.Scan(&c.Attempt.ID, &c.Attempt.ExecutionID, &c.Attempt.State, &c.Attempt.ExecutorID, &c.Attempt.CoordinationEpoch, &c.Attempt.PID, &c.Attempt.ProcessIdentity, &c.Attempt.ExitCode, &c.Attempt.ExitSignal, &c.Attempt.ContinuationRef, &progress, &c.Attempt.StartedAt, &c.Attempt.LastHeartbeatAt, &c.Attempt.CheckpointedAt, &c.Attempt.ExitedAt, &c.Attempt.QuiescedAt,
			&c.Lease.ID, &c.Lease.ExecutionID, &c.Lease.AttemptID, &c.Lease.ExecutorID, &c.Lease.CoordinationEpoch, &c.Lease.State, &c.Lease.CreatedAt, &c.Lease.ExpiresAt); err != nil {
			rows.Close()
			return nil, err
		}
		if progress.Valid {
			c.Attempt.Progress = json.RawMessage(progress.String)
		}
		out = append(out, c)
	}
	if err = rows.Close(); err != nil {
		return nil, err
	}
	for i := range out {
		out[i].Processes, err = s.ListAttemptProcesses(ctx, out[i].Attempt.ID, true)
		if err != nil {
			return nil, err
		}
		out[i].Resources, err = s.listLeaseResources(ctx, out[i].Lease.ID)
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (s *Store) ListAttemptProcesses(ctx context.Context, attemptID string, liveOnly bool) ([]AttemptProcess, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT attempt_id,role,namespace,rank,pid,process_identity,registered_at,last_seen_at,exited_at FROM attempt_processes WHERE attempt_id=? AND (?=0 OR exited_at IS NULL) ORDER BY role,rank`, attemptID, liveOnly)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []AttemptProcess
	for rows.Next() {
		var process AttemptProcess
		if err = rows.Scan(&process.AttemptID, &process.Role, &process.Namespace, &process.Rank, &process.PID, &process.ProcessIdentity, &process.RegisteredAt, &process.LastSeenAt, &process.ExitedAt); err != nil {
			return nil, err
		}
		out = append(out, process)
	}
	return out, rows.Err()
}

func (s *Store) MarkAttemptProcessExited(ctx context.Context, attemptID, role string, rank int, identity string) error {
	res, err := s.db.ExecContext(ctx, `UPDATE attempt_processes SET exited_at=?,last_seen_at=? WHERE attempt_id=? AND role=? AND rank=? AND process_identity=? AND exited_at IS NULL`, now(), now(), attemptID, role, rank, identity)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		var exited sql.NullString
		if scanErr := s.db.QueryRowContext(ctx, `SELECT exited_at FROM attempt_processes WHERE attempt_id=? AND role=? AND rank=? AND process_identity=?`, attemptID, role, rank, identity).Scan(&exited); scanErr != nil || !exited.Valid {
			return ErrNotFound
		}
	}
	return nil
}

func (s *Store) listLeaseResources(ctx context.Context, leaseID string) ([]ResourceInstance, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT r.id,r.node_id,r.provider_id,r.kind,r.stable_identity,r.binding_json,r.attributes_json,r.admin_state,r.quarantine_reason
	 FROM lease_items li JOIN resource_instances r ON r.id=li.resource_id WHERE li.lease_id=? AND li.resource_id IS NOT NULL ORDER BY r.id`, leaseID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ResourceInstance
	for rows.Next() {
		var resource ResourceInstance
		var binding, attributes string
		if err = rows.Scan(&resource.ID, &resource.NodeID, &resource.ProviderID, &resource.Kind, &resource.StableIdentity, &binding, &attributes, &resource.AdminState, &resource.QuarantineReason); err != nil {
			return nil, err
		}
		resource.Binding = json.RawMessage(binding)
		resource.Attributes = json.RawMessage(attributes)
		out = append(out, resource)
	}
	return out, rows.Err()
}

func (s *Store) FinalizeQuiescence(ctx context.Context, attemptID, leaseID string, epoch int64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = validateEpochTx(ctx, tx, attemptID, leaseID, epoch); err != nil {
		return err
	}
	var executionID, attemptState, leaseState string
	if err = tx.QueryRowContext(ctx, `SELECT a.execution_id,a.state,l.state FROM attempts a JOIN leases l ON l.id=? AND l.attempt_id=a.id WHERE a.id=?`, leaseID, attemptID).Scan(&executionID, &attemptState, &leaseState); err != nil {
		return err
	}
	if attemptState == "quiesced" && leaseState == "released" {
		return tx.Commit()
	}
	if attemptState != "exited" || leaseState != "releasing" {
		return fmt.Errorf("attempt/lease not ready for quiescence: %s/%s", attemptState, leaseState)
	}
	var live int
	if err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM attempt_processes WHERE attempt_id=? AND exited_at IS NULL`, attemptID).Scan(&live); err != nil {
		return err
	}
	if live != 0 {
		return fmt.Errorf("attempt still has %d live registered processes", live)
	}
	// Exclusive GPU ownership is not released back to scheduling while reality
	// is stale or a claim forbidden by the execution's admission policy remains.
	// In particular, an unattributed claim that was explicitly allowed at
	// admission must not make lease release impossible on a desktop GPU.
	var unsafe int
	if err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM lease_items li
	 LEFT JOIN resource_observations o ON o.id=(SELECT id FROM resource_observations WHERE resource_id=li.resource_id ORDER BY id DESC LIMIT 1)
	 WHERE li.lease_id=? AND li.kind='gpu' AND li.resource_id IS NOT NULL AND
	 (o.id IS NULL OR julianday(o.valid_until)<=julianday(?)
	 OR julianday(o.observed_at)<=julianday((SELECT exited_at FROM attempts WHERE id=?))
	 OR EXISTS(SELECT 1 FROM attempt_processes ap WHERE ap.attempt_id=? AND julianday(o.observed_at)<=julianday(ap.exited_at))
	 OR EXISTS(SELECT 1 FROM external_claims c WHERE c.resource_id=li.resource_id AND c.cleared_at IS NULL
	   AND (c.claim_kind='external_process' OR c.claim_kind='unattributed_activity' AND COALESCE(
	     (SELECT json_extract(rr.policy_json,'$.on_unattributed_activity') FROM resource_requests rr
	      JOIN leases policy_lease ON policy_lease.execution_id=rr.execution_id
	      WHERE policy_lease.id=li.lease_id AND rr.request_type='exclusive' AND rr.kind=li.kind LIMIT 1),
	     'wait')!='allow'))
	 )`, leaseID, now(), attemptID, attemptID).Scan(&unsafe); err != nil {
		return err
	}
	if unsafe != 0 {
		return errors.New("leased GPU still lacks a fresh observation allowed by its conflict policy")
	}
	t := now()
	if _, err = tx.ExecContext(ctx, `UPDATE attempts SET state='quiesced',quiesced_at=? WHERE id=? AND state='exited'`, t, attemptID); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE leases SET state='released',released_at=? WHERE id=? AND state='releasing'`, t, leaseID); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE pause_targets SET state='quiesced',updated_at=? WHERE execution_id=? AND attempt_id=?`, t, executionID, attemptID); err != nil {
		return err
	}
	projectID, err := executionProjectIDTx(ctx, tx, executionID)
	if err != nil {
		return err
	}
	if err = appendCoordinationEventTx(ctx, tx, "attempt_quiesced", &projectID, "attempt", attemptID, &epoch, map[string]any{"execution_id": executionID, "lease_id": leaseID}); err != nil {
		return err
	}
	if err = appendCoordinationEventTx(ctx, tx, "lease_released", &projectID, "lease", leaseID, &epoch, map[string]any{"execution_id": executionID, "attempt_id": attemptID}); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) MarkExecutorUnknown(ctx context.Context, executorID string) (int, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT a.id,a.execution_id,a.coordination_epoch,l.id FROM attempts a JOIN leases l ON l.attempt_id=a.id WHERE a.executor_id=? AND a.state IN('authorized','running','quiescing') AND l.state IN('prepared','active','releasing','revocation_requested')`, executorID)
	if err != nil {
		return 0, err
	}
	type lost struct {
		attempt, execution, lease string
		epoch                     int64
	}
	var values []lost
	for rows.Next() {
		var value lost
		if err = rows.Scan(&value.attempt, &value.execution, &value.epoch, &value.lease); err != nil {
			rows.Close()
			return 0, err
		}
		values = append(values, value)
	}
	if err = rows.Close(); err != nil {
		return 0, err
	}
	// A daemon can die after reserving or physically preparing resources but
	// before it creates an attempt. Those leases are equally in-doubt and must
	// block allocation until provider/OS reconciliation proves them free.
	orphanRows, err := tx.QueryContext(ctx, `SELECT l.id,l.execution_id,l.coordination_epoch
		FROM leases l WHERE l.executor_id=? AND l.attempt_id IS NULL
		AND l.state IN('reserved','prepared','revocation_requested')`, executorID)
	if err != nil {
		return 0, err
	}
	type orphan struct {
		lease, execution string
		epoch            int64
	}
	var orphans []orphan
	for orphanRows.Next() {
		var value orphan
		if err = orphanRows.Scan(&value.lease, &value.execution, &value.epoch); err != nil {
			orphanRows.Close()
			return 0, err
		}
		orphans = append(orphans, value)
	}
	if err = orphanRows.Close(); err != nil {
		return 0, err
	}
	t := now()
	for _, value := range values {
		if _, err = tx.ExecContext(ctx, `UPDATE attempts SET state='lost',coordination_epoch=coordination_epoch+1 WHERE id=?`, value.attempt); err != nil {
			return 0, err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE leases SET state='stale',stale_at=?,coordination_epoch=coordination_epoch+1 WHERE id=?`, t, value.lease); err != nil {
			return 0, err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE launch_authorizations SET state='revoked',revoked_at=? WHERE attempt_id=? AND state='issued'`, t, value.attempt); err != nil {
			return 0, err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE execution_requests SET state='terminal',terminal_cause='executor_identity_lost',terminal_at=? WHERE id=? AND state!='terminal'`, t, value.execution); err != nil {
			return 0, err
		}
		projectID, projectErr := executionProjectIDTx(ctx, tx, value.execution)
		if projectErr != nil {
			return 0, projectErr
		}
		if err = appendCoordinationEventTx(ctx, tx, "attempt_lost", &projectID, "attempt", value.attempt, &value.epoch, map[string]any{"execution_id": value.execution, "lease_id": value.lease}); err != nil {
			return 0, err
		}
	}
	for _, value := range orphans {
		if _, err = tx.ExecContext(ctx, `UPDATE leases SET state='stale',stale_at=?,coordination_epoch=coordination_epoch+1 WHERE id=?`, t, value.lease); err != nil {
			return 0, err
		}
		projectID, projectErr := executionProjectIDTx(ctx, tx, value.execution)
		if projectErr != nil {
			return 0, projectErr
		}
		if err = appendCoordinationEventTx(ctx, tx, "lease_stale", &projectID, "lease", value.lease, &value.epoch, map[string]any{"execution_id": value.execution, "cause": "executor_identity_lost_before_authorization"}); err != nil {
			return 0, err
		}
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return len(values) + len(orphans), nil
}
