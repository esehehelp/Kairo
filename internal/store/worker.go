package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"kairo/internal/id"
)

func (s *Store) HeartbeatV1(ctx context.Context, attemptID, leaseID string, epoch int64, progress json.RawMessage) error {
	if len(progress) == 0 {
		progress = json.RawMessage(`{}`)
	}
	if !json.Valid(progress) {
		return errors.New("progress must be valid JSON")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = validateEpochTx(ctx, tx, attemptID, leaseID, epoch); err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx, `UPDATE attempts SET last_heartbeat_at=?,progress_json=? WHERE id=? AND state IN('running','suspend_requested')`, now(), string(progress), attemptID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return errors.New("attempt is not running")
	}
	return tx.Commit()
}

func (s *Store) PollCommandsV1(ctx context.Context, attemptID, leaseID string, epoch int64) ([]Command, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if err = validateEpochTx(ctx, tx, attemptID, leaseID, epoch); err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT id,COALESCE(task_id,''),attempt_id,kind,reason,state,delivery_count,created_at,payload_json FROM commands WHERE attempt_id=? AND coordination_epoch=? AND state NOT IN('completed','rejected') ORDER BY created_at`, attemptID, epoch)
	if err != nil {
		return nil, err
	}
	var commands []Command
	for rows.Next() {
		var c Command
		var taskID, payload string
		if err := rows.Scan(&c.ID, &taskID, &c.AttemptID, &c.Kind, &c.Reason, &c.State, &c.DeliveryCount, &c.CreatedAt, &payload); err != nil {
			rows.Close()
			return nil, err
		}
		c.WorkloadID = taskID
		c.Payload = json.RawMessage(payload)
		c.DeliveryCount++
		commands = append(commands, c)
	}
	rows.Close()
	t := now()
	for _, c := range commands {
		if _, err = tx.ExecContext(ctx, `UPDATE commands SET delivery_count=delivery_count+1,updated_at=? WHERE id=?`, t, c.ID); err != nil {
			return nil, err
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO command_deliveries(command_id,attempt_id,coordination_epoch,delivered_at) VALUES(?,?,?,?)`, c.ID, attemptID, epoch, t); err != nil {
			return nil, err
		}
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return commands, nil
}

func (s *Store) AckCommandV1(ctx context.Context, attemptID, leaseID string, epoch int64, commandID, phase string, payload json.RawMessage) error {
	state, ok := ackState(phase)
	if !ok {
		return fmt.Errorf("unsupported acknowledgement phase %q", phase)
	}
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	if !json.Valid(payload) {
		return errors.New("ack payload must be valid JSON")
	}
	var continuationRef string
	if phase == "checkpointed" {
		var body struct {
			ContinuationRef string `json:"continuation_ref"`
		}
		if err := json.Unmarshal(payload, &body); err != nil {
			return err
		}
		continuationRef = strings.TrimSpace(body.ContinuationRef)
		if continuationRef == "" {
			return errors.New("checkpointed acknowledgement requires continuation_ref")
		}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = validateEpochTx(ctx, tx, attemptID, leaseID, epoch); err != nil {
		return err
	}
	var current, attemptState, leaseState string
	var published sql.NullString
	if err = tx.QueryRowContext(ctx, `SELECT c.state,a.state,l.state,a.continuation_ref FROM commands c JOIN attempts a ON a.id=c.attempt_id JOIN leases l ON l.id=c.lease_id WHERE c.id=? AND c.attempt_id=? AND c.coordination_epoch=?`, commandID, attemptID, epoch).Scan(&current, &attemptState, &leaseState, &published); errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	} else if err != nil {
		return err
	}
	if (attemptState != "running" && attemptState != "suspend_requested") || (leaseState != "active" && leaseState != "releasing") {
		return errors.New("attempt is no longer active")
	}
	if phase == "checkpointed" && commandStateRank(current) >= commandStateRank("checkpointed") && published.Valid && published.String != continuationRef {
		return fmt.Errorf("checkpoint continuation is already published as %q", published.String)
	}
	t := now()
	if _, err = tx.ExecContext(ctx, `INSERT INTO command_acks(command_id,attempt_id,coordination_epoch,phase,payload_json,created_at) VALUES(?,?,?,?,?,?)`, commandID, attemptID, epoch, phase, string(payload), t); err != nil {
		return err
	}
	advance := commandStateRank(state) > commandStateRank(current) && current != "completed" && current != "rejected"
	if state == "rejected" && commandStateRank(current) >= commandStateRank("checkpointed") {
		advance = false
	}
	if advance {
		if _, err = tx.ExecContext(ctx, `UPDATE commands SET state=?,updated_at=? WHERE id=?`, state, t, commandID); err != nil {
			return err
		}
	}
	if phase == "rejected" && advance {
		if _, err = tx.ExecContext(ctx, `UPDATE attempts SET state='running' WHERE id=? AND state='suspend_requested'`, attemptID); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE leases SET state='active',releasing_at=NULL WHERE id=? AND state='releasing'`, leaseID); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE task_revisions SET scheduling_state='running' WHERE task_id=(SELECT task_id FROM attempts WHERE id=?) AND revision=(SELECT task_revision FROM attempts WHERE id=?) AND scheduling_state='preempting'`, attemptID, attemptID); err != nil {
			return err
		}
	}
	if phase == "checkpointed" && advance {
		if _, err = tx.ExecContext(ctx, `UPDATE attempts SET checkpointed_at=?,continuation_ref=? WHERE id=?`, t, continuationRef, attemptID); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) TerminalV1(ctx context.Context, attemptID, leaseID string, epoch int64, exitCode int, failureClass string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = validateEpochTx(ctx, tx, attemptID, leaseID, epoch); err != nil {
		return err
	}
	var taskID string
	var revision int
	var state string
	var checkpointed, continuation sql.NullString
	if err = tx.QueryRowContext(ctx, `SELECT task_id,task_revision,state,checkpointed_at,continuation_ref FROM attempts WHERE id=?`, attemptID).Scan(&taskID, &revision, &state, &checkpointed, &continuation); err != nil {
		return err
	}
	if state == "quiesced" {
		return tx.Commit()
	}
	if state == "lost" {
		return fmt.Errorf("attempt is lost and requires reconciliation")
	}
	if failureClass == "" && exitCode != 0 {
		failureClass = "exit_nonzero"
	}
	t := now()
	if _, err = tx.ExecContext(ctx, `UPDATE attempts SET state='quiesced',exit_code=?,failure_class=?,exited_at=?,quiesced_at=? WHERE id=?`, exitCode, nullable(failureClass), t, t, attemptID); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE attempt_processes SET exited_at=?,last_seen_at=? WHERE attempt_id=? AND exited_at IS NULL`, t, t, attemptID); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE leases SET state='released',releasing_at=COALESCE(releasing_at,?),released_at=? WHERE id=? AND state IN('prepared','active','releasing')`, t, t, leaseID); err != nil {
		return err
	}
	var suspendCommandID, cancelCommandID sql.NullString
	_ = tx.QueryRowContext(ctx, `SELECT id FROM commands WHERE attempt_id=? AND kind='suspend' AND state!='rejected' ORDER BY created_at DESC LIMIT 1`, attemptID).Scan(&suspendCommandID)
	_ = tx.QueryRowContext(ctx, `SELECT id FROM commands WHERE attempt_id=? AND kind='cancel' AND state!='rejected' ORDER BY created_at DESC LIMIT 1`, attemptID).Scan(&cancelCommandID)
	next := "failed"
	preempted := suspendCommandID.Valid && checkpointed.Valid && !cancelCommandID.Valid
	if cancelCommandID.Valid {
		next = "cancelled"
		if _, err = tx.ExecContext(ctx, `UPDATE commands SET state='completed',updated_at=?,completed_at=? WHERE attempt_id=? AND state NOT IN('completed','rejected')`, t, t, attemptID); err != nil {
			return err
		}
	} else if preempted {
		next = "pending"
		if _, err = tx.ExecContext(ctx, `UPDATE commands SET state='completed',updated_at=?,completed_at=? WHERE id=?`, t, t, suspendCommandID.String); err != nil {
			return err
		}
	} else if exitCode == 0 {
		next = "succeeded"
	} else {
		retry, delay, err := retryDecision(ctx, tx, taskID, revision, failureClass)
		if err != nil {
			return err
		}
		if retry {
			next = "backoff"
			nextAt := time.Now().UTC().Add(delay).Format(time.RFC3339Nano)
			if _, err = tx.ExecContext(ctx, `UPDATE task_revisions SET next_retry_at=? WHERE task_id=? AND revision=?`, nextAt, taskID, revision); err != nil {
				return err
			}
		}
		if _, err = tx.ExecContext(ctx, `UPDATE attempts SET failure_counted=1 WHERE id=?`, attemptID); err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(ctx, `UPDATE task_revisions SET scheduling_state=? WHERE task_id=? AND revision=?`, next, taskID, revision); err != nil {
		return err
	}
	var current int
	var desired string
	if err = tx.QueryRowContext(ctx, `SELECT current_revision,desired_state FROM tasks WHERE id=?`, taskID).Scan(&current, &desired); err != nil {
		return err
	}
	if current == revision {
		summary := next
		if desired == "cancelled" {
			summary = "cancelled"
		}
		if _, err = tx.ExecContext(ctx, `UPDATE tasks SET scheduling_state=?,updated_at=? WHERE id=?`, summary, t, taskID); err != nil {
			return err
		}
	}
	if err = appendEvent(ctx, tx, "attempt_terminal", "attempt", attemptID, &epoch, map[string]any{"exit_code": exitCode, "failure_class": failureClass, "preempted": preempted}); err != nil {
		return err
	}
	return tx.Commit()
}

func retryDecision(ctx context.Context, tx *sql.Tx, taskID string, revision int, class string) (bool, time.Duration, error) {
	var retryJSON string
	if err := tx.QueryRowContext(ctx, `SELECT effective_retry_json FROM tasks WHERE id=?`, taskID).Scan(&retryJSON); err != nil {
		return false, 0, err
	}
	var p struct {
		Max     int      `json:"max_failure_attempts"`
		On      []string `json:"retry_on"`
		Initial int      `json:"initial_backoff_seconds"`
		Maximum int      `json:"max_backoff_seconds"`
	}
	if err := json.Unmarshal([]byte(retryJSON), &p); err != nil {
		return false, 0, err
	}
	allowed := false
	for _, v := range p.On {
		if v == class {
			allowed = true
		}
	}
	var failures int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM attempts WHERE task_id=? AND task_revision=? AND failure_counted=1`, taskID, revision).Scan(&failures); err != nil {
		return false, 0, err
	}
	failures++ // current attempt is counted in the same transaction after this decision.
	if !allowed || failures >= p.Max {
		return false, 0, nil
	}
	seconds := float64(p.Initial) * math.Pow(2, float64(failures-1))
	if seconds > float64(p.Maximum) {
		seconds = float64(p.Maximum)
	}
	return true, time.Duration(seconds) * time.Second, nil
}

func (s *Store) WakeRetries(ctx context.Context) error {
	t := now()
	_, err := s.db.ExecContext(ctx, `UPDATE task_revisions SET scheduling_state='pending',became_runnable_at=?,next_retry_at=NULL WHERE scheduling_state='backoff' AND next_retry_at<=?`, t, t)
	return err
}

func (s *Store) EnqueueCommandV1(ctx context.Context, attemptID, kind, reason string) (string, error) {
	if kind != "suspend" && kind != "cancel" {
		return "", errors.New("unsupported command")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	var taskID, leaseID string
	var epoch int64
	if err = tx.QueryRowContext(ctx, `SELECT a.task_id,l.id,a.coordination_epoch FROM attempts a JOIN leases l ON l.attempt_id=a.id WHERE a.id=? AND a.state IN('running','suspend_requested')`, attemptID).Scan(&taskID, &leaseID, &epoch); errors.Is(err, sql.ErrNoRows) {
		return "", ErrNotFound
	} else if err != nil {
		return "", err
	}
	var existing string
	if err = tx.QueryRowContext(ctx, `SELECT id FROM commands WHERE attempt_id=? AND kind=? AND state NOT IN('completed','rejected') LIMIT 1`, attemptID, kind).Scan(&existing); err == nil {
		return existing, tx.Commit()
	} else if !errors.Is(err, sql.ErrNoRows) {
		return "", err
	}
	commandID, t := id.New("cmd"), now()
	if _, err = tx.ExecContext(ctx, `INSERT INTO commands(id,task_id,attempt_id,lease_id,coordination_epoch,kind,reason,state,created_at,updated_at) VALUES(?,?,?,?,?,?,?,'pending',?,?)`, commandID, taskID, attemptID, leaseID, epoch, kind, reason, t, t); err != nil {
		return "", err
	}
	if kind == "suspend" {
		if _, err = tx.ExecContext(ctx, `UPDATE attempts SET state='suspend_requested' WHERE id=?`, attemptID); err != nil {
			return "", err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE leases SET state='releasing',releasing_at=? WHERE id=? AND state='active'`, t, leaseID); err != nil {
			return "", err
		}
		_, _ = tx.ExecContext(ctx, `UPDATE task_revisions SET scheduling_state='preempting' WHERE task_id=? AND revision=(SELECT task_revision FROM attempts WHERE id=?)`, taskID, attemptID)
	}
	if err = tx.Commit(); err != nil {
		return "", err
	}
	return commandID, nil
}

func (s *Store) SetAttemptLogPaths(ctx context.Context, attemptID string, epoch int64, stdoutPath, stderrPath string) error {
	res, err := s.db.ExecContext(ctx, `UPDATE attempts SET stdout_path=?,stderr_path=? WHERE id=? AND coordination_epoch=?`, stdoutPath, stderrPath, attemptID, epoch)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return ErrStaleEpoch
	}
	return nil
}

// MarkExecutorUnknown converts restart uncertainty into a coordination fence.
// The stale lease continues to exclude its resources until reconciliation.
func (s *Store) MarkExecutorUnknown(ctx context.Context, executorID string) (int, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	t := now()
	res, err := tx.ExecContext(ctx, `UPDATE leases SET state='stale',stale_at=?,coordination_epoch=coordination_epoch+1 WHERE executor_id=? AND state IN('reserved','prepared','active','releasing')`, t, executorID)
	if err != nil {
		return 0, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE attempts SET state='lost',failure_class='lost_after_reconcile',coordination_epoch=coordination_epoch+1 WHERE executor_id=? AND state IN('authorized','running','suspend_requested')`, executorID); err != nil {
		return 0, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE task_revisions SET scheduling_state='blocked' WHERE (task_id,revision) IN(SELECT task_id,task_revision FROM attempts WHERE executor_id=? AND state='lost')`, executorID); err != nil {
		return 0, err
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}
