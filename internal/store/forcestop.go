package store

// Force stop: `pause --force`. A forced pause operation closes the scope's
// admission gate like any pause, but ends the executions it captures instead
// of asking them to checkpoint:
//
//   - an execution that has not started is withdrawn (terminal cause
//     withdrawn_before_start) and recorded as force_stops state 'withdrawn';
//   - a started execution gets a force stop that the executor of its attempt
//     carries out on the attempt's whole process tree: a graceful stop
//     (Windows CTRL_BREAK, Linux/WSL SIGTERM), then after the grace period a
//     kill of every process left. Its execution ends with terminal cause
//     force_stopped. The lease is released only through the ordinary
//     quiescence proof (registered processes absent, fresh provider
//     observation), never because the kill was sent.
//
// A task whose current execution (any rank of a gang) was force stopped or
// withdrawn by a force stop ends in task state 'stopped'; its dependants are
// blocked as for a failed task. No continuation or restart is planned, even
// for a checkpointable task.
//
// Delivery to executors follows the existing termination channel's model: the
// executor polls its orders and acknowledges 'signalled' then 'terminated'; an
// order no executor picks up within ForceStopPickupTimeout blocks the pause
// target with a reason that names why (agent too old, or not reachable).

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// ForceStopPickupTimeout is how long a force stop may stay unacknowledged
// before the pause operation reports it as blocked.
var ForceStopPickupTimeout = 30 * time.Second

// forceSweepWithdrawals withdraws every execution of the operation's scope
// that has not started, recording each as withdrawn by the force stop.
func (s *Store) forceSweepWithdrawals(ctx context.Context, operation PauseOperation) error {
	rows, err := s.db.QueryContext(ctx, `WITH RECURSIVE descendants(id) AS (
	 SELECT ? UNION ALL SELECT s.id FROM coordination_scopes s JOIN descendants d ON s.parent_id=d.id
	)
	SELECT e.id FROM execution_requests e
	WHERE e.state IN('waiting','authorized')
	AND (e.project_scope_id IN (SELECT id FROM descendants) OR e.queue_scope_id IN (SELECT id FROM descendants) OR e.task_scope_id IN (SELECT id FROM descendants))
	ORDER BY e.submitted_at,e.id`, operation.ScopeID)
	if err != nil {
		return err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	if err = rows.Close(); err != nil {
		return err
	}
	for _, executionID := range ids {
		if err := s.forceWithdraw(ctx, operation, executionID); err != nil && !errors.Is(err, ErrExecutionStarted) {
			return fmt.Errorf("withdraw %s: %w", executionID, err)
		}
	}
	return nil
}

func (s *Store) forceWithdraw(ctx context.Context, operation PauseOperation, executionID string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	withdrawn, err := withdrawExecutionTx(ctx, tx, executionID)
	if err != nil {
		return err
	}
	if !withdrawn {
		return tx.Commit()
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO force_stops(execution_id,operation_id,grace_seconds,state,requested_at) VALUES(?,?,?,'withdrawn',?) ON CONFLICT(execution_id) DO NOTHING`, executionID, operation.ID, operation.Force.GraceSeconds, now()); err != nil {
		return err
	}
	projectID, err := executionProjectIDTx(ctx, tx, executionID)
	if err != nil {
		return err
	}
	if err = appendCoordinationEventTx(ctx, tx, "force_stop_withdrew", &projectID, "execution", executionID, nil, map[string]any{"pause_operation_id": operation.ID}); err != nil {
		return err
	}
	return tx.Commit()
}

// requestForceStop records that the attempt's process tree must be stopped by
// its executor. Idempotent: an execution keeps its first force stop.
func (s *Store) requestForceStop(ctx context.Context, operation PauseOperation, executionID, attemptID string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var executorID string
	if err = tx.QueryRowContext(ctx, `SELECT executor_id FROM attempts WHERE id=? AND execution_id=?`, attemptID, executionID).Scan(&executorID); errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	} else if err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx, `INSERT INTO force_stops(execution_id,operation_id,attempt_id,executor_id,grace_seconds,state,requested_at) VALUES(?,?,?,?,?,'pending',?) ON CONFLICT(execution_id) DO NOTHING`, executionID, operation.ID, attemptID, executorID, operation.Force.GraceSeconds, now())
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 1 {
		projectID, err := executionProjectIDTx(ctx, tx, executionID)
		if err != nil {
			return err
		}
		if err = appendCoordinationEventTx(ctx, tx, "force_stop_requested", &projectID, "attempt", attemptID, nil, map[string]any{"execution_id": executionID, "executor_id": executorID, "pause_operation_id": operation.ID, "grace_seconds": operation.Force.GraceSeconds, "reason": operation.Force.Reason}); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// forceStopBlocker reports why the force stop of an execution is not making
// progress: nil while it is, or once it was picked up.
func (s *Store) forceStopBlocker(ctx context.Context, executionID string) (*string, error) {
	var state, requested string
	var lastSeen sql.NullString
	err := s.db.QueryRowContext(ctx, `SELECT f.state,f.requested_at,x.last_seen_at FROM force_stops f LEFT JOIN executors x ON x.id=f.executor_id WHERE f.execution_id=?`, executionID).Scan(&state, &requested, &lastSeen)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil || state != "pending" {
		return nil, err
	}
	requestedAt, err := time.Parse(time.RFC3339Nano, requested)
	if err != nil || time.Since(requestedAt) < ForceStopPickupTimeout {
		return nil, err
	}
	// Every executor version stamps last_seen_at while it polls for quiescence;
	// one that is polling but never picks the order up predates force stop.
	if lastSeen.Valid {
		if seen, err := time.Parse(time.RFC3339Nano, lastSeen.String); err == nil && time.Since(seen) < ForceStopPickupTimeout {
			return stringPointer("agent_lacks_force_stop"), nil
		}
	}
	return stringPointer("executor_unreachable"), nil
}

// touchExecutor records that an executor is polling (at most every few seconds).
func (s *Store) touchExecutor(ctx context.Context, executorID string) error {
	t := time.Now().UTC()
	_, err := s.db.ExecContext(ctx, `UPDATE executors SET last_seen_at=? WHERE id=? AND (last_seen_at IS NULL OR julianday(last_seen_at)<julianday(?))`, t.Format(time.RFC3339Nano), executorID, t.Add(-5*time.Second).Format(time.RFC3339Nano))
	return err
}

// ForceStopOrders returns the force stops executorID still has to carry out.
func (s *Store) ForceStopOrders(ctx context.Context, executorID string) ([]ForceStopOrder, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT f.attempt_id,f.execution_id,a.pid,a.process_identity,f.state,f.grace_seconds,f.signalled_at
		FROM force_stops f JOIN attempts a ON a.id=f.attempt_id
		WHERE f.executor_id=? AND f.state IN('pending','signalled') ORDER BY f.requested_at,f.execution_id`, executorID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]ForceStopOrder, 0)
	for rows.Next() {
		var o ForceStopOrder
		var pid sql.NullInt64
		var identity, signalled sql.NullString
		if err = rows.Scan(&o.AttemptID, &o.ExecutionID, &pid, &identity, &o.State, &o.GraceSeconds, &signalled); err != nil {
			return nil, err
		}
		if pid.Valid {
			v := int(pid.Int64)
			o.PID = &v
		}
		if identity.Valid {
			o.ProcessIdentity = &identity.String
		}
		o.KillAfterSeconds = float64(o.GraceSeconds)
		if signalled.Valid {
			if at, err := time.Parse(time.RFC3339Nano, signalled.String); err == nil {
				o.KillAfterSeconds = max(0, float64(o.GraceSeconds)-time.Since(at).Seconds())
			}
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

// AckForceStop records an executor's progress on a force stop: 'signalled'
// (the graceful stop was sent; the grace period starts now) or 'terminated'
// (no process of the attempt's tree is left). Detail is merged into the
// record. Acknowledgements are idempotent and never move a force stop back.
func (s *Store) AckForceStop(ctx context.Context, attemptID, phase string, detail json.RawMessage) error {
	if phase != "signalled" && phase != "terminated" {
		return errors.New("force stop acknowledgement must be signalled or terminated")
	}
	if len(detail) == 0 || string(detail) == "null" {
		detail = json.RawMessage(`{}`)
	}
	if !json.Valid(detail) {
		return errors.New("force stop detail must be valid JSON")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var executionID string
	if err = tx.QueryRowContext(ctx, `SELECT execution_id FROM force_stops WHERE attempt_id=?`, attemptID).Scan(&executionID); errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	} else if err != nil {
		return err
	}
	t := now()
	var res sql.Result
	if phase == "signalled" {
		res, err = tx.ExecContext(ctx, `UPDATE force_stops SET state='signalled',signalled_at=?,detail_json=json_patch(detail_json,?) WHERE attempt_id=? AND state='pending'`, t, string(detail), attemptID)
	} else {
		res, err = tx.ExecContext(ctx, `UPDATE force_stops SET state='terminated',signalled_at=COALESCE(signalled_at,?),terminated_at=?,detail_json=json_patch(detail_json,?) WHERE attempt_id=? AND state IN('pending','signalled')`, t, t, string(detail), attemptID)
	}
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 1 {
		projectID, err := executionProjectIDTx(ctx, tx, executionID)
		if err != nil {
			return err
		}
		if err = appendCoordinationEventTx(ctx, tx, "force_stop_"+phase, &projectID, "attempt", attemptID, nil, map[string]any{"execution_id": executionID, "detail": detail}); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// ListForceStops returns the force stops of a pause operation.
func (s *Store) ListForceStops(ctx context.Context, operationID string) ([]ForceStop, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT execution_id,operation_id,attempt_id,executor_id,grace_seconds,state,detail_json,requested_at,signalled_at,terminated_at FROM force_stops WHERE operation_id=? ORDER BY execution_id`, operationID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]ForceStop, 0)
	for rows.Next() {
		var f ForceStop
		var detail string
		if err = rows.Scan(&f.ExecutionID, &f.OperationID, &f.AttemptID, &f.ExecutorID, &f.GraceSeconds, &f.State, &detail, &f.RequestedAt, &f.SignalledAt, &f.TerminatedAt); err != nil {
			return nil, err
		}
		f.Detail = json.RawMessage(detail)
		out = append(out, f)
	}
	return out, rows.Err()
}

// forceStoppedTx reports whether a force stop ended the execution or, for a
// gang leader, any rank of its gang: terminal cause force_stopped, or
// withdrawn by a force stop. It returns what the task result records.
func forceStoppedTx(ctx context.Context, tx *sql.Tx, executionID string) (map[string]any, error) {
	var stopped, operationID, actor, reason string
	err := tx.QueryRowContext(ctx, `SELECT e.id,f.operation_id,p.actor,COALESCE(o.reason,'')
		FROM execution_requests e JOIN force_stops f ON f.execution_id=e.id
		JOIN pause_operations p ON p.id=f.operation_id LEFT JOIN force_stop_operations o ON o.operation_id=f.operation_id
		WHERE (e.id=? OR e.id IN(SELECT m.execution_id FROM gang_members m JOIN execution_gangs g ON g.id=m.gang_id WHERE g.leader_execution_id=?))
		AND (e.terminal_cause='force_stopped' OR f.state='withdrawn')
		ORDER BY e.id LIMIT 1`, executionID, executionID).Scan(&stopped, &operationID, &actor, &reason)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return map[string]any{"reason": "force stopped", "execution_id": executionID, "stopped_execution_id": stopped, "pause_operation_id": operationID, "actor": actor, "stop_reason": reason}, nil
}
