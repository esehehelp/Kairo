package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

func (s *Store) ListTasks(ctx context.Context, queueID string) ([]Task, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT t.id,t.queue_id,t.task_key,t.current_revision,t.desired_state,t.scheduling_state,t.effective_priority,t.became_runnable_at,(SELECT reason_code FROM admission_blocks b WHERE b.task_id=t.id AND b.cleared_at IS NULL ORDER BY created_at DESC LIMIT 1) FROM tasks t WHERE (?='' OR t.queue_id=?) ORDER BY t.effective_priority DESC,t.task_key`, queueID, queueID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Task
	for rows.Next() {
		var t Task
		if err := rows.Scan(&t.ID, &t.QueueID, &t.Key, &t.CurrentRevision, &t.DesiredState, &t.SchedulingState, &t.Priority, &t.BecameRunnableAt, &t.BlockReason); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for i := range out {
		if out[i].BlockReason == nil {
			reason, err := s.deriveTaskReason(ctx, out[i])
			if err != nil {
				return nil, err
			}
			out[i].BlockReason = reason
		}
	}
	return out, nil
}
func (s *Store) GetTask(ctx context.Context, taskID string) (Task, error) {
	var t Task
	err := s.db.QueryRowContext(ctx, `SELECT t.id,t.queue_id,t.task_key,t.current_revision,t.desired_state,t.scheduling_state,t.effective_priority,t.became_runnable_at,(SELECT reason_code FROM admission_blocks b WHERE b.task_id=t.id AND b.cleared_at IS NULL ORDER BY created_at DESC LIMIT 1) FROM tasks t WHERE t.id=?`, taskID).Scan(&t.ID, &t.QueueID, &t.Key, &t.CurrentRevision, &t.DesiredState, &t.SchedulingState, &t.Priority, &t.BecameRunnableAt, &t.BlockReason)
	if errors.Is(err, sql.ErrNoRows) {
		err = ErrNotFound
	}
	if err == nil && t.BlockReason == nil {
		t.BlockReason, err = s.deriveTaskReason(ctx, t)
	}
	return t, err
}

func (s *Store) deriveTaskReason(ctx context.Context, t Task) (*string, error) {
	value := func(s string) *string { return &s }
	if t.DesiredState == "paused" {
		return value("task_paused"), nil
	}
	if t.DesiredState == "cancelled" {
		return value("task_cancelled"), nil
	}
	var queueState string
	if err := s.db.QueryRowContext(ctx, `SELECT desired_state FROM queues WHERE id=?`, t.QueueID).Scan(&queueState); err != nil {
		return nil, err
	}
	if queueState == "paused" {
		return value("queue_paused"), nil
	}
	switch t.SchedulingState {
	case "backoff":
		return value("retry_backoff"), nil
	case "preempting":
		return value("preemption_wait"), nil
	case "blocked":
		return value("reconciliation_required"), nil
	}
	if t.SchedulingState != "pending" {
		return nil, nil
	}
	var selector string
	if err := s.db.QueryRowContext(ctx, `SELECT executor_selector_json FROM task_revisions WHERE task_id=? AND revision=?`, t.ID, t.CurrentRevision).Scan(&selector); err != nil {
		return nil, err
	}
	executorRows, err := s.db.QueryContext(ctx, `SELECT attributes_json FROM executors WHERE enabled=1`)
	if err != nil {
		return nil, err
	}
	matched := false
	for executorRows.Next() {
		var attrs string
		if err := executorRows.Scan(&attrs); err != nil {
			executorRows.Close()
			return nil, err
		}
		if selectorMatches(selector, attrs) {
			matched = true
		}
	}
	executorRows.Close()
	if !matched {
		return value("executor_selector_unmatched"), nil
	}
	var dependencies int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM task_dependencies d JOIN task_revisions dep ON dep.task_id=d.dependency_task_id AND dep.revision=d.dependency_revision WHERE d.task_id=? AND d.task_revision=? AND dep.scheduling_state!='succeeded'`, t.ID, t.CurrentRevision).Scan(&dependencies); err != nil {
		return nil, err
	}
	if dependencies > 0 {
		return value("dependency_wait"), nil
	}
	var needed int64
	err = s.db.QueryRowContext(ctx, `SELECT quantity FROM resource_requests WHERE task_id=? AND task_revision=? AND request_type='exclusive' AND kind='gpu' LIMIT 1`, t.ID, t.CurrentRevision).Scan(&needed)
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
	var preemptible, nonpreemptible int
	if err = s.db.QueryRowContext(ctx, `SELECT COALESCE(SUM(CASE WHEN tr.checkpointable=1 THEN 1 ELSE 0 END),0),COALESCE(SUM(CASE WHEN tr.checkpointable=0 THEN 1 ELSE 0 END),0) FROM attempts a JOIN tasks running ON running.id=a.task_id JOIN task_revisions tr ON tr.task_id=a.task_id AND tr.revision=a.task_revision JOIN leases l ON l.attempt_id=a.id WHERE a.state='running' AND l.state='active' AND running.effective_priority<?`, t.Priority).Scan(&preemptible, &nonpreemptible); err != nil {
		return nil, err
	}
	if preemptible == 0 && nonpreemptible > 0 {
		return value("blocked_preemption"), nil
	}
	return value("resource_unavailable"), nil
}
func (s *Store) SetQueueState(ctx context.Context, queueID, state string) error {
	if state != "active" && state != "paused" {
		return errors.New("queue state must be active or paused")
	}
	res, err := s.db.ExecContext(ctx, `UPDATE queues SET desired_state=?,updated_at=? WHERE id=?`, state, now(), queueID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}
func (s *Store) SetTaskState(ctx context.Context, taskID, action string) (string, error) {
	if action != "pause" && action != "resume" && action != "cancel" && action != "retry" {
		return "", errors.New("invalid task action")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	var desired, state string
	var rev int
	if err = tx.QueryRowContext(ctx, `SELECT desired_state,scheduling_state,current_revision FROM tasks WHERE id=?`, taskID).Scan(&desired, &state, &rev); errors.Is(err, sql.ErrNoRows) {
		return "", ErrNotFound
	} else if err != nil {
		return "", err
	}
	if (action == "pause" || action == "cancel") && (state == "running" || state == "preempting") {
		var attemptID string
		var attemptRevision int
		if err = tx.QueryRowContext(ctx, `SELECT id,task_revision FROM attempts WHERE task_id=? AND state IN('running','suspend_requested') ORDER BY authorized_at DESC LIMIT 1`, taskID).Scan(&attemptID, &attemptRevision); err != nil {
			return "", err
		}
		newDesired := "paused"
		if action == "cancel" {
			newDesired = "cancelled"
		}
		if _, err = tx.ExecContext(ctx, `UPDATE tasks SET desired_state=?,updated_at=? WHERE id=?`, newDesired, now(), taskID); err != nil {
			return "", err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE task_revisions SET desired_state=? WHERE task_id=? AND revision IN(?,?)`, newDesired, taskID, rev, attemptRevision); err != nil {
			return "", err
		}
		if err = tx.Commit(); err != nil {
			return "", err
		}
		kind := "suspend"
		if action == "cancel" {
			kind = "cancel"
		}
		return s.EnqueueCommandV1(ctx, attemptID, kind, "manual_"+action)
	}
	t := now()
	switch action {
	case "pause":
		_, err = tx.ExecContext(ctx, `UPDATE tasks SET desired_state='paused',scheduling_state='paused',updated_at=? WHERE id=?`, t, taskID)
		if err == nil {
			_, err = tx.ExecContext(ctx, `UPDATE task_revisions SET desired_state='paused',scheduling_state='paused' WHERE task_id=? AND revision=?`, taskID, rev)
		}
	case "resume":
		if desired != "paused" {
			return "", fmt.Errorf("task is not paused")
		}
		_, err = tx.ExecContext(ctx, `UPDATE tasks SET desired_state='active',scheduling_state='pending',updated_at=? WHERE id=?`, t, taskID)
		if err == nil {
			_, err = tx.ExecContext(ctx, `UPDATE task_revisions SET desired_state='active',scheduling_state='pending',became_runnable_at=NULL WHERE task_id=? AND revision=?`, taskID, rev)
		}
	case "cancel":
		_, err = tx.ExecContext(ctx, `UPDATE tasks SET desired_state='cancelled',scheduling_state='cancelled',updated_at=? WHERE id=?`, t, taskID)
		if err == nil {
			_, err = tx.ExecContext(ctx, `UPDATE task_revisions SET desired_state='cancelled',scheduling_state='cancelled' WHERE task_id=? AND revision=?`, taskID, rev)
		}
	case "retry":
		if state != "failed" && state != "failed_admission" && state != "blocked" {
			return "", fmt.Errorf("task is not retryable from state %s", state)
		}
		_, err = tx.ExecContext(ctx, `UPDATE admission_blocks SET cleared_at=? WHERE task_id=? AND cleared_at IS NULL`, t, taskID)
		if err == nil {
			_, err = tx.ExecContext(ctx, `UPDATE tasks SET desired_state='active',scheduling_state='pending',became_runnable_at=NULL,updated_at=? WHERE id=?`, t, taskID)
		}
		if err == nil {
			_, err = tx.ExecContext(ctx, `UPDATE task_revisions SET desired_state='active',scheduling_state='pending',became_runnable_at=NULL,next_retry_at=NULL WHERE task_id=? AND revision=?`, taskID, rev)
		}
	}
	if err != nil {
		return "", err
	}
	return "", tx.Commit()
}

func (s *Store) Status(ctx context.Context) (StatusSnapshot, error) {
	var out StatusSnapshot
	var err error
	if out.Queues, err = s.ListQueues(ctx); err != nil {
		return out, err
	}
	if out.Tasks, err = s.ListTasks(ctx, ""); err != nil {
		return out, err
	}
	if out.Resources, err = s.ListResourceStatus(ctx); err != nil {
		return out, err
	}
	if out.Claims, err = s.ListClaims(ctx); err != nil {
		return out, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id,attempt_id,task_id,task_revision,executor_id,coordination_epoch,state,created_at,expires_at FROM leases ORDER BY created_at`)
	if err != nil {
		return out, err
	}
	for rows.Next() {
		var l Lease
		if err := rows.Scan(&l.ID, &l.AttemptID, &l.TaskID, &l.TaskRevision, &l.ExecutorID, &l.CoordinationEpoch, &l.State, &l.CreatedAt, &l.ExpiresAt); err != nil {
			rows.Close()
			return out, err
		}
		out.Leases = append(out.Leases, l)
	}
	rows.Close()
	rows, err = s.db.QueryContext(ctx, `SELECT id,task_id,task_revision,ordinal,state,executor_id,coordination_epoch,pid,process_identity,exit_code,continuation_ref,progress_json,started_at,last_heartbeat_at,checkpointed_at,exited_at,quiesced_at FROM attempts WHERE task_id IS NOT NULL ORDER BY authorized_at`)
	if err != nil {
		return out, err
	}
	for rows.Next() {
		var a Attempt
		var progress sql.NullString
		if err := rows.Scan(&a.ID, &a.TaskID, &a.TaskRevision, &a.Ordinal, &a.State, &a.ExecutorID, &a.CoordinationEpoch, &a.PID, &a.ProcessIdentity, &a.ExitCode, &a.ContinuationRef, &progress, &a.StartedAt, &a.LastHeartbeatAt, &a.CheckpointedAt, &a.ExitedAt, &a.QuiescedAt); err != nil {
			rows.Close()
			return out, err
		}
		if progress.Valid {
			a.Progress = []byte(progress.String)
		}
		out.Attempts = append(out.Attempts, a)
	}
	rows.Close()
	return out, rows.Err()
}
