package store

// Task cancel and rerun: the two operator actions on one declared task. Both
// act only through the RunToCompletion@v1 planner's own records (task state,
// planned decisions, the next execution ordinal); neither interprets the
// workflow beyond it.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"kairo/internal/id"
)

// ErrTaskConflict: the task's state (or one of its executions) does not allow
// the requested cancel or rerun.
var ErrTaskConflict = errors.New("task state does not allow this")

// taskConflict is an ErrTaskConflict whose message is only its own text.
type taskConflict struct{ msg string }

func (e taskConflict) Error() string        { return e.msg }
func (e taskConflict) Is(target error) bool { return target == ErrTaskConflict }

type CancelResult struct {
	Task      string   `json:"task"`
	State     string   `json:"state"`
	Withdrawn []string `json:"withdrawn"`
	DryRun    bool     `json:"dry_run"`
}

type RerunResult struct {
	Task      string   `json:"task"`
	State     string   `json:"state"`
	Ordinal   int      `json:"ordinal"`
	Unblocked []string `json:"unblocked"`
}

type taskRow struct {
	scopeID, projectID, project, queue, name, state string
	result                                          sql.NullString
}

func loadTaskTx(ctx context.Context, q queryer, project, name string) (taskRow, error) {
	var t taskRow
	err := q.QueryRowContext(ctx, `SELECT t.task_scope_id,t.project_scope_id,p.external_key,t.queue_name,t.name,t.state,t.result_json FROM orchestration_tasks t JOIN coordination_scopes p ON p.id=t.project_scope_id WHERE p.kind='project' AND p.external_key=? AND t.name=?`, project, name).Scan(&t.scopeID, &t.projectID, &t.project, &t.queue, &t.name, &t.state, &t.result)
	if errors.Is(err, sql.ErrNoRows) {
		return taskRow{}, ErrNotFound
	}
	return t, err
}

type liveExecution struct {
	id, state string
	started   bool
}

// liveTaskExecutionsTx lists the executions of a task's scope that are not
// terminal, every gang rank included.
func liveTaskExecutionsTx(ctx context.Context, tx *sql.Tx, task taskRow) ([]liveExecution, error) {
	rows, err := tx.QueryContext(ctx, `SELECT id,state,started_at IS NOT NULL FROM execution_requests WHERE project_scope_id=? AND task_scope_id=? AND state!='terminal' ORDER BY submitted_at,id`, task.projectID, task.scopeID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []liveExecution
	for rows.Next() {
		var e liveExecution
		if err = rows.Scan(&e.id, &e.state, &e.started); err != nil {
			return nil, err
		}
		e.started = e.started || e.state == "started"
		out = append(out, e)
	}
	return out, rows.Err()
}

// abandonPlannedDecisionsTx makes sure no decision of the task that was planned
// but not submitted is ever submitted (markOrchestrationDecisionSubmitted
// withdraws one whose submission was already under way).
func abandonPlannedDecisionsTx(ctx context.Context, tx *sql.Tx, taskScopeID string) error {
	_, err := tx.ExecContext(ctx, `UPDATE orchestration_decisions SET state='abandoned' WHERE task_scope_id=? AND state='planned'`, taskScopeID)
	return err
}

// withdrawExecutionAndRanksTx withdraws an execution and, for a gang leader,
// every other rank. A rank that already started is left alone.
func withdrawExecutionAndRanksTx(ctx context.Context, tx *sql.Tx, executionID string) ([]string, error) {
	ids := []string{executionID}
	gangID, _, leaderID, err := gangOfExecutionTx(ctx, tx, executionID)
	if err != nil {
		return nil, err
	}
	if gangID != "" && leaderID == executionID {
		members, err := gangMembersTx(ctx, tx, gangID)
		if err != nil {
			return nil, err
		}
		ids = ids[:0]
		for _, m := range members {
			ids = append(ids, m.candidate.execution.ID)
		}
	}
	var withdrawn []string
	for _, executionID := range ids {
		ok, err := withdrawExecutionTx(ctx, tx, executionID)
		if errors.Is(err, ErrExecutionStarted) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if ok {
			withdrawn = append(withdrawn, executionID)
		}
	}
	return withdrawn, nil
}

// CancelTask ends a task that has not started, or whose executions are all
// paused or ended: every execution of it that has not started is withdrawn
// (every gang rank included), no planned execution is submitted any more, and
// the task becomes 'cancelled', which blocks its dependents like a failure. It
// refuses while an execution of the task is started and not terminal. A dry
// run reports what would be withdrawn and changes nothing.
func (s *Store) CancelTask(ctx context.Context, project, name, actor string, dryRun bool) (CancelResult, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return CancelResult{}, err
	}
	defer tx.Rollback()
	task, err := loadTaskTx(ctx, tx, project, name)
	if err != nil {
		return CancelResult{}, err
	}
	out := CancelResult{Task: name, State: "cancelled", Withdrawn: []string{}, DryRun: dryRun}
	switch task.state {
	case "cancelled":
		var previous struct {
			Withdrawn []string `json:"withdrawn"`
		}
		if task.result.Valid && json.Unmarshal([]byte(task.result.String), &previous) == nil && previous.Withdrawn != nil {
			out.Withdrawn = previous.Withdrawn
		}
		return out, nil
	case "succeeded", "failed", "stopped":
		return CancelResult{}, taskConflict{fmt.Sprintf("task %s already ended (%s)", name, task.state)}
	}
	live, err := liveTaskExecutionsTx(ctx, tx, task)
	if err != nil {
		return CancelResult{}, err
	}
	for _, e := range live {
		if e.started {
			return CancelResult{}, taskConflict{fmt.Sprintf("execution %s of %s/%s/%s is running (%s); pause the task first, or end it with task pause --force", e.id, task.project, task.queue, task.name, e.state)}
		}
	}
	for _, e := range live {
		withdrawn, err := withdrawExecutionTx(ctx, tx, e.id)
		if err != nil {
			return CancelResult{}, fmt.Errorf("withdraw %s: %w", e.id, err)
		}
		if withdrawn {
			out.Withdrawn = append(out.Withdrawn, e.id)
		}
	}
	if err = abandonPlannedDecisionsTx(ctx, tx, task.scopeID); err != nil {
		return CancelResult{}, err
	}
	if err = updateTaskState(ctx, tx, task.scopeID, "cancelled", map[string]any{"reason": "cancelled", "actor": actor, "withdrawn": out.Withdrawn}); err != nil {
		return CancelResult{}, err
	}
	if err = appendCoordinationEventTx(ctx, tx, "task_cancelled", &task.projectID, "task", task.scopeID, nil, map[string]any{"task": name, "actor": actor, "withdrawn": out.Withdrawn}); err != nil {
		return CancelResult{}, err
	}
	if dryRun {
		return out, nil
	}
	return out, tx.Commit()
}

// RerunTask runs a failed, stopped, cancelled or blocked task's immutable
// declaration again: the task goes back to pending with a rerun ordinal past
// every execution planned for it so far, and the planner submits a new
// execution at that ordinal once its dependencies have succeeded. Every
// transitive dependent blocked only because a dependency did not succeed goes
// back to pending too (the planner blocks it again if another dependency is
// still unsuccessful). A repeated request ID is answered with the first
// answer and reruns nothing.
func (s *Store) RerunTask(ctx context.Context, project, name, actor, requestID string) (RerunResult, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return RerunResult{}, err
	}
	defer tx.Rollback()
	task, err := loadTaskTx(ctx, tx, project, name)
	if err != nil {
		return RerunResult{}, err
	}
	if requestID != "" {
		var previous string
		err = tx.QueryRowContext(ctx, `SELECT response_json FROM orchestration_task_reruns WHERE task_scope_id=? AND request_id=? AND response_json IS NOT NULL`, task.scopeID, requestID).Scan(&previous)
		if err == nil {
			var out RerunResult
			return out, json.Unmarshal([]byte(previous), &out)
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return RerunResult{}, err
		}
	} else {
		requestID = id.New("rrn")
	}
	switch task.state {
	case "failed", "stopped", "cancelled", "blocked":
	default:
		return RerunResult{}, taskConflict{fmt.Sprintf("task %s is %s; only failed, stopped, cancelled or blocked tasks can be rerun", name, task.state)}
	}
	live, err := liveTaskExecutionsTx(ctx, tx, task)
	if err != nil {
		return RerunResult{}, err
	}
	if len(live) > 0 {
		return RerunResult{}, taskConflict{fmt.Sprintf("execution %s of %s/%s/%s is not terminal (%s); a task is rerun only once all of its executions ended", live[0].id, task.project, task.queue, task.name, live[0].state)}
	}
	ordinal, err := markTaskRerunTx(ctx, tx, task.scopeID, requestID, actor)
	if err != nil {
		return RerunResult{}, err
	}
	out := RerunResult{Task: name, State: "pending", Ordinal: ordinal, Unblocked: []string{}}
	dependents, err := tx.QueryContext(ctx, `WITH RECURSIVE dependents(id) AS (
	 SELECT task_scope_id FROM orchestration_task_dependencies WHERE dependency_task_scope_id=?
	 UNION SELECT d.task_scope_id FROM orchestration_task_dependencies d JOIN dependents ON d.dependency_task_scope_id=dependents.id
	)
	SELECT t.task_scope_id,t.name,EXISTS(SELECT 1 FROM orchestration_decisions o WHERE o.task_scope_id=t.task_scope_id)
	FROM orchestration_tasks t JOIN dependents ON dependents.id=t.task_scope_id
	WHERE t.state='blocked' AND json_valid(t.result_json) AND json_extract(t.result_json,'$.reason')='dependency did not succeed'
	ORDER BY t.name`, task.scopeID)
	if err != nil {
		return RerunResult{}, err
	}
	type dependent struct {
		scopeID, name string
		ran           bool
	}
	var unblock []dependent
	for dependents.Next() {
		var d dependent
		if err = dependents.Scan(&d.scopeID, &d.name, &d.ran); err != nil {
			dependents.Close()
			return RerunResult{}, err
		}
		unblock = append(unblock, d)
	}
	if err = dependents.Close(); err != nil {
		return RerunResult{}, err
	}
	for _, d := range unblock {
		if d.ran {
			// Not reached by the planner today (a dependent is blocked before it
			// runs); a dependent with executions is rerun too, so none of them
			// is read as its outcome.
			if _, err = markTaskRerunTx(ctx, tx, d.scopeID, requestID, actor); err != nil {
				return RerunResult{}, err
			}
		} else if err = updateTaskState(ctx, tx, d.scopeID, "pending", nil); err != nil {
			return RerunResult{}, err
		}
		out.Unblocked = append(out.Unblocked, d.name)
	}
	sort.Strings(out.Unblocked)
	response, err := json.Marshal(out)
	if err != nil {
		return RerunResult{}, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE orchestration_task_reruns SET response_json=? WHERE task_scope_id=? AND execution_ordinal=?`, string(response), task.scopeID, ordinal); err != nil {
		return RerunResult{}, err
	}
	if err = appendCoordinationEventTx(ctx, tx, "task_rerun", &task.projectID, "task", task.scopeID, nil, map[string]any{"task": name, "actor": actor, "request_id": requestID, "ordinal": ordinal, "unblocked": out.Unblocked}); err != nil {
		return RerunResult{}, err
	}
	return out, tx.Commit()
}

// markTaskRerunTx puts a task back to pending with the ordinal its rerun
// execution gets: past every decision ever planned for it, abandoned ones
// included, so the decision is new and nothing earlier is read as its outcome.
func markTaskRerunTx(ctx context.Context, tx *sql.Tx, taskScopeID, requestID, actor string) (int, error) {
	var ordinal int
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(execution_ordinal)+1,0) FROM orchestration_decisions WHERE task_scope_id=?`, taskScopeID).Scan(&ordinal); err != nil {
		return 0, err
	}
	var predecessor sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT execution_id FROM orchestration_task_executions WHERE task_scope_id=? ORDER BY execution_ordinal DESC LIMIT 1`, taskScopeID).Scan(&predecessor)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}
	if err = abandonPlannedDecisionsTx(ctx, tx, taskScopeID); err != nil {
		return 0, err
	}
	stamp := now()
	if _, err = tx.ExecContext(ctx, `UPDATE orchestration_tasks SET state='pending',result_json=NULL,rerun_ordinal=?,updated_at=? WHERE task_scope_id=?`, ordinal, stamp, taskScopeID); err != nil {
		return 0, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO orchestration_task_reruns(task_scope_id,execution_ordinal,request_id,actor,predecessor_execution_id,created_at) VALUES(?,?,?,?,?,?)`, taskScopeID, ordinal, requestID, actor, predecessor, stamp); err != nil {
		return 0, err
	}
	return ordinal, nil
}

// rerunTriggerTx is the trigger of a rerun decision: the request that asked
// for it and the execution it follows.
func rerunTriggerTx(ctx context.Context, tx *sql.Tx, taskScopeID string, ordinal int) (map[string]any, error) {
	var requestID string
	var predecessor sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT request_id,predecessor_execution_id FROM orchestration_task_reruns WHERE task_scope_id=? AND execution_ordinal=?`, taskScopeID, ordinal).Scan(&requestID, &predecessor)
	if errors.Is(err, sql.ErrNoRows) {
		return map[string]any{"rerun": true}, nil
	}
	if err != nil {
		return nil, err
	}
	trigger := map[string]any{"request_id": requestID}
	if predecessor.Valid {
		trigger["predecessor_execution_id"] = predecessor.String
	}
	return trigger, nil
}
