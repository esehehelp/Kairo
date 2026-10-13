package store

// Read views of declared tasks: a project's status (optionally filtered) and
// the detail of one task. A view costs a fixed number of queries whatever the
// number of tasks: filters are applied in SQL, and dependencies and executions
// are read for every selected task at once.

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

// TaskStates are the states a declared task can be in.
var TaskStates = []string{"pending", "running", "succeeded", "failed", "blocked", "stopped", "cancelled"}

// TaskFilter selects the tasks of a project status; every set field must
// match. The zero value selects every task.
type TaskFilter struct {
	Names  []string  // exact task names
	Prefix string    // task name prefix
	States []string  // task states
	Live   bool      // only pending or running tasks
	Since  time.Time // only tasks updated at or after this time
}

func (f TaskFilter) validate() error {
	for _, state := range f.States {
		if !stringIn(state, TaskStates...) {
			return fmt.Errorf("unknown task state %q (want one of %s)", state, strings.Join(TaskStates, ", "))
		}
	}
	return nil
}

// where returns the conditions of the filter on orchestration_tasks alias o.
func (f TaskFilter) where() (string, []any) {
	var conditions []string
	var args []any
	in := func(column string, values []string) {
		conditions = append(conditions, column+` IN(`+strings.TrimSuffix(strings.Repeat("?,", len(values)), ",")+`)`)
		for _, v := range values {
			args = append(args, v)
		}
	}
	if len(f.Names) > 0 {
		in("o.name", f.Names)
	}
	if f.Prefix != "" {
		conditions = append(conditions, `substr(o.name,1,?)=?`)
		args = append(args, utf8.RuneCountInString(f.Prefix), f.Prefix)
	}
	if len(f.States) > 0 {
		in("o.state", f.States)
	}
	if f.Live {
		conditions = append(conditions, `o.state IN('pending','running')`)
	}
	if !f.Since.IsZero() {
		conditions = append(conditions, `julianday(o.updated_at)>=julianday(?)`)
		args = append(args, f.Since.UTC().Format(time.RFC3339Nano))
	}
	if len(conditions) == 0 {
		return "", nil
	}
	return ` AND ` + strings.Join(conditions, ` AND `), args
}

// taskExecutionColumns reads a TaskExecution (scanTaskExecution) from
// orchestration_task_executions te, execution_requests e and attempts a.
const taskExecutionColumns = `te.execution_ordinal,te.execution_id,te.reason,e.state,e.terminal_cause,a.exit_code,a.exit_signal,a.id,
 COALESCE(a.executor_id,(SELECT l.executor_id FROM leases l WHERE l.execution_id=e.id ORDER BY l.created_at DESC LIMIT 1)),
 e.submitted_at,e.started_at,e.terminal_at,a.started_at,a.exited_at,
 CASE WHEN NOT json_valid(a.progress_json) THEN NULL WHEN json_type(a.progress_json)='object' THEN json_remove(a.progress_json,'$.detail') ELSE a.progress_json END`

func scanTaskExecution(rows *sql.Rows, extra ...any) (TaskExecution, error) {
	var v TaskExecution
	var progress sql.NullString
	dest := append(extra, &v.Ordinal, &v.ExecutionID, &v.Reason, &v.State, &v.TerminalCause, &v.ExitCode, &v.ExitSignal, &v.AttemptID, &v.ExecutorID, &v.SubmittedAt, &v.StartedAt, &v.TerminalAt, &v.AttemptStartedAt, &v.ExitedAt, &progress)
	if err := rows.Scan(dest...); err != nil {
		return TaskExecution{}, err
	}
	if progress.Valid && progress.String != "{}" {
		v.Progress = json.RawMessage(progress.String)
	}
	return v, nil
}

func (s *Store) GetProjectStatus(ctx context.Context, project string) (ProjectStatus, error) {
	return s.QueryProjectStatus(ctx, project, TaskFilter{})
}

// QueryProjectStatus is a project's status with only the tasks the filter
// selects; StateCounts still counts every task.
func (s *Store) QueryProjectStatus(ctx context.Context, project string, filter TaskFilter) (ProjectStatus, error) {
	if err := filter.validate(); err != nil {
		return ProjectStatus{}, err
	}
	scopes, err := s.GetScopeByPath(ctx, ScopePath{Project: project})
	if err != nil {
		return ProjectStatus{}, err
	}
	projectID := scopes.Project.ID
	status := ProjectStatus{Name: project, SpecDigests: []string{}, StateCounts: map[string]int{}, Tasks: []TaskStatus{}}
	rows, err := s.db.QueryContext(ctx, `SELECT spec_digest FROM project_declarations WHERE project_scope_id=? ORDER BY applied_at,spec_digest`, projectID)
	if err != nil {
		return ProjectStatus{}, err
	}
	for rows.Next() {
		var digest string
		if err := rows.Scan(&digest); err != nil {
			rows.Close()
			return ProjectStatus{}, err
		}
		status.SpecDigests = append(status.SpecDigests, digest)
	}
	if err := rows.Close(); err != nil {
		return ProjectStatus{}, err
	}
	if len(status.SpecDigests) == 0 {
		return ProjectStatus{}, ErrNotFound
	}
	if rows, err = s.db.QueryContext(ctx, `SELECT state,COUNT(*) FROM orchestration_tasks WHERE project_scope_id=? GROUP BY state`, projectID); err != nil {
		return ProjectStatus{}, err
	}
	for rows.Next() {
		var state string
		var n int
		if err := rows.Scan(&state, &n); err != nil {
			rows.Close()
			return ProjectStatus{}, err
		}
		status.StateCounts[state] = n
	}
	if err := rows.Close(); err != nil {
		return ProjectStatus{}, err
	}

	where, args := filter.where()
	args = append([]any{projectID}, args...)
	if rows, err = s.db.QueryContext(ctx, `SELECT o.task_scope_id,o.name,o.queue_name,o.policy_id,o.policy_version,o.state,o.spec_digest,o.result_json,o.created_at,o.updated_at FROM orchestration_tasks o WHERE o.project_scope_id=?`+where+` ORDER BY o.name`, args...); err != nil {
		return ProjectStatus{}, err
	}
	index := map[string]int{}
	for rows.Next() {
		var task TaskStatus
		var scopeID, policyID string
		var policyVersion int
		var result sql.NullString
		if err := rows.Scan(&scopeID, &task.Name, &task.Queue, &policyID, &policyVersion, &task.State, &task.SpecDigest, &result, &task.CreatedAt, &task.UpdatedAt); err != nil {
			rows.Close()
			return ProjectStatus{}, err
		}
		task.Policy = fmt.Sprintf("%s@v%d", policyID, policyVersion)
		task.DependsOn, task.Executions = []string{}, []TaskExecution{}
		if result.Valid {
			task.Result = json.RawMessage(result.String)
		}
		index[scopeID] = len(status.Tasks)
		status.Tasks = append(status.Tasks, task)
	}
	if err := rows.Close(); err != nil {
		return ProjectStatus{}, err
	}
	if len(status.Tasks) == 0 {
		return status, nil
	}

	if rows, err = s.db.QueryContext(ctx, `SELECT d.task_scope_id,t.name FROM orchestration_tasks o JOIN orchestration_task_dependencies d ON d.task_scope_id=o.task_scope_id JOIN orchestration_tasks t ON t.task_scope_id=d.dependency_task_scope_id WHERE o.project_scope_id=?`+where+` ORDER BY t.name`, args...); err != nil {
		return ProjectStatus{}, err
	}
	for rows.Next() {
		var scopeID, name string
		if err := rows.Scan(&scopeID, &name); err != nil {
			rows.Close()
			return ProjectStatus{}, err
		}
		if i, ok := index[scopeID]; ok {
			status.Tasks[i].DependsOn = append(status.Tasks[i].DependsOn, name)
		}
	}
	if err := rows.Close(); err != nil {
		return ProjectStatus{}, err
	}

	if rows, err = s.db.QueryContext(ctx, `SELECT te.task_scope_id,`+taskExecutionColumns+` FROM orchestration_tasks o JOIN orchestration_task_executions te ON te.task_scope_id=o.task_scope_id JOIN execution_requests e ON e.id=te.execution_id LEFT JOIN attempts a ON a.execution_id=e.id WHERE o.project_scope_id=?`+where+` ORDER BY te.task_scope_id,te.execution_ordinal`, args...); err != nil {
		return ProjectStatus{}, err
	}
	for rows.Next() {
		var scopeID string
		execution, err := scanTaskExecution(rows, &scopeID)
		if err != nil {
			rows.Close()
			return ProjectStatus{}, err
		}
		if i, ok := index[scopeID]; ok {
			status.Tasks[i].Executions = append(status.Tasks[i].Executions, execution)
		}
	}
	if err := rows.Close(); err != nil {
		return ProjectStatus{}, err
	}
	return status, nil
}

// TaskDetail is one task with its declaration, its neighbours in the DAG, its
// admission gates and every execution with its attempts, gang ranks and what
// it waits for.
type TaskDetail struct {
	TaskStatus
	Spec         json.RawMessage       `json:"spec"`
	Dependencies []TaskRef             `json:"dependencies"`
	Dependents   []TaskRef             `json:"dependents"`
	Executions   []TaskExecutionDetail `json:"executions"`
	Gates        []TaskGate            `json:"gates"`
}

type TaskRef struct {
	Name  string `json:"name"`
	State string `json:"state"`
}

type TaskGate struct {
	Scope string `json:"scope"` // project, queue or task
	State string `json:"state"` // open or closed
}

type TaskExecutionDetail struct {
	TaskExecution
	Attempts []AttemptDetail `json:"attempts"`
	Gang     []TaskGangRank  `json:"gang,omitempty"`
	// Waiting says why an execution that has not started waits, besides its
	// dependencies.
	Waiting []WaitReason `json:"waiting,omitempty"`
}

// AttemptDetail is an attempt with its log paths and node. The API fills in
// the current sizes of the logs.
type AttemptDetail struct {
	AttemptView
	StdoutBytes *int64  `json:"stdout_bytes,omitempty"`
	StderrBytes *int64  `json:"stderr_bytes,omitempty"`
	NodeID      *string `json:"node_id,omitempty"`
}

type TaskGangRank struct {
	Rank        int     `json:"rank"`
	ExecutionID string  `json:"execution_id"`
	State       string  `json:"state"`
	AttemptID   *string `json:"attempt_id,omitempty"`
	ExecutorID  *string `json:"executor_id,omitempty"`
	NodeID      *string `json:"node_id,omitempty"`
}

// WaitReason is an admission block of the execution, or a closed admission
// gate of one of its scopes (reason_code scope_gate_closed).
type WaitReason struct {
	ReasonCode string          `json:"reason_code"`
	Scope      string          `json:"scope,omitempty"`
	Detail     json.RawMessage `json:"detail,omitempty"`
	CreatedAt  string          `json:"created_at,omitempty"`
}

func (s *Store) GetTaskDetail(ctx context.Context, project, name string) (TaskDetail, error) {
	status, err := s.QueryProjectStatus(ctx, project, TaskFilter{Names: []string{name}})
	if err != nil {
		return TaskDetail{}, err
	}
	if len(status.Tasks) != 1 {
		return TaskDetail{}, ErrNotFound
	}
	detail := TaskDetail{TaskStatus: status.Tasks[0], Executions: []TaskExecutionDetail{}}
	var taskScopeID, spec string
	var queueScopeID, projectScopeID string
	if err := s.db.QueryRowContext(ctx, `SELECT t.task_scope_id,t.spec_json,ts.parent_id,t.project_scope_id FROM orchestration_tasks t JOIN coordination_scopes p ON p.id=t.project_scope_id JOIN coordination_scopes ts ON ts.id=t.task_scope_id WHERE p.kind='project' AND p.external_key=? AND t.name=?`, project, name).Scan(&taskScopeID, &spec, &queueScopeID, &projectScopeID); err != nil {
		return TaskDetail{}, err
	}
	detail.Spec = json.RawMessage(spec)
	if detail.Dependencies, err = s.taskRefs(ctx, `SELECT t.name,t.state FROM orchestration_task_dependencies d JOIN orchestration_tasks t ON t.task_scope_id=d.dependency_task_scope_id WHERE d.task_scope_id=? ORDER BY t.name`, taskScopeID); err != nil {
		return TaskDetail{}, err
	}
	if detail.Dependents, err = s.taskRefs(ctx, `SELECT t.name,t.state FROM orchestration_task_dependencies d JOIN orchestration_tasks t ON t.task_scope_id=d.task_scope_id WHERE d.dependency_task_scope_id=? ORDER BY t.name`, taskScopeID); err != nil {
		return TaskDetail{}, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT s.kind,COALESCE(g.state,'open') FROM coordination_scopes s LEFT JOIN admission_gates g ON g.scope_id=s.id WHERE s.id IN(?,?,?) ORDER BY CASE s.kind WHEN 'project' THEN 0 WHEN 'queue' THEN 1 ELSE 2 END`, projectScopeID, queueScopeID, taskScopeID)
	if err != nil {
		return TaskDetail{}, err
	}
	detail.Gates = []TaskGate{}
	for rows.Next() {
		var gate TaskGate
		if err = rows.Scan(&gate.Scope, &gate.State); err != nil {
			rows.Close()
			return TaskDetail{}, err
		}
		detail.Gates = append(detail.Gates, gate)
	}
	if err = rows.Close(); err != nil {
		return TaskDetail{}, err
	}
	for _, execution := range detail.TaskStatus.Executions {
		item := TaskExecutionDetail{TaskExecution: execution}
		if item.Attempts, err = s.attemptDetails(ctx, execution.ExecutionID); err != nil {
			return TaskDetail{}, err
		}
		if item.Gang, err = s.taskGangRanks(ctx, execution.ExecutionID); err != nil {
			return TaskDetail{}, err
		}
		if execution.State == "waiting" || execution.State == "authorized" {
			if item.Waiting, err = s.waitReasons(ctx, execution.ExecutionID); err != nil {
				return TaskDetail{}, err
			}
		}
		detail.Executions = append(detail.Executions, item)
	}
	return detail, nil
}

func (s *Store) taskRefs(ctx context.Context, query, taskScopeID string) ([]TaskRef, error) {
	rows, err := s.db.QueryContext(ctx, query, taskScopeID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []TaskRef{}
	for rows.Next() {
		var ref TaskRef
		if err = rows.Scan(&ref.Name, &ref.State); err != nil {
			return nil, err
		}
		out = append(out, ref)
	}
	return out, rows.Err()
}

func (s *Store) attemptDetails(ctx context.Context, executionID string) ([]AttemptDetail, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT a.id,a.execution_id,a.state,a.executor_id,a.coordination_epoch,a.pid,a.process_identity,a.exit_code,a.exit_signal,a.continuation_ref,a.progress_json,a.started_at,a.last_heartbeat_at,a.checkpointed_at,a.exited_at,a.quiesced_at,a.stdout_path,a.stderr_path,x.node_id
		FROM attempts a LEFT JOIN executors x ON x.id=a.executor_id WHERE a.execution_id=? ORDER BY COALESCE(a.authorized_at,a.started_at),a.id`, executionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []AttemptDetail{}
	for rows.Next() {
		var v AttemptDetail
		var progress sql.NullString
		if err = rows.Scan(&v.ID, &v.ExecutionID, &v.State, &v.ExecutorID, &v.CoordinationEpoch, &v.PID, &v.ProcessIdentity, &v.ExitCode, &v.ExitSignal, &v.ContinuationRef, &progress, &v.StartedAt, &v.LastHeartbeatAt, &v.CheckpointedAt, &v.ExitedAt, &v.QuiescedAt, &v.StdoutPath, &v.StderrPath, &v.NodeID); err != nil {
			return nil, err
		}
		if progress.Valid && json.Valid([]byte(progress.String)) {
			v.Progress = json.RawMessage(progress.String)
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// taskGangRanks lists the ranks of the gang an execution leads (nil if it
// leads none).
func (s *Store) taskGangRanks(ctx context.Context, executionID string) ([]TaskGangRank, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT m.rank,m.execution_id,e.state,a.id,
		COALESCE(a.executor_id,(SELECT l.executor_id FROM leases l WHERE l.execution_id=e.id ORDER BY l.created_at DESC LIMIT 1)),
		(SELECT x.node_id FROM executors x WHERE x.id=COALESCE(a.executor_id,(SELECT l.executor_id FROM leases l WHERE l.execution_id=e.id ORDER BY l.created_at DESC LIMIT 1)))
		FROM execution_gangs g JOIN gang_members m ON m.gang_id=g.id JOIN execution_requests e ON e.id=m.execution_id LEFT JOIN attempts a ON a.execution_id=e.id
		WHERE g.leader_execution_id=? ORDER BY m.rank`, executionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TaskGangRank
	for rows.Next() {
		var r TaskGangRank
		if err = rows.Scan(&r.Rank, &r.ExecutionID, &r.State, &r.AttemptID, &r.ExecutorID, &r.NodeID); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// waitReasons lists the admission blocks of an execution that has not started
// and the closed admission gates of its project, queue and task scopes.
func (s *Store) waitReasons(ctx context.Context, executionID string) ([]WaitReason, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT reason_code,detail_json,created_at FROM admission_blocks WHERE execution_id=? AND cleared_at IS NULL ORDER BY created_at,id`, executionID)
	if err != nil {
		return nil, err
	}
	var out []WaitReason
	for rows.Next() {
		var w WaitReason
		var detail string
		if err = rows.Scan(&w.ReasonCode, &detail, &w.CreatedAt); err != nil {
			rows.Close()
			return nil, err
		}
		if json.Valid([]byte(detail)) {
			w.Detail = json.RawMessage(detail)
		}
		out = append(out, w)
	}
	if err = rows.Close(); err != nil {
		return nil, err
	}
	rows, err = s.db.QueryContext(ctx, `SELECT s.kind FROM execution_requests e JOIN coordination_scopes s ON s.id IN(e.project_scope_id,e.queue_scope_id,e.task_scope_id) JOIN admission_gates g ON g.scope_id=s.id WHERE e.id=? AND g.state='closed' ORDER BY CASE s.kind WHEN 'project' THEN 0 WHEN 'queue' THEN 1 ELSE 2 END`, executionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		w := WaitReason{ReasonCode: "scope_gate_closed"}
		if err = rows.Scan(&w.Scope); err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
