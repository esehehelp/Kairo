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

// gateCloserColumns reads who closed admission gate g and when: the first
// pause operation at the gate's current generation (NULL for an open gate).
const gateCloserColumns = `(SELECT p.actor FROM pause_operations p WHERE g.state='closed' AND p.scope_id=g.scope_id AND p.scope_generation=g.generation ORDER BY p.created_at,p.id LIMIT 1),
 (SELECT p.created_at FROM pause_operations p WHERE g.state='closed' AND p.scope_id=g.scope_id AND p.scope_generation=g.generation ORDER BY p.created_at,p.id LIMIT 1)`

// latestLeaseJoin joins the latest lease of execution e as l: until an attempt
// runs the execution, the executor holding that lease is the one it waits on.
const latestLeaseJoin = ` LEFT JOIN leases l ON l.id=(SELECT id FROM leases WHERE execution_id=e.id ORDER BY created_at DESC LIMIT 1)`

// taskExecutionColumns reads a TaskExecution (scanTaskExecution) from
// orchestration_task_executions te, execution_requests e, attempts a and the
// latest lease l (latestLeaseJoin).
const taskExecutionColumns = `te.execution_ordinal,te.execution_id,te.reason,e.state,e.terminal_cause,a.exit_code,a.exit_signal,a.id,
 COALESCE(a.executor_id,l.executor_id),
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

// eachRow runs a query and calls scan on each of its rows.
func eachRow(ctx context.Context, q queryer, query string, args []any, scan func(*sql.Rows) error) error {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		if err = scan(rows); err != nil {
			return err
		}
	}
	return rows.Err()
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
	status := ProjectStatus{Name: project, SpecDigests: []string{}, StateCounts: map[string]int{}}
	err = eachRow(ctx, s.db, `SELECT spec_digest FROM project_declarations WHERE project_scope_id=? ORDER BY applied_at,spec_digest`, []any{projectID}, func(rows *sql.Rows) error {
		var digest string
		if err := rows.Scan(&digest); err != nil {
			return err
		}
		status.SpecDigests = append(status.SpecDigests, digest)
		return nil
	})
	if err != nil {
		return ProjectStatus{}, err
	}
	if len(status.SpecDigests) == 0 {
		return ProjectStatus{}, ErrNotFound
	}
	err = eachRow(ctx, s.db, `SELECT state,COUNT(*) FROM orchestration_tasks WHERE project_scope_id=? GROUP BY state`, []any{projectID}, func(rows *sql.Rows) error {
		var state string
		var n int
		if err := rows.Scan(&state, &n); err != nil {
			return err
		}
		status.StateCounts[state] = n
		return nil
	})
	if err != nil {
		return ProjectStatus{}, err
	}
	if status.Tasks, err = s.taskStatuses(ctx, projectID, filter); err != nil {
		return ProjectStatus{}, err
	}
	return status, nil
}

// taskStatuses reads the tasks of a project that the filter selects, with
// their dependencies and executions.
func (s *Store) taskStatuses(ctx context.Context, projectID string, filter TaskFilter) ([]TaskStatus, error) {
	where, args := filter.where()
	args = append([]any{projectID}, args...)
	tasks := []TaskStatus{}
	index := map[string]int{}
	err := eachRow(ctx, s.db, `SELECT o.task_scope_id,o.name,o.queue_name,o.policy_id,o.policy_version,o.state,o.spec_digest,o.result_json,o.created_at,o.updated_at FROM orchestration_tasks o WHERE o.project_scope_id=?`+where+` ORDER BY o.name`, args, func(rows *sql.Rows) error {
		var task TaskStatus
		var scopeID, policyID string
		var policyVersion int
		var result sql.NullString
		if err := rows.Scan(&scopeID, &task.Name, &task.Queue, &policyID, &policyVersion, &task.State, &task.SpecDigest, &result, &task.CreatedAt, &task.UpdatedAt); err != nil {
			return err
		}
		task.Policy = fmt.Sprintf("%s@v%d", policyID, policyVersion)
		task.DependsOn, task.Executions = []string{}, []TaskExecution{}
		if result.Valid {
			task.Result = json.RawMessage(result.String)
		}
		index[scopeID] = len(tasks)
		tasks = append(tasks, task)
		return nil
	})
	if err != nil || len(tasks) == 0 {
		return tasks, err
	}
	err = eachRow(ctx, s.db, `SELECT d.task_scope_id,t.name FROM orchestration_tasks o JOIN orchestration_task_dependencies d ON d.task_scope_id=o.task_scope_id JOIN orchestration_tasks t ON t.task_scope_id=d.dependency_task_scope_id WHERE o.project_scope_id=?`+where+` ORDER BY t.name`, args, func(rows *sql.Rows) error {
		var scopeID, name string
		if err := rows.Scan(&scopeID, &name); err != nil {
			return err
		}
		if i, ok := index[scopeID]; ok {
			tasks[i].DependsOn = append(tasks[i].DependsOn, name)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	// A live task is paused while a gate of one of its scopes (project,
	// queue, task) is closed; the innermost closed one is reported.
	err = eachRow(ctx, s.db, `SELECT o.task_scope_id,s.kind,`+gateCloserColumns+` FROM orchestration_tasks o JOIN coordination_scopes ts ON ts.id=o.task_scope_id
		JOIN coordination_scopes s ON s.id IN(o.project_scope_id,ts.parent_id,o.task_scope_id) JOIN admission_gates g ON g.scope_id=s.id AND g.state='closed'
		WHERE o.project_scope_id=? AND o.state IN('pending','running')`+where+` ORDER BY o.task_scope_id,CASE s.kind WHEN 'project' THEN 0 WHEN 'queue' THEN 1 ELSE 2 END`, args, func(rows *sql.Rows) error {
		var scopeID string
		gate := TaskGate{State: "closed"}
		var closedBy, closedAt sql.NullString
		if err := rows.Scan(&scopeID, &gate.Scope, &closedBy, &closedAt); err != nil {
			return err
		}
		gate.ClosedBy, gate.ClosedAt = closedBy.String, closedAt.String
		if i, ok := index[scopeID]; ok {
			tasks[i].Paused = &gate
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	err = eachRow(ctx, s.db, `SELECT te.task_scope_id,`+taskExecutionColumns+` FROM orchestration_tasks o JOIN orchestration_task_executions te ON te.task_scope_id=o.task_scope_id JOIN execution_requests e ON e.id=te.execution_id LEFT JOIN attempts a ON a.execution_id=e.id`+latestLeaseJoin+` WHERE o.project_scope_id=?`+where+` ORDER BY te.task_scope_id,te.execution_ordinal`, args, func(rows *sql.Rows) error {
		var scopeID string
		execution, err := scanTaskExecution(rows, &scopeID)
		if err != nil {
			return err
		}
		if i, ok := index[scopeID]; ok {
			tasks[i].Executions = append(tasks[i].Executions, execution)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return tasks, nil
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
	// A closed gate: who closed it and when (the pause operation that closed
	// it at its current generation).
	ClosedBy string `json:"closed_by,omitempty"`
	ClosedAt string `json:"closed_at,omitempty"`
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
	// LogProblems are what went wrong with the attempt's logs while it ran.
	LogProblems []LogProblem `json:"log_problems,omitempty"`
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

// GetTaskDetail reads one task in a fixed number of queries: the attempts,
// gang ranks and admission blocks of all its executions are read at once.
func (s *Store) GetTaskDetail(ctx context.Context, project, name string) (TaskDetail, error) {
	task, err := loadTaskTx(ctx, s.db, project, name)
	if err != nil {
		return TaskDetail{}, err
	}
	tasks, err := s.taskStatuses(ctx, task.projectID, TaskFilter{Names: []string{name}})
	if err != nil {
		return TaskDetail{}, err
	}
	if len(tasks) != 1 {
		return TaskDetail{}, ErrNotFound
	}
	detail := TaskDetail{TaskStatus: tasks[0], Spec: json.RawMessage(task.spec), Executions: []TaskExecutionDetail{}, Gates: []TaskGate{}}
	if detail.Dependencies, err = s.taskRefs(ctx, `SELECT t.name,t.state FROM orchestration_task_dependencies d JOIN orchestration_tasks t ON t.task_scope_id=d.dependency_task_scope_id WHERE d.task_scope_id=? ORDER BY t.name`, task.scopeID); err != nil {
		return TaskDetail{}, err
	}
	if detail.Dependents, err = s.taskRefs(ctx, `SELECT t.name,t.state FROM orchestration_task_dependencies d JOIN orchestration_tasks t ON t.task_scope_id=d.task_scope_id WHERE d.dependency_task_scope_id=? ORDER BY t.name`, task.scopeID); err != nil {
		return TaskDetail{}, err
	}
	err = eachRow(ctx, s.db, `SELECT s.kind,COALESCE(g.state,'open'),`+gateCloserColumns+` FROM coordination_scopes s LEFT JOIN admission_gates g ON g.scope_id=s.id WHERE s.id IN(?,?,?) ORDER BY CASE s.kind WHEN 'project' THEN 0 WHEN 'queue' THEN 1 ELSE 2 END`, []any{task.projectID, task.queueScopeID, task.scopeID}, func(rows *sql.Rows) error {
		var gate TaskGate
		var closedBy, closedAt sql.NullString
		if err := rows.Scan(&gate.Scope, &gate.State, &closedBy, &closedAt); err != nil {
			return err
		}
		gate.ClosedBy, gate.ClosedAt = closedBy.String, closedAt.String
		detail.Gates = append(detail.Gates, gate)
		return nil
	})
	if err != nil {
		return TaskDetail{}, err
	}
	attempts, err := s.taskAttemptDetails(ctx, task.scopeID)
	if err != nil {
		return TaskDetail{}, err
	}
	gangs, err := s.taskGangRanks(ctx, task.scopeID)
	if err != nil {
		return TaskDetail{}, err
	}
	blocks, err := s.taskAdmissionBlocks(ctx, task.scopeID)
	if err != nil {
		return TaskDetail{}, err
	}
	logProblems, err := s.taskLogProblems(ctx, task.scopeID)
	if err != nil {
		return TaskDetail{}, err
	}
	for _, list := range attempts {
		for i := range list {
			list[i].LogProblems = logProblems[list[i].ID]
		}
	}
	// The task's executions are in its project, queue and task scopes: a
	// closed gate of the task holds back each of them that has not started.
	var closed []WaitReason
	for _, gate := range detail.Gates {
		if gate.State == "closed" {
			var who json.RawMessage
			if gate.ClosedBy != "" {
				who, _ = json.Marshal(map[string]string{"closed_by": gate.ClosedBy})
			}
			closed = append(closed, WaitReason{ReasonCode: "scope_gate_closed", Scope: gate.Scope, Detail: who, CreatedAt: gate.ClosedAt})
		}
	}
	for _, execution := range detail.TaskStatus.Executions {
		item := TaskExecutionDetail{TaskExecution: execution, Attempts: attempts[execution.ExecutionID], Gang: gangs[execution.ExecutionID]}
		if item.Attempts == nil {
			item.Attempts = []AttemptDetail{}
		}
		if execution.State == "waiting" || execution.State == "authorized" {
			item.Waiting = append(blocks[execution.ExecutionID], closed...)
		}
		detail.Executions = append(detail.Executions, item)
	}
	return detail, nil
}

func (s *Store) taskRefs(ctx context.Context, query, taskScopeID string) ([]TaskRef, error) {
	out := []TaskRef{}
	err := eachRow(ctx, s.db, query, []any{taskScopeID}, func(rows *sql.Rows) error {
		var ref TaskRef
		if err := rows.Scan(&ref.Name, &ref.State); err != nil {
			return err
		}
		out = append(out, ref)
		return nil
	})
	return out, err
}

// taskAttemptDetails lists the attempts of a task's executions by execution.
func (s *Store) taskAttemptDetails(ctx context.Context, taskScopeID string) (map[string][]AttemptDetail, error) {
	out := map[string][]AttemptDetail{}
	err := eachRow(ctx, s.db, `SELECT x.node_id,`+attemptColumns+` FROM orchestration_task_executions te JOIN attempts a ON a.execution_id=te.execution_id LEFT JOIN executors x ON x.id=a.executor_id WHERE te.task_scope_id=? ORDER BY COALESCE(a.authorized_at,a.started_at),a.id`, []any{taskScopeID}, func(rows *sql.Rows) error {
		var nodeID *string
		v, err := scanAttempt(rows, &nodeID)
		if err != nil {
			return err
		}
		out[v.ExecutionID] = append(out[v.ExecutionID], AttemptDetail{AttemptView: v, NodeID: nodeID})
		return nil
	})
	return out, err
}

// taskGangRanks lists the ranks of the gangs a task's executions lead, by
// leader.
func (s *Store) taskGangRanks(ctx context.Context, taskScopeID string) (map[string][]TaskGangRank, error) {
	out := map[string][]TaskGangRank{}
	err := eachRow(ctx, s.db, `SELECT g.leader_execution_id,m.rank,m.execution_id,e.state,a.id,COALESCE(a.executor_id,l.executor_id),x.node_id
		FROM orchestration_task_executions te JOIN execution_gangs g ON g.leader_execution_id=te.execution_id JOIN gang_members m ON m.gang_id=g.id JOIN execution_requests e ON e.id=m.execution_id LEFT JOIN attempts a ON a.execution_id=e.id`+latestLeaseJoin+`
		LEFT JOIN executors x ON x.id=COALESCE(a.executor_id,l.executor_id) WHERE te.task_scope_id=? ORDER BY m.rank`, []any{taskScopeID}, func(rows *sql.Rows) error {
		var leaderID string
		var r TaskGangRank
		if err := rows.Scan(&leaderID, &r.Rank, &r.ExecutionID, &r.State, &r.AttemptID, &r.ExecutorID, &r.NodeID); err != nil {
			return err
		}
		out[leaderID] = append(out[leaderID], r)
		return nil
	})
	return out, err
}

// taskAdmissionBlocks lists the admission blocks of a task's executions that
// have not started, by execution.
func (s *Store) taskAdmissionBlocks(ctx context.Context, taskScopeID string) (map[string][]WaitReason, error) {
	out := map[string][]WaitReason{}
	err := eachRow(ctx, s.db, `SELECT b.execution_id,b.reason_code,b.detail_json,b.created_at FROM orchestration_task_executions te JOIN execution_requests e ON e.id=te.execution_id JOIN admission_blocks b ON b.execution_id=e.id
		WHERE te.task_scope_id=? AND e.state IN('waiting','authorized') AND b.cleared_at IS NULL ORDER BY b.created_at,b.id`, []any{taskScopeID}, func(rows *sql.Rows) error {
		var executionID, detail string
		var w WaitReason
		if err := rows.Scan(&executionID, &w.ReasonCode, &detail, &w.CreatedAt); err != nil {
			return err
		}
		if json.Valid([]byte(detail)) {
			w.Detail = json.RawMessage(detail)
		}
		out[executionID] = append(out[executionID], w)
		return nil
	})
	return out, err
}
