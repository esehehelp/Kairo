package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"kairo/internal/orchestration"
)

type ProjectStatus struct {
	Name        string       `json:"name"`
	SpecDigests []string     `json:"spec_digests"`
	Tasks       []TaskStatus `json:"tasks"`
}

type TaskStatus struct {
	Name       string          `json:"name"`
	Queue      string          `json:"queue"`
	Policy     string          `json:"policy"`
	State      string          `json:"state"`
	SpecDigest string          `json:"spec_digest"`
	DependsOn  []string        `json:"depends_on"`
	Executions []TaskExecution `json:"executions"`
	Result     json.RawMessage `json:"result,omitempty"`
}

type TaskExecution struct {
	Ordinal     int     `json:"ordinal"`
	ExecutionID string  `json:"execution_id"`
	Reason      string  `json:"reason"`
	State       string  `json:"state"`
	ExitCode    *int    `json:"exit_code,omitempty"`
	ExitSignal  *string `json:"exit_signal,omitempty"`
}

type plannedOrchestrationDecision struct {
	DecisionKey string
	TaskScopeID string
	Ordinal     int
	Reason      string
	Spec        ExecutionSpec
}

type currentTaskExecution struct {
	Ordinal         int
	ExecutionID     string
	ExecutionState  string
	TerminalCause   *string
	AttemptID       *string
	AttemptState    *string
	ExitCode        *int
	ExitSignal      *string
	ContinuationRef *string
	LeaseState      *string
}

type orchestrationTaskRow struct {
	taskScopeID   string
	name          string
	queue         string
	policyID      string
	template      string
	state         string
	project       string
	policyVersion int
}

func (s *Store) ApplyProject(ctx context.Context, spec orchestration.Validated) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	projectScope, _, err := ensureScopeTx(ctx, tx, "project", nil, spec.Manifest.Project.Name)
	if err != nil {
		return false, err
	}
	result, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO project_declarations(project_scope_id,spec_digest,spec_json,applied_at) VALUES(?,?,?,?)`, projectScope.ID, spec.Digest, string(spec.Normalized), now())
	if err != nil {
		return false, err
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return false, err
	}

	taskScopes := make(map[string]string, len(spec.Manifest.Tasks))
	for _, task := range spec.Manifest.Tasks {
		scopes, err := ensureScopePathTx(ctx, tx, ScopePath{Project: spec.Manifest.Project.Name, Queue: task.Queue, Task: task.Name})
		if err != nil {
			return false, err
		}
		taskScopes[task.Name] = scopes.Task.ID
		taskBody, err := json.Marshal(task)
		if err != nil {
			return false, err
		}
		executionBody, err := json.Marshal(task.Execution)
		if err != nil {
			return false, err
		}
		var existingDigest, existingScopeID string
		err = tx.QueryRowContext(ctx, `SELECT spec_digest,task_scope_id FROM orchestration_tasks WHERE project_scope_id=? AND name=?`, projectScope.ID, task.Name).Scan(&existingDigest, &existingScopeID)
		if err == nil {
			if existingDigest != spec.TaskDigests[task.Name] || existingScopeID != scopes.Task.ID {
				return false, fmt.Errorf("%w: %s/%s", ErrOrchestrationConflict, spec.Manifest.Project.Name, task.Name)
			}
			continue
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return false, err
		}
		stamp := now()
		if _, err = tx.ExecContext(ctx, `INSERT INTO orchestration_tasks(task_scope_id,project_scope_id,name,queue_name,policy_id,policy_version,spec_digest,spec_json,execution_template_json,state,created_at,updated_at) VALUES(?,?,?,?,?,1,?,?,?,'pending',?,?)`, scopes.Task.ID, projectScope.ID, task.Name, task.Queue, "RunToCompletion", spec.TaskDigests[task.Name], string(taskBody), string(executionBody), stamp, stamp); err != nil {
			return false, err
		}
	}
	for _, task := range spec.Manifest.Tasks {
		for _, dependency := range task.DependsOn {
			if _, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO orchestration_task_dependencies(task_scope_id,dependency_task_scope_id) VALUES(?,?)`, taskScopes[task.Name], taskScopes[dependency]); err != nil {
				return false, err
			}
		}
	}
	if err = tx.Commit(); err != nil {
		return false, err
	}
	return inserted == 0, nil
}

func (s *Store) ReconcileProjectTasks(ctx context.Context) error {
	if err := s.planProjectTasks(ctx); err != nil {
		return err
	}
	decisions, err := s.pendingOrchestrationDecisions(ctx)
	if err != nil {
		return err
	}
	for _, decision := range decisions {
		execution, _, err := s.SubmitExecution(ctx, decision.Spec)
		if err != nil {
			return err
		}
		if err := s.markOrchestrationDecisionSubmitted(ctx, decision, execution.ID); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) planProjectTasks(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT t.task_scope_id,t.project_scope_id,t.name,t.queue_name,t.policy_id,t.policy_version,t.execution_template_json,t.state,p.external_key FROM orchestration_tasks t JOIN coordination_scopes p ON p.id=t.project_scope_id WHERE t.state IN('pending','running') ORDER BY p.external_key,t.name`)
	if err != nil {
		return err
	}
	var tasks []orchestrationTaskRow
	for rows.Next() {
		var task orchestrationTaskRow
		var projectScopeID string
		if err := rows.Scan(&task.taskScopeID, &projectScopeID, &task.name, &task.queue, &task.policyID, &task.policyVersion, &task.template, &task.state, &task.project); err != nil {
			rows.Close()
			return err
		}
		tasks = append(tasks, task)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, task := range tasks {
		if task.policyID != "RunToCompletion" || task.policyVersion != 1 {
			return fmt.Errorf("unsupported journaled policy %s@v%d", task.policyID, task.policyVersion)
		}
		ready, blocked, err := taskDependenciesReady(ctx, tx, task.taskScopeID)
		if err != nil {
			return err
		}
		if blocked {
			if err := updateTaskState(ctx, tx, task.taskScopeID, "blocked", map[string]string{"reason": "dependency did not succeed"}); err != nil {
				return err
			}
			continue
		}
		if !ready {
			continue
		}
		current, err := loadCurrentTaskExecution(ctx, tx, task.taskScopeID)
		if errors.Is(err, sql.ErrNoRows) {
			if err := planTaskExecution(ctx, tx, task, 0, "initial", map[string]any{"dependencies_succeeded": true}, nil); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return err
		}
		if current.ExecutionState != "terminal" {
			if err := updateTaskState(ctx, tx, task.taskScopeID, "running", nil); err != nil {
				return err
			}
			continue
		}
		if current.AttemptID == nil {
			if err := updateTaskState(ctx, tx, task.taskScopeID, "failed", map[string]any{"reason": "execution terminal before attempt", "execution_id": current.ExecutionID, "terminal_cause": current.TerminalCause}); err != nil {
				return err
			}
			continue
		}
		if current.AttemptState != nil && *current.AttemptState == "lost" {
			if err := updateTaskState(ctx, tx, task.taskScopeID, "blocked", map[string]any{"reason": "attempt identity lost", "execution_id": current.ExecutionID, "attempt_id": current.AttemptID}); err != nil {
				return err
			}
			continue
		}
		if current.AttemptState == nil || *current.AttemptState != "quiesced" || current.LeaseState == nil || *current.LeaseState != "released" {
			continue
		}
		commandID, commandOrigin, commandState, err := latestAttemptCommand(ctx, tx, *current.AttemptID)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err == nil && commandState == "checkpointed" && current.ContinuationRef != nil {
			trigger := map[string]any{"predecessor_execution_id": current.ExecutionID, "attempt_id": *current.AttemptID, "command_id": commandID, "origin": commandOrigin, "continuation_ref": *current.ContinuationRef}
			if err := planTaskExecution(ctx, tx, task, current.Ordinal+1, "continuation", trigger, current.ContinuationRef); err != nil {
				return err
			}
			continue
		}
		if current.ExitCode != nil && *current.ExitCode == 0 && (current.ExitSignal == nil || *current.ExitSignal == "") {
			if err := updateTaskState(ctx, tx, task.taskScopeID, "succeeded", map[string]any{"reason": "exit zero", "execution_id": current.ExecutionID, "attempt_id": *current.AttemptID}); err != nil {
				return err
			}
			continue
		}
		if err == nil {
			if err := updateTaskState(ctx, tx, task.taskScopeID, "failed", map[string]any{"reason": "suspend did not publish a continuation", "execution_id": current.ExecutionID, "attempt_id": *current.AttemptID, "command_id": commandID, "origin": commandOrigin, "command_state": commandState}); err != nil {
				return err
			}
			continue
		}
		if err := updateTaskState(ctx, tx, task.taskScopeID, "failed", map[string]any{"reason": "nonzero or signaled exit", "execution_id": current.ExecutionID, "attempt_id": *current.AttemptID, "exit_code": current.ExitCode, "exit_signal": current.ExitSignal}); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func taskDependenciesReady(ctx context.Context, tx *sql.Tx, taskScopeID string) (bool, bool, error) {
	rows, err := tx.QueryContext(ctx, `SELECT t.state FROM orchestration_task_dependencies d JOIN orchestration_tasks t ON t.task_scope_id=d.dependency_task_scope_id WHERE d.task_scope_id=?`, taskScopeID)
	if err != nil {
		return false, false, err
	}
	defer rows.Close()
	ready := true
	for rows.Next() {
		var state string
		if err := rows.Scan(&state); err != nil {
			return false, false, err
		}
		if state == "failed" || state == "blocked" {
			return false, true, nil
		}
		if state != "succeeded" {
			ready = false
		}
	}
	return ready, false, rows.Err()
}

func loadCurrentTaskExecution(ctx context.Context, tx *sql.Tx, taskScopeID string) (currentTaskExecution, error) {
	var value currentTaskExecution
	err := tx.QueryRowContext(ctx, `SELECT te.execution_ordinal,e.id,e.state,e.terminal_cause,a.id,a.state,a.exit_code,a.exit_signal,a.continuation_ref,l.state FROM orchestration_task_executions te JOIN execution_requests e ON e.id=te.execution_id LEFT JOIN attempts a ON a.execution_id=e.id LEFT JOIN leases l ON l.id=(SELECT id FROM leases WHERE execution_id=e.id ORDER BY created_at DESC LIMIT 1) WHERE te.task_scope_id=? ORDER BY te.execution_ordinal DESC LIMIT 1`, taskScopeID).Scan(&value.Ordinal, &value.ExecutionID, &value.ExecutionState, &value.TerminalCause, &value.AttemptID, &value.AttemptState, &value.ExitCode, &value.ExitSignal, &value.ContinuationRef, &value.LeaseState)
	return value, err
}

func latestAttemptCommand(ctx context.Context, tx *sql.Tx, attemptID string) (string, string, string, error) {
	var id, origin, state string
	err := tx.QueryRowContext(ctx, `SELECT id,origin,state FROM commands WHERE attempt_id=? ORDER BY created_at DESC LIMIT 1`, attemptID).Scan(&id, &origin, &state)
	return id, origin, state, err
}

func planTaskExecution(ctx context.Context, tx *sql.Tx, task orchestrationTaskRow, ordinal int, reason string, trigger map[string]any, continuation *string) error {
	triggerBody, err := json.Marshal(trigger)
	if err != nil {
		return err
	}
	decisionKey := orchestrationDecisionKey(task.taskScopeID, ordinal, reason, string(triggerBody))
	var template orchestration.Execution
	if err := json.Unmarshal([]byte(task.template), &template); err != nil {
		return err
	}
	executor, err := json.Marshal(template.Executor)
	if err != nil {
		return err
	}
	spec := ExecutionSpec{
		ClientRequestID:      orchestrationClientRequestID(decisionKey),
		Scope:                ScopePath{Project: task.project, Queue: task.queue, Task: task.name},
		Argv:                 template.Argv,
		CWD:                  template.CWD,
		ExecutorSelector:     executor,
		Priority:             template.Priority,
		Checkpointable:       template.Checkpointable,
		Preemptible:          template.Preemptible,
		InputContinuationRef: continuation,
		Exclusive:            convertExclusive(template.Exclusive),
		Capacity:             convertCapacity(template.Capacity),
	}
	specBody, err := json.Marshal(spec)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO orchestration_decisions(decision_key,task_scope_id,execution_ordinal,reason,trigger_json,execution_spec_json,state,created_at) VALUES(?,?,?,?,?,?,'planned',?)`, decisionKey, task.taskScopeID, ordinal, reason, string(triggerBody), string(specBody), now())
	return err
}

func (s *Store) pendingOrchestrationDecisions(ctx context.Context) ([]plannedOrchestrationDecision, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT decision_key,task_scope_id,execution_ordinal,reason,execution_spec_json FROM orchestration_decisions WHERE state='planned' ORDER BY created_at,decision_key`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var values []plannedOrchestrationDecision
	for rows.Next() {
		var value plannedOrchestrationDecision
		var body string
		if err := rows.Scan(&value.DecisionKey, &value.TaskScopeID, &value.Ordinal, &value.Reason, &body); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(body), &value.Spec); err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, rows.Err()
}

func (s *Store) markOrchestrationDecisionSubmitted(ctx context.Context, decision plannedOrchestrationDecision, executionID string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var state string
	var existingExecution *string
	if err := tx.QueryRowContext(ctx, `SELECT state,execution_id FROM orchestration_decisions WHERE decision_key=?`, decision.DecisionKey).Scan(&state, &existingExecution); err != nil {
		return err
	}
	if state == "submitted" {
		if existingExecution == nil || *existingExecution != executionID {
			return errors.New("orchestration decision submission changed execution ID")
		}
		return tx.Commit()
	}
	stamp := now()
	if _, err := tx.ExecContext(ctx, `UPDATE orchestration_decisions SET state='submitted',execution_id=?,submitted_at=? WHERE decision_key=?`, executionID, stamp, decision.DecisionKey); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO orchestration_task_executions(task_scope_id,execution_ordinal,execution_id,decision_key,reason,created_at) VALUES(?,?,?,?,?,?)`, decision.TaskScopeID, decision.Ordinal, executionID, decision.DecisionKey, decision.Reason, stamp); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE orchestration_tasks SET state='running',result_json=NULL,updated_at=? WHERE task_scope_id=?`, stamp, decision.TaskScopeID); err != nil {
		return err
	}
	return tx.Commit()
}

func updateTaskState(ctx context.Context, tx *sql.Tx, taskScopeID, state string, result any) error {
	var body any
	if result != nil {
		encoded, err := json.Marshal(result)
		if err != nil {
			return err
		}
		body = string(encoded)
	}
	_, err := tx.ExecContext(ctx, `UPDATE orchestration_tasks SET state=?,result_json=?,updated_at=? WHERE task_scope_id=? AND (state!=? OR COALESCE(result_json,'')!=COALESCE(?,''))`, state, body, now(), taskScopeID, state, body)
	return err
}

func (s *Store) GetProjectStatus(ctx context.Context, project string) (ProjectStatus, error) {
	scopes, err := s.GetScopeByPath(ctx, ScopePath{Project: project})
	if err != nil {
		return ProjectStatus{}, err
	}
	status := ProjectStatus{Name: project, SpecDigests: []string{}, Tasks: []TaskStatus{}}
	rows, err := s.db.QueryContext(ctx, `SELECT spec_digest FROM project_declarations WHERE project_scope_id=? ORDER BY applied_at,spec_digest`, scopes.Project.ID)
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
	taskRows, err := s.db.QueryContext(ctx, `SELECT task_scope_id,name,queue_name,policy_id,policy_version,state,spec_digest,result_json FROM orchestration_tasks WHERE project_scope_id=? ORDER BY name`, scopes.Project.ID)
	if err != nil {
		return ProjectStatus{}, err
	}
	type statusRow struct {
		taskScopeID, policyID string
		policyVersion         int
		task                  TaskStatus
		result                *string
	}
	var pending []statusRow
	for taskRows.Next() {
		var row statusRow
		if err := taskRows.Scan(&row.taskScopeID, &row.task.Name, &row.task.Queue, &row.policyID, &row.policyVersion, &row.task.State, &row.task.SpecDigest, &row.result); err != nil {
			taskRows.Close()
			return ProjectStatus{}, err
		}
		pending = append(pending, row)
	}
	if err := taskRows.Close(); err != nil {
		return ProjectStatus{}, err
	}
	for _, row := range pending {
		task := row.task
		task.Policy = fmt.Sprintf("%s@v%d", row.policyID, row.policyVersion)
		task.DependsOn, err = s.taskDependencyNames(ctx, row.taskScopeID)
		if err != nil {
			return ProjectStatus{}, err
		}
		task.Executions, err = s.taskExecutionStatus(ctx, row.taskScopeID)
		if err != nil {
			return ProjectStatus{}, err
		}
		if row.result != nil {
			task.Result = json.RawMessage(*row.result)
		}
		status.Tasks = append(status.Tasks, task)
	}
	return status, nil
}

func (s *Store) taskDependencyNames(ctx context.Context, taskScopeID string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT t.name FROM orchestration_task_dependencies d JOIN orchestration_tasks t ON t.task_scope_id=d.dependency_task_scope_id WHERE d.task_scope_id=? ORDER BY t.name`, taskScopeID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	values := []string{}
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, rows.Err()
}

func (s *Store) taskExecutionStatus(ctx context.Context, taskScopeID string) ([]TaskExecution, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT te.execution_ordinal,te.execution_id,te.reason,e.state,a.exit_code,a.exit_signal FROM orchestration_task_executions te JOIN execution_requests e ON e.id=te.execution_id LEFT JOIN attempts a ON a.execution_id=e.id WHERE te.task_scope_id=? ORDER BY te.execution_ordinal`, taskScopeID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	values := []TaskExecution{}
	for rows.Next() {
		var value TaskExecution
		if err := rows.Scan(&value.Ordinal, &value.ExecutionID, &value.Reason, &value.State, &value.ExitCode, &value.ExitSignal); err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, rows.Err()
}

func orchestrationDecisionKey(taskScopeID string, ordinal int, reason, trigger string) string {
	return "orchestration-v1-decision-" + stableDigest(fmt.Sprintf("%s\x00%d\x00%s\x00%s", taskScopeID, ordinal, reason, trigger))
}

func orchestrationClientRequestID(decisionKey string) string {
	return "orchestration-v1-" + stableDigest(decisionKey)
}

func stableDigest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func convertExclusive(values []orchestration.ExclusiveRequest) []ExclusiveRequest {
	result := make([]ExclusiveRequest, len(values))
	for i, value := range values {
		result[i] = ExclusiveRequest(value)
	}
	return result
}

func convertCapacity(value orchestration.CapacityRequest) CapacityRequest {
	disks := make([]DiskRequest, len(value.Disks))
	for i, disk := range value.Disks {
		disks[i] = DiskRequest(disk)
	}
	return CapacityRequest{CPUMillis: value.CPUMillis, RAMBytes: value.RAMBytes, Strength: value.Strength, Disks: disks}
}
