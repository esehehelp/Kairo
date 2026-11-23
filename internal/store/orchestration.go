package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"kairo/internal/orchestration"
)

type ProjectStatus struct {
	Name        string   `json:"name"`
	SpecDigests []string `json:"spec_digests"`
	// StateCounts counts every task of the project by state, whatever filter
	// selected Tasks.
	StateCounts map[string]int `json:"state_counts"`
	Tasks       []TaskStatus   `json:"tasks"`
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
	CreatedAt  string          `json:"created_at"`
	UpdatedAt  string          `json:"updated_at"`
	// Paused is the closed admission gate holding a pending or running task
	// back (the innermost of its project, queue and task gates), nil when all
	// are open. It is a view, not a state: the task stays pending or running.
	Paused *TaskGate `json:"paused,omitempty"`
}

type TaskExecution struct {
	Ordinal     int     `json:"ordinal"`
	ExecutionID string  `json:"execution_id"`
	Reason      string  `json:"reason"`
	State       string  `json:"state"`
	ExitCode    *int    `json:"exit_code,omitempty"`
	ExitSignal  *string `json:"exit_signal,omitempty"`
	// TerminalCause tells how a terminal execution ended: process_exit,
	// force_stopped, withdrawn_before_start, executor_identity_lost, ...
	TerminalCause *string `json:"terminal_cause,omitempty"`
	AttemptID     *string `json:"attempt_id,omitempty"`
	// ExecutorID runs the attempt, or holds the latest lease before one.
	ExecutorID       *string `json:"executor_id,omitempty"`
	SubmittedAt      string  `json:"submitted_at,omitempty"`
	StartedAt        *string `json:"started_at,omitempty"`
	TerminalAt       *string `json:"terminal_at,omitempty"`
	AttemptStartedAt *string `json:"attempt_started_at,omitempty"`
	ExitedAt         *string `json:"exited_at,omitempty"`
	// Progress is the attempt's latest progress without its free-form detail.
	Progress json.RawMessage `json:"progress,omitempty"`
	// AdmissionWait is the reason code admission planning last gave for an
	// execution that waits (admission_waits).
	AdmissionWait *string `json:"admission_wait,omitempty"`
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
	// rerunOrdinal: the task was rerun; its next execution gets this ordinal.
	rerunOrdinal *int
}

// ApplyResult is what applying a ProjectSpec did: Created names the tasks it
// declared, Unchanged counts those already declared with the same content.
type ApplyResult struct {
	Idempotent bool
	Created    []string
	Unchanged  int
}

func (s *Store) ApplyProject(ctx context.Context, spec orchestration.Validated) (ApplyResult, error) {
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return ApplyResult{}, err
	}
	defer tx.Rollback()
	applied, err := applyProjectTx(ctx, tx, spec)
	if err != nil {
		return ApplyResult{}, err
	}
	return applied, tx.Commit()
}

// applyProjectTx is ApplyProject within tx (a hardware bench declares its
// task in the transaction that creates its run).
func applyProjectTx(ctx context.Context, tx *sql.Tx, spec orchestration.Validated) (ApplyResult, error) {
	projectScope, _, err := ensureScopeTx(ctx, tx, "project", nil, spec.Manifest.Project.Name)
	if err != nil {
		return ApplyResult{}, err
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO project_declarations(project_scope_id,spec_digest,spec_json,applied_at) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING`, projectScope.ID, spec.Digest, string(spec.Normalized), now())
	if err != nil {
		return ApplyResult{}, err
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return ApplyResult{}, err
	}
	applied := ApplyResult{Created: []string{}}
	var conflicts []string
	// the snapshot each source was applied with last (kairo task rerun
	// --source current runs in it), whatever the tasks bound to it
	for name, id := range spec.Manifest.Snapshots {
		known, err := sourceKnownTx(ctx, tx, id)
		if err != nil {
			return ApplyResult{}, err
		}
		if !known {
			continue // a task that runs in it is refused below, saying why
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO project_sources(project_scope_id,source_name,snapshot_id,applied_at) VALUES($1,$2,$3,$4)
			ON CONFLICT(project_scope_id,source_name) DO UPDATE SET snapshot_id=excluded.snapshot_id,applied_at=excluded.applied_at`, projectScope.ID, name, id, now()); err != nil {
			return ApplyResult{}, err
		}
	}

	taskScopes := make(map[string]string, len(spec.Manifest.Tasks))
	for _, task := range spec.Manifest.Tasks {
		scopes, err := ensureScopePathTx(ctx, tx, ScopePath{Project: spec.Manifest.Project.Name, Queue: task.Queue, Task: task.Name})
		if err != nil {
			return ApplyResult{}, err
		}
		taskScopes[task.Name] = scopes.Task.ID
		taskBody, err := json.Marshal(task)
		if err != nil {
			return ApplyResult{}, err
		}
		executionBody, err := json.Marshal(task.Execution)
		if err != nil {
			return ApplyResult{}, err
		}
		var existingDigest, existingScopeID string
		err = tx.QueryRowContext(ctx, `SELECT spec_digest,task_scope_id FROM orchestration_tasks WHERE project_scope_id=$1 AND name=$2`, projectScope.ID, task.Name).Scan(&existingDigest, &existingScopeID)
		if err == nil {
			if existingDigest != spec.TaskDigests[task.Name] || existingScopeID != scopes.Task.ID {
				conflicts = append(conflicts, spec.Manifest.Project.Name+"/"+task.Name)
			} else {
				applied.Unchanged++
			}
			continue
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return ApplyResult{}, err
		}
		// bound to the snapshot this apply made of its source, for good
		var snapshot any
		if source := task.Execution.Source; source != "" {
			id := spec.Manifest.Snapshots[source]
			if id == "" {
				return ApplyResult{}, fmt.Errorf("task %q runs in source %q, of which this apply sent no snapshot (apply with a kairo that makes them)", task.Name, source)
			}
			known, err := sourceKnownTx(ctx, tx, id)
			if err != nil {
				return ApplyResult{}, err
			}
			if !known {
				return ApplyResult{}, fmt.Errorf("task %q: snapshot %s of source %q was not sent to the daemon", task.Name, id, source)
			}
			snapshot = id
		}
		// bound to the digest this apply resolved its image to, for good
		var image any
		if name := task.Execution.Image; name != "" {
			pin := spec.Manifest.ImagePins[name]
			if pin == "" {
				return ApplyResult{}, fmt.Errorf("task %q runs in image %q, of which this apply sent no digest (apply with a kairo that resolves them)", task.Name, name)
			}
			image = pin
		}
		stamp := now()
		if _, err = tx.ExecContext(ctx, `INSERT INTO orchestration_tasks(task_scope_id,project_scope_id,name,queue_name,policy_id,policy_version,spec_digest,spec_json,execution_template_json,state,created_at,updated_at,source_snapshot_id,image_ref) VALUES($1,$2,$3,$4,$5,1,$6,$7,$8,'pending',$9,$10,$11,$12)`, scopes.Task.ID, projectScope.ID, task.Name, task.Queue, "RunToCompletion", spec.TaskDigests[task.Name], string(taskBody), string(executionBody), stamp, stamp, snapshot, image); err != nil {
			return ApplyResult{}, err
		}
		applied.Created = append(applied.Created, task.Name)
	}
	if len(conflicts) > 0 {
		// Every conflicting task is named, so the operator sees them all at once.
		return ApplyResult{}, fmt.Errorf("%w: %s", ErrOrchestrationConflict, strings.Join(conflicts, ", "))
	}
	// A dependency the file does not declare is a task the project already
	// has, declared by another file (issue #12): it is depended on as one the
	// file declares (succeeded: satisfied; not: the dependant is blocked).
	var unknown []string
	for _, name := range spec.ExternalDependencies() {
		var scopeID string
		err := tx.QueryRowContext(ctx, `SELECT task_scope_id FROM orchestration_tasks WHERE project_scope_id=$1 AND name=$2`, projectScope.ID, name).Scan(&scopeID)
		if errors.Is(err, sql.ErrNoRows) {
			unknown = append(unknown, name)
			continue
		}
		if err != nil {
			return ApplyResult{}, err
		}
		taskScopes[name] = scopeID
	}
	if len(unknown) > 0 {
		return ApplyResult{}, fmt.Errorf("depends on %s, which neither this file nor an earlier apply of project %s declares", strings.Join(unknown, ", "), spec.Manifest.Project.Name)
	}
	for _, task := range spec.Manifest.Tasks {
		for _, dependency := range task.DependsOn {
			if _, err = tx.ExecContext(ctx, `INSERT INTO orchestration_task_dependencies(task_scope_id,dependency_task_scope_id) VALUES($1,$2) ON CONFLICT DO NOTHING`, taskScopes[task.Name], taskScopes[dependency]); err != nil {
				return ApplyResult{}, err
			}
		}
	}
	applied.Idempotent = inserted == 0
	return applied, nil
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
		if err := s.submitOrchestrationDecision(ctx, decision); err != nil {
			return err
		}
	}
	if err := s.orderInputFetches(ctx); err != nil {
		return err
	}
	return s.orderRegenerations(ctx)
}

func (s *Store) planProjectTasks(ctx context.Context) error {
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT t.task_scope_id,t.project_scope_id,t.name,t.queue_name,t.policy_id,t.policy_version,t.execution_template_json,t.state,p.external_key,t.rerun_ordinal FROM orchestration_tasks t JOIN coordination_scopes p ON p.id=t.project_scope_id WHERE t.state IN('pending','running') ORDER BY p.external_key,t.name`)
	if err != nil {
		return err
	}
	var tasks []orchestrationTaskRow
	for rows.Next() {
		var task orchestrationTaskRow
		var projectScopeID string
		if err := rows.Scan(&task.taskScopeID, &projectScopeID, &task.name, &task.queue, &task.policyID, &task.policyVersion, &task.template, &task.state, &task.project, &task.rerunOrdinal); err != nil {
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
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		// A rerun runs the declaration again from an ordinal past every earlier
		// execution, which is never read as the task's outcome again.
		if task.rerunOrdinal != nil && (err != nil || current.Ordinal < *task.rerunOrdinal) {
			trigger, continuation, err := rerunTriggerTx(ctx, tx, task.taskScopeID, *task.rerunOrdinal)
			if err != nil {
				return err
			}
			if err := planTaskExecution(ctx, tx, task, *task.rerunOrdinal, "rerun", trigger, continuation); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			if err := planTaskExecution(ctx, tx, task, 0, "initial", map[string]any{"dependencies_succeeded": true}, nil); err != nil {
				return err
			}
			continue
		}
		// A gang is read as a whole: terminal only when every rank is, and so on.
		gang, err := gangAggregateTx(ctx, tx, &current)
		if err != nil {
			return err
		}
		if current.ExecutionState != "terminal" {
			if err := updateTaskState(ctx, tx, task.taskScopeID, "running", nil); err != nil {
				return err
			}
			continue
		}
		// A forced pause ended this execution (or a rank of its gang): the task
		// is stopped once nothing of it holds resources. No continuation or
		// restart is planned, whatever the exit or checkpoint.
		stopped, err := forceStoppedTx(ctx, tx, current.ExecutionID)
		if err != nil {
			return err
		}
		if stopped != nil && (current.AttemptState == nil || *current.AttemptState != "lost") {
			if (current.AttemptID == nil || (current.AttemptState != nil && *current.AttemptState == "quiesced")) && (current.LeaseState == nil || *current.LeaseState == "released") {
				if err := updateTaskState(ctx, tx, task.taskScopeID, "stopped", stopped); err != nil {
					return err
				}
			}
			continue
		}
		// Withdrawn before it started by an execution withdraw: the task was
		// cancelled, it did not fail. (A task cancel sets the state itself.)
		if current.AttemptID == nil && current.TerminalCause != nil && *current.TerminalCause == "withdrawn_before_start" {
			if err := updateTaskState(ctx, tx, task.taskScopeID, "cancelled", map[string]any{"reason": "withdrawn before start", "execution_id": current.ExecutionID}); err != nil {
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
		// A lost attempt of a task declared continue_after_loss: once nothing of
		// it can run any more (its lease released: by the daemon after a reboot
		// or a launch cut short, or by kairo resource reconcile), the task goes
		// on from its latest published checkpoint, on the node that published
		// it (the reference is a path there); with none it is blocked as any.
		if !gang.gang && current.TerminalCause != nil && *current.TerminalCause == "executor_identity_lost" {
			var template orchestration.Execution
			if err := json.Unmarshal([]byte(task.template), &template); err != nil {
				return err
			}
			if template.ContinueAfterLoss {
				if (current.AttemptState != nil && *current.AttemptState == "lost") || (current.LeaseState != nil && *current.LeaseState != "released") {
					if err := updateTaskState(ctx, tx, task.taskScopeID, "running", map[string]any{"reason": "attempt identity lost: goes on from its latest checkpoint once its lease is released", "execution_id": current.ExecutionID, "attempt_id": current.AttemptID}); err != nil {
						return err
					}
					continue
				}
				checkpoint, found, err := taskCheckpointTx(ctx, tx, task.taskScopeID, "")
				if err != nil {
					return err
				}
				if found {
					trigger := map[string]any{"predecessor_execution_id": current.ExecutionID, "attempt_id": *current.AttemptID, "origin": "attempt_lost", "command_id": checkpoint.commandID, "continuation_ref": checkpoint.ref, "node": checkpoint.node}
					if err := planTaskExecution(ctx, tx, task, current.Ordinal+1, "loss_continuation", trigger, &checkpoint.ref); err != nil {
						return err
					}
					continue
				}
				if err := updateTaskState(ctx, tx, task.taskScopeID, "blocked", map[string]any{"reason": "attempt identity lost; no checkpoint to continue from", "execution_id": current.ExecutionID, "attempt_id": current.AttemptID}); err != nil {
					return err
				}
				continue
			}
		}
		// lost, or lost and reconciled before this pass saw it (no exit then)
		if (current.AttemptState != nil && *current.AttemptState == "lost") || (current.TerminalCause != nil && *current.TerminalCause == "executor_identity_lost" && current.ExitCode == nil) {
			if err := updateTaskState(ctx, tx, task.taskScopeID, "blocked", map[string]any{"reason": "attempt identity lost", "execution_id": current.ExecutionID, "attempt_id": current.AttemptID}); err != nil {
				return err
			}
			continue
		}
		if current.AttemptState == nil || *current.AttemptState != "quiesced" || current.LeaseState == nil || *current.LeaseState != "released" {
			continue
		}
		succeeded := current.ExitCode != nil && *current.ExitCode == 0 && (current.ExitSignal == nil || *current.ExitSignal == "")
		// A gang rank failed to launch after other ranks had started: the gang
		// never ran as a whole, so run it again from the start.
		if gang.gang && gang.launchAborted && !succeeded {
			trigger := map[string]any{"predecessor_execution_id": current.ExecutionID, "attempt_id": *current.AttemptID, "origin": "gang_launch"}
			if err := planTaskExecution(ctx, tx, task, current.Ordinal+1, "gang_restart", trigger, nil); err != nil {
				return err
			}
			continue
		}
		// A node quarantine terminated this attempt (it could not checkpoint):
		// run the task again from the start instead of failing it. For a gang,
		// any rank terminated by a quarantine counts.
		terminated := gang.quarantined
		if !gang.gang {
			if terminated, err = quarantineTerminatedTx(ctx, tx, *current.AttemptID); err != nil {
				return err
			}
		}
		if terminated && !succeeded {
			trigger := map[string]any{"predecessor_execution_id": current.ExecutionID, "attempt_id": *current.AttemptID, "origin": "node_quarantine"}
			if err := planTaskExecution(ctx, tx, task, current.Ordinal+1, "quarantine_restart", trigger, nil); err != nil {
				return err
			}
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
	rows, err := tx.QueryContext(ctx, `SELECT t.state FROM orchestration_task_dependencies d JOIN orchestration_tasks t ON t.task_scope_id=d.dependency_task_scope_id WHERE d.task_scope_id=$1`, taskScopeID)
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
		if state == "failed" || state == "blocked" || state == "stopped" || state == "cancelled" {
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
	err := tx.QueryRowContext(ctx, `SELECT te.execution_ordinal,e.id,e.state,e.terminal_cause,a.id,a.state,a.exit_code,a.exit_signal,a.continuation_ref,l.state FROM orchestration_task_executions te JOIN execution_requests e ON e.id=te.execution_id LEFT JOIN attempts a ON a.execution_id=e.id`+latestLeaseJoin+` WHERE te.task_scope_id=$1 ORDER BY te.execution_ordinal DESC LIMIT 1`, taskScopeID).Scan(&value.Ordinal, &value.ExecutionID, &value.ExecutionState, &value.TerminalCause, &value.AttemptID, &value.AttemptState, &value.ExitCode, &value.ExitSignal, &value.ContinuationRef, &value.LeaseState)
	return value, err
}

func latestAttemptCommand(ctx context.Context, tx *sql.Tx, attemptID string) (string, string, string, error) {
	var id, origin, state string
	err := tx.QueryRowContext(ctx, `SELECT id,origin,state FROM commands WHERE attempt_id=$1 AND kind='suspend' ORDER BY created_at DESC LIMIT 1`, attemptID).Scan(&id, &origin, &state)
	return id, origin, state, err
}

// taskCheckpoint is a checkpoint a task published: the command acknowledged
// checkpointed with it (a suspend's or a checkpoint command's), its
// reference, and the node of the attempt that published it (a path there).
type taskCheckpoint struct {
	commandID, ref, node string
}

// taskCheckpointTx is the task's latest published checkpoint, or the one of
// command commandID when it names one; found is false when there is none.
func taskCheckpointTx(ctx context.Context, q queryer, taskScopeID, commandID string) (taskCheckpoint, bool, error) {
	query := `SELECT c.id,c.payload_json::jsonb->>'continuation_ref',x.node_id FROM commands c
		JOIN orchestration_task_executions te ON te.execution_id=c.execution_id
		JOIN attempts a ON a.id=c.attempt_id JOIN executors x ON x.id=a.executor_id
		WHERE te.task_scope_id=$1 AND c.state='checkpointed' AND COALESCE(c.payload_json::jsonb->>'continuation_ref','')<>''`
	args := []any{taskScopeID}
	if commandID != "" {
		query += ` AND c.id=$2`
		args = append(args, commandID)
	}
	query += ` ORDER BY c.updated_at::timestamptz DESC,c.id DESC LIMIT 1`
	var cp taskCheckpoint
	err := q.QueryRowContext(ctx, query, args...).Scan(&cp.commandID, &cp.ref, &cp.node)
	if errors.Is(err, sql.ErrNoRows) {
		return taskCheckpoint{}, false, nil
	}
	return cp, err == nil, err
}

func planTaskExecution(ctx context.Context, tx *sql.Tx, task orchestrationTaskRow, ordinal int, reason string, trigger map[string]any, continuation *string) error {
	_, err := planTaskExecutionDecision(ctx, tx, task, ordinal, reason, trigger, continuation)
	return err
}

// planTaskExecutionDecision is planTaskExecution, returning the decision it
// planned (or found already planned).
func planTaskExecutionDecision(ctx context.Context, tx *sql.Tx, task orchestrationTaskRow, ordinal int, reason string, trigger map[string]any, continuation *string) (plannedOrchestrationDecision, error) {
	triggerBody, err := json.Marshal(trigger)
	if err != nil {
		return plannedOrchestrationDecision{}, err
	}
	decisionKey := orchestrationDecisionKey(task.taskScopeID, ordinal, reason, string(triggerBody))
	var template orchestration.Execution
	if err := json.Unmarshal([]byte(task.template), &template); err != nil {
		return plannedOrchestrationDecision{}, err
	}
	spec, err := taskExecutionSpec(ScopePath{Project: task.project, Queue: task.queue, Task: task.name}, template, orchestrationClientRequestID(decisionKey), continuation)
	if err != nil {
		return plannedOrchestrationDecision{}, err
	}
	if spec.SourceSnapshot, err = taskSourceTx(ctx, tx, task.taskScopeID); err != nil {
		return plannedOrchestrationDecision{}, err
	}
	if spec.Image, err = taskImageTx(ctx, tx, task.taskScopeID); err != nil {
		return plannedOrchestrationDecision{}, err
	}
	// a rerun in another snapshot of the task's source (kairo task rerun
	// --source) runs in it; the execution records which
	if id, _ := trigger["source_snapshot_id"].(string); reason == "rerun" && id != "" {
		spec.SourceSnapshot = id
	}
	// a rerun ordered to make temporal data again goes on the node that lacks
	// it, one from a checkpoint on the node of the checkpoint; so does a
	// continuation after a loss (its reference is a path there)
	if reason == "rerun" {
		node, err := rerunNodeTx(ctx, tx, task.taskScopeID, ordinal)
		if err != nil {
			return plannedOrchestrationDecision{}, err
		}
		if node != "" && spec.Gang == nil {
			if err = pinNodeSpec(&spec, node); err != nil {
				return plannedOrchestrationDecision{}, err
			}
		}
	}
	if reason == "loss_continuation" {
		if node, _ := trigger["node"].(string); node != "" && spec.Gang == nil {
			if err = pinNodeSpec(&spec, node); err != nil {
				return plannedOrchestrationDecision{}, err
			}
		}
	}
	if err = pinInputNodeTx(ctx, tx, task.project, &spec); err != nil {
		return plannedOrchestrationDecision{}, err
	}
	specBody, err := json.Marshal(spec)
	if err != nil {
		return plannedOrchestrationDecision{}, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO orchestration_decisions(decision_key,task_scope_id,execution_ordinal,reason,trigger_json,execution_spec_json,state,created_at) VALUES($1,$2,$3,$4,$5,$6,'planned',$7) ON CONFLICT DO NOTHING`, decisionKey, task.taskScopeID, ordinal, reason, string(triggerBody), string(specBody), now())
	return plannedOrchestrationDecision{DecisionKey: decisionKey, TaskScopeID: task.taskScopeID, Ordinal: ordinal, Reason: reason, Spec: spec}, err
}

// taskExecutionSpec is the execution a task's template declares.
func taskExecutionSpec(scope ScopePath, template orchestration.Execution, clientRequestID string, continuation *string) (ExecutionSpec, error) {
	executor, err := json.Marshal(template.Executor)
	if err != nil {
		return ExecutionSpec{}, err
	}
	spec := ExecutionSpec{
		ClientRequestID:      clientRequestID,
		Scope:                scope,
		Argv:                 template.Argv,
		CWD:                  template.CWD,
		ExecutorSelector:     executor,
		Priority:             template.Priority,
		Checkpointable:       template.Checkpointable,
		Preemptible:          template.Preemptible,
		InputContinuationRef: continuation,
		Exclusive:            convertExclusive(template.Exclusive),
		Capacity:             convertCapacity(template.Capacity),
		Env:                  template.Env,
		ScratchTmpdir:        template.ScratchTmpdir,
		Inputs:               template.Inputs,
		Confine:              template.Confine,
		Store:                template.Store,
		Regenerable:          template.Regenerable,
		NotBefore:            string(template.NotBefore),
		FinishBy:             string(template.FinishBy),
	}
	// A continuation goes on from where its run was suspended: it started
	// long ago, so a start_by says nothing of it.
	if continuation == nil {
		spec.StartBy = string(template.StartBy)
	}
	if template.Gang != nil {
		gang := GangSpec{Size: template.Gang.Size, Port: template.Gang.Port}
		for _, r := range template.Gang.Ranks {
			rank := GangRank{Rank: r.Rank, Argv: r.Argv, CWD: r.CWD, Env: r.Env}
			if r.Executor != nil {
				if rank.ExecutorSelector, err = json.Marshal(r.Executor); err != nil {
					return ExecutionSpec{}, err
				}
			}
			if r.Exclusive != nil {
				exclusive := convertExclusive(*r.Exclusive)
				rank.Exclusive = &exclusive
			}
			if r.Capacity != nil {
				capacity := convertCapacity(*r.Capacity)
				rank.Capacity = &capacity
			}
			gang.Ranks = append(gang.Ranks, rank)
		}
		spec.Gang = &gang
	}
	return spec, nil
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

// submitOrchestrationDecision submits a planned decision's execution and
// records it as the task's, in one transaction that first checks the decision
// is still planned: a decision a cancel or rerun abandoned after it was read is
// never submitted, and a cancel committed after the submission finds the
// execution live and withdraws it.
func (s *Store) submitOrchestrationDecision(ctx context.Context, decision plannedOrchestrationDecision) error {
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, _, err = submitOrchestrationDecisionTx(ctx, tx, decision); err != nil {
		return err
	}
	return tx.Commit()
}

// submitOrchestrationDecisionTx is submitOrchestrationDecision within tx: the
// execution as submitted and its id ("" when the decision is no longer to be
// submitted).
func submitOrchestrationDecisionTx(ctx context.Context, tx *sql.Tx, decision plannedOrchestrationDecision) (ExecutionSpec, string, error) {
	spec, _, digest, err := normalizeExecutionSpec(decision.Spec)
	if err != nil {
		return spec, "", err
	}
	var state, taskState string
	if err = tx.QueryRowContext(ctx, `SELECT d.state,t.state FROM orchestration_decisions d JOIN orchestration_tasks t ON t.task_scope_id=d.task_scope_id WHERE d.decision_key=$1`, decision.DecisionKey).Scan(&state, &taskState); err != nil {
		return spec, "", err
	}
	if state != "planned" || taskState == "cancelled" {
		return spec, "", nil
	}
	executionID, _, err := submitExecutionTx(ctx, tx, spec, digest)
	if err != nil {
		return spec, "", err
	}
	stamp := now()
	if _, err = tx.ExecContext(ctx, `UPDATE orchestration_decisions SET state='submitted',execution_id=$1,submitted_at=$2 WHERE decision_key=$3`, executionID, stamp, decision.DecisionKey); err != nil {
		return spec, "", err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO orchestration_task_executions(task_scope_id,execution_ordinal,execution_id,decision_key,reason,created_at) VALUES($1,$2,$3,$4,$5,$6)`, decision.TaskScopeID, decision.Ordinal, executionID, decision.DecisionKey, decision.Reason, stamp); err != nil {
		return spec, "", err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE orchestration_tasks SET state='running',result_json=NULL,updated_at=$1 WHERE task_scope_id=$2`, stamp, decision.TaskScopeID); err != nil {
		return spec, "", err
	}
	// A hardware bench's task: its run carries on in the new execution.
	return spec, executionID, benchTaskExecutionTx(ctx, tx, decision.TaskScopeID, executionID, decision.Reason)
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
	_, err := tx.ExecContext(ctx, `UPDATE orchestration_tasks SET state=$1,result_json=$2,updated_at=$3 WHERE task_scope_id=$4 AND (state!=$5 OR COALESCE(result_json,'')!=COALESCE($6,''))`, state, body, now(), taskScopeID, state, body)
	return err
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
		result[i] = ExclusiveRequest{Kind: value.Kind, Count: value.Count, Device: value.Device, SameNode: value.SameNode, MinTotalMemoryBytes: value.MinTotalMemoryBytes,
			MinObservedFreeMemoryBytes: value.MinObservedFreeMemoryBytes, OnExternalClaim: value.OnExternalClaim,
			OnUnattributedActivity: value.OnUnattributedActivity, OnStaleObservation: value.OnStaleObservation}
	}
	return result
}

func convertCapacity(value orchestration.CapacityRequest) CapacityRequest {
	disks := make([]DiskRequest, len(value.Disks))
	for i, disk := range value.Disks {
		disks[i] = DiskRequest(disk)
	}
	return CapacityRequest{CPUMillis: value.CPUMillis, CPUMillisMin: value.CPUMillisMin, RAMBytes: value.RAMBytes, Strength: value.Strength, Disks: disks}
}
