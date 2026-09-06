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

	"kairo/internal/id"
)

func normalizeExecutionSpec(spec ExecutionSpec) (ExecutionSpec, []byte, string, error) {
	path, err := normalizeScopePath(spec.Scope)
	if err != nil {
		return ExecutionSpec{}, nil, "", err
	}
	spec.Scope = path
	spec.ClientRequestID = strings.TrimSpace(spec.ClientRequestID)
	spec.CWD = strings.TrimSpace(spec.CWD)
	if spec.ClientRequestID == "" {
		return ExecutionSpec{}, nil, "", errors.New("client request ID is required")
	}
	if len(spec.Argv) == 0 || strings.TrimSpace(spec.Argv[0]) == "" || spec.CWD == "" {
		return ExecutionSpec{}, nil, "", errors.New("argv and cwd are required")
	}
	if spec.Preemptible && !spec.Checkpointable {
		return ExecutionSpec{}, nil, "", errors.New("preemptible execution must be checkpointable")
	}
	if len(spec.ExecutorSelector) == 0 {
		spec.ExecutorSelector = json.RawMessage(`{}`)
	}
	var selector map[string]any
	if !json.Valid(spec.ExecutorSelector) || json.Unmarshal(spec.ExecutorSelector, &selector) != nil || selector == nil {
		return ExecutionSpec{}, nil, "", errors.New("executor selector must be a JSON object")
	}
	canonicalSelector, err := json.Marshal(selector)
	if err != nil {
		return ExecutionSpec{}, nil, "", err
	}
	spec.ExecutorSelector = canonicalSelector
	if spec.Exclusive == nil {
		spec.Exclusive = []ExclusiveRequest{}
	}
	for i := range spec.Exclusive {
		r := &spec.Exclusive[i]
		r.Kind = strings.TrimSpace(r.Kind)
		if r.Kind != "gpu" || r.Count < 1 {
			return ExecutionSpec{}, nil, "", fmt.Errorf("exclusive request %d must be a positive GPU request", i)
		}
		if r.MinTotalMemoryBytes < 0 || r.MinObservedFreeMemoryBytes < 0 {
			return ExecutionSpec{}, nil, "", fmt.Errorf("exclusive request %d has negative memory", i)
		}
		if r.OnExternalClaim == "" {
			r.OnExternalClaim = "wait"
		}
		if r.OnUnattributedActivity == "" {
			r.OnUnattributedActivity = "wait"
		}
		if r.OnStaleObservation == "" {
			r.OnStaleObservation = "wait"
		}
		if !stringIn(r.OnExternalClaim, "wait", "fail") || !stringIn(r.OnUnattributedActivity, "wait", "fail", "allow") || !stringIn(r.OnStaleObservation, "wait", "fail") {
			return ExecutionSpec{}, nil, "", fmt.Errorf("exclusive request %d has an invalid conflict policy", i)
		}
	}
	if spec.Capacity.Strength == "" {
		spec.Capacity.Strength = "admitted"
	}
	if spec.Capacity.Strength != "admitted" {
		return ExecutionSpec{}, nil, "", errors.New("only admitted capacity is supported")
	}
	if spec.Capacity.CPUMillis < 0 || spec.Capacity.RAMBytes < 0 {
		return ExecutionSpec{}, nil, "", errors.New("capacity cannot be negative")
	}
	if spec.Capacity.Disks == nil {
		spec.Capacity.Disks = []DiskRequest{}
	}
	for i := range spec.Capacity.Disks {
		d := &spec.Capacity.Disks[i]
		d.Filesystem = strings.TrimSpace(d.Filesystem)
		if d.Filesystem == "" || d.ReserveBytes < 0 || d.MinFreeAfterBytes < 0 {
			return ExecutionSpec{}, nil, "", fmt.Errorf("disk request %d is invalid", i)
		}
	}
	if spec.InputContinuationRef != nil && *spec.InputContinuationRef == "" {
		spec.InputContinuationRef = nil
	}
	digestValue := struct {
		Scope                ScopePath
		Argv                 []string
		CWD                  string
		ExecutorSelector     json.RawMessage
		Priority             int
		Checkpointable       bool
		Preemptible          bool
		InputContinuationRef *string
		Exclusive            []ExclusiveRequest
		Capacity             CapacityRequest
	}{spec.Scope, spec.Argv, spec.CWD, spec.ExecutorSelector, spec.Priority, spec.Checkpointable, spec.Preemptible, spec.InputContinuationRef, spec.Exclusive, spec.Capacity}
	normalized, err := json.Marshal(digestValue)
	if err != nil {
		return ExecutionSpec{}, nil, "", err
	}
	sum := sha256.Sum256(normalized)
	return spec, normalized, hex.EncodeToString(sum[:]), nil
}

func stringIn(value string, options ...string) bool {
	for _, option := range options {
		if value == option {
			return true
		}
	}
	return false
}

func (s *Store) SubmitExecution(ctx context.Context, input ExecutionSpec) (ExecutionRequest, bool, error) {
	spec, _, digest, err := normalizeExecutionSpec(input)
	if err != nil {
		return ExecutionRequest{}, false, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ExecutionRequest{}, false, err
	}
	defer tx.Rollback()
	project, _, err := ensureScopeTx(ctx, tx, "project", nil, spec.Scope.Project)
	if err != nil {
		return ExecutionRequest{}, false, err
	}
	var existingID, existingDigest string
	err = tx.QueryRowContext(ctx, `SELECT id,spec_digest FROM execution_requests WHERE project_scope_id=? AND client_request_id=?`, project.ID, spec.ClientRequestID).Scan(&existingID, &existingDigest)
	if err == nil {
		if existingDigest != digest {
			return ExecutionRequest{}, false, ErrIdempotencyConflict
		}
		if err = tx.Commit(); err != nil {
			return ExecutionRequest{}, false, err
		}
		execution, err := s.GetExecution(ctx, existingID)
		return execution, true, err
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return ExecutionRequest{}, false, err
	}
	scopes, err := ensureScopePathTx(ctx, tx, spec.Scope)
	if err != nil {
		return ExecutionRequest{}, false, err
	}
	argv, err := json.Marshal(spec.Argv)
	if err != nil {
		return ExecutionRequest{}, false, err
	}
	executionID, stamp := id.New("exe"), now()
	var queueID, taskID *string
	if scopes.Queue != nil {
		queueID = &scopes.Queue.ID
	}
	if scopes.Task != nil {
		taskID = &scopes.Task.ID
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO execution_requests(id,project_scope_id,queue_scope_id,task_scope_id,client_request_id,spec_digest,state,argv_json,cwd,executor_selector_json,priority,checkpointable,preemptible,input_continuation_ref,submitted_at) VALUES(?,?,?,?,?,?,'waiting',?,?,?,?,?,?,?,?)`, executionID, scopes.Project.ID, queueID, taskID, spec.ClientRequestID, digest, string(argv), spec.CWD, string(spec.ExecutorSelector), spec.Priority, spec.Checkpointable, spec.Preemptible, spec.InputContinuationRef, stamp); err != nil {
		return ExecutionRequest{}, false, err
	}
	if err = insertExecutionResourcesTx(ctx, tx, executionID, spec); err != nil {
		return ExecutionRequest{}, false, err
	}
	if err = appendCoordinationEventTx(ctx, tx, "execution_submitted", &scopes.Project.ID, "execution", executionID, nil, map[string]any{"client_request_id": spec.ClientRequestID, "spec_digest": digest}); err != nil {
		return ExecutionRequest{}, false, err
	}
	if err = tx.Commit(); err != nil {
		return ExecutionRequest{}, false, err
	}
	execution, err := s.GetExecution(ctx, executionID)
	return execution, false, err
}

func insertExecutionResourcesTx(ctx context.Context, tx *sql.Tx, executionID string, spec ExecutionSpec) error {
	for _, request := range spec.Exclusive {
		constraints, err := json.Marshal(map[string]any{"same_node": request.SameNode, "min_total_memory_bytes": request.MinTotalMemoryBytes, "min_observed_free_memory_bytes": request.MinObservedFreeMemoryBytes})
		if err != nil {
			return err
		}
		policy, err := json.Marshal(map[string]string{"on_external_claim": request.OnExternalClaim, "on_unattributed_activity": request.OnUnattributedActivity, "on_stale_observation": request.OnStaleObservation})
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO resource_requests(id,execution_id,request_type,kind,quantity,constraints_json,policy_json) VALUES(?,?,'exclusive',?,?,?,?)`, id.New("req"), executionID, request.Kind, request.Count, string(constraints), string(policy)); err != nil {
			return err
		}
	}
	strength := spec.Capacity.Strength
	if spec.Capacity.CPUMillis > 0 {
		if _, err := tx.ExecContext(ctx, `INSERT INTO resource_requests(id,execution_id,request_type,kind,quantity,strength) VALUES(?,?,'capacity','cpu',?,?)`, id.New("req"), executionID, spec.Capacity.CPUMillis, strength); err != nil {
			return err
		}
	}
	if spec.Capacity.RAMBytes > 0 {
		if _, err := tx.ExecContext(ctx, `INSERT INTO resource_requests(id,execution_id,request_type,kind,quantity,strength) VALUES(?,?,'capacity','ram',?,?)`, id.New("req"), executionID, spec.Capacity.RAMBytes, strength); err != nil {
			return err
		}
	}
	for _, disk := range spec.Capacity.Disks {
		constraints, err := json.Marshal(map[string]int64{"min_free_after_bytes": disk.MinFreeAfterBytes})
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO resource_requests(id,execution_id,request_type,kind,quantity,filesystem,constraints_json,strength) VALUES(?,?,'capacity','disk',?,?,?,?)`, id.New("req"), executionID, disk.ReserveBytes, disk.Filesystem, string(constraints), strength); err != nil {
			return err
		}
	}
	return nil
}

func scanExecution(row interface{ Scan(...any) error }) (ExecutionRequest, error) {
	var execution ExecutionRequest
	var argv, selector string
	err := row.Scan(&execution.ID, &execution.ProjectScopeID, &execution.QueueScopeID, &execution.TaskScopeID, &execution.ClientRequestID, &execution.SpecDigest, &execution.State, &argv, &execution.CWD, &selector, &execution.Priority, &execution.Checkpointable, &execution.Preemptible, &execution.InputContinuationRef, &execution.TerminalCause, &execution.SubmittedAt, &execution.AuthorizedAt, &execution.StartedAt, &execution.TerminalAt)
	if err != nil {
		return ExecutionRequest{}, err
	}
	if err = json.Unmarshal([]byte(argv), &execution.Argv); err != nil {
		return ExecutionRequest{}, err
	}
	execution.ExecutorSelector = json.RawMessage(selector)
	return execution, nil
}

const executionSelect = `SELECT id,project_scope_id,queue_scope_id,task_scope_id,client_request_id,spec_digest,state,argv_json,cwd,executor_selector_json,priority,checkpointable,preemptible,input_continuation_ref,terminal_cause,submitted_at,authorized_at,started_at,terminal_at FROM execution_requests`

func (s *Store) GetExecution(ctx context.Context, executionID string) (ExecutionRequest, error) {
	execution, err := scanExecution(s.db.QueryRowContext(ctx, executionSelect+` WHERE id=?`, executionID))
	if errors.Is(err, sql.ErrNoRows) {
		return ExecutionRequest{}, ErrNotFound
	}
	if err != nil {
		return ExecutionRequest{}, err
	}
	execution.Resources, err = s.listExecutionResources(ctx, executionID)
	return execution, err
}

func (s *Store) ListExecutions(ctx context.Context, filter ExecutionFilter) ([]ExecutionRequest, error) {
	query := executionSelect
	conditions := make([]string, 0, 3)
	args := make([]any, 0, 4)
	if filter.ProjectScopeID != "" {
		conditions = append(conditions, `project_scope_id=?`)
		args = append(args, filter.ProjectScopeID)
	}
	if filter.ScopeID != "" {
		conditions = append(conditions, `(project_scope_id IN (WITH RECURSIVE d(id) AS (SELECT ? UNION ALL SELECT s.id FROM coordination_scopes s JOIN d ON s.parent_id=d.id) SELECT id FROM d) OR queue_scope_id IN (WITH RECURSIVE d(id) AS (SELECT ? UNION ALL SELECT s.id FROM coordination_scopes s JOIN d ON s.parent_id=d.id) SELECT id FROM d) OR task_scope_id IN (WITH RECURSIVE d(id) AS (SELECT ? UNION ALL SELECT s.id FROM coordination_scopes s JOIN d ON s.parent_id=d.id) SELECT id FROM d))`)
		args = append(args, filter.ScopeID, filter.ScopeID, filter.ScopeID)
	}
	if filter.State != "" {
		conditions = append(conditions, `state=?`)
		args = append(args, filter.State)
	}
	if len(conditions) > 0 {
		query += ` WHERE ` + strings.Join(conditions, ` AND `)
	}
	query += ` ORDER BY submitted_at,id LIMIT ?`
	limit := filter.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	out := make([]ExecutionRequest, 0)
	for rows.Next() {
		execution, scanErr := scanExecution(rows)
		if scanErr != nil {
			rows.Close()
			return nil, scanErr
		}
		out = append(out, execution)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	for i := range out {
		out[i].Resources, err = s.listExecutionResources(ctx, out[i].ID)
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (s *Store) listExecutionResources(ctx context.Context, executionID string) ([]ResourceRequest, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id,execution_id,request_type,kind,quantity,filesystem,constraints_json,policy_json,strength FROM resource_requests WHERE execution_id=? ORDER BY request_type,kind,id`, executionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]ResourceRequest, 0)
	for rows.Next() {
		var request ResourceRequest
		var constraints, policy string
		if err := rows.Scan(&request.ID, &request.ExecutionID, &request.RequestType, &request.Kind, &request.Quantity, &request.Filesystem, &constraints, &policy, &request.Strength); err != nil {
			return nil, err
		}
		request.Constraints = json.RawMessage(constraints)
		request.Policy = json.RawMessage(policy)
		out = append(out, request)
	}
	return out, rows.Err()
}

func (s *Store) WithdrawExecution(ctx context.Context, executionID string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var state, projectID string
	var startedAt, terminalCause sql.NullString
	if err = tx.QueryRowContext(ctx, `SELECT state,project_scope_id,started_at,terminal_cause FROM execution_requests WHERE id=?`, executionID).Scan(&state, &projectID, &startedAt, &terminalCause); errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	} else if err != nil {
		return err
	}
	if startedAt.Valid || state == "started" {
		return ErrExecutionStarted
	}
	if state == "terminal" {
		if !terminalCause.Valid || terminalCause.String != "withdrawn_before_start" {
			return ErrExecutionStarted
		}
		return tx.Commit()
	}
	t := now()
	if _, err = tx.ExecContext(ctx, `UPDATE execution_requests SET state='terminal',terminal_cause='withdrawn_before_start',terminal_at=? WHERE id=?`, t, executionID); err != nil {
		return err
	}
	if state == "authorized" {
		if _, err = tx.ExecContext(ctx, `UPDATE launch_authorizations SET state='revoked',revoked_at=? WHERE attempt_id=(SELECT id FROM attempts WHERE execution_id=?) AND state='issued'`, t, executionID); err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(ctx, `UPDATE leases SET state='revocation_requested',revocation_requested_at=? WHERE execution_id=? AND state IN('reserved','prepared')`, t, executionID); err != nil {
		return err
	}
	if err = appendCoordinationEventTx(ctx, tx, "execution_withdrawn", &projectID, "execution", executionID, nil, map[string]string{"cause": "withdrawn_before_start"}); err != nil {
		return err
	}
	return tx.Commit()
}
