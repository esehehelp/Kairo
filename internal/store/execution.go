package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
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
	if spec.Gang != nil {
		gang, err := normalizeGangSpec(*spec.Gang)
		if err != nil {
			return ExecutionSpec{}, nil, "", err
		}
		spec.Gang = &gang
	}
	// Gang is left out of the digest when absent, so a non-gang execution keeps
	// the digest it had before gangs existed.
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
		Gang                 *GangSpec `json:",omitempty"`
	}{spec.Scope, spec.Argv, spec.CWD, spec.ExecutorSelector, spec.Priority, spec.Checkpointable, spec.Preemptible, spec.InputContinuationRef, spec.Exclusive, spec.Capacity, spec.Gang}
	normalized, err := json.Marshal(digestValue)
	if err != nil {
		return ExecutionSpec{}, nil, "", err
	}
	sum := sha256.Sum256(normalized)
	return spec, normalized, hex.EncodeToString(sum[:]), nil
}

// normalizeGangSpec checks a gang and puts its rank overrides in rank order with
// canonical selectors.
func normalizeGangSpec(gang GangSpec) (GangSpec, error) {
	if gang.Size < 2 {
		return GangSpec{}, errors.New("gang size must be at least 2")
	}
	if gang.Port != 0 && (gang.Port < 1024 || gang.Port > 65535) {
		return GangSpec{}, errors.New("gang port must be 1024-65535 or 0")
	}
	seen := map[int]bool{}
	for i := range gang.Ranks {
		r := &gang.Ranks[i]
		if r.Rank < 0 || r.Rank >= gang.Size {
			return GangSpec{}, fmt.Errorf("gang rank override %d is outside 0..%d", r.Rank, gang.Size-1)
		}
		if seen[r.Rank] {
			return GangSpec{}, fmt.Errorf("gang rank %d is overridden twice", r.Rank)
		}
		seen[r.Rank] = true
		r.CWD = strings.TrimSpace(r.CWD)
		if len(r.Argv) != 0 && strings.TrimSpace(r.Argv[0]) == "" {
			return GangSpec{}, fmt.Errorf("gang rank %d has an empty argv", r.Rank)
		}
		if len(r.ExecutorSelector) != 0 {
			var selector map[string]any
			if json.Unmarshal(r.ExecutorSelector, &selector) != nil || selector == nil {
				return GangSpec{}, fmt.Errorf("gang rank %d executor selector must be a JSON object", r.Rank)
			}
			canonical, err := json.Marshal(selector)
			if err != nil {
				return GangSpec{}, err
			}
			r.ExecutorSelector = canonical
		}
	}
	sort.Slice(gang.Ranks, func(i, j int) bool { return gang.Ranks[i].Rank < gang.Ranks[j].Rank })
	if gang.Ranks == nil {
		gang.Ranks = []GangRank{}
	}
	return gang, nil
}

// rankSpec is the execution of one gang rank: the gang's spec with that rank's
// overrides applied.
func rankSpec(spec ExecutionSpec, rank int) ExecutionSpec {
	out := spec
	out.Gang = nil
	for _, r := range spec.Gang.Ranks {
		if r.Rank != rank {
			continue
		}
		if len(r.Argv) != 0 {
			out.Argv = r.Argv
		}
		if r.CWD != "" {
			out.CWD = r.CWD
		}
		if len(r.ExecutorSelector) != 0 {
			out.ExecutorSelector = r.ExecutorSelector
		}
	}
	return out
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
	var queueID, taskID *string
	if scopes.Queue != nil {
		queueID = &scopes.Queue.ID
	}
	if scopes.Task != nil {
		taskID = &scopes.Task.ID
	}
	insert := func(spec ExecutionSpec, clientRequestID, digest string) (string, error) {
		argv, err := json.Marshal(spec.Argv)
		if err != nil {
			return "", err
		}
		executionID, stamp := id.New("exe"), now()
		if _, err = tx.ExecContext(ctx, `INSERT INTO execution_requests(id,project_scope_id,queue_scope_id,task_scope_id,client_request_id,spec_digest,state,argv_json,cwd,executor_selector_json,priority,checkpointable,preemptible,input_continuation_ref,submitted_at) VALUES(?,?,?,?,?,?,'waiting',?,?,?,?,?,?,?,?)`, executionID, scopes.Project.ID, queueID, taskID, clientRequestID, digest, string(argv), spec.CWD, string(spec.ExecutorSelector), spec.Priority, spec.Checkpointable, spec.Preemptible, spec.InputContinuationRef, stamp); err != nil {
			return "", err
		}
		if err = insertExecutionResourcesTx(ctx, tx, executionID, spec); err != nil {
			return "", err
		}
		return executionID, appendCoordinationEventTx(ctx, tx, "execution_submitted", &scopes.Project.ID, "execution", executionID, nil, map[string]any{"client_request_id": clientRequestID, "spec_digest": digest})
	}
	leaderSpec := spec
	if spec.Gang != nil {
		leaderSpec = rankSpec(spec, 0)
	}
	executionID, err := insert(leaderSpec, spec.ClientRequestID, digest)
	if err != nil {
		return ExecutionRequest{}, false, err
	}
	if spec.Gang != nil {
		// Members carry the leader's request ID and digest with their rank, so a
		// resubmitted gang is idempotent through its leader alone.
		gangID, stamp := id.New("gng"), now()
		if _, err = tx.ExecContext(ctx, `INSERT INTO execution_gangs(id,leader_execution_id,size,port,created_at) VALUES(?,?,?,?,?)`, gangID, executionID, spec.Gang.Size, spec.Gang.Port, stamp); err != nil {
			return ExecutionRequest{}, false, err
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO gang_members(gang_id,rank,execution_id) VALUES(?,0,?)`, gangID, executionID); err != nil {
			return ExecutionRequest{}, false, err
		}
		for rank := 1; rank < spec.Gang.Size; rank++ {
			memberID, err := insert(rankSpec(spec, rank), fmt.Sprintf("%s#rank%d", spec.ClientRequestID, rank), fmt.Sprintf("%s#rank%d", digest, rank))
			if err != nil {
				return ExecutionRequest{}, false, err
			}
			if _, err = tx.ExecContext(ctx, `INSERT INTO gang_members(gang_id,rank,execution_id) VALUES(?,?,?)`, gangID, rank, memberID); err != nil {
				return ExecutionRequest{}, false, err
			}
		}
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
	if _, err = withdrawExecutionTx(ctx, tx, executionID); err != nil {
		return err
	}
	return tx.Commit()
}

// withdrawExecutionTx ends an execution that has not started (terminal cause
// withdrawn_before_start). It reports whether this call withdrew it; an
// execution withdrawn earlier is not an error.
func withdrawExecutionTx(ctx context.Context, tx *sql.Tx, executionID string) (bool, error) {
	var state, projectID string
	var startedAt, terminalCause sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT state,project_scope_id,started_at,terminal_cause FROM execution_requests WHERE id=?`, executionID).Scan(&state, &projectID, &startedAt, &terminalCause); errors.Is(err, sql.ErrNoRows) {
		return false, ErrNotFound
	} else if err != nil {
		return false, err
	}
	if startedAt.Valid || state == "started" {
		return false, ErrExecutionStarted
	}
	if state == "terminal" {
		if !terminalCause.Valid || terminalCause.String != "withdrawn_before_start" {
			return false, ErrExecutionStarted
		}
		return false, nil
	}
	t := now()
	if _, err := tx.ExecContext(ctx, `UPDATE execution_requests SET state='terminal',terminal_cause='withdrawn_before_start',terminal_at=? WHERE id=?`, t, executionID); err != nil {
		return false, err
	}
	if state == "authorized" {
		if _, err := tx.ExecContext(ctx, `UPDATE launch_authorizations SET state='revoked',revoked_at=? WHERE attempt_id=(SELECT id FROM attempts WHERE execution_id=?) AND state='issued'`, t, executionID); err != nil {
			return false, err
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE leases SET state='revocation_requested',revocation_requested_at=? WHERE execution_id=? AND state IN('reserved','prepared')`, t, executionID); err != nil {
		return false, err
	}
	if err := appendCoordinationEventTx(ctx, tx, "execution_withdrawn", &projectID, "execution", executionID, nil, map[string]string{"cause": "withdrawn_before_start"}); err != nil {
		return false, err
	}
	return true, nil
}
