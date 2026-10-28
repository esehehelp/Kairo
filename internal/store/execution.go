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

	"kairo/internal/envspec"
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
	if spec.Exclusive, err = normalizeExclusive(spec.Exclusive); err != nil {
		return ExecutionSpec{}, nil, "", err
	}
	if err = normalizeCapacity(&spec.Capacity); err != nil {
		return ExecutionSpec{}, nil, "", err
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
	// The execution's env reaches every rank, so it is checked against a GPU
	// any rank leases; a rank's env against its own.
	if spec.Env, err = envspec.Normalize(spec.Env, anyRankLeasesGPU(spec)); err != nil {
		return ExecutionSpec{}, nil, "", err
	}
	if spec.Gang != nil {
		for i := range spec.Gang.Ranks {
			r := &spec.Gang.Ranks[i]
			if r.Env, err = envspec.Normalize(r.Env, len(rankSpec(spec, r.Rank).Exclusive) != 0); err != nil {
				return ExecutionSpec{}, nil, "", fmt.Errorf("gang rank %d: %w", r.Rank, err)
			}
		}
	}
	// Gang and Env are left out of the digest when absent, so an execution
	// without them keeps the digest it had before they existed.
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
		Gang                 *GangSpec         `json:",omitempty"`
		Env                  map[string]string `json:",omitempty"`
	}{spec.Scope, spec.Argv, spec.CWD, spec.ExecutorSelector, spec.Priority, spec.Checkpointable, spec.Preemptible, spec.InputContinuationRef, spec.Exclusive, spec.Capacity, spec.Gang, spec.Env}
	normalized, err := json.Marshal(digestValue)
	if err != nil {
		return ExecutionSpec{}, nil, "", err
	}
	sum := sha256.Sum256(normalized)
	return spec, normalized, hex.EncodeToString(sum[:]), nil
}

func normalizeExclusive(requests []ExclusiveRequest) ([]ExclusiveRequest, error) {
	if requests == nil {
		requests = []ExclusiveRequest{}
	}
	for i := range requests {
		r := &requests[i]
		r.Kind = strings.TrimSpace(r.Kind)
		r.Device = strings.TrimSpace(r.Device)
		if r.Kind != "gpu" || r.Count < 1 {
			return nil, fmt.Errorf("exclusive request %d must be a positive GPU request", i)
		}
		if r.Device != "" && r.Count != 1 {
			return nil, fmt.Errorf("exclusive request %d names device %s: its count must be 1", i, r.Device)
		}
		if r.MinTotalMemoryBytes < 0 || r.MinObservedFreeMemoryBytes < 0 {
			return nil, fmt.Errorf("exclusive request %d has negative memory", i)
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
			return nil, fmt.Errorf("exclusive request %d has an invalid conflict policy", i)
		}
	}
	return requests, nil
}

func normalizeCapacity(c *CapacityRequest) error {
	if c.Strength == "" {
		c.Strength = "admitted"
	}
	if c.Strength != "admitted" {
		return errors.New("only admitted capacity is supported")
	}
	if c.CPUMillis < 0 || c.RAMBytes < 0 {
		return errors.New("capacity cannot be negative")
	}
	if c.CPUMillisMin < 0 || c.CPUMillisMin > c.CPUMillis {
		return errors.New("cpu_millis_min must be between 0 and cpu_millis")
	}
	if c.Disks == nil {
		c.Disks = []DiskRequest{}
	}
	for i := range c.Disks {
		d := &c.Disks[i]
		d.Filesystem = strings.TrimSpace(d.Filesystem)
		if d.Filesystem == "" || d.ReserveBytes < 0 || d.MinFreeAfterBytes < 0 {
			return fmt.Errorf("disk request %d is invalid", i)
		}
	}
	return nil
}

// anyRankLeasesGPU reports whether the execution, or any rank of its gang,
// asks for an exclusive GPU.
func anyRankLeasesGPU(spec ExecutionSpec) bool {
	if spec.Gang == nil {
		return len(spec.Exclusive) != 0
	}
	for rank := 0; rank < spec.Gang.Size; rank++ {
		if len(rankSpec(spec, rank).Exclusive) != 0 {
			return true
		}
	}
	return false
}

// normalizeGangSpec checks a gang and puts its rank overrides in rank order with
// canonical selectors and requests. Rank envs are checked by the caller, which
// knows each rank's resources.
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
		if r.Exclusive != nil {
			exclusive, err := normalizeExclusive(*r.Exclusive)
			if err != nil {
				return GangSpec{}, fmt.Errorf("gang rank %d: %w", r.Rank, err)
			}
			r.Exclusive = &exclusive
		}
		if r.Capacity != nil {
			capacity := *r.Capacity
			if err := normalizeCapacity(&capacity); err != nil {
				return GangSpec{}, fmt.Errorf("gang rank %d: %w", r.Rank, err)
			}
			r.Capacity = &capacity
		}
	}
	sort.Slice(gang.Ranks, func(i, j int) bool { return gang.Ranks[i].Rank < gang.Ranks[j].Rank })
	if gang.Ranks == nil {
		gang.Ranks = []GangRank{}
	}
	return gang, nil
}

// rankSpec is the execution of one gang rank: the gang's spec with that rank's
// overrides applied (its exclusive and capacity replace the gang's) and its env
// merged over the gang's.
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
		if r.Exclusive != nil {
			out.Exclusive = *r.Exclusive
		}
		if r.Capacity != nil {
			out.Capacity = *r.Capacity
		}
		out.Env = envspec.Merge(spec.Env, r.Env)
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
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return ExecutionRequest{}, false, err
	}
	defer tx.Rollback()
	executionID, idempotent, err := submitExecutionTx(ctx, tx, spec, digest)
	if err != nil {
		return ExecutionRequest{}, false, err
	}
	if err = tx.Commit(); err != nil {
		return ExecutionRequest{}, false, err
	}
	execution, err := s.GetExecution(ctx, executionID)
	return execution, idempotent, err
}

// submitExecutionTx inserts a normalized execution (every rank of a gang), or
// finds the one already submitted with its client request ID and digest
// (idempotent).
func submitExecutionTx(ctx context.Context, tx *sql.Tx, spec ExecutionSpec, digest string) (string, bool, error) {
	project, _, err := ensureScopeTx(ctx, tx, "project", nil, spec.Scope.Project)
	if err != nil {
		return "", false, err
	}
	var existingID, existingDigest string
	err = tx.QueryRowContext(ctx, `SELECT id,spec_digest FROM execution_requests WHERE project_scope_id=$1 AND client_request_id=$2`, project.ID, spec.ClientRequestID).Scan(&existingID, &existingDigest)
	if err == nil {
		if existingDigest != digest {
			return "", false, ErrIdempotencyConflict
		}
		return existingID, true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return "", false, err
	}
	scopes, err := ensureScopePathTx(ctx, tx, spec.Scope)
	if err != nil {
		return "", false, err
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
		var env *string
		if len(spec.Env) != 0 {
			body, err := json.Marshal(spec.Env)
			if err != nil {
				return "", err
			}
			value := string(body)
			env = &value
		}
		executionID, stamp := id.New("exe"), now()
		if _, err = tx.ExecContext(ctx, `INSERT INTO execution_requests(id,project_scope_id,queue_scope_id,task_scope_id,client_request_id,spec_digest,state,argv_json,cwd,executor_selector_json,priority,checkpointable,preemptible,input_continuation_ref,submitted_at,env_json) VALUES($1,$2,$3,$4,$5,$6,'waiting',$7,$8,$9,$10,$11,$12,$13,$14,$15)`, executionID, scopes.Project.ID, queueID, taskID, clientRequestID, digest, string(argv), spec.CWD, string(spec.ExecutorSelector), spec.Priority, spec.Checkpointable, spec.Preemptible, spec.InputContinuationRef, stamp, env); err != nil {
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
		return "", false, err
	}
	if spec.Gang != nil {
		// Members carry the leader's request ID and digest with their rank, so a
		// resubmitted gang is idempotent through its leader alone.
		gangID, stamp := id.New("gng"), now()
		if _, err = tx.ExecContext(ctx, `INSERT INTO execution_gangs(id,leader_execution_id,size,port,created_at) VALUES($1,$2,$3,$4,$5)`, gangID, executionID, spec.Gang.Size, spec.Gang.Port, stamp); err != nil {
			return "", false, err
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO gang_members(gang_id,rank,execution_id) VALUES($1,0,$2)`, gangID, executionID); err != nil {
			return "", false, err
		}
		for rank := 1; rank < spec.Gang.Size; rank++ {
			memberID, err := insert(rankSpec(spec, rank), fmt.Sprintf("%s#rank%d", spec.ClientRequestID, rank), fmt.Sprintf("%s#rank%d", digest, rank))
			if err != nil {
				return "", false, err
			}
			if _, err = tx.ExecContext(ctx, `INSERT INTO gang_members(gang_id,rank,execution_id) VALUES($1,$2,$3)`, gangID, rank, memberID); err != nil {
				return "", false, err
			}
		}
	}
	return executionID, false, nil
}

func insertExecutionResourcesTx(ctx context.Context, tx *sql.Tx, executionID string, spec ExecutionSpec) error {
	for _, request := range spec.Exclusive {
		fields := map[string]any{"same_node": request.SameNode, "min_total_memory_bytes": request.MinTotalMemoryBytes, "min_observed_free_memory_bytes": request.MinObservedFreeMemoryBytes}
		if request.Device != "" {
			fields["device"] = request.Device
		}
		constraints, err := json.Marshal(fields)
		if err != nil {
			return err
		}
		policy, err := json.Marshal(map[string]string{"on_external_claim": request.OnExternalClaim, "on_unattributed_activity": request.OnUnattributedActivity, "on_stale_observation": request.OnStaleObservation})
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO resource_requests(id,execution_id,request_type,kind,quantity,constraints_json,policy_json) VALUES($1,$2,'exclusive',$3,$4,$5,$6)`, id.New("req"), executionID, request.Kind, request.Count, string(constraints), string(policy)); err != nil {
			return err
		}
	}
	strength := spec.Capacity.Strength
	if spec.Capacity.CPUMillis > 0 {
		constraints := "{}"
		if spec.Capacity.CPUMillisMin > 0 {
			constraints = fmt.Sprintf(`{"min_quantity":%d}`, spec.Capacity.CPUMillisMin)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO resource_requests(id,execution_id,request_type,kind,quantity,strength,constraints_json) VALUES($1,$2,'capacity','cpu',$3,$4,$5)`, id.New("req"), executionID, spec.Capacity.CPUMillis, strength, constraints); err != nil {
			return err
		}
	}
	if spec.Capacity.RAMBytes > 0 {
		if _, err := tx.ExecContext(ctx, `INSERT INTO resource_requests(id,execution_id,request_type,kind,quantity,strength) VALUES($1,$2,'capacity','ram',$3,$4)`, id.New("req"), executionID, spec.Capacity.RAMBytes, strength); err != nil {
			return err
		}
	}
	for _, disk := range spec.Capacity.Disks {
		constraints, err := json.Marshal(map[string]int64{"min_free_after_bytes": disk.MinFreeAfterBytes})
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO resource_requests(id,execution_id,request_type,kind,quantity,filesystem,constraints_json,strength) VALUES($1,$2,'capacity','disk',$3,$4,$5,$6)`, id.New("req"), executionID, disk.ReserveBytes, disk.Filesystem, string(constraints), strength); err != nil {
			return err
		}
	}
	return nil
}

func scanExecution(row interface{ Scan(...any) error }) (ExecutionRequest, error) {
	var execution ExecutionRequest
	var argv, selector string
	var env sql.NullString
	err := row.Scan(&execution.ID, &execution.ProjectScopeID, &execution.QueueScopeID, &execution.TaskScopeID, &execution.ClientRequestID, &execution.SpecDigest, &execution.State, &argv, &execution.CWD, &selector, &execution.Priority, &execution.Checkpointable, &execution.Preemptible, &execution.InputContinuationRef, &execution.TerminalCause, &execution.SubmittedAt, &execution.AuthorizedAt, &execution.StartedAt, &execution.TerminalAt, &env)
	if err != nil {
		return ExecutionRequest{}, err
	}
	if err = json.Unmarshal([]byte(argv), &execution.Argv); err != nil {
		return ExecutionRequest{}, err
	}
	execution.ExecutorSelector = json.RawMessage(selector)
	if execution.Env, err = decodeEnv(env); err != nil {
		return ExecutionRequest{}, err
	}
	return execution, nil
}

// decodeEnv reads an execution's env_json (NULL when it declares no env).
func decodeEnv(column sql.NullString) (map[string]string, error) {
	if !column.Valid || column.String == "" {
		return nil, nil
	}
	var env map[string]string
	if err := json.Unmarshal([]byte(column.String), &env); err != nil {
		return nil, fmt.Errorf("decode env_json: %w", err)
	}
	return env, nil
}

const executionSelect = `SELECT id,project_scope_id,queue_scope_id,task_scope_id,client_request_id,spec_digest,state,argv_json,cwd,executor_selector_json,priority,checkpointable,preemptible,input_continuation_ref,terminal_cause,submitted_at,authorized_at,started_at,terminal_at,env_json FROM execution_requests`

func (s *Store) GetExecution(ctx context.Context, executionID string) (ExecutionRequest, error) {
	execution, err := scanExecution(s.db.QueryRowContext(ctx, executionSelect+` WHERE id=$1`, executionID))
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
		conditions = append(conditions, `project_scope_id=`+bind(&args, filter.ProjectScopeID))
	}
	if filter.ScopeID != "" {
		within := `(WITH RECURSIVE d(id) AS (SELECT ` + bind(&args, filter.ScopeID) + `::text UNION ALL SELECT s.id FROM coordination_scopes s JOIN d ON s.parent_id=d.id) SELECT id FROM d)`
		conditions = append(conditions, `(project_scope_id IN `+within+` OR queue_scope_id IN `+within+` OR task_scope_id IN `+within+`)`)
	}
	if filter.State != "" {
		conditions = append(conditions, `state=`+bind(&args, filter.State))
	}
	if len(conditions) > 0 {
		query += ` WHERE ` + strings.Join(conditions, ` AND `)
	}
	query += ` ORDER BY submitted_at,id LIMIT `
	limit := filter.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	query += bind(&args, limit)
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
	ids := make([]string, len(out))
	for i := range out {
		ids[i] = out[i].ID
	}
	resources, err := s.executionResources(ctx, ids)
	if err != nil {
		return nil, err
	}
	for i := range out {
		out[i].Resources = resources[out[i].ID]
	}
	return out, nil
}

func (s *Store) listExecutionResources(ctx context.Context, executionID string) ([]ResourceRequest, error) {
	resources, err := s.executionResources(ctx, []string{executionID})
	return resources[executionID], err
}

// executionResources returns the resource requests of each execution (an
// empty list for one without any), in one query.
func (s *Store) executionResources(ctx context.Context, executionIDs []string) (map[string][]ResourceRequest, error) {
	out := make(map[string][]ResourceRequest, len(executionIDs))
	for _, id := range executionIDs {
		out[id] = []ResourceRequest{}
	}
	err := eachRow(ctx, s.db, `SELECT id,execution_id,request_type,kind,quantity,filesystem,constraints_json,policy_json,strength FROM resource_requests WHERE execution_id=ANY($1::text[]) ORDER BY execution_id,request_type,kind,id`, []any{executionIDs}, func(rows *sql.Rows) error {
		var request ResourceRequest
		var constraints, policy string
		if err := rows.Scan(&request.ID, &request.ExecutionID, &request.RequestType, &request.Kind, &request.Quantity, &request.Filesystem, &constraints, &policy, &request.Strength); err != nil {
			return err
		}
		request.Constraints = json.RawMessage(constraints)
		request.Policy = json.RawMessage(policy)
		out[request.ExecutionID] = append(out[request.ExecutionID], request)
		return nil
	})
	return out, err
}

func (s *Store) WithdrawExecution(ctx context.Context, executionID string) error {
	tx, err := s.beginWrite(ctx)
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
	if err := tx.QueryRowContext(ctx, `SELECT state,project_scope_id,started_at,terminal_cause FROM execution_requests WHERE id=$1`, executionID).Scan(&state, &projectID, &startedAt, &terminalCause); errors.Is(err, sql.ErrNoRows) {
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
	if _, err := tx.ExecContext(ctx, `UPDATE execution_requests SET state='terminal',terminal_cause='withdrawn_before_start',terminal_at=$1 WHERE id=$2`, t, executionID); err != nil {
		return false, err
	}
	if state == "authorized" {
		if _, err := tx.ExecContext(ctx, `UPDATE launch_authorizations SET state='revoked',revoked_at=$1 WHERE attempt_id=(SELECT id FROM attempts WHERE execution_id=$2) AND state='issued'`, t, executionID); err != nil {
			return false, err
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE leases SET state='revocation_requested',revocation_requested_at=$1 WHERE execution_id=$2 AND state IN('reserved','prepared')`, t, executionID); err != nil {
		return false, err
	}
	if err := appendCoordinationEventTx(ctx, tx, "execution_withdrawn", &projectID, "execution", executionID, nil, map[string]string{"cause": "withdrawn_before_start"}); err != nil {
		return false, err
	}
	return true, nil
}
