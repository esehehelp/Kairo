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

	"kairo/internal/datainput"
	"kairo/internal/envspec"
	"kairo/internal/id"
	"kairo/internal/imageref"
	"kairo/internal/instant"
	"kairo/internal/orchestration"
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
	if err := datainput.Validate(spec.Inputs, true); err != nil {
		return ExecutionSpec{}, nil, "", err
	}
	spec.Store = strings.TrimSpace(spec.Store)
	if len(spec.ExecutorSelector) == 0 {
		spec.ExecutorSelector = json.RawMessage(`{}`)
	}
	var selector map[string]any
	if !json.Valid(spec.ExecutorSelector) || json.Unmarshal(spec.ExecutorSelector, &selector) != nil || selector == nil {
		return ExecutionSpec{}, nil, "", errors.New("executor selector must be a JSON object")
	}
	// An execution in an image runs on an executor that runs containers,
	// and only it does (selectorMatches): the selector says which, from the
	// image, never from the caller.
	delete(selector, "container")
	if spec.Image = strings.TrimSpace(spec.Image); spec.Image != "" {
		ref, err := imageref.ParsePinned(spec.Image)
		if err != nil {
			return ExecutionSpec{}, nil, "", err
		}
		spec.Image = ref.Pinned()
		selector["container"] = true
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
	if spec.Env, err = envspec.Normalize(spec.Env, len(spec.Exclusive) != 0); err != nil {
		return ExecutionSpec{}, nil, "", err
	}
	if spec.NotBefore, err = instant.Normalize(spec.NotBefore); err != nil {
		return ExecutionSpec{}, nil, "", fmt.Errorf("not_before: %w", err)
	}
	if spec.StartBy, err = instant.Normalize(spec.StartBy); err != nil {
		return ExecutionSpec{}, nil, "", fmt.Errorf("start_by: %w", err)
	}
	if spec.FinishBy, err = instant.Normalize(spec.FinishBy); err != nil {
		return ExecutionSpec{}, nil, "", fmt.Errorf("finish_by: %w", err)
	}
	if err = instant.CheckOrder(spec.NotBefore, spec.StartBy, spec.FinishBy); err != nil {
		return ExecutionSpec{}, nil, "", err
	}
	// Env, ScratchTmpdir and the times are left out of the digest when
	// absent, so an execution without them keeps the digest it had before
	// they existed (and a gang, when there were gangs, was left out the
	// same way).
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
		Env                  map[string]string          `json:",omitempty"`
		ScratchTmpdir        bool                       `json:",omitempty"`
		NotBefore            string                     `json:",omitempty"`
		StartBy              string                     `json:",omitempty"`
		FinishBy             string                     `json:",omitempty"`
		Inputs               []datainput.Input          `json:",omitempty"`
		Confine              bool                       `json:",omitempty"`
		Store                string                     `json:",omitempty"`
		Regenerable          bool                       `json:",omitempty"`
		SourceSnapshot       string                     `json:",omitempty"`
		Image                string                     `json:",omitempty"`
		Streams              []orchestration.StreamDecl `json:",omitempty"`
		InputCheckpoint      string                     `json:",omitempty"`
		StreamSnapshot       string                     `json:",omitempty"`
	}{spec.Scope, spec.Argv, spec.CWD, spec.ExecutorSelector, spec.Priority, spec.Checkpointable, spec.Preemptible, spec.InputContinuationRef, spec.Exclusive, spec.Capacity, spec.Env, spec.ScratchTmpdir, spec.NotBefore, spec.StartBy, spec.FinishBy, spec.Inputs, spec.Confine, spec.Store, spec.Regenerable, spec.SourceSnapshot, spec.Image, spec.Streams, spec.InputCheckpointCommand, spec.StreamSnapshot}
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

// submitExecutionTx inserts a normalized execution, or
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
		var inputs *string
		if len(spec.Inputs) != 0 {
			body, err := json.Marshal(spec.Inputs)
			if err != nil {
				return "", err
			}
			value := string(body)
			inputs = &value
		}
		var streamsJSON *string
		if len(spec.Streams) != 0 {
			if spec.Image == "" {
				return "", errors.New("an execution reads streams in a container (an image)")
			}
			body, err := json.Marshal(spec.Streams)
			if err != nil {
				return "", err
			}
			value := string(body)
			streamsJSON = &value
		}
		if spec.StreamSnapshot != "" && (len(spec.Streams) == 0 || spec.StreamSnapshot == spec.SourceSnapshot) {
			return "", errors.New("a stream snapshot is for an execution reading streams in another snapshot than its program")
		}
		for _, snapshot := range []string{spec.SourceSnapshot, spec.StreamSnapshot} {
			if snapshot == "" {
				continue
			}
			known, err := sourceKnownTx(ctx, tx, snapshot)
			if err != nil {
				return "", err
			}
			if !known {
				return "", fmt.Errorf("source snapshot %s is not recorded (kairo source)", snapshot)
			}
		}
		executionID, stamp := id.New("exe"), now()
		if _, err = tx.ExecContext(ctx, `INSERT INTO execution_requests(id,project_scope_id,queue_scope_id,task_scope_id,client_request_id,spec_digest,state,argv_json,cwd,executor_selector_json,priority,checkpointable,preemptible,input_continuation_ref,submitted_at,env_json,scratch_tmpdir,not_before,start_by,finish_by,inputs_json,confine,store_id,regenerable,source_snapshot_id,image_ref,streams_json,input_checkpoint_command_id,stream_snapshot_id) VALUES($1,$2,$3,$4,$5,$6,'waiting',$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28)`, executionID, scopes.Project.ID, queueID, taskID, clientRequestID, digest, string(argv), spec.CWD, string(spec.ExecutorSelector), spec.Priority, spec.Checkpointable, spec.Preemptible, spec.InputContinuationRef, stamp, env, spec.ScratchTmpdir, nullable(spec.NotBefore), nullable(spec.StartBy), nullable(spec.FinishBy), inputs, spec.Confine, nullable(spec.Store), spec.Regenerable, nullable(spec.SourceSnapshot), nullable(spec.Image), streamsJSON, nullable(spec.InputCheckpointCommand), nullable(spec.StreamSnapshot)); err != nil {
			return "", err
		}
		if err = insertExecutionResourcesTx(ctx, tx, executionID, spec); err != nil {
			return "", err
		}
		return executionID, appendCoordinationEventTx(ctx, tx, "execution_submitted", &scopes.Project.ID, "execution", executionID, nil, map[string]any{"client_request_id": clientRequestID, "spec_digest": digest})
	}
	if err = pinInputNodeTx(ctx, tx, spec.Scope.Project, &spec); err != nil {
		return "", false, err
	}
	executionID, err := insert(spec, spec.ClientRequestID, digest)
	if err != nil {
		return "", false, err
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
	err := row.Scan(&execution.ID, &execution.ProjectScopeID, &execution.QueueScopeID, &execution.TaskScopeID, &execution.ClientRequestID, &execution.SpecDigest, &execution.State, &argv, &execution.CWD, &selector, &execution.Priority, &execution.Checkpointable, &execution.Preemptible, &execution.InputContinuationRef, &execution.TerminalCause, &execution.SubmittedAt, &execution.AuthorizedAt, &execution.StartedAt, &execution.TerminalAt, &env, &execution.NotBefore, &execution.StartBy, &execution.FinishBy)
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

const executionSelect = `SELECT id,project_scope_id,queue_scope_id,task_scope_id,client_request_id,spec_digest,state,argv_json,cwd,executor_selector_json,priority,checkpointable,preemptible,input_continuation_ref,terminal_cause,submitted_at,authorized_at,started_at,terminal_at,env_json,not_before,start_by,finish_by FROM execution_requests`

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
	out, _, err := s.ListExecutionsPage(ctx, filter)
	return out, err
}

// ListExecutionsPage lists executions oldest first (by submission), up to
// the filter's limit (100 by default, at most 1000) after its cursor; next is
// the cursor to read the rest from, "" when there is no more.
func (s *Store) ListExecutionsPage(ctx context.Context, filter ExecutionFilter) (out []ExecutionRequest, next string, err error) {
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
	if filter.After != "" {
		after, err := parseListCursor(filter.After)
		if err != nil || after.key == nil {
			return nil, "", ErrBadCursor
		}
		conditions = append(conditions, `(submitted_at,id)>(`+bind(&args, *after.key)+`,`+bind(&args, after.id)+`)`)
	}
	if len(conditions) > 0 {
		query += ` WHERE ` + strings.Join(conditions, ` AND `)
	}
	query += ` ORDER BY submitted_at,id LIMIT `
	limit := listLimit(filter.Limit)
	query += bind(&args, limit+1)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", err
	}
	out = make([]ExecutionRequest, 0)
	for rows.Next() {
		execution, scanErr := scanExecution(rows)
		if scanErr != nil {
			rows.Close()
			return nil, "", scanErr
		}
		out = append(out, execution)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return nil, "", err
	}
	rows.Close()
	if len(out) > limit {
		out = out[:limit]
		last := out[limit-1]
		next = listCursor{key: &last.SubmittedAt, id: last.ID}.String()
	}
	ids := make([]string, len(out))
	for i := range out {
		ids[i] = out[i].ID
	}
	resources, err := s.executionResources(ctx, ids)
	if err != nil {
		return nil, "", err
	}
	for i := range out {
		out[i].Resources = resources[out[i].ID]
	}
	return out, next, nil
}

// listLimit is a list's page size: 100 by default, at most 1000.
func listLimit(limit int) int {
	if limit <= 0 {
		return 100
	}
	return min(limit, 1000)
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
