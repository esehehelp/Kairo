package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"kairo/internal/id"
)

func normalizeScopePath(path ScopePath) (ScopePath, error) {
	path.Project = strings.TrimSpace(path.Project)
	path.Queue = strings.TrimSpace(path.Queue)
	path.Task = strings.TrimSpace(path.Task)
	if path.Project == "" {
		return ScopePath{}, errors.New("project scope is required")
	}
	if path.Task != "" && path.Queue == "" {
		return ScopePath{}, errors.New("task scope requires a queue scope")
	}
	return path, nil
}

func (s *Store) EnsureScopePath(ctx context.Context, path ScopePath) (ScopeSet, error) {
	path, err := normalizeScopePath(path)
	if err != nil {
		return ScopeSet{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ScopeSet{}, err
	}
	defer tx.Rollback()
	set, err := ensureScopePathTx(ctx, tx, path)
	if err != nil {
		return ScopeSet{}, err
	}
	return set, tx.Commit()
}

func ensureScopePathTx(ctx context.Context, tx *sql.Tx, path ScopePath) (ScopeSet, error) {
	project, _, err := ensureScopeTx(ctx, tx, "project", nil, path.Project)
	if err != nil {
		return ScopeSet{}, err
	}
	set := ScopeSet{Project: project}
	if path.Queue == "" {
		return set, nil
	}
	queue, _, err := ensureScopeTx(ctx, tx, "queue", &project.ID, path.Queue)
	if err != nil {
		return ScopeSet{}, err
	}
	set.Queue = &queue
	if path.Task == "" {
		return set, nil
	}
	task, _, err := ensureScopeTx(ctx, tx, "task", &queue.ID, path.Task)
	if err != nil {
		return ScopeSet{}, err
	}
	set.Task = &task
	return set, nil
}

func ensureScopeTx(ctx context.Context, tx *sql.Tx, kind string, parentID *string, key string) (Scope, bool, error) {
	scope, err := getScopeByKeyTx(ctx, tx, kind, parentID, key)
	if err == nil {
		return scope, false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Scope{}, false, err
	}
	t := now()
	scope = Scope{ID: id.New("scp"), Kind: kind, ParentID: parentID, ExternalKey: key, Admission: "open", Generation: 1, CreatedAt: t, UpdatedAt: t}
	if _, err = tx.ExecContext(ctx, `INSERT INTO coordination_scopes(id,kind,parent_id,external_key,created_at,updated_at) VALUES(?,?,?,?,?,?)`, scope.ID, kind, parentID, key, t, t); err != nil {
		return Scope{}, false, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO admission_gates(scope_id,state,generation,updated_at) VALUES(?,'open',1,?)`, scope.ID, t); err != nil {
		return Scope{}, false, err
	}
	projectID := scope.ID
	if kind != "project" {
		projectID, err = projectScopeIDTx(ctx, tx, scope.ID)
		if err != nil {
			return Scope{}, false, err
		}
	}
	if err = appendCoordinationEventTx(ctx, tx, "scope_created", &projectID, "scope", scope.ID, nil, map[string]any{"kind": kind, "external_key": key, "parent_id": parentID}); err != nil {
		return Scope{}, false, err
	}
	return scope, true, nil
}

func getScopeByKeyTx(ctx context.Context, tx *sql.Tx, kind string, parentID *string, key string) (Scope, error) {
	var scope Scope
	err := tx.QueryRowContext(ctx, `SELECT s.id,s.kind,s.parent_id,s.external_key,g.state,g.generation,s.created_at,g.updated_at FROM coordination_scopes s JOIN admission_gates g ON g.scope_id=s.id WHERE s.kind=? AND ((? IS NULL AND s.parent_id IS NULL) OR s.parent_id=?) AND s.external_key=?`, kind, parentID, parentID, key).Scan(&scope.ID, &scope.Kind, &scope.ParentID, &scope.ExternalKey, &scope.Admission, &scope.Generation, &scope.CreatedAt, &scope.UpdatedAt)
	return scope, err
}

func (s *Store) GetScope(ctx context.Context, scopeID string) (Scope, error) {
	var scope Scope
	err := s.db.QueryRowContext(ctx, `SELECT s.id,s.kind,s.parent_id,s.external_key,g.state,g.generation,s.created_at,g.updated_at FROM coordination_scopes s JOIN admission_gates g ON g.scope_id=s.id WHERE s.id=?`, scopeID).Scan(&scope.ID, &scope.Kind, &scope.ParentID, &scope.ExternalKey, &scope.Admission, &scope.Generation, &scope.CreatedAt, &scope.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		err = ErrNotFound
	}
	return scope, err
}

func (s *Store) GetScopeByPath(ctx context.Context, path ScopePath) (ScopeSet, error) {
	path, err := normalizeScopePath(path)
	if err != nil {
		return ScopeSet{}, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return ScopeSet{}, err
	}
	defer tx.Rollback()
	project, err := getScopeByKeyTx(ctx, tx, "project", nil, path.Project)
	if errors.Is(err, sql.ErrNoRows) {
		return ScopeSet{}, ErrNotFound
	}
	if err != nil {
		return ScopeSet{}, err
	}
	set := ScopeSet{Project: project}
	if path.Queue != "" {
		queue, err := getScopeByKeyTx(ctx, tx, "queue", &project.ID, path.Queue)
		if errors.Is(err, sql.ErrNoRows) {
			return ScopeSet{}, ErrNotFound
		}
		if err != nil {
			return ScopeSet{}, err
		}
		set.Queue = &queue
		if path.Task != "" {
			task, err := getScopeByKeyTx(ctx, tx, "task", &queue.ID, path.Task)
			if errors.Is(err, sql.ErrNoRows) {
				return ScopeSet{}, ErrNotFound
			}
			if err != nil {
				return ScopeSet{}, err
			}
			set.Task = &task
		}
	}
	return set, tx.Commit()
}

func (s *Store) ListScopes(ctx context.Context, filter ScopeFilter) ([]Scope, error) {
	query := `SELECT DISTINCT s.id,s.kind,s.parent_id,s.external_key,g.state,g.generation,s.created_at,g.updated_at FROM coordination_scopes s JOIN admission_gates g ON g.scope_id=s.id`
	args := make([]any, 0, 3)
	conditions := make([]string, 0, 3)
	if filter.Project != "" {
		query += ` JOIN coordination_scopes p ON p.kind='project' AND p.external_key=? AND (s.id=p.id OR s.parent_id=p.id OR s.parent_id IN(SELECT id FROM coordination_scopes WHERE parent_id=p.id))`
		args = append(args, filter.Project)
	}
	if filter.Kind != "" {
		conditions = append(conditions, `s.kind=?`)
		args = append(args, filter.Kind)
	}
	if filter.ParentID != "" {
		conditions = append(conditions, `s.parent_id=?`)
		args = append(args, filter.ParentID)
	}
	if len(conditions) > 0 {
		query += ` WHERE ` + strings.Join(conditions, ` AND `)
	}
	query += ` ORDER BY CASE s.kind WHEN 'project' THEN 0 WHEN 'queue' THEN 1 ELSE 2 END,s.external_key`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]Scope, 0)
	for rows.Next() {
		var scope Scope
		if err := rows.Scan(&scope.ID, &scope.Kind, &scope.ParentID, &scope.ExternalKey, &scope.Admission, &scope.Generation, &scope.CreatedAt, &scope.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, scope)
	}
	return out, rows.Err()
}

// ExecutionAdmission reports the current effective gate state for an immutable
// execution request. Scheduler launch boundaries must additionally compare the
// generations captured in lease_gate_snapshots.
func (s *Store) ExecutionAdmission(ctx context.Context, executionID string) (bool, []AdmissionGate, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT g.scope_id,g.state,g.generation,g.updated_at
	FROM execution_requests e
	JOIN admission_gates g ON g.scope_id IN(e.project_scope_id,e.queue_scope_id,e.task_scope_id)
	WHERE e.id=? ORDER BY g.scope_id`, executionID)
	if err != nil {
		return false, nil, err
	}
	defer rows.Close()
	open := true
	gates := make([]AdmissionGate, 0, 3)
	for rows.Next() {
		var gate AdmissionGate
		if err := rows.Scan(&gate.ScopeID, &gate.State, &gate.Generation, &gate.UpdatedAt); err != nil {
			return false, nil, err
		}
		if gate.State != "open" {
			open = false
		}
		gates = append(gates, gate)
	}
	if err := rows.Err(); err != nil {
		return false, nil, err
	}
	if len(gates) == 0 {
		return false, nil, ErrNotFound
	}
	return open, gates, nil
}

func projectScopeIDTx(ctx context.Context, tx *sql.Tx, scopeID string) (string, error) {
	current := scopeID
	for depth := 0; depth < 3; depth++ {
		var kind string
		var parent sql.NullString
		if err := tx.QueryRowContext(ctx, `SELECT kind,parent_id FROM coordination_scopes WHERE id=?`, current).Scan(&kind, &parent); err != nil {
			return "", err
		}
		if kind == "project" {
			return current, nil
		}
		if !parent.Valid {
			break
		}
		current = parent.String
	}
	return "", fmt.Errorf("scope %s has invalid ancestry", scopeID)
}

func (s *Store) PauseScope(ctx context.Context, scopeID, actor, requestID string) (PauseOperation, error) {
	actor, requestID = strings.TrimSpace(actor), strings.TrimSpace(requestID)
	if requestID == "" {
		return PauseOperation{}, errors.New("request ID is required")
	}
	if actor == "" {
		actor = "unknown"
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return PauseOperation{}, err
	}
	defer tx.Rollback()
	if operation, err := getPauseOperationByRequestTx(ctx, tx, scopeID, requestID); err == nil {
		return operation, tx.Commit()
	} else if !errors.Is(err, sql.ErrNoRows) {
		return PauseOperation{}, err
	}
	var gate AdmissionGate
	if err = tx.QueryRowContext(ctx, `SELECT scope_id,state,generation,updated_at FROM admission_gates WHERE scope_id=?`, scopeID).Scan(&gate.ScopeID, &gate.State, &gate.Generation, &gate.UpdatedAt); errors.Is(err, sql.ErrNoRows) {
		return PauseOperation{}, ErrNotFound
	} else if err != nil {
		return PauseOperation{}, err
	}
	t := now()
	gateChanged := gate.State == "open"
	if gateChanged {
		gate.State = "closed"
		gate.Generation++
		gate.UpdatedAt = t
		if _, err = tx.ExecContext(ctx, `UPDATE admission_gates SET state='closed',generation=?,updated_at=? WHERE scope_id=?`, gate.Generation, t, scopeID); err != nil {
			return PauseOperation{}, err
		}
	}
	operation := PauseOperation{ID: id.New("pop"), ScopeID: scopeID, ScopeGeneration: gate.Generation, RequestID: requestID, Actor: actor, State: "requested", Detail: json.RawMessage(`{}`), CreatedAt: t, UpdatedAt: t}
	if _, err = tx.ExecContext(ctx, `INSERT INTO pause_operations(id,scope_id,scope_generation,request_id,actor,state,detail_json,created_at,updated_at) VALUES(?,?,?,?,?,'requested','{}',?,?)`, operation.ID, scopeID, gate.Generation, requestID, actor, t, t); err != nil {
		return PauseOperation{}, err
	}
	projectID, err := projectScopeIDTx(ctx, tx, scopeID)
	if err != nil {
		return PauseOperation{}, err
	}
	if gateChanged {
		if err = appendCoordinationEventTx(ctx, tx, "admission_gate_closed", &projectID, "scope", scopeID, nil, map[string]any{"generation": gate.Generation, "actor": actor}); err != nil {
			return PauseOperation{}, err
		}
	}
	if err = appendCoordinationEventTx(ctx, tx, "pause_requested", &projectID, "pause_operation", operation.ID, nil, map[string]any{"scope_id": scopeID, "generation": gate.Generation, "actor": actor}); err != nil {
		return PauseOperation{}, err
	}
	if err = discoverPauseTargetsTx(ctx, tx, operation.ID, scopeID, t); err != nil {
		return PauseOperation{}, err
	}
	return operation, tx.Commit()
}

type AdmissionGate struct {
	ScopeID    string `json:"scope_id"`
	State      string `json:"state"`
	Generation int64  `json:"generation"`
	UpdatedAt  string `json:"updated_at"`
}

func (s *Store) ResumeScope(ctx context.Context, scopeID, actor string) (AdmissionGate, error) {
	actor = strings.TrimSpace(actor)
	if actor == "" {
		actor = "unknown"
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return AdmissionGate{}, err
	}
	defer tx.Rollback()
	var gate AdmissionGate
	if err = tx.QueryRowContext(ctx, `SELECT scope_id,state,generation,updated_at FROM admission_gates WHERE scope_id=?`, scopeID).Scan(&gate.ScopeID, &gate.State, &gate.Generation, &gate.UpdatedAt); errors.Is(err, sql.ErrNoRows) {
		return AdmissionGate{}, ErrNotFound
	} else if err != nil {
		return AdmissionGate{}, err
	}
	if gate.State == "closed" {
		gate.State = "open"
		gate.Generation++
		gate.UpdatedAt = now()
		if _, err = tx.ExecContext(ctx, `UPDATE admission_gates SET state='open',generation=?,updated_at=? WHERE scope_id=?`, gate.Generation, gate.UpdatedAt, scopeID); err != nil {
			return AdmissionGate{}, err
		}
		projectID, projectErr := projectScopeIDTx(ctx, tx, scopeID)
		if projectErr != nil {
			return AdmissionGate{}, projectErr
		}
		if err = appendCoordinationEventTx(ctx, tx, "admission_gate_opened", &projectID, "scope", scopeID, nil, map[string]any{"generation": gate.Generation, "actor": actor}); err != nil {
			return AdmissionGate{}, err
		}
	}
	return gate, tx.Commit()
}

func discoverPauseTargetsTx(ctx context.Context, tx *sql.Tx, operationID, scopeID, stamp string) error {
	_, err := tx.ExecContext(ctx, `WITH RECURSIVE descendants(id) AS (
	 SELECT ? UNION ALL SELECT s.id FROM coordination_scopes s JOIN descendants d ON s.parent_id=d.id
	)
	INSERT INTO pause_targets(operation_id,execution_id,attempt_id,lease_id,state,updated_at)
	SELECT ?,e.id,a.id,l.id,'pending',?
	FROM execution_requests e
	LEFT JOIN attempts a ON a.execution_id=e.id AND a.state!='quiesced'
	LEFT JOIN leases l ON l.execution_id=e.id AND l.state!='released'
	WHERE (e.project_scope_id IN (SELECT id FROM descendants) OR e.queue_scope_id IN (SELECT id FROM descendants) OR e.task_scope_id IN (SELECT id FROM descendants))
	AND (a.id IS NOT NULL OR l.id IS NOT NULL)
	ON CONFLICT(operation_id,execution_id) DO NOTHING`, scopeID, operationID, stamp)
	return err
}

func (s *Store) DiscoverPauseTargets(ctx context.Context, operationID string) ([]PauseTarget, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var scopeID string
	if err = tx.QueryRowContext(ctx, `SELECT scope_id FROM pause_operations WHERE id=?`, operationID).Scan(&scopeID); errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	} else if err != nil {
		return nil, err
	}
	if err = discoverPauseTargetsTx(ctx, tx, operationID, scopeID, now()); err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return s.ListPauseTargets(ctx, operationID)
}

func getPauseOperationByRequestTx(ctx context.Context, tx *sql.Tx, scopeID, requestID string) (PauseOperation, error) {
	var operation PauseOperation
	var detail string
	err := tx.QueryRowContext(ctx, `SELECT id,scope_id,scope_generation,request_id,actor,state,detail_json,created_at,updated_at FROM pause_operations WHERE scope_id=? AND request_id=?`, scopeID, requestID).Scan(&operation.ID, &operation.ScopeID, &operation.ScopeGeneration, &operation.RequestID, &operation.Actor, &operation.State, &detail, &operation.CreatedAt, &operation.UpdatedAt)
	operation.Detail = json.RawMessage(detail)
	return operation, err
}

func (s *Store) GetPauseOperation(ctx context.Context, operationID string) (PauseOperation, error) {
	var operation PauseOperation
	var detail string
	err := s.db.QueryRowContext(ctx, `SELECT id,scope_id,scope_generation,request_id,actor,state,detail_json,created_at,updated_at FROM pause_operations WHERE id=?`, operationID).Scan(&operation.ID, &operation.ScopeID, &operation.ScopeGeneration, &operation.RequestID, &operation.Actor, &operation.State, &detail, &operation.CreatedAt, &operation.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return PauseOperation{}, ErrNotFound
	}
	operation.Detail = json.RawMessage(detail)
	return operation, err
}

func (s *Store) ListPauseOperations(ctx context.Context, filter PauseOperationFilter) ([]PauseOperation, error) {
	query := `SELECT id,scope_id,scope_generation,request_id,actor,state,detail_json,created_at,updated_at FROM pause_operations`
	conditions := make([]string, 0, 2)
	args := make([]any, 0, 3)
	if filter.ScopeID != "" {
		conditions = append(conditions, `scope_id=?`)
		args = append(args, filter.ScopeID)
	}
	if filter.Unfinished {
		conditions = append(conditions, `state IN('requested','quiescing','blocked')`)
	}
	if len(conditions) > 0 {
		query += ` WHERE ` + strings.Join(conditions, ` AND `)
	}
	limit := filter.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	query += ` ORDER BY created_at,id LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]PauseOperation, 0)
	for rows.Next() {
		var operation PauseOperation
		var detail string
		if err := rows.Scan(&operation.ID, &operation.ScopeID, &operation.ScopeGeneration, &operation.RequestID, &operation.Actor, &operation.State, &detail, &operation.CreatedAt, &operation.UpdatedAt); err != nil {
			return nil, err
		}
		operation.Detail = json.RawMessage(detail)
		out = append(out, operation)
	}
	return out, rows.Err()
}

func (s *Store) ListPauseTargets(ctx context.Context, operationID string) ([]PauseTarget, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT operation_id,execution_id,attempt_id,lease_id,command_id,state,blocker_reason,updated_at FROM pause_targets WHERE operation_id=? ORDER BY execution_id`, operationID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]PauseTarget, 0)
	for rows.Next() {
		var target PauseTarget
		if err := rows.Scan(&target.OperationID, &target.ExecutionID, &target.AttemptID, &target.LeaseID, &target.CommandID, &target.State, &target.BlockerReason, &target.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, target)
	}
	return out, rows.Err()
}

func (s *Store) SetPauseOperationState(ctx context.Context, operationID, state string, detail json.RawMessage) error {
	if state != "requested" && state != "quiescing" && state != "quiesced" && state != "blocked" {
		return errors.New("invalid pause operation state")
	}
	if len(detail) == 0 {
		detail = json.RawMessage(`{}`)
	}
	if !json.Valid(detail) {
		return errors.New("pause operation detail must be valid JSON")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var current, currentDetail, scopeID string
	if err = tx.QueryRowContext(ctx, `SELECT state,detail_json,scope_id FROM pause_operations WHERE id=?`, operationID).Scan(&current, &currentDetail, &scopeID); errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	} else if err != nil {
		return err
	}
	if current == state && currentDetail == string(detail) {
		return tx.Commit()
	}
	if _, err = tx.ExecContext(ctx, `UPDATE pause_operations SET state=?,detail_json=?,updated_at=? WHERE id=?`, state, string(detail), now(), operationID); err != nil {
		return err
	}
	if current != state {
		projectID, projectErr := projectScopeIDTx(ctx, tx, scopeID)
		if projectErr != nil {
			return projectErr
		}
		if err = appendCoordinationEventTx(ctx, tx, "pause_"+state, &projectID, "pause_operation", operationID, nil, json.RawMessage(detail)); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) SetPauseTarget(ctx context.Context, operationID, executionID string, update PauseTargetUpdate) error {
	if !stringIn(update.State, "pending", "revoking", "quiescing", "quiesced", "blocked") {
		return errors.New("invalid pause target state")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, `UPDATE pause_targets SET attempt_id=COALESCE(?,attempt_id),lease_id=COALESCE(?,lease_id),command_id=COALESCE(?,command_id),state=?,blocker_reason=?,updated_at=? WHERE operation_id=? AND execution_id=?`, update.AttemptID, update.LeaseID, update.CommandID, update.State, update.BlockerReason, now(), operationID, executionID)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	var projectID string
	if err = tx.QueryRowContext(ctx, `SELECT e.project_scope_id FROM execution_requests e WHERE e.id=?`, executionID).Scan(&projectID); err != nil {
		return err
	}
	if err = appendCoordinationEventTx(ctx, tx, "pause_target_"+update.State, &projectID, "execution", executionID, nil, map[string]any{"pause_operation_id": operationID, "attempt_id": update.AttemptID, "lease_id": update.LeaseID, "command_id": update.CommandID, "blocker_reason": update.BlockerReason}); err != nil {
		return err
	}
	return tx.Commit()
}
