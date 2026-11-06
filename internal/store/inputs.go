package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"kairo/internal/datainput"
	"kairo/internal/machine"
)

// Inputs (datainput) are what an execution reads, handed to its attempts by
// path as KAIRO_INPUT_<NAME>, never named by a path the task chose:
//   - a task input is the output directory of the latest run of a task of
//     the same project (the declaring task depends on it, so that run has
//     succeeded before this one is planned);
//   - a data input is the newest data object of the project with that name
//     (an imported directory, say); an object input is a data object by id.
//
// For now an input is read in place: a run's output directory that is still
// on its volatile store, or an imported directory still where it was
// imported from. An orchestrated execution with inputs is planned onto the
// node they are on (its executor selector naming no node of its own), and
// once launched each input is checked to be on the attempt's node, in its
// executor's namespace; one that is not (elsewhere, or deleted once copied)
// aborts the launch saying why (launch_aborted: ...): bringing an object
// from a durable store comes next. While a task that is pending or running,
// or an execution not yet ended, names an input, the directory it is read
// from is not deleted once copied (pinnedSourceTx): it is packed and copied
// as usual and deleted once no one waits to read it.

// LaunchInput is an input as an attempt is handed it: the directory it is
// read from (Path), or why it cannot be had here now (Problem).
type LaunchInput struct {
	Name    string `json:"name"`
	Path    string `json:"path,omitempty"`
	Problem string `json:"problem,omitempty"`
}

// inputPlace is where an input can be read in place: on node, by executor
// (a run's output directory: served from store) or, for an imported
// directory, by executors of that namespace (executor its packing one).
type inputPlace struct {
	node, executor, storeID, path string
}

// latestRunTx is the latest run of a task of a project (its newest
// execution that is not a continuation) and the task's state; ErrNotFound
// for a task the project does not declare or one never run.
func latestRunTx(ctx context.Context, q queryer, projectScopeID, task string) (string, error) {
	var runID string
	err := q.QueryRowContext(ctx, `SELECT o.execution_id FROM orchestration_task_executions o JOIN orchestration_tasks t ON t.task_scope_id=o.task_scope_id
		WHERE t.project_scope_id=$1 AND t.name=$2 AND o.reason<>'continuation' ORDER BY o.execution_ordinal DESC LIMIT 1`, projectScopeID, task).Scan(&runID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrNotFound
	}
	return runID, err
}

// inputPlaceTx is where an input of an execution of a project can be read in
// place now, or why it cannot.
func inputPlaceTx(ctx context.Context, q queryer, declared []machine.Store, projectScopeID string, in datainput.Input) (inputPlace, string, error) {
	if in.Task != "" {
		runID, err := latestRunTx(ctx, q, projectScopeID, in.Task)
		if errors.Is(err, ErrNotFound) {
			return inputPlace{}, "task " + in.Task + " has no run", nil
		}
		if err != nil {
			return inputPlace{}, "", err
		}
		return runOutputPlaceTx(ctx, q, declared, runID, "task "+in.Task+"'s latest run "+runID)
	}
	var objectID, kind, sourceID, executorID, root, path, sourceState string
	var storeID string
	query := `SELECT d.id,d.kind,d.source_id,d.executor_id,d.store_id,d.root,d.source_path,d.source_state FROM data_objects d
		WHERE d.project_scope_id=$1 AND d.name=$2 ORDER BY d.created_at DESC,d.id DESC LIMIT 1`
	arg, what := in.Data, "data "+in.Data
	if in.Object != "" {
		query = `SELECT d.id,d.kind,d.source_id,d.executor_id,d.store_id,d.root,d.source_path,d.source_state FROM data_objects d
			WHERE d.project_scope_id=$1 AND d.id=$2`
		arg, what = in.Object, "object "+in.Object
	}
	err := q.QueryRowContext(ctx, query, projectScopeID, arg).Scan(&objectID, &kind, &sourceID, &executorID, &storeID, &root, &path, &sourceState)
	if errors.Is(err, sql.ErrNoRows) {
		return inputPlace{}, "the project has no " + what, nil
	}
	if err != nil {
		return inputPlace{}, "", err
	}
	switch kind {
	case ObjectOutput:
		return runOutputPlaceTx(ctx, q, declared, sourceID, what+" (run "+sourceID+")")
	case ObjectImport:
		if sourceState != "present" && sourceState != SourceKept || sourceState == SourceKept && !importKeptInPlace(ctx, q, objectID) {
			return inputPlace{}, what + " (" + objectID + ") is no longer where it was imported from (" + sourceState + "); bringing it from its durable copy is not done yet", nil
		}
		info, err := executorInfoTx(ctx, q, executorID)
		if err != nil {
			return inputPlace{}, "", err
		}
		if why := importOwnership(declared, info.nodeID, storeID, root, path); why != "" {
			return inputPlace{}, what + " (" + objectID + "): " + why, nil
		}
		return inputPlace{node: info.nodeID, executor: executorID, path: path}, "", nil
	}
	return inputPlace{}, what + " (" + objectID + ") is kept scratch, not data to read", nil
}

// importKeptInPlace reports whether an imported directory settled kept was
// kept where it is by its import (without delete_source), not for having
// changed since it was packed.
func importKeptInPlace(ctx context.Context, q queryer, objectID string) bool {
	var deleteSource bool
	err := q.QueryRowContext(ctx, `SELECT delete_source FROM data_objects WHERE id=$1`, objectID).Scan(&deleteSource)
	return err == nil && !deleteSource
}

// runOutputPlaceTx is where a run's output directory is (rank 0's), while it
// is on a volatile store as the declaration makes it.
func runOutputPlaceTx(ctx context.Context, q queryer, declared []machine.Store, runID, what string) (inputPlace, string, error) {
	var p inputPlace
	var root, state string
	err := q.QueryRowContext(ctx, `SELECT o.store_id,o.root,o.path,o.state,o.executor_id,x.node_id FROM run_outputs o JOIN executors x ON x.id=o.executor_id
		WHERE o.run_id=$1 ORDER BY o.updated_at DESC LIMIT 1`, runID).Scan(&p.storeID, &root, &p.path, &state, &p.executor, &p.node)
	if errors.Is(err, sql.ErrNoRows) {
		return inputPlace{}, what + " has no output directory (its executor served no volatile store)", nil
	}
	if err != nil {
		return inputPlace{}, "", err
	}
	if state != "active" {
		return inputPlace{}, what + "'s output directory is " + state + "; bringing it from its durable copy is not done yet", nil
	}
	if why := outputOwnership(declared, p.node, p.storeID, root, p.path, runID); why != "" {
		return inputPlace{}, what + ": " + why, nil
	}
	return p, "", nil
}

// resolveInputsTx is the inputs of an execution as its attempt on executor
// is handed them: each read in place on the executor's node in its
// namespace, or the problem.
func resolveInputsTx(ctx context.Context, q queryer, executionID, projectScopeID, executorID string, inputsJSON sql.NullString) ([]LaunchInput, error) {
	if !inputsJSON.Valid || inputsJSON.String == "" {
		return nil, nil
	}
	var inputs []datainput.Input
	if err := json.Unmarshal([]byte(inputsJSON.String), &inputs); err != nil {
		return nil, fmt.Errorf("execution %s inputs: %w", executionID, err)
	}
	declared, err := declaredStores(ctx, q)
	if err != nil {
		return nil, err
	}
	me, err := executorInfoTx(ctx, q, executorID)
	if err != nil {
		return nil, err
	}
	out := make([]LaunchInput, 0, len(inputs))
	for _, in := range inputs {
		li := LaunchInput{Name: in.Name}
		p, problem, err := inputPlaceTx(ctx, q, declared, projectScopeID, in)
		if err != nil {
			return nil, err
		}
		switch {
		case problem != "":
			li.Problem = problem
		case p.node != me.nodeID:
			li.Problem = fmt.Sprintf("it is on node %s, not on %s; bringing it here is not done yet", p.node, me.nodeID)
		case !sameNamespace(ctx, q, declared, p, me, executorID):
			li.Problem = fmt.Sprintf("it is in executor %s's namespace, not %s's", p.executor, executorID)
		default:
			li.Path = p.path
		}
		out = append(out, li)
	}
	return out, nil
}

// sameNamespace reports whether executor (me) reaches the path of an input
// as written: a run's output directory on a store it serves, or an imported
// directory of its own or, for an executor not of WSL, of another such of
// its node.
func sameNamespace(ctx context.Context, q queryer, declared []machine.Store, p inputPlace, me executorInfo, executorID string) bool {
	if p.storeID != "" {
		for _, s := range declared {
			if s.ID == p.storeID {
				return me.serves(s, executorID)
			}
		}
		return false
	}
	if p.executor == executorID {
		return true
	}
	other, err := executorInfoTx(ctx, q, p.executor)
	return err == nil && !other.wsl && !me.wsl
}

// pinInputNodeTx plans an orchestrated execution with inputs onto the node
// they are on, unless its executor selector names a node itself or they are
// on more than one (or none: the launch then says why). A gang is not
// pinned: its ranks go on distinct nodes (rank 0 reads the inputs).
func pinInputNodeTx(ctx context.Context, q queryer, project string, spec *ExecutionSpec) error {
	if len(spec.Inputs) == 0 || spec.Gang != nil {
		return nil
	}
	var selector map[string]any
	if err := json.Unmarshal(spec.ExecutorSelector, &selector); err != nil || selector == nil {
		return err
	}
	if node, _ := selector["node"].(string); node != "" {
		return nil
	}
	var projectScopeID string
	if err := q.QueryRowContext(ctx, `SELECT id FROM coordination_scopes WHERE kind='project' AND external_key=$1`, project).Scan(&projectScopeID); err != nil {
		return err
	}
	declared, err := declaredStores(ctx, q)
	if err != nil {
		return err
	}
	nodes := map[string]bool{}
	for _, in := range spec.Inputs {
		p, problem, err := inputPlaceTx(ctx, q, declared, projectScopeID, in)
		if err != nil {
			return err
		}
		if problem == "" {
			nodes[p.node] = true
		}
	}
	if len(nodes) != 1 {
		return nil
	}
	for node := range nodes {
		selector["node"] = node
	}
	body, err := json.Marshal(selector)
	if err != nil {
		return err
	}
	spec.ExecutorSelector = body
	return nil
}

// pinnedSourceTx says why an object's source is not to be deleted yet (""
// when it may be): a task of its project that is pending or running, or an
// execution of it not yet ended, names it as an input (its task's latest
// run's output, its name, or its id).
func pinnedSourceTx(ctx context.Context, q queryer, objectID string) (string, error) {
	var kind, sourceID string
	var projectID, name sql.NullString
	if err := q.QueryRowContext(ctx, `SELECT kind,source_id,project_scope_id,name FROM data_objects WHERE id=$1`, objectID).Scan(&kind, &sourceID, &projectID, &name); err != nil {
		return "", err
	}
	if !projectID.Valid {
		return "", nil
	}
	// each a key and value an input names it by, as the inputs' JSON has them
	refs := []map[string]string{{"object": objectID}}
	if name.Valid && name.String != "" {
		refs = append(refs, map[string]string{"data": name.String})
	}
	if kind == ObjectOutput {
		var task, taskProject string
		err := q.QueryRowContext(ctx, `SELECT t.name,t.project_scope_id FROM orchestration_task_executions o JOIN orchestration_tasks t ON t.task_scope_id=o.task_scope_id
			WHERE o.execution_id=$1`, sourceID).Scan(&task, &taskProject)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return "", err
		}
		if err == nil {
			if latest, err := latestRunTx(ctx, q, taskProject, task); err == nil && latest == sourceID {
				refs = append(refs, map[string]string{"task": task})
			}
		}
	}
	for _, ref := range refs {
		probe, err := json.Marshal([]map[string]string{ref})
		if err != nil {
			return "", err
		}
		var who string
		err = q.QueryRowContext(ctx, `SELECT name FROM orchestration_tasks WHERE project_scope_id=$1 AND state IN('pending','running')
			AND (execution_template_json::jsonb->'inputs') @> $2::jsonb LIMIT 1`, projectID.String, string(probe)).Scan(&who)
		if err == nil {
			return "task " + who + " reads it", nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return "", err
		}
		err = q.QueryRowContext(ctx, `SELECT id FROM execution_requests WHERE project_scope_id=$1 AND state<>'terminal' AND inputs_json IS NOT NULL
			AND inputs_json::jsonb @> $2::jsonb LIMIT 1`, projectID.String, string(probe)).Scan(&who)
		if err == nil {
			return "execution " + who + " reads it", nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return "", err
		}
	}
	return "", nil
}
