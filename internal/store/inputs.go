package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"kairo/internal/datainput"
	"kairo/internal/machine"
)

// Inputs (datainput) are what an execution reads, handed to its attempts by
// path as KAIRO_INPUT_<NAME>, never named by a path the task chose:
//   - a task input is the output of the latest run of a task of the same
//     project (the declaring task depends on it, so that run has succeeded
//     before this one is planned);
//   - a data input is the newest data object of the project with that name
//     (an imported directory, say); an object input is a data object by id.
//
// An input is read where it is on the node an attempt runs on: a run's
// output directory still on its volatile store, an imported directory still
// where it was imported from, or the object brought to that node
// (placements.go: fetched from its durable copy and unpacked under
// <root>/inputs/<object id>). An execution with inputs is planned onto a
// node (its executor selector naming none of its own): the one every input
// is on, else the one the first is on or was made on; it waits
// (inputs_not_ready) until each is there, those missing being brought there
// meanwhile, and once launched each is checked to be there, in its
// executor's namespace (a launch that finds one not there aborts saying
// why). While a task that is pending or running, or an execution not yet
// ended, names an input, the directory it was made in is not deleted once
// copied (pinnedSourceTx): it is packed and copied as usual and deleted once
// no one waits to read it.

// LaunchInput is an input as an attempt is handed it: the directory it is
// read from (Path), or why it cannot be had here now (Problem).
type LaunchInput struct {
	Name    string `json:"name"`
	Path    string `json:"path,omitempty"`
	Problem string `json:"problem,omitempty"`
}

// inputPlace is where an input can be read: on node, at path; a run's
// output directory or an object brought to a node is on storeID (served by
// whoever serves the store), an imported directory is in the namespace of
// executor (the one that packs it).
type inputPlace struct {
	node, executor, storeID, path string
}

// inputSource is what an input names now: its data object (objectID, ""
// while its run's output is not packed yet; packed once it is) and where it
// can be read (places), or, with none of either, why not (problem).
type inputSource struct {
	objectID string
	packed   bool
	places   []inputPlace
	problem  string
}

// on reports whether the input can be read on node.
func (src inputSource) on(node string) bool {
	for _, p := range src.places {
		if p.node == node {
			return true
		}
	}
	return false
}

// selectorNode is the node an executor selector names ("" for none).
func selectorNode(selector string) string {
	var sel struct {
		Node string `json:"node"`
	}
	_ = json.Unmarshal([]byte(selector), &sel)
	return sel.Node
}

// decodeInputs reads an execution's inputs_json.
func decodeInputs(body string) ([]datainput.Input, error) {
	var inputs []datainput.Input
	if body == "" {
		return nil, nil
	}
	err := json.Unmarshal([]byte(body), &inputs)
	return inputs, err
}

// latestRunTx is the latest run of a task of a project (its newest
// execution that is not a continuation); ErrNotFound for a task the project
// does not declare or one never run.
func latestRunTx(ctx context.Context, q queryer, projectScopeID, task string) (string, error) {
	var runID string
	err := q.QueryRowContext(ctx, `SELECT o.execution_id FROM orchestration_task_executions o JOIN orchestration_tasks t ON t.task_scope_id=o.task_scope_id
		WHERE t.project_scope_id=$1 AND t.name=$2 AND o.reason<>'continuation' ORDER BY o.execution_ordinal DESC LIMIT 1`, projectScopeID, task).Scan(&runID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrNotFound
	}
	return runID, err
}

// inputSourceTx is what an input of an execution of a project names now
// (see inputSource).
func inputSourceTx(ctx context.Context, q queryer, declared []machine.Store, projectScopeID string, in datainput.Input) (inputSource, error) {
	var src inputSource
	what := ""
	runID := ""
	switch {
	case in.Task != "":
		what = "task " + in.Task
		var err error
		runID, err = latestRunTx(ctx, q, projectScopeID, in.Task)
		if errors.Is(err, ErrNotFound) {
			return inputSource{problem: what + " has no run"}, nil
		}
		if err != nil {
			return src, err
		}
		what += "'s latest run " + runID
		var state string
		err = q.QueryRowContext(ctx, `SELECT id,state FROM data_objects WHERE kind='output' AND source_id=$1 ORDER BY created_at DESC LIMIT 1`, runID).Scan(&src.objectID, &state)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return src, err
		}
		src.packed = state == "packed"
	default:
		query := `SELECT id,state FROM data_objects WHERE project_scope_id=$1 AND name=$2 ORDER BY created_at DESC,id DESC LIMIT 1`
		arg := in.Data
		what = "data " + in.Data
		if in.Object != "" {
			query = `SELECT id,state FROM data_objects WHERE project_scope_id=$1 AND id=$2`
			arg, what = in.Object, "object "+in.Object
		}
		var state string
		err := q.QueryRowContext(ctx, query, projectScopeID, arg).Scan(&src.objectID, &state)
		if errors.Is(err, sql.ErrNoRows) {
			return inputSource{problem: "the project has no " + what}, nil
		}
		if err != nil {
			return src, err
		}
		src.packed = state == "packed"
		var kind, sourceID, executorID, storeID, root, path, sourceState string
		var deleteSource bool
		if err = q.QueryRowContext(ctx, `SELECT kind,source_id,executor_id,store_id,root,source_path,source_state,delete_source FROM data_objects WHERE id=$1`, src.objectID).
			Scan(&kind, &sourceID, &executorID, &storeID, &root, &path, &sourceState, &deleteSource); err != nil {
			return src, err
		}
		switch kind {
		case ObjectOutput:
			runID = sourceID
		case ObjectImport:
			// in place while it is where it was imported from (not deleted,
			// nor kept for having changed since it was packed)
			if sourceState == "present" || sourceState == SourceKept && !deleteSource {
				info, err := executorInfoTx(ctx, q, executorID)
				if err != nil {
					return src, err
				}
				if importOwnership(declared, info.nodeID, storeID, root, path) == "" {
					src.places = append(src.places, inputPlace{node: info.nodeID, executor: executorID, path: path})
				}
			}
		default:
			return inputSource{problem: what + " (" + src.objectID + ") is kept scratch, not data to read"}, nil
		}
	}
	if runID != "" {
		if p, ok, err := runOutputPlaceTx(ctx, q, declared, runID); err != nil {
			return src, err
		} else if ok {
			src.places = append(src.places, p)
		}
	}
	if src.objectID != "" {
		err := eachRow(ctx, q, `SELECT p.store_id,p.root,p.path,x.node_id FROM object_placements p JOIN executors x ON x.id=p.executor_id
			WHERE p.object_id=$1 AND p.state='ready' ORDER BY p.store_id`, []any{src.objectID}, func(rows *sql.Rows) error {
			var p inputPlace
			var root string
			if err := rows.Scan(&p.storeID, &root, &p.path, &p.node); err != nil {
				return err
			}
			for _, d := range declared {
				if d.ID == p.storeID && d.Class == machine.StoreVolatile && d.Root == root && d.Node == p.node {
					src.places = append(src.places, p)
				}
			}
			return nil
		})
		if err != nil {
			return src, err
		}
	}
	if len(src.places) == 0 && src.objectID == "" {
		src.problem = what + " left no output here (its executor served no volatile store), nor is it packed yet"
	}
	return src, nil
}

// runOutputPlaceTx is where a run's output directory is (rank 0's), while it
// is on a volatile store as the declaration makes it.
func runOutputPlaceTx(ctx context.Context, q queryer, declared []machine.Store, runID string) (inputPlace, bool, error) {
	var p inputPlace
	var root, state string
	err := q.QueryRowContext(ctx, `SELECT o.store_id,o.root,o.path,o.state,o.executor_id,x.node_id FROM run_outputs o JOIN executors x ON x.id=o.executor_id
		WHERE o.run_id=$1 ORDER BY o.updated_at DESC LIMIT 1`, runID).Scan(&p.storeID, &root, &p.path, &state, &p.executor, &p.node)
	if errors.Is(err, sql.ErrNoRows) {
		return p, false, nil
	}
	if err != nil {
		return p, false, err
	}
	return p, state == "active" && outputOwnership(declared, p.node, p.storeID, root, p.path, runID) == "", nil
}

// resolveInputsTx is the inputs of an execution as its attempt on executor
// is handed them: each read where it is on the executor's node, in its
// namespace, or the problem.
func resolveInputsTx(ctx context.Context, q queryer, executionID, projectScopeID, executorID string, inputsJSON sql.NullString) ([]LaunchInput, error) {
	if !inputsJSON.Valid || inputsJSON.String == "" {
		return nil, nil
	}
	inputs, err := decodeInputs(inputsJSON.String)
	if err != nil {
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
		src, err := inputSourceTx(ctx, q, declared, projectScopeID, in)
		if err != nil {
			return nil, err
		}
		for _, p := range src.places {
			if p.node == me.nodeID && sameNamespace(ctx, q, declared, p, me, executorID) {
				li.Path = p.path
				break
			}
		}
		if li.Path == "" {
			switch {
			case src.problem != "":
				li.Problem = src.problem
			case src.on(me.nodeID):
				li.Problem = "it is on node " + me.nodeID + " in another executor's namespace"
			default:
				li.Problem = "it is not on node " + me.nodeID + " (yet)"
			}
		}
		out = append(out, li)
	}
	return out, nil
}

// sameNamespace reports whether executor (me) reaches the path of an input
// as written: on a store it serves, or an imported directory of its own or,
// for an executor not of WSL, of another such of its node.
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

// inputsNodeTx is the node an execution with inputs is planned onto: the
// first (by id) every input is on, else the first the first input is on,
// else the node its data object was made on ("" when none is known).
func inputsNodeTx(ctx context.Context, q queryer, declared []machine.Store, projectScopeID string, inputs []datainput.Input) (string, error) {
	var sources []inputSource
	for _, in := range inputs {
		src, err := inputSourceTx(ctx, q, declared, projectScopeID, in)
		if err != nil {
			return "", err
		}
		sources = append(sources, src)
	}
	nodes := map[string]bool{}
	for _, p := range sources[0].places {
		nodes[p.node] = true
	}
	var common []string
	for n := range nodes {
		all := true
		for _, src := range sources[1:] {
			all = all && src.on(n)
		}
		if all {
			common = append(common, n)
		}
	}
	sort.Strings(common)
	if len(common) > 0 {
		return common[0], nil
	}
	if len(sources[0].places) > 0 {
		return sources[0].places[0].node, nil
	}
	if sources[0].objectID != "" {
		var node string
		err := q.QueryRowContext(ctx, `SELECT x.node_id FROM data_objects d JOIN executors x ON x.id=d.executor_id WHERE d.id=$1`, sources[0].objectID).Scan(&node)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return "", err
		}
		return node, nil
	}
	return "", nil
}

// pinInputNodeTx plans an execution onto the node of the store it names
// (spec.Store), else, with inputs, onto the node they are on
// (inputsNodeTx), unless its executor selector names a node itself. A gang
// is not pinned: its ranks go on distinct nodes (rank 0 reads the inputs).
func pinInputNodeTx(ctx context.Context, q queryer, project string, spec *ExecutionSpec) error {
	if len(spec.Inputs) == 0 && spec.Store == "" || spec.Gang != nil {
		return nil
	}
	var selector map[string]any
	if err := json.Unmarshal(spec.ExecutorSelector, &selector); err != nil || selector == nil {
		return err
	}
	if node, _ := selector["node"].(string); node != "" {
		return nil
	}
	if spec.Store != "" {
		declared, err := declaredStores(ctx, q)
		if err != nil {
			return err
		}
		for _, d := range declared {
			if d.ID == spec.Store {
				if d.Class != machine.StoreVolatile {
					return nil // its launch is refused, saying why
				}
				selector["node"] = d.Node
				body, err := json.Marshal(selector)
				if err != nil {
					return err
				}
				spec.ExecutorSelector = body
				return nil
			}
		}
		return nil // not declared: its launch is refused, saying why
	}
	var projectScopeID string
	if err := q.QueryRowContext(ctx, `SELECT id FROM coordination_scopes WHERE kind='project' AND external_key=$1`, project).Scan(&projectScopeID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil // a project made by this very submission has no data yet
		}
		return err
	}
	declared, err := declaredStores(ctx, q)
	if err != nil {
		return err
	}
	node, err := inputsNodeTx(ctx, q, declared, projectScopeID, spec.Inputs)
	if err != nil || node == "" {
		return err
	}
	selector["node"] = node
	body, err := json.Marshal(selector)
	if err != nil {
		return err
	}
	spec.ExecutorSelector = body
	return nil
}

// inputsWaitTx says why an execution with inputs is not admitted yet (""
// when it may be): an input that cannot be had at all, or one not on the
// node it is planned onto (yet: those that are packed objects are brought
// there, orderInputFetches).
func inputsWaitTx(ctx context.Context, q queryer, declared []machine.Store, projectScopeID, selector, inputsJSON string) (string, error) {
	inputs, err := decodeInputs(inputsJSON)
	if err != nil {
		return "its inputs cannot be read: " + err.Error(), nil
	}
	node := selectorNode(selector)
	var missing []string
	for _, in := range inputs {
		src, err := inputSourceTx(ctx, q, declared, projectScopeID, in)
		if err != nil {
			return "", err
		}
		switch {
		case src.problem != "":
			return "input " + in.Name + ": " + src.problem, nil
		case node == "" && len(src.places) == 0:
			missing = append(missing, in.Name+" (on no node: name one in its executor selector)")
		case node != "" && !src.on(node) && !src.packed:
			missing = append(missing, in.Name+" (brought to "+node+" once packed)")
		case node != "" && !src.on(node):
			missing = append(missing, in.Name+" (being brought to "+node+")")
		}
	}
	if len(missing) == 0 {
		return "", nil
	}
	return "waiting for its inputs: " + strings.Join(missing, ", "), nil
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

// loadInputsWaits notes, on each candidate with inputs (of ids), why it is
// not admitted yet for them (inputsWaitTx). A gang's inputs are its rank
// 0's, which is not planned onto them: they are not waited for.
func loadInputsWaits(ctx context.Context, tx *sql.Tx, candidates []*planCandidate, ids []string) error {
	inputs := map[string]string{}
	err := eachRow(ctx, tx, `SELECT id,inputs_json FROM execution_requests WHERE id=ANY($1::text[]) AND inputs_json IS NOT NULL`, []any{ids}, func(rows *sql.Rows) error {
		var id, body string
		if err := rows.Scan(&id, &body); err != nil {
			return err
		}
		inputs[id] = body
		return nil
	})
	if err != nil || len(inputs) == 0 {
		return err
	}
	declared, err := declaredStores(ctx, tx)
	if err != nil {
		return err
	}
	for _, c := range candidates {
		body, ok := inputs[c.execution.ID]
		if !ok || c.gangID != "" {
			continue
		}
		if c.inputsWait, err = inputsWaitTx(ctx, tx, declared, c.execution.ProjectScopeID, c.selectorJSON, body); err != nil {
			return err
		}
	}
	return nil
}
