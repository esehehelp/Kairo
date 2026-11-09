package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// Making temporal data again (temporal.go): an execution waiting on a node
// for an input that is temporal data not on that node (evicted, or made
// elsewhere) has it made again there by its producing task, once that task
// has succeeded (a task pending or running is waited for; one that failed,
// stopped, was cancelled or blocked is for its project to rerun, nothing is
// retried): the task is rerun (event regeneration_ordered; its rerun request
// "regenerate:<object id>", actor kairo) with its execution planned onto
// that node, and the object remembers the rerun's ordinal
// (regeneration_ordinal). It is ordered once per object.
//
// Each output a producing task's run leaves is compared, once identified,
// with the objects of that task made again by it: its file list against
// each one's (each file's path, size and sha256; not its mtime). One that
// holds the same files is the object made again (regenerated_as; it takes
// the object's name; event regenerated), read in its place by every input
// that names the object, or its name. An output of a run ordered to make an
// object again (an ordinal at or past regeneration_ordinal) that differs
// leaves the object a problem (regeneration_problem; event
// regeneration_differs) its readers wait with; a later run that matches
// clears it. An output of another run (one rerun by hand, say) that holds
// the same files as an evicted object is linked the same way.

// regenerationRequest is the rerun request id ordering object made again.
func regenerationRequest(objectID string) string { return "regenerate:" + objectID }

// orderRegenerations orders made again, on its node, each temporal input an
// execution waits for there and lacks.
func (s *Store) orderRegenerations(ctx context.Context) error {
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	declared, err := declaredStores(ctx, tx)
	if err != nil {
		return err
	}
	type waiting struct{ projectID, selector, inputs string }
	var found []waiting
	err = eachRow(ctx, tx, `SELECT project_scope_id,executor_selector_json,inputs_json FROM execution_requests WHERE state='waiting' AND inputs_json IS NOT NULL ORDER BY submitted_at,id`, nil, func(rows *sql.Rows) error {
		var w waiting
		if err := rows.Scan(&w.projectID, &w.selector, &w.inputs); err != nil {
			return err
		}
		found = append(found, w)
		return nil
	})
	if err != nil {
		return err
	}
	for _, w := range found {
		node := selectorNode(w.selector)
		inputs, err := decodeInputs(w.inputs)
		if node == "" || err != nil {
			continue
		}
		for _, in := range inputs {
			src, err := inputSourceTx(ctx, tx, declared, w.projectID, in)
			if err != nil {
				return err
			}
			if src.problem != "" || !src.temporal || src.objectID == "" || src.on(node) {
				continue
			}
			if err = regenerateTx(ctx, tx, src.objectID, node); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

// regenerateTx reruns the task that makes a temporal object, planned onto
// node, unless it was ordered already or the task has not succeeded (or
// still holds a lease).
func regenerateTx(ctx context.Context, tx *sql.Tx, objectID, node string) error {
	var producer, projectID sql.NullString
	var ordered sql.NullInt64
	if err := tx.QueryRowContext(ctx, `SELECT producer_task_scope_id,regeneration_ordinal,project_scope_id FROM data_objects WHERE id=$1 FOR UPDATE`, objectID).
		Scan(&producer, &ordered, &projectID); err != nil {
		return err
	}
	if !producer.Valid || ordered.Valid {
		return nil
	}
	var state, name string
	if err := tx.QueryRowContext(ctx, `SELECT state,name FROM orchestration_tasks WHERE task_scope_id=$1`, producer.String).Scan(&state, &name); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return err
	}
	if state != "succeeded" {
		return nil
	}
	var held bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM execution_requests e JOIN leases l ON l.execution_id=e.id WHERE e.task_scope_id=$1 AND (e.state<>'terminal' OR l.state<>'released'))`, producer.String).Scan(&held); err != nil || held {
		return err
	}
	if runs, err := producerRunsOnTx(ctx, tx, producer.String, node); err != nil || !runs {
		return err
	}
	ordinal, err := markTaskRerunTx(ctx, tx, producer.String, regenerationRequest(objectID), "kairo")
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE orchestration_task_reruns SET node_id=$1 WHERE task_scope_id=$2 AND execution_ordinal=$3`, node, producer.String, ordinal); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE data_objects SET regeneration_ordinal=$1,regeneration_problem=NULL,updated_at=$2 WHERE id=$3`, ordinal, now(), objectID); err != nil {
		return err
	}
	var project *string
	if projectID.Valid {
		project = &projectID.String
	}
	return appendCoordinationEventTx(ctx, tx, "regeneration_ordered", project, "object", objectID, nil, map[string]any{"task": name, "ordinal": ordinal, "node": node})
}

// producerRunsOnTx reports whether the task that makes a temporal object can
// run on node: an enabled executor there has the executor labels of its
// latest execution (a task never run is given the benefit of the doubt).
// Temporal data is not brought between nodes, so one made where its reader
// cannot make it again (a dataset built from files on one machine only)
// waits, saying so, rather than ordering a rerun that never starts.
func producerRunsOnTx(ctx context.Context, q queryer, producer, node string) (bool, error) {
	var selector string
	err := q.QueryRowContext(ctx, `SELECT executor_selector_json FROM execution_requests WHERE task_scope_id=$1 ORDER BY submitted_at DESC,id DESC LIMIT 1`, producer).Scan(&selector)
	if errors.Is(err, sql.ErrNoRows) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	spec := ExecutionSpec{ExecutorSelector: json.RawMessage(selector)}
	if pinNodeSpec(&spec, node) != nil {
		return true, nil
	}
	rows, err := q.QueryContext(ctx, `SELECT attributes_json FROM executors WHERE node_id=$1 AND enabled`, node)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var attributes string
		if err = rows.Scan(&attributes); err != nil {
			return false, err
		}
		if selectorMatches(string(spec.ExecutorSelector), node, attributes) {
			return true, nil
		}
	}
	return false, rows.Err()
}

// rerunNodeTx is the node a task's rerun at ordinal is to be planned onto
// ("" for none: a rerun asked for by hand).
func rerunNodeTx(ctx context.Context, tx *sql.Tx, taskScopeID string, ordinal int) (string, error) {
	var node sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT node_id FROM orchestration_task_reruns WHERE task_scope_id=$1 AND execution_ordinal=$2`, taskScopeID, ordinal).Scan(&node)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return node.String, err
}

// pinNodeSpec plans spec onto node (its executor selector's node).
func pinNodeSpec(spec *ExecutionSpec, node string) error {
	var selector map[string]any
	if err := json.Unmarshal(spec.ExecutorSelector, &selector); err != nil || selector == nil {
		return fmt.Errorf("executor selector: %v", err)
	}
	selector["node"] = node
	body, err := json.Marshal(selector)
	if err != nil {
		return err
	}
	spec.ExecutorSelector = body
	return nil
}

// listedFile is a file as a file list has it, its mtime aside.
type listedFile struct{ bytes, sha256 string }

// parseFileList reads a file list (path, bytes, sha256, mtime_ns).
func parseFileList(list string) map[string]listedFile {
	out := map[string]listedFile{}
	for i, line := range strings.Split(strings.TrimSuffix(list, "\n"), "\n") {
		fields := strings.Split(line, "\t")
		if i == 0 || len(fields) != 4 {
			continue
		}
		out[fields[0]] = listedFile{fields[1], fields[2]}
	}
	return out
}

// compareFileLists says how list differs from want ("" when it holds the
// same files: paths, sizes and sha256s).
func compareFileLists(want, list string) string {
	a, b := parseFileList(want), parseFileList(list)
	var missing, added, changed int
	first := ""
	note := func(path string) {
		if first == "" || path < first {
			first = path
		}
	}
	for path, f := range a {
		g, ok := b[path]
		switch {
		case !ok:
			missing++
			note(path)
		case g != f:
			changed++
			note(path)
		}
	}
	for path := range b {
		if _, ok := a[path]; !ok {
			added++
			note(path)
		}
	}
	if missing+added+changed == 0 {
		return ""
	}
	return fmt.Sprintf("%d files missing, %d added, %d changed (%s)", missing, added, changed, first)
}

// linkRegenerationsTx compares a producing task's run's output, just
// identified (objectID, its file list list), with the objects that task is
// to make again (see above).
func linkRegenerationsTx(ctx context.Context, tx *sql.Tx, objectID, runID, list string) error {
	var producer sql.NullString
	var ordinal sql.NullInt64
	err := tx.QueryRowContext(ctx, `SELECT d.producer_task_scope_id,o.execution_ordinal FROM data_objects d LEFT JOIN orchestration_task_executions o ON o.execution_id=d.source_id WHERE d.id=$1`, objectID).Scan(&producer, &ordinal)
	if err != nil || !producer.Valid || !ordinal.Valid {
		return err
	}
	var exit sql.NullInt64
	var signal sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT a.exit_code,a.exit_signal FROM run_outputs r JOIN attempts a ON a.id=r.last_attempt_id WHERE r.run_id=$1 ORDER BY r.updated_at DESC LIMIT 1`, runID).Scan(&exit, &signal)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if !exit.Valid || exit.Int64 != 0 || signal.String != "" {
		return nil // a run that did not succeed made nothing again
	}
	type older struct {
		id, manifest, sourceState string
		name, projectID           sql.NullString
		ordered                   sql.NullInt64
	}
	var candidates []older
	err = eachRow(ctx, tx, `SELECT id,COALESCE(manifest,''),source_state,name,project_scope_id,regeneration_ordinal FROM data_objects
		WHERE producer_task_scope_id=$1 AND lifecycle='temporal' AND state='packed' AND regenerated_as IS NULL AND id<>$2
		AND (regeneration_ordinal IS NOT NULL OR source_state IN('evicting','evicted')) ORDER BY created_at,id`, []any{producer.String, objectID}, func(rows *sql.Rows) error {
		var o older
		if err := rows.Scan(&o.id, &o.manifest, &o.sourceState, &o.name, &o.projectID, &o.ordered); err != nil {
			return err
		}
		candidates = append(candidates, o)
		return nil
	})
	if err != nil {
		return err
	}
	t := now()
	for _, o := range candidates {
		var project *string
		if o.projectID.Valid {
			project = &o.projectID.String
		}
		differs := compareFileLists(o.manifest, list)
		if differs == "" {
			if _, err = tx.ExecContext(ctx, `UPDATE data_objects SET regenerated_as=$1,regeneration_problem=NULL,updated_at=$2 WHERE id=$3`, objectID, t, o.id); err != nil {
				return err
			}
			if o.name.Valid && o.name.String != "" {
				if _, err = tx.ExecContext(ctx, `UPDATE data_objects SET name=$1 WHERE id=$2 AND name IS NULL`, o.name.String, objectID); err != nil {
					return err
				}
			}
			if err = appendCoordinationEventTx(ctx, tx, "regenerated", project, "object", o.id, nil, map[string]any{"as": objectID, "run_id": runID}); err != nil {
				return err
			}
			continue
		}
		if !o.ordered.Valid || ordinal.Int64 < o.ordered.Int64 {
			continue
		}
		problem := "made again by run " + runID + ", it differs from what it was: " + differs
		if _, err = tx.ExecContext(ctx, `UPDATE data_objects SET regeneration_problem=$1,updated_at=$2 WHERE id=$3`, problem, t, o.id); err != nil {
			return err
		}
		if err = appendCoordinationEventTx(ctx, tx, "regeneration_differs", project, "object", o.id, nil, map[string]any{"object_id": objectID, "run_id": runID, "differs": differs}); err != nil {
			return err
		}
	}
	return nil
}

// regenerationWaitTx says, of a temporal object not on node, how it is to
// be had there: a problem (hard) its reader cannot wait out, or what it
// waits for.
func regenerationWaitTx(ctx context.Context, q queryer, objectID, node string) (string, bool, error) {
	var producer, problem sql.NullString
	var ordered sql.NullInt64
	err := q.QueryRowContext(ctx, `SELECT producer_task_scope_id,regeneration_problem,regeneration_ordinal FROM data_objects WHERE id=$1`, objectID).Scan(&producer, &problem, &ordered)
	if err != nil {
		return "", false, err
	}
	if problem.Valid {
		return problem.String, true, nil
	}
	if !producer.Valid {
		return "temporal data no task makes again", true, nil
	}
	var state, name string
	if err = q.QueryRowContext(ctx, `SELECT state,name FROM orchestration_tasks WHERE task_scope_id=$1`, producer.String).Scan(&state, &name); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "the task that makes it again is no longer declared", true, nil
		}
		return "", false, err
	}
	if !ordered.Valid {
		runs, err := producerRunsOnTx(ctx, q, producer.String, node)
		if err != nil {
			return "", false, err
		}
		if !runs {
			return "temporal data, which task " + name + " cannot make again on " + node + " (no executor there has its labels) and is not brought between nodes: make it a durable output, or read it where it is made", true, nil
		}
	}
	switch {
	case ordered.Valid && (state == "pending" || state == "running"):
		return "temporal data, being made again on " + node + " by task " + name, false, nil
	case ordered.Valid && state == "succeeded":
		return "temporal data, made again by task " + name + " (its output to be identified)", false, nil
	case state == "pending" || state == "running":
		return "temporal data, made again on " + node + " by task " + name + " once it is no longer " + state, false, nil
	case state == "succeeded":
		return "temporal data, to be made again on " + node + " by task " + name, false, nil
	}
	return "temporal data, made again by task " + name + ", which is " + state + ": rerun it", false, nil
}
