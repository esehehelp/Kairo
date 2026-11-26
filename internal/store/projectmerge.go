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

// Merging a project into another (kairo project merge; README: Merging
// projects): the repositories of one piece of work may each have declared a
// project of their own, and work reads its inputs (task, data and object
// inputs) only within its project. A merge moves everything of project FROM
// into project INTO, ids unchanged: its tasks (their scopes and names, with
// their executions, dependencies, reruns and decisions), its data objects,
// its events, its declarations and its sources. A queue of FROM whose name
// INTO has too is merged into INTO's (its tasks moved there), others move
// as they are. A task name INTO has too must be given a new name
// (renames); one not given refuses the merge, naming them all. FROM is left
// empty, its project scope kept (its id stays in what referred to it).
//
// It is refused while FROM runs or waits (a live execution, a pending or
// running task) or a gate of FROM or of one of its queues is closed: what
// is moved is then only history and data. Its caches (<store>/cache/<project
// scope id>) are not moved: INTO's own fill again.

// MergeRequest is kairo project merge.
type MergeRequest struct {
	From    string            `json:"from"`
	Into    string            `json:"into"`
	Renames map[string]string `json:"renames,omitempty"`
	DryRun  bool              `json:"dry_run,omitempty"`
	Actor   string            `json:"actor,omitempty"`
}

// MergeResult is what a merge moved (or, with DryRun, would move).
type MergeResult struct {
	From       string            `json:"from"`
	Into       string            `json:"into"`
	Tasks      int64             `json:"tasks"`
	Executions int64             `json:"executions"`
	Objects    int64             `json:"objects"`
	Queues     map[string]string `json:"queues"` // FROM's queue -> merged or moved
	Renamed    map[string]string `json:"renamed,omitempty"`
	// SharedDataNames are data names both projects use: a data input naming
	// one reads the newest object of that name, now of either.
	SharedDataNames []string `json:"shared_data_names,omitempty"`
	DryRun          bool     `json:"dry_run,omitempty"`
}

// ErrMergeRefused: a merge that cannot be made as asked.
var ErrMergeRefused = errors.New("project merge refused")

// MergeProject merges a project into another (see above).
func (s *Store) MergeProject(ctx context.Context, req MergeRequest) (MergeResult, error) {
	out := MergeResult{From: req.From, Into: req.Into, Queues: map[string]string{}, Renamed: map[string]string{}, DryRun: req.DryRun}
	if req.From == "" || req.Into == "" || req.From == req.Into {
		return out, fmt.Errorf("%w: two different projects are needed", ErrMergeRefused)
	}
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	projectID := func(name string) (string, error) {
		var id string
		err := tx.QueryRowContext(ctx, `SELECT id FROM coordination_scopes WHERE kind='project' AND external_key=$1`, name).Scan(&id)
		if errors.Is(err, sql.ErrNoRows) {
			return "", fmt.Errorf("%w: project %s", ErrNotFound, name)
		}
		return id, err
	}
	from, err := projectID(req.From)
	if err != nil {
		return out, err
	}
	into, err := projectID(req.Into)
	if err != nil {
		return out, err
	}
	// nothing of FROM runs or waits, and its gates are open
	var live, active, closed int
	if err = tx.QueryRowContext(ctx, `SELECT
		(SELECT COUNT(*) FROM execution_requests WHERE project_scope_id=$1 AND terminal_at IS NULL),
		(SELECT COUNT(*) FROM orchestration_tasks WHERE project_scope_id=$1 AND state IN('pending','running')),
		(SELECT COUNT(*) FROM admission_gates g JOIN coordination_scopes s ON s.id=g.scope_id WHERE g.state='closed' AND (s.id=$1 OR s.kind='queue' AND s.parent_id=$1))`, from).Scan(&live, &active, &closed); err != nil {
		return out, err
	}
	if live+active+closed > 0 {
		return out, fmt.Errorf("%w: project %s has %d live executions, %d pending or running tasks and %d closed project or queue gates; merge it once none is left",
			ErrMergeRefused, req.From, live, active, closed)
	}
	// task names INTO has too: each renamed, or the merge refused
	taken := map[string]bool{}
	err = eachRow(ctx, tx, `SELECT name FROM orchestration_tasks WHERE project_scope_id=$1`, []any{into}, func(rows *sql.Rows) error {
		var name string
		if err := rows.Scan(&name); err != nil {
			return err
		}
		taken[name] = true
		return nil
	})
	if err != nil {
		return out, err
	}
	type task struct{ scope, name, queue string }
	var tasks []task
	mine := map[string]bool{}
	err = eachRow(ctx, tx, `SELECT t.task_scope_id,t.name,q.id FROM orchestration_tasks t JOIN coordination_scopes ts ON ts.id=t.task_scope_id JOIN coordination_scopes q ON q.id=ts.parent_id
		WHERE t.project_scope_id=$1 ORDER BY t.name`, []any{from}, func(rows *sql.Rows) error {
		var t task
		if err := rows.Scan(&t.scope, &t.name, &t.queue); err != nil {
			return err
		}
		tasks = append(tasks, t)
		mine[t.name] = true
		return nil
	})
	if err != nil {
		return out, err
	}
	var unnamed []string
	for _, t := range tasks {
		if !taken[t.name] {
			continue
		}
		renamed, ok := req.Renames[t.name]
		switch {
		case !ok || renamed == "":
			unnamed = append(unnamed, t.name)
		case taken[renamed] || mine[renamed] || out.Renamed[t.name] != "":
			return out, fmt.Errorf("%w: task %s cannot be renamed %s: a task of either project has that name", ErrMergeRefused, t.name, renamed)
		default:
			out.Renamed[t.name] = renamed
		}
	}
	if len(unnamed) > 0 {
		return out, fmt.Errorf("%w: project %s has tasks of these names too; give each a new name (--rename OLD=NEW): %s", ErrMergeRefused, req.Into, strings.Join(unnamed, ", "))
	}
	for name := range req.Renames {
		if _, ok := out.Renamed[name]; !ok {
			return out, fmt.Errorf("%w: %s is no task of %s that %s has too", ErrMergeRefused, name, req.From, req.Into)
		}
	}
	for _, t := range tasks {
		if renamed, ok := out.Renamed[t.name]; ok {
			// its declaration under its new name, so that declaring it so
			// again is the same task (its digest is its declaration's)
			var body string
			if err = tx.QueryRowContext(ctx, `SELECT spec_json FROM orchestration_tasks WHERE task_scope_id=$1`, t.scope).Scan(&body); err != nil {
				return out, err
			}
			var declared orchestration.Task
			if err = json.Unmarshal([]byte(body), &declared); err != nil {
				return out, fmt.Errorf("task %s: its declaration: %w", t.name, err)
			}
			declared.Name = renamed
			spec, err := json.Marshal(declared)
			if err != nil {
				return out, err
			}
			sum := sha256.Sum256(spec)
			if _, err = tx.ExecContext(ctx, `UPDATE orchestration_tasks SET name=$1,spec_json=$2,spec_digest=$3 WHERE task_scope_id=$4`, renamed, string(spec), hex.EncodeToString(sum[:]), t.scope); err != nil {
				return out, err
			}
			if _, err = tx.ExecContext(ctx, `UPDATE coordination_scopes SET external_key=$1,updated_at=$2 WHERE id=$3`, renamed, now(), t.scope); err != nil {
				return out, err
			}
		}
	}
	// queues: merged into INTO's of the same name, else moved
	type queue struct{ id, name, intoID string }
	var queues []queue
	err = eachRow(ctx, tx, `SELECT q.id,q.external_key,COALESCE(i.id,'') FROM coordination_scopes q
		LEFT JOIN coordination_scopes i ON i.kind='queue' AND i.parent_id=$2 AND i.external_key=q.external_key
		WHERE q.kind='queue' AND q.parent_id=$1 ORDER BY q.external_key`, []any{from, into}, func(rows *sql.Rows) error {
		var q queue
		if err := rows.Scan(&q.id, &q.name, &q.intoID); err != nil {
			return err
		}
		queues = append(queues, q)
		return nil
	})
	if err != nil {
		return out, err
	}
	t := now()
	for _, q := range queues {
		if q.intoID == "" {
			if _, err = tx.ExecContext(ctx, `UPDATE coordination_scopes SET parent_id=$1,updated_at=$2 WHERE id=$3`, into, t, q.id); err != nil {
				return out, err
			}
			out.Queues[q.name] = "moved"
			continue
		}
		if _, err = tx.ExecContext(ctx, `UPDATE coordination_scopes SET parent_id=$1,updated_at=$2 WHERE kind='task' AND parent_id=$3`, q.intoID, t, q.id); err != nil {
			return out, err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE execution_requests SET queue_scope_id=$1 WHERE queue_scope_id=$2`, q.intoID, q.id); err != nil {
			return out, err
		}
		out.Queues[q.name] = "merged"
	}
	// everything of FROM, ids unchanged
	moved := func(query string) (int64, error) {
		result, err := tx.ExecContext(ctx, query, into, from)
		if err != nil {
			return 0, err
		}
		return result.RowsAffected()
	}
	if out.Tasks, err = moved(`UPDATE orchestration_tasks SET project_scope_id=$1 WHERE project_scope_id=$2`); err != nil {
		return out, err
	}
	if out.Executions, err = moved(`UPDATE execution_requests SET project_scope_id=$1 WHERE project_scope_id=$2`); err != nil {
		return out, err
	}
	// (names both use, before they are one project's)
	err = eachRow(ctx, tx, `SELECT DISTINCT a.name FROM data_objects a JOIN data_objects b ON b.name=a.name
		WHERE a.project_scope_id=$1 AND b.project_scope_id=$2 AND a.name IS NOT NULL ORDER BY a.name`, []any{from, into}, func(rows *sql.Rows) error {
		var name string
		if err := rows.Scan(&name); err != nil {
			return err
		}
		out.SharedDataNames = append(out.SharedDataNames, name)
		return nil
	})
	if err != nil {
		return out, err
	}
	if out.Objects, err = moved(`UPDATE data_objects SET project_scope_id=$1 WHERE project_scope_id=$2`); err != nil {
		return out, err
	}
	if _, err = moved(`UPDATE coordination_events SET project_scope_id=$1 WHERE project_scope_id=$2`); err != nil {
		return out, err
	}
	// declarations kept as INTO's; a source INTO has too keeps the newer
	// snapshot
	if _, err = moved(`INSERT INTO project_declarations(project_scope_id,spec_digest,spec_json,applied_at)
		SELECT $1,spec_digest,spec_json,applied_at FROM project_declarations WHERE project_scope_id=$2 ON CONFLICT DO NOTHING`); err != nil {
		return out, err
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM project_declarations WHERE project_scope_id=$1`, from); err != nil {
		return out, err
	}
	if _, err = moved(`UPDATE project_sources i SET snapshot_id=f.snapshot_id,applied_at=f.applied_at FROM project_sources f
		WHERE i.project_scope_id=$1 AND f.project_scope_id=$2 AND f.source_name=i.source_name AND f.applied_at::timestamptz>i.applied_at::timestamptz`); err != nil {
		return out, err
	}
	if _, err = moved(`INSERT INTO project_sources(project_scope_id,source_name,snapshot_id,applied_at)
		SELECT $1,source_name,snapshot_id,applied_at FROM project_sources WHERE project_scope_id=$2 ON CONFLICT DO NOTHING`); err != nil {
		return out, err
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM project_sources WHERE project_scope_id=$1`, from); err != nil {
		return out, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE source_snapshots SET project=$1 WHERE project=$2`, req.Into, req.From); err != nil {
		return out, err
	}
	if err = appendCoordinationEventTx(ctx, tx, "project_merged", &into, "project", into, nil, map[string]any{
		"from": req.From, "from_scope_id": from, "tasks": out.Tasks, "executions": out.Executions, "objects": out.Objects,
		"queues": out.Queues, "renamed": out.Renamed, "actor": req.Actor}); err != nil {
		return out, err
	}
	if req.DryRun {
		return out, nil // rolled back
	}
	return out, tx.Commit()
}
