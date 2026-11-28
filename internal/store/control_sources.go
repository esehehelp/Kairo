package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

// Kairo keeps its code snapshots once (README: Kairo keeps itself). A code
// snapshot never changes: the hourly control snapshot (snapshot-<slot>)
// holds the database's dump and the configuration only, and a
// sources-<slot> task, declared while a code snapshot is kept by no run yet,
// takes those that are not (ControlSourcesToCarry). A code snapshot is kept
// by a sources run that succeeded with it: its attempt exited 0 and its
// output is packed (or being packed), never thinned; or by one still
// running. A run that failed keeps nothing: the next takes its code
// snapshots again.

// Control task names: the hourly snapshot and the code's.
const (
	controlSnapshotPrefix = "snapshot-"
	controlSourcesPrefix  = "sources-"
)

// ControlPart is what a control-snapshot task's attempt is handed: the
// database and configuration (ControlPartSnapshot) or the code snapshots no
// run keeps yet (ControlPartSources).
const (
	ControlPartSnapshot = "snapshot"
	ControlPartSources  = "sources"
)

// keptSourcesSQL are the code snapshots a sources run keeps (or is taking).
const keptSourcesSQL = `SELECT r.source_id FROM control_source_runs r JOIN execution_requests e ON e.id=r.execution_id
	WHERE e.state<>'terminal' OR (EXISTS(SELECT 1 FROM attempts a WHERE a.execution_id=e.id AND a.exit_code=0)
		AND EXISTS(SELECT 1 FROM data_objects d WHERE d.kind='output' AND d.source_id=e.id AND d.dropped_at IS NULL AND d.state IN('packing','packed')))`

// unkeptSourcesTx are the code snapshots no sources run keeps yet, oldest
// first.
func unkeptSourcesTx(ctx context.Context, q queryer) ([]string, error) {
	var out []string
	err := eachRow(ctx, q, `SELECT id FROM source_snapshots WHERE id NOT IN(`+keptSourcesSQL+`) ORDER BY created_at,id`, nil, func(rows *sql.Rows) error {
		var id string
		if err := rows.Scan(&id); err != nil {
			return err
		}
		out = append(out, id)
		return nil
	})
	return out, err
}

// ControlPartOf is what the attempt of a control-snapshot task is handed,
// by its task's name, and its execution.
func (s *Store) ControlPartOf(ctx context.Context, attemptID string) (part, executionID string, err error) {
	var task string
	err = s.db.QueryRowContext(ctx, `SELECT e.id,COALESCE(t.external_key,'') FROM attempts a JOIN execution_requests e ON e.id=a.execution_id
		LEFT JOIN coordination_scopes t ON t.id=e.task_scope_id WHERE a.id=$1`, attemptID).Scan(&executionID, &task)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", ErrNotFound
	}
	if err != nil {
		return "", "", err
	}
	if strings.HasPrefix(task, controlSourcesPrefix) {
		return ControlPartSources, executionID, nil
	}
	return ControlPartSnapshot, executionID, nil
}

// ControlSourcesToCarry are the code snapshots the sources run
// executionID takes: those no run keeps, recorded as its own (a run that
// asks again is handed what it was).
func (s *Store) ControlSourcesToCarry(ctx context.Context, executionID string) ([]string, error) {
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var mine []string
	if err := eachRow(ctx, tx, `SELECT source_id FROM control_source_runs WHERE execution_id=$1 ORDER BY source_id`, []any{executionID}, func(rows *sql.Rows) error {
		var id string
		if err := rows.Scan(&id); err != nil {
			return err
		}
		mine = append(mine, id)
		return nil
	}); err != nil {
		return nil, err
	}
	if len(mine) > 0 {
		return mine, tx.Commit()
	}
	ids, err := unkeptSourcesTx(ctx, tx)
	if err != nil {
		return nil, err
	}
	for _, id := range ids {
		if _, err := tx.ExecContext(ctx, `INSERT INTO control_source_runs(execution_id,source_id) VALUES($1,$2) ON CONFLICT DO NOTHING`, executionID, id); err != nil {
			return nil, err
		}
	}
	return ids, tx.Commit()
}

// ControlSourcesTask declares the sources task of the slot at (sources-<UTC
// time>) when a code snapshot is kept by no run and no sources run is under
// way: its name, or "" when none is declared.
func (s *Store) ControlSourcesTask(ctx context.Context, at time.Time, every time.Duration) (string, error) {
	if every <= 0 {
		return "", errors.New("a control snapshot interval must be positive")
	}
	tx, err := s.beginRead(ctx)
	if err != nil {
		return "", err
	}
	unkept, err := unkeptSourcesTx(ctx, tx)
	if err != nil || len(unkept) == 0 {
		tx.Rollback()
		return "", err
	}
	var running bool
	err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM orchestration_tasks t JOIN coordination_scopes p ON p.id=t.project_scope_id
		WHERE p.kind='project' AND p.external_key=$1 AND starts_with(t.name,$2) AND t.state IN('pending','running'))`, ControlProject, controlSourcesPrefix).Scan(&running)
	tx.Rollback()
	if err != nil || running {
		return "", err
	}
	return s.declareControlTask(ctx, controlSourcesPrefix+at.UTC().Truncate(every).Format("20060102T1504Z"))
}

// Control snapshot retention: every one of the last day, then the last of
// each day for two weeks, then the last of each week.
const (
	controlKeepHourly = 24 * time.Hour
	controlKeepDaily  = 14 * 24 * time.Hour
)

// controlKept are the snapshots of created (RFC 3339, any order) kept at
// now: every one within controlKeepHourly, the latest of each UTC day within
// controlKeepDaily, the latest of each ISO week before, and the latest of
// all.
func controlKept(created map[string]time.Time, now time.Time) map[string]bool {
	ids := make([]string, 0, len(created))
	for id := range created {
		ids = append(ids, id)
	}
	// newest first: the first of a day or week seen is its latest
	sort.Slice(ids, func(i, j int) bool {
		if !created[ids[i]].Equal(created[ids[j]]) {
			return created[ids[i]].After(created[ids[j]])
		}
		return ids[i] > ids[j]
	})
	keep := map[string]bool{}
	days, weeks := map[string]bool{}, map[string]bool{}
	for i, id := range ids {
		t := created[id]
		age := now.Sub(t)
		switch {
		case i == 0 || age < controlKeepHourly:
			keep[id] = true
		case age < controlKeepDaily:
			day := t.UTC().Format("2006-01-02")
			if !days[day] {
				days[day], keep[id] = true, true
			}
		default:
			year, week := t.UTC().ISOWeek()
			key := fmt.Sprintf("%d-%02d", year, week)
			if !weeks[key] {
				weeks[key], keep[id] = true, true
			}
		}
	}
	return keep
}

// ThinControlSnapshots drops the outputs of the hourly control snapshots
// (snapshot-<slot>) that controlKept does not keep, nor is the base of the
// dump of one it keeps (a delta is rebuilt from its base: control_dumps.go),
// once every code snapshot is kept by a sources run (until then an older
// snapshot, which held all of them, may be the only place one is): the
// objects dropped.
func (s *Store) ThinControlSnapshots(ctx context.Context, now time.Time) ([]DroppedObject, error) {
	unkept, err := unkeptSourcesTx(ctx, s.db)
	if err != nil || len(unkept) > 0 {
		return nil, err
	}
	created := map[string]time.Time{}
	// each snapshot's run, and the run of its dump's base (a delta's)
	runOf, baseOf, objectOf := map[string]string{}, map[string]string{}, map[string]string{}
	err = eachRow(ctx, s.db, `SELECT d.id,d.created_at,e.id,COALESCE(c.base_execution_id,'') FROM data_objects d JOIN coordination_scopes p ON p.id=d.project_scope_id
		JOIN execution_requests e ON e.id=d.source_id JOIN coordination_scopes t ON t.id=e.task_scope_id
		LEFT JOIN control_dumps c ON c.execution_id=e.id
		WHERE p.kind='project' AND p.external_key=$1 AND d.kind='output' AND d.dropped_at IS NULL AND d.state='packed' AND starts_with(t.external_key,$2)`,
		[]any{ControlProject, controlSnapshotPrefix}, func(rows *sql.Rows) error {
			var id, at, run, base string
			if err := rows.Scan(&id, &at, &run, &base); err != nil {
				return err
			}
			runOf[id], objectOf[run] = run, id
			if base != "" {
				baseOf[id] = base
			}
			t, err := time.Parse(time.RFC3339Nano, at)
			if err != nil {
				return fmt.Errorf("object %s: created %q: %w", id, at, err)
			}
			created[id] = t
			return nil
		})
	if err != nil {
		return nil, err
	}
	keep := controlKept(created, now)
	for id := range keep {
		if base, ok := baseOf[id]; ok {
			if object, ok := objectOf[base]; ok {
				keep[object] = true
			}
		}
	}
	var drop []string
	for id := range created {
		if !keep[id] {
			drop = append(drop, id)
		}
	}
	if len(drop) == 0 {
		return nil, nil
	}
	sort.Strings(drop)
	return s.DropData(ctx, DropRequest{Project: ControlProject, Objects: drop, Reason: "control snapshot thinned (README: Kairo keeps itself)", Confirm: true}, "kairo")
}
