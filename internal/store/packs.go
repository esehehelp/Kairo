package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"

	"kairo/internal/id"
	"kairo/internal/machine"
)

// What Kairo keeps is packed where it was written, once nothing writes there
// any more: a run's output directory once the run has ended (runEndedTx) and
// the last attempt that got it has quiesced, and a failed attempt's kept
// scratch once that attempt has quiesced (a lost one's processes may still
// write: it waits until it is reconciled). Each becomes a data object
// (data_objects), packed by the executor that made the directory into
// <root>/packs/<object id>.tar.zst with its file list
// <object id>.tar.zst.files.tsv, only while the store's declaration still
// makes the path that directory. Moving the pack to a durable store, and
// deleting what was packed, come next.

// Kinds of data objects (PackOrder.Kind).
const (
	ObjectOutput  = "output"
	ObjectScratch = "scratch"
)

// PackOrder is a directory its executor is to pack now: a run's output
// directory (SourceID the run) or a kept scratch (SourceID the attempt), on
// StoreID at Root. The executor derives the directory from Root, Kind and
// SourceID; Bytes is what reports last measured there.
type PackOrder struct {
	ObjectID string `json:"object_id"`
	Kind     string `json:"kind"`
	SourceID string `json:"source_id"`
	StoreID  string `json:"store_id"`
	Root     string `json:"root"`
	Path     string `json:"path"`
	Bytes    *int64 `json:"bytes,omitempty"`
}

// PackMade is a pack an executor made for an object: where (the pack and its
// file list), how big, what it holds (regular files and their bytes; links
// and special files skipped, not packed) and the sha256 of both files.
type PackMade struct {
	ObjectID       string `json:"object_id"`
	ExecutorID     string `json:"executor_id"`
	PackPath       string `json:"pack_path"`
	ManifestPath   string `json:"manifest_path"`
	PackBytes      int64  `json:"pack_bytes"`
	ContentBytes   int64  `json:"content_bytes"`
	Files          int64  `json:"files"`
	Skipped        int64  `json:"skipped"`
	PackSHA256     string `json:"pack_sha256"`
	ManifestSHA256 string `json:"manifest_sha256"`
}

var rankRun = regexp.MustCompile(`^(.+)-rank[0-9]+$`)

// runEndedTx reports whether a run (runIDTx) has ended: a Task's run once the
// Task is no longer pending or running, or once a later run of it (a rerun)
// was made; a direct execution's once it is terminal. A gang rank's run ends
// with its leader's.
func runEndedTx(ctx context.Context, q queryer, runID string) (bool, error) {
	base := runID
	if m := rankRun.FindStringSubmatch(runID); m != nil {
		base = m[1]
	}
	var taskState sql.NullString
	var ordinal sql.NullInt64
	var executionState string
	err := q.QueryRowContext(ctx, `SELECT e.state,t.state,o.execution_ordinal FROM execution_requests e
		LEFT JOIN orchestration_task_executions o ON o.execution_id=e.id LEFT JOIN orchestration_tasks t ON t.task_scope_id=o.task_scope_id
		WHERE e.id=$1`, base).Scan(&executionState, &taskState, &ordinal)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !ordinal.Valid {
		return executionState == "terminal", nil
	}
	if taskState.String != "pending" && taskState.String != "running" {
		return true, nil
	}
	var later bool
	err = q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM orchestration_task_executions o JOIN orchestration_task_executions b ON b.task_scope_id=o.task_scope_id
		WHERE b.execution_id=$1 AND o.execution_ordinal>b.execution_ordinal AND o.reason<>'continuation')`, base).Scan(&later)
	return later, err
}

// PacksToMake is what an executor is to pack now: the objects it was told to
// pack and has not reported packed, and, made objects now, the directories
// of its that have become due since (the rules above).
func (s *Store) PacksToMake(ctx context.Context, executorID string) ([]PackOrder, error) {
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	declared, err := declaredStores(ctx, tx)
	if err != nil {
		return nil, err
	}
	type due struct {
		kind, sourceID, storeID, root, path, projectID string
	}
	var found []due
	err = eachRow(ctx, tx, `SELECT o.run_id,o.store_id,o.root,o.path,x.node_id,e.project_scope_id
		FROM run_outputs o JOIN attempts a ON a.id=o.last_attempt_id JOIN execution_requests e ON e.id=a.execution_id JOIN executors x ON x.id=o.executor_id
		WHERE o.executor_id=$1 AND o.state='active' AND a.state='quiesced'
		AND NOT EXISTS(SELECT 1 FROM data_objects d WHERE d.kind='output' AND d.source_id=o.run_id AND d.store_id=o.store_id)
		ORDER BY o.created_at,o.run_id`, []any{executorID}, func(rows *sql.Rows) error {
		var d due
		var nodeID string
		if err := rows.Scan(&d.sourceID, &d.storeID, &d.root, &d.path, &nodeID, &d.projectID); err != nil {
			return err
		}
		if outputOwnership(declared, nodeID, d.storeID, d.root, d.path, d.sourceID) == "" {
			d.kind = ObjectOutput
			found = append(found, d)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	err = eachRow(ctx, tx, `SELECT k.attempt_id,k.store_id,k.root,k.path,x.node_id,e.project_scope_id
		FROM attempt_scratch k JOIN attempts a ON a.id=k.attempt_id JOIN execution_requests e ON e.id=a.execution_id JOIN executors x ON x.id=k.executor_id
		WHERE k.executor_id=$1 AND k.state='kept' AND a.state='quiesced'
		AND NOT EXISTS(SELECT 1 FROM data_objects d WHERE d.kind='scratch' AND d.source_id=k.attempt_id AND d.store_id=k.store_id)
		ORDER BY k.settled_at,k.attempt_id`, []any{executorID}, func(rows *sql.Rows) error {
		var d due
		var nodeID string
		if err := rows.Scan(&d.sourceID, &d.storeID, &d.root, &d.path, &nodeID, &d.projectID); err != nil {
			return err
		}
		if scratchOwnership(declared, nodeID, d.storeID, d.root, d.path, d.sourceID) == "" {
			d.kind = ObjectScratch
			found = append(found, d)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	t := now()
	for _, d := range found {
		if d.kind == ObjectOutput {
			ended, err := runEndedTx(ctx, tx, d.sourceID)
			if err != nil {
				return nil, err
			}
			if !ended {
				continue
			}
		}
		objectID := id.New("obj")
		if _, err = tx.ExecContext(ctx, `INSERT INTO data_objects(id,kind,source_id,store_id,executor_id,root,source_path,project_scope_id,state,created_at,updated_at)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,'packing',$9,$10)`, objectID, d.kind, d.sourceID, d.storeID, executorID, d.root, d.path, d.projectID, t, t); err != nil {
			return nil, err
		}
		if err = appendCoordinationEventTx(ctx, tx, "pack_ordered", &d.projectID, "object", objectID, nil, map[string]any{"kind": d.kind, "source_id": d.sourceID, "store_id": d.storeID, "path": d.path}); err != nil {
			return nil, err
		}
	}
	// Every order still to be packed, while the declaration still makes its
	// directory Kairo's on a volatile store (one changed since waits).
	out := []PackOrder{}
	err = eachRow(ctx, tx, `SELECT d.id,d.kind,d.source_id,d.store_id,d.root,d.source_path,x.node_id,
		CASE WHEN d.kind='output' THEN (SELECT o.bytes FROM run_outputs o WHERE o.run_id=d.source_id AND o.store_id=d.store_id)
		ELSE (SELECT k.bytes FROM attempt_scratch k WHERE k.attempt_id=d.source_id) END
		FROM data_objects d JOIN executors x ON x.id=d.executor_id WHERE d.executor_id=$1 AND d.state='packing' ORDER BY d.created_at,d.id`, []any{executorID}, func(rows *sql.Rows) error {
		var o PackOrder
		var nodeID string
		var bytes sql.NullInt64
		if err := rows.Scan(&o.ObjectID, &o.Kind, &o.SourceID, &o.StoreID, &o.Root, &o.Path, &nodeID, &bytes); err != nil {
			return err
		}
		if sourceOwnership(declared, o.Kind, nodeID, o.StoreID, o.Root, o.Path, o.SourceID) != "" {
			return nil
		}
		if bytes.Valid {
			o.Bytes = &bytes.Int64
		}
		out = append(out, o)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, tx.Commit()
}

// RecordPack records the pack an executor made for an object it was told to
// pack: at <root>/packs/<object id>.tar.zst with its file list beside it.
// Recording the same pack again is no error.
func (s *Store) RecordPack(ctx context.Context, made PackMade) error {
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var executorID, root, state, kind, sourceID string
	var projectID sql.NullString
	var packSHA sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT executor_id,root,state,kind,source_id,project_scope_id,pack_sha256 FROM data_objects WHERE id=$1 FOR UPDATE`, made.ObjectID).
		Scan(&executorID, &root, &state, &kind, &sourceID, &projectID, &packSHA)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if executorID != made.ExecutorID {
		return fmt.Errorf("object %s is executor %s's to pack, not %s's", made.ObjectID, executorID, made.ExecutorID)
	}
	if state == "packed" {
		if packSHA.String == made.PackSHA256 {
			return tx.Commit()
		}
		return fmt.Errorf("object %s is already packed (sha256 %s)", made.ObjectID, packSHA.String)
	}
	switch {
	case !machine.IsPackPath(made.PackPath, root, made.ObjectID, ""):
		return fmt.Errorf("pack %s of object %s is not <root>/packs/<object id>.tar.zst under %s", made.PackPath, made.ObjectID, root)
	case !machine.IsPackPath(made.ManifestPath, root, made.ObjectID, ".files.tsv"):
		return fmt.Errorf("file list %s of object %s is not beside its pack", made.ManifestPath, made.ObjectID)
	case !sha256Hex.MatchString(made.PackSHA256) || !sha256Hex.MatchString(made.ManifestSHA256):
		return fmt.Errorf("pack of object %s: a sha256 is not 64 hex digits", made.ObjectID)
	case made.PackBytes <= 0 || made.ContentBytes < 0 || made.Files < 0 || made.Skipped < 0:
		return fmt.Errorf("pack of object %s: impossible sizes", made.ObjectID)
	}
	t := now()
	if _, err = tx.ExecContext(ctx, `UPDATE data_objects SET state='packed',pack_path=$1,manifest_path=$2,content_bytes=$3,files=$4,skipped=$5,pack_bytes=$6,pack_sha256=$7,manifest_sha256=$8,packed_at=$9,updated_at=$10 WHERE id=$11`,
		made.PackPath, made.ManifestPath, made.ContentBytes, made.Files, made.Skipped, made.PackBytes, made.PackSHA256, made.ManifestSHA256, t, t, made.ObjectID); err != nil {
		return err
	}
	var project *string
	if projectID.Valid {
		project = &projectID.String
	}
	if err = appendCoordinationEventTx(ctx, tx, "packed", project, "object", made.ObjectID, nil, map[string]any{"kind": kind, "source_id": sourceID,
		"pack_bytes": made.PackBytes, "content_bytes": made.ContentBytes, "files": made.Files, "skipped": made.Skipped, "pack_sha256": made.PackSHA256}); err != nil {
		return err
	}
	return tx.Commit()
}

var sha256Hex = regexp.MustCompile(`^[0-9a-f]{64}$`)
