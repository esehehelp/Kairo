package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"kairo/internal/machine"
)

// InputGone: executorID was about to launch an attempt reading object
// objectID at path, and path is not there (store/inputs.go reads an input in
// place while its record says it is there; something outside Kairo deleted
// it). What the record said is put right, so that the next plan brings the
// object from a durable copy instead: an imported source settled deleted
// ("gone from its path"), a run's output directory missing, a brought copy
// forgotten (fetched again). A path that is none of these changes nothing.
func (s *Store) InputGone(ctx context.Context, executorID, objectID, path string) error {
	if objectID == "" {
		return nil
	}
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	me, err := executorInfoTx(ctx, tx, executorID)
	if err != nil {
		return err
	}
	var kind, sourceID, sourcePath, sourceState, owner string
	var projectID sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT kind,source_id,source_path,source_state,executor_id,project_scope_id FROM data_objects WHERE id=$1 FOR UPDATE`, objectID).
		Scan(&kind, &sourceID, &sourcePath, &sourceState, &owner, &projectID)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: object %s", ErrNotFound, objectID)
	}
	if err != nil {
		return err
	}
	t := now()
	what := ""
	same := func(a, b string) bool { return machine.StoreRootKey(a) == machine.StoreRootKey(b) }
	switch {
	case kind == ObjectImport && same(path, sourcePath) && (sourceState == "present" || sourceState == SourceKept):
		ownerInfo, err := executorInfoTx(ctx, tx, owner)
		if err != nil {
			return err
		}
		if ownerInfo.nodeID != me.nodeID {
			return fmt.Errorf("object %s's source is on node %s, not %s", objectID, ownerInfo.nodeID, me.nodeID)
		}
		if err := checkTransition(recSource, sourceState, SourceDeleted); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE data_objects SET source_state='deleted',source_reason=$1,source_deleted_at=$2,updated_at=$3 WHERE id=$4`,
			"gone from its path (found missing when an attempt was to read it)", t, t, objectID); err != nil {
			return err
		}
		what = "import source"
	default:
		if err := checkTransition(recRunOutput, "active", "missing"); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `UPDATE run_outputs SET state='missing',missing_at=$1,updated_at=$2 WHERE run_id=$3 AND state='active' AND path=$4
			AND executor_id IN (SELECT id FROM executors WHERE node_id=$5)`, t, t, sourceID, path, me.nodeID)
		if err != nil {
			return err
		}
		if n, _ := result.RowsAffected(); n > 0 {
			what = "run output"
			break
		}
		if err := checkTransition(recPlacement, "ready", noRecord); err != nil {
			return err
		}
		result, err = tx.ExecContext(ctx, `DELETE FROM object_placements WHERE object_id=$1 AND path=$2 AND state='ready'
			AND executor_id IN (SELECT id FROM executors WHERE node_id=$3)`, objectID, path, me.nodeID)
		if err != nil {
			return err
		}
		if n, _ := result.RowsAffected(); n > 0 {
			what = "brought copy"
		}
	}
	if what == "" {
		return tx.Commit()
	}
	var project *string
	if projectID.Valid {
		project = &projectID.String
	}
	if err = appendCoordinationEventTx(ctx, tx, "input_gone", project, "object", objectID, nil, map[string]any{"path": path, "what": what, "executor_id": executorID}); err != nil {
		return err
	}
	return tx.Commit()
}

// InPlaceSource is a path Kairo's records say an object can be read at, on
// the node of the executor asking (InPlaceSources).
type InPlaceSource struct {
	ObjectID string `json:"object_id"`
	Path     string `json:"path"`
}

// InPlaceSources are the paths on executorID's node that inputs are read in
// place at: imported sources still settled there, the output directories of
// packed runs, brought copies; for its executor to check they are still
// there (InputGone for those that are not).
func (s *Store) InPlaceSources(ctx context.Context, executorID string) ([]InPlaceSource, error) {
	me, err := executorInfoTx(ctx, s.db, executorID)
	if err != nil {
		return nil, err
	}
	var out []InPlaceSource
	err = eachRow(ctx, s.db, `SELECT d.id,d.source_path FROM data_objects d JOIN executors x ON x.id=d.executor_id
		WHERE x.node_id=$1 AND d.kind='import' AND (d.source_state='present' OR d.source_state='kept' AND NOT d.delete_source) AND NOT d.source_file
		UNION ALL SELECT d.id,o.path FROM run_outputs o JOIN executors x ON x.id=o.executor_id JOIN data_objects d ON d.kind='output' AND d.source_id=o.run_id AND d.store_id=o.store_id
		WHERE x.node_id=$1 AND o.state='active'
		UNION ALL SELECT p.object_id,p.path FROM object_placements p JOIN executors x ON x.id=p.executor_id WHERE x.node_id=$1 AND p.state='ready'`,
		[]any{me.nodeID}, func(rows *sql.Rows) error {
			var src InPlaceSource
			if err := rows.Scan(&src.ObjectID, &src.Path); err != nil {
				return err
			}
			out = append(out, src)
			return nil
		})
	return out, err
}

// InputIsFile: executorID found that the source of import objectID, read in
// place at path on its node, is a regular file and not a directory (an
// import of one file packed before such imports were marked, issue #27).
// An input is a directory: the import is marked a file (source_file), so
// the next plan brings it, a directory holding the file, instead of handing
// over the file itself. Anything but such an import changes nothing.
func (s *Store) InputIsFile(ctx context.Context, executorID, objectID, path string) error {
	if objectID == "" {
		return nil
	}
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	me, err := executorInfoTx(ctx, tx, executorID)
	if err != nil {
		return err
	}
	var kind, sourcePath, owner string
	var sourceFile bool
	var projectID sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT kind,source_path,executor_id,source_file,project_scope_id FROM data_objects WHERE id=$1`, objectID).
		Scan(&kind, &sourcePath, &owner, &sourceFile, &projectID)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: object %s", ErrNotFound, objectID)
	}
	if err != nil {
		return err
	}
	if kind != ObjectImport || sourceFile || machine.StoreRootKey(path) != machine.StoreRootKey(sourcePath) {
		return tx.Commit()
	}
	ownerInfo, err := executorInfoTx(ctx, tx, owner)
	if err != nil {
		return err
	}
	if ownerInfo.nodeID != me.nodeID {
		return fmt.Errorf("object %s's source is on node %s, not %s", objectID, ownerInfo.nodeID, me.nodeID)
	}
	if _, err = tx.ExecContext(ctx, `UPDATE data_objects SET source_file=true,updated_at=$1 WHERE id=$2`, now(), objectID); err != nil {
		return err
	}
	var project *string
	if projectID.Valid {
		project = &projectID.String
	}
	if err = appendCoordinationEventTx(ctx, tx, "input_is_file", project, "object", objectID, nil, map[string]any{"path": path, "executor_id": executorID}); err != nil {
		return err
	}
	return tx.Commit()
}
