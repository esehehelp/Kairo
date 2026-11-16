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
	err = tx.QueryRowContext(ctx, `SELECT kind,source_id,source_path,source_state,executor_id,project_scope_id FROM data_objects WHERE id=$1`, objectID).
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
		if _, err = tx.ExecContext(ctx, `UPDATE data_objects SET source_state='deleted',source_reason=$1,source_deleted_at=$2,updated_at=$3 WHERE id=$4`,
			"gone from its path (found missing when an attempt was to read it)", t, t, objectID); err != nil {
			return err
		}
		what = "import source"
	default:
		result, err := tx.ExecContext(ctx, `UPDATE run_outputs SET state='missing',missing_at=$1,updated_at=$2 WHERE run_id=$3 AND state='active' AND path=$4
			AND executor_id IN (SELECT id FROM executors WHERE node_id=$5)`, t, t, sourceID, path, me.nodeID)
		if err != nil {
			return err
		}
		if n, _ := result.RowsAffected(); n > 0 {
			what = "run output"
			break
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
