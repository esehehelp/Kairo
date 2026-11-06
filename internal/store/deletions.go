package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"kairo/internal/machine"
)

// Once a data object's copy on a durable store is verified, what it was made
// from goes: the run's output directory or the kept scratch, and the pack and
// file list made of it, deleted by the executor that packed it, only while
// the declaration still makes the directory Kairo's. As for scratch, the
// executor first claims the deletion (source_state deleting, taken only while
// it may be deleted now); until it is settled deleted, or kept when the
// deletion is refused for safety, a machine declaration under which the
// directory would no longer be Kairo's is refused. A deletion on a node asks
// its host's controller to trim it (trims.go).

// Source states of a data object (ObjectDeletion.State).
const (
	SourceDeleting = "deleting"
	SourceDeleted  = "deleted"
	SourceKept     = "kept"
)

// DeletionOrder is a source its executor is to delete now: the directory of
// Kind and SourceID under Root of StoreID, and the object's pack and list.
type DeletionOrder struct {
	ObjectID string `json:"object_id"`
	Kind     string `json:"kind"`
	SourceID string `json:"source_id"`
	StoreID  string `json:"store_id"`
	Root     string `json:"root"`
	Path     string `json:"path"`
}

// ObjectDeletion is what an executor did with a source: claimed it
// (SourceDeleting), deleted it, or kept it (Reason: why the deletion was
// refused).
type ObjectDeletion struct {
	ObjectID   string `json:"object_id"`
	ExecutorID string `json:"executor_id"`
	State      string `json:"state"`
	Reason     string `json:"reason,omitempty"`
}

// sourceOwnership is scratchOwnership or outputOwnership by the object's kind.
func sourceOwnership(declared []machine.Store, kind, nodeID, storeID, root, path, sourceID string) string {
	if kind == ObjectScratch {
		return scratchOwnership(declared, nodeID, storeID, root, path, sourceID)
	}
	return outputOwnership(declared, nodeID, storeID, root, path, sourceID)
}

// DeletionsToMake is what an executor is to delete now: the sources of its
// objects with a verified copy that the declaration still makes Kairo's, and
// those it has claimed (the declaration cannot have changed under them).
func (s *Store) DeletionsToMake(ctx context.Context, executorID string) ([]DeletionOrder, error) {
	tx, err := s.beginRead(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	declared, err := declaredStores(ctx, tx)
	if err != nil {
		return nil, err
	}
	out := []DeletionOrder{}
	err = eachRow(ctx, tx, `SELECT d.id,d.kind,d.source_id,d.store_id,d.root,d.source_path,d.source_state,x.node_id FROM data_objects d JOIN executors x ON x.id=d.executor_id
		WHERE d.executor_id=$1 AND d.state='packed' AND d.source_state IN('present','deleting')
		AND EXISTS(SELECT 1 FROM object_copies c WHERE c.object_id=d.id AND c.state='verified') ORDER BY d.packed_at,d.id`, []any{executorID}, func(rows *sql.Rows) error {
		var o DeletionOrder
		var state, nodeID string
		if err := rows.Scan(&o.ObjectID, &o.Kind, &o.SourceID, &o.StoreID, &o.Root, &o.Path, &state, &nodeID); err != nil {
			return err
		}
		if state == SourceDeleting || sourceOwnership(declared, o.Kind, nodeID, o.StoreID, o.Root, o.Path, o.SourceID) == "" {
			out = append(out, o)
		}
		return nil
	})
	return out, err
}

// SettleDeletion records what an executor did with a source: the claim it
// makes before deleting (taken only while it may be deleted now: a verified
// copy, the declaration making it Kairo's), its deletion (only once claimed),
// or keeping it. Settling it again the same way is no error. A deletion marks
// the run's output directory or the scratch deleted and asks for the node to
// be trimmed.
func (s *Store) SettleDeletion(ctx context.Context, settled ObjectDeletion) error {
	switch settled.State {
	case SourceDeleting, SourceDeleted, SourceKept:
	default:
		return fmt.Errorf("a source is claimed deleting, deleted or kept, not %q", settled.State)
	}
	tx, err := s.beginUnlockedWrite(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var executorID, kind, sourceID, storeID, root, path, state, nodeID string
	var projectID sql.NullString
	var bytes sql.NullInt64
	var verified bool
	err = tx.QueryRowContext(ctx, `SELECT d.executor_id,d.kind,d.source_id,d.store_id,d.root,d.source_path,d.source_state,x.node_id,d.project_scope_id,
		COALESCE(d.content_bytes,0)+COALESCE(d.pack_bytes,0),EXISTS(SELECT 1 FROM object_copies c WHERE c.object_id=d.id AND c.state='verified')
		FROM data_objects d JOIN executors x ON x.id=d.executor_id WHERE d.id=$1 FOR UPDATE OF d`, settled.ObjectID).
		Scan(&executorID, &kind, &sourceID, &storeID, &root, &path, &state, &nodeID, &projectID, &bytes, &verified)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if executorID != settled.ExecutorID {
		return fmt.Errorf("object %s is executor %s's, not %s's", settled.ObjectID, executorID, settled.ExecutorID)
	}
	if state == settled.State {
		return tx.Commit()
	}
	switch settled.State {
	case SourceDeleting:
		if state != "present" || !verified {
			return fmt.Errorf("%w: object %s's source is %s, verified copy %v: not to be deleted", ErrStaleEpoch, settled.ObjectID, state, verified)
		}
		declared, err := declaredStores(ctx, tx)
		if err != nil {
			return err
		}
		if why := sourceOwnership(declared, kind, nodeID, storeID, root, path, sourceID); why != "" {
			return fmt.Errorf("%w: object %s's source is not deleted: %s", ErrStaleEpoch, settled.ObjectID, why)
		}
	case SourceDeleted:
		if state != SourceDeleting {
			return fmt.Errorf("object %s's source is deleted only once claimed (%s), not while %s", settled.ObjectID, SourceDeleting, state)
		}
	case SourceKept:
		if state != "present" && state != SourceDeleting {
			return fmt.Errorf("object %s's source is %s", settled.ObjectID, state)
		}
	}
	t := now()
	deletedAt := any(nil)
	if settled.State == SourceDeleted {
		deletedAt = t
	}
	if _, err = tx.ExecContext(ctx, `UPDATE data_objects SET source_state=$1,source_reason=$2,source_deleted_at=$3,updated_at=$4 WHERE id=$5`,
		settled.State, nullable(settled.Reason), deletedAt, t, settled.ObjectID); err != nil {
		return err
	}
	if settled.State == SourceDeleted {
		if kind == ObjectOutput {
			_, err = tx.ExecContext(ctx, `UPDATE run_outputs SET state='deleted',bytes=0,updated_at=$1 WHERE run_id=$2 AND store_id=$3`, t, sourceID, storeID)
		} else {
			_, err = tx.ExecContext(ctx, `UPDATE attempt_scratch SET state='deleted',bytes=0,reason=COALESCE(reason,'')||'; deleted once copied to a durable store',updated_at=$1 WHERE attempt_id=$2`, t, sourceID)
		}
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO node_trims(node_id,requested_at,requested_bytes,updated_at) VALUES($1,$2,$3,$4)
			ON CONFLICT(node_id) DO UPDATE SET requested_at=excluded.requested_at,requested_bytes=node_trims.requested_bytes+excluded.requested_bytes,updated_at=excluded.updated_at`,
			nodeID, t, bytes.Int64, t); err != nil {
			return err
		}
	}
	if settled.State != SourceDeleting {
		var project *string
		if projectID.Valid {
			project = &projectID.String
		}
		if err = appendCoordinationEventTx(ctx, tx, "source_"+settled.State, project, "object", settled.ObjectID, nil, map[string]any{"kind": kind, "source_id": sourceID, "store_id": storeID, "path": path, "reason": settled.Reason}); err != nil {
			return err
		}
	}
	return tx.Commit()
}
