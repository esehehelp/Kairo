package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// An import not packed yet is abandoned when its source is gone (its
// executor found no such file or directory: AbandonPack) or an operator
// cancels it (kairo data cancel: CancelImport). Nothing packs it any more;
// its source, which was never Kairo's, is settled kept (so the same path can
// be imported again). A pack already being made when it is cancelled is
// still recorded when it is done (RecordPack), and then copied like any.

// ErrNotAbandonable: the object is not an import waiting to be packed.
var ErrNotAbandonable = errors.New("not an import waiting to be packed")

// AbandonPack: executorID was to pack the import objectID and found its
// source gone (reason says how).
func (s *Store) AbandonPack(ctx context.Context, executorID, objectID, reason string) error {
	return s.abandonImport(ctx, objectID, executorID, "source gone: "+reason, "executor "+executorID)
}

// CancelImport: an operator (actor) abandons the import objectID before it is
// packed.
func (s *Store) CancelImport(ctx context.Context, objectID, actor string) error {
	return s.abandonImport(ctx, objectID, "", "cancelled by "+actor, actor)
}

func (s *Store) abandonImport(ctx context.Context, objectID, executorID, reason, actor string) error {
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var kind, state, owner string
	var projectID *string
	err = tx.QueryRowContext(ctx, `SELECT kind,state,executor_id,project_scope_id FROM data_objects WHERE id=$1 FOR UPDATE`, objectID).Scan(&kind, &state, &owner, &projectID)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: object %s", ErrNotFound, objectID)
	}
	if err != nil {
		return err
	}
	switch {
	case executorID != "" && owner != executorID:
		return fmt.Errorf("%w: object %s is packed by executor %s, not %s", ErrNotAbandonable, objectID, owner, executorID)
	case state == "abandoned":
		return tx.Commit()
	case kind != ObjectImport || state != "packing":
		return fmt.Errorf("%w: object %s is a %s object, %s", ErrNotAbandonable, objectID, kind, state)
	}
	t := now()
	if _, err = tx.ExecContext(ctx, `UPDATE data_objects SET state='abandoned',source_state='kept',source_reason=$1,updated_at=$2 WHERE id=$3`, reason, t, objectID); err != nil {
		return err
	}
	if err = appendCoordinationEventTx(ctx, tx, "import_abandoned", projectID, "object", objectID, nil, map[string]any{"reason": reason, "actor": actor}); err != nil {
		return err
	}
	return tx.Commit()
}
