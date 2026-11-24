package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"kairo/internal/machine"
)

// A durable store below its reserve sheds copies (README: Data stores): when
// its free bytes as last reported, less the copies being made there or
// verified there since, plus the copies shed there since (a report is minutes
// old: without these, every round would see the same shortfall again), fall
// more than shedMinShort under its reserve_free_bytes, its executor is
// ordered to remove copies of objects that have at least minOtherCopies
// verified, undamaged copies on other durable stores as declared, the biggest
// first, until what they hold covers the shortfall, shedBatch at a time; not
// a copy a replica or a fetch is reading. It claims each (ClaimShed: the
// record goes after rechecking the other copies and readers under the
// daemon's write lock) before it removes the files; one whose files outlive
// a failed removal is adopted again by the next inventory, and shed again.

const (
	minOtherCopies = 2
	shedBatch      = 50
)

// shedMinShort: a shortfall this small is left alone (a pack being made on
// the same filesystem comes and goes).
var shedMinShort int64 = 1 << 30

// shedOrdersTx are the copies executor me is to remove from the durable
// stores it serves that are below their reserve.
func shedOrdersTx(ctx context.Context, tx *sql.Tx, me executorInfo, executorID string, targets []machine.Store, byID map[string]machine.Store, room map[string]int64) ([]CopyOrder, error) {
	var out []CopyOrder
	for _, s := range targets {
		if !me.serves(s, executorID) {
			continue
		}
		// copies of dropped objects go whatever room there is
		dropped, err := dropShedOrdersTx(ctx, tx, s)
		if err != nil {
			return nil, err
		}
		out = append(out, dropped...)
		short := -room[s.ID]
		if short <= shedMinShort {
			continue
		}
		var others []string
		for _, d := range byID {
			if d.Class == machine.StoreDurable && d.ID != s.ID && d.MovedFrom == "" {
				others = append(others, d.ID)
			}
		}
		err = eachRow(ctx, tx, `SELECT c.object_id,c.month,c.root,COALESCE(d.pack_bytes,0) FROM object_copies c JOIN data_objects d ON d.id=c.object_id
			WHERE c.store_id=$1 AND c.state='verified' AND d.dropped_at IS NULL
			AND (SELECT COUNT(*) FROM object_copies o WHERE o.object_id=c.object_id AND o.store_id=ANY($2::text[]) AND o.state='verified' AND o.check_problem IS NULL)>=$3
			AND NOT EXISTS(SELECT 1 FROM object_copies r WHERE r.object_id=c.object_id AND r.state='copying' AND (r.source_store_id=c.store_id OR r.source_store_id IS NULL))
			AND NOT EXISTS(SELECT 1 FROM object_placements p WHERE p.object_id=c.object_id AND p.state='fetching' AND p.source_store_id=c.store_id)
			ORDER BY d.pack_bytes DESC,c.verified_at DESC,c.object_id LIMIT $4`, []any{s.ID, others, minOtherCopies, shedBatch}, func(rows *sql.Rows) error {
			if short <= 0 {
				return nil
			}
			o := CopyOrder{StoreID: s.ID, Shed: true}
			if err := rows.Scan(&o.ObjectID, &o.Month, &o.Root, &o.PackBytes); err != nil {
				return err
			}
			if o.Root != s.Root {
				return nil
			}
			out = append(out, o)
			short -= o.PackBytes
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// ErrShedRefused: a copy that is not to be removed now.
var ErrShedRefused = errors.New("copy not to be shed")

// ClaimShed: executorID is about to remove its copy of objectID on durable
// store storeID; the record goes first, if the object still has
// minOtherCopies verified, undamaged copies on other durable stores as
// declared and no replica or fetch is reading from it (ErrShedRefused
// otherwise: the files are not to be removed).
func (s *Store) ClaimShed(ctx context.Context, executorID, objectID, storeID string) error {
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	me, err := executorInfoTx(ctx, tx, executorID)
	if err != nil {
		return err
	}
	declared, err := declaredStores(ctx, tx)
	if err != nil {
		return err
	}
	var others []string
	served := false
	for _, d := range declared {
		if d.Class != machine.StoreDurable || d.MovedFrom != "" {
			continue
		}
		if d.ID == storeID {
			served = me.serves(d, executorID)
			continue
		}
		others = append(others, d.ID)
	}
	if !served {
		return fmt.Errorf("%w: executor %s does not serve durable store %s", ErrShedRefused, executorID, storeID)
	}
	var state string
	var projectID *string
	var dropped, identityKept bool
	err = tx.QueryRowContext(ctx, `SELECT c.state,d.project_scope_id,d.dropped_at IS NOT NULL,d.manifest IS NOT NULL FROM object_copies c JOIN data_objects d ON d.id=c.object_id WHERE c.object_id=$1 AND c.store_id=$2 FOR UPDATE OF c`,
		objectID, storeID).Scan(&state, &projectID, &dropped, &identityKept)
	if errors.Is(err, sql.ErrNoRows) {
		return nil // claimed before (a retried call)
	}
	if err != nil {
		return err
	}
	if state != "verified" {
		return fmt.Errorf("%w: the copy of %s on %s is %s", ErrShedRefused, objectID, storeID, state)
	}
	var count int
	var reading bool
	if err = tx.QueryRowContext(ctx, `SELECT
		(SELECT COUNT(*) FROM object_copies WHERE object_id=$1 AND store_id=ANY($2::text[]) AND state='verified' AND check_problem IS NULL),
		EXISTS(SELECT 1 FROM object_copies WHERE object_id=$1 AND state='copying' AND (source_store_id=$3 OR source_store_id IS NULL))
		OR EXISTS(SELECT 1 FROM object_placements WHERE object_id=$1 AND state='fetching' AND source_store_id=$3)`,
		objectID, others, storeID).Scan(&count, &reading); err != nil {
		return err
	}
	switch {
	case dropped && !identityKept:
		return fmt.Errorf("%w: %s is dropped, but its file list is not kept yet (KeepIdentity)", ErrShedRefused, objectID)
	case !dropped && count < minOtherCopies:
		return fmt.Errorf("%w: %s has %d other good copies, not %d", ErrShedRefused, objectID, count, minOtherCopies)
	}
	if reading {
		return fmt.Errorf("%w: a replica or fetch is reading the copy of %s on %s", ErrShedRefused, objectID, storeID)
	}
	if err := checkTransition(recCopy, state, noRecord); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM object_copies WHERE object_id=$1 AND store_id=$2`, objectID, storeID); err != nil {
		return err
	}
	if err = appendCoordinationEventTx(ctx, tx, "copy_shed", projectID, "object", objectID, nil, map[string]any{"store_id": storeID, "executor_id": executorID, "other_copies": count, "dropped": dropped}); err != nil {
		return err
	}
	return tx.Commit()
}
