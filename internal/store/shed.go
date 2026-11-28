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
// ordered to remove copies of objects that have verified, undamaged copies
// on other durable stores, at their declared roots, on at least
// minOtherNodes nodes (two stores of one machine are lost with it), the biggest
// first, until what they hold covers the shortfall, shedBatch at a time; not
// a copy a replica or a fetch is reading. It claims each (ClaimShed: the
// record goes after rechecking the other copies and readers under the
// daemon's write lock) before it removes the files; one whose files outlive
// a failed removal is adopted again by the next inventory, and shed again.

const (
	minOtherNodes = 2
	shedBatch     = 50
)

// otherDurable are the durable stores of stores other than id, not being
// moved: their ids, declared roots and nodes, as otherNodesSQL takes them.
func otherDurable(stores []machine.Store, id string) (ids, roots, nodes []string) {
	for _, d := range stores {
		if d.Class == machine.StoreDurable && d.ID != id && d.MovedFrom == "" {
			ids, roots, nodes = append(ids, d.ID), append(roots, d.Root), append(nodes, d.Node)
		}
	}
	return ids, roots, nodes
}

// otherNodesSQL counts the nodes with a verified, undamaged copy of object
// %[1]s on the stores %[2]s, %[3]s and %[4]s say (ids, declared roots,
// nodes: otherDurable): a record of a copy at another root (one declared
// before) is not one.
const otherNodesSQL = `(SELECT COUNT(DISTINCT n.node) FROM object_copies o
	JOIN unnest(%[2]s::text[],%[3]s::text[],%[4]s::text[]) AS n(store_id,root,node) ON n.store_id=o.store_id AND n.root=o.root
	WHERE o.object_id=%[1]s AND o.state='verified' AND o.check_problem IS NULL)`

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
		all := make([]machine.Store, 0, len(byID))
		for _, d := range byID {
			all = append(all, d)
		}
		ids, roots, nodes := otherDurable(all, s.ID)
		err = eachRow(ctx, tx, `SELECT c.object_id,c.month,c.root,COALESCE(d.pack_bytes,0) FROM object_copies c JOIN data_objects d ON d.id=c.object_id
			WHERE c.store_id=$1 AND c.state='verified' AND d.dropped_at IS NULL
			AND `+fmt.Sprintf(otherNodesSQL, "c.object_id", "$2", "$3", "$4")+`>=$5
			AND NOT EXISTS(SELECT 1 FROM object_copies r WHERE r.object_id=c.object_id AND r.state='copying' AND (r.source_store_id=c.store_id OR r.source_store_id IS NULL))
			AND NOT EXISTS(SELECT 1 FROM object_placements p WHERE p.object_id=c.object_id AND p.state='fetching' AND p.source_store_id=c.store_id)
			ORDER BY d.pack_bytes DESC,c.verified_at DESC,c.object_id LIMIT $6`, []any{s.ID, ids, roots, nodes, minOtherNodes, shedBatch}, func(rows *sql.Rows) error {
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
// store storeID; the record goes first, if the object still has verified,
// undamaged copies on other durable stores as declared on minOtherNodes
// nodes and no replica or fetch is reading from it (ErrShedRefused
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
	served := false
	for _, d := range declared {
		if d.Class == machine.StoreDurable && d.MovedFrom == "" && d.ID == storeID {
			served = me.serves(d, executorID)
		}
	}
	ids, roots, nodes := otherDurable(declared, storeID)
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
	if err = tx.QueryRowContext(ctx, `SELECT `+fmt.Sprintf(otherNodesSQL, "$1", "$2", "$3", "$4")+`,
		EXISTS(SELECT 1 FROM object_copies WHERE object_id=$1 AND state='copying' AND (source_store_id=$5 OR source_store_id IS NULL))
		OR EXISTS(SELECT 1 FROM object_placements WHERE object_id=$1 AND state='fetching' AND source_store_id=$5)`,
		objectID, ids, roots, nodes, storeID).Scan(&count, &reading); err != nil {
		return err
	}
	switch {
	case dropped && !identityKept:
		return fmt.Errorf("%w: %s is dropped, but its file list is not kept yet (KeepIdentity)", ErrShedRefused, objectID)
	case !dropped && count < minOtherNodes:
		return fmt.Errorf("%w: %s has good copies on %d other nodes, not %d", ErrShedRefused, objectID, count, minOtherNodes)
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
	if err = appendCoordinationEventTx(ctx, tx, "copy_shed", projectID, "object", objectID, nil, map[string]any{"store_id": storeID, "executor_id": executorID, "other_nodes": count, "dropped": dropped}); err != nil {
		return err
	}
	return tx.Commit()
}
