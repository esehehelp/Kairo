package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"kairo/internal/machine"
)

// A durable store below its reserve sheds copies (README: Data stores): when
// its free bytes as last reported, less the copies being made there or
// verified there since, plus the copies shed there since (a report is minutes
// old: without these, every round would see the same shortfall again), fall
// more than shedMinShort under its reserve_free_bytes, its executor is
// ordered to remove copies of objects that have verified, undamaged copies
// on other durable stores, at their declared roots, on at least
// minOtherHosts hosts (machine.Host, as replicas count them: the stores of
// the nodes of one host are lost with it), the biggest first, until what
// they hold covers the shortfall, shedBatch at a time; not a copy a replica
// or a fetch is reading. It claims each (ClaimShed: the record goes after
// rechecking the other copies and readers, the same way, under the daemon's
// write lock) before it removes the files; one whose files outlive a failed
// removal is adopted again by the next inventory, and shed again.
//
// A drained store (machine.Store.Drain) sheds, whatever its room, each copy
// whose object has a verified, undamaged copy on a durable store not drained
// (one is copied there first for an object that has none: orderReplicasTx);
// copies on drained stores count for no other store's shedding.

const (
	minOtherHosts = 2
	shedBatch     = 50
)

// otherDurableTx are the durable stores of declared other than id, neither
// being moved nor drained: their ids, declared roots and hosts
// (durableStoreHostsTx), as otherHostsSQL takes them.
func otherDurableTx(ctx context.Context, q queryer, declared []machine.Store, id string) (ids, roots, hosts []string, err error) {
	hostOf, err := nodeHostsTx(ctx, q)
	if err != nil {
		return nil, nil, nil, err
	}
	for _, d := range declared {
		if d.Class == machine.StoreDurable && d.ID != id && d.MovedFrom == "" && !d.Drain {
			ids, roots, hosts = append(ids, d.ID), append(roots, d.Root), append(hosts, hostOf(d.Node))
		}
	}
	return ids, roots, hosts, nil
}

// otherHostsSQL counts the hosts with a verified, undamaged copy of object
// %[1]s on the stores %[2]s, %[3]s and %[4]s say (ids, declared roots,
// hosts: otherDurableTx), verified or read back since %[5]s (a timestamp;
// -infinity: whenever): a record of a copy at another root (one declared
// before) is not one.
const otherHostsSQL = `(SELECT COUNT(DISTINCT n.host) FROM object_copies o
	JOIN unnest(%[2]s::text[],%[3]s::text[],%[4]s::text[]) AS n(store_id,root,host) ON n.store_id=o.store_id AND n.root=o.root
	WHERE o.object_id=%[1]s AND o.state='verified' AND o.check_problem IS NULL
	AND COALESCE(o.checked_at,o.verified_at)::timestamptz>%[5]s::timestamptz)`

// anyTime: a copy counts however long ago it was last read (otherHostsSQL).
const anyTime = "-infinity"

// drainFresh is since when the copy a drained store's copy leaves in its
// place must have been verified or read back: within a scrub interval, so
// the copy kept is not one a read would find damaged.
func drainFresh(scrub time.Duration) string {
	return time.Now().UTC().Add(-scrub).Format(time.RFC3339Nano)
}

// shedMinShort: a shortfall this small is left alone (a pack being made on
// the same filesystem comes and goes).
var shedMinShort int64 = 1 << 30

// shedOrdersTx are the copies executor me is to remove from the durable
// stores it serves that are below their reserve (targets) or drained.
func shedOrdersTx(ctx context.Context, tx *sql.Tx, me executorInfo, executorID string, targets []machine.Store, byID map[string]machine.Store, room map[string]int64, scrub time.Duration) ([]CopyOrder, error) {
	all := make([]machine.Store, 0, len(byID))
	for _, d := range byID {
		all = append(all, d)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })
	var out []CopyOrder
	// shed orders copies of store s whose objects have good copies (read
	// since since) on need hosts of the other stores, the biggest first,
	// until they cover short
	shed := func(s machine.Store, need int, short int64, since string) error {
		ids, roots, hosts, err := otherDurableTx(ctx, tx, all, s.ID)
		if err != nil {
			return err
		}
		return eachRow(ctx, tx, `SELECT c.object_id,c.month,c.root,COALESCE(d.pack_bytes,0) FROM object_copies c JOIN data_objects d ON d.id=c.object_id
			WHERE c.store_id=$1 AND c.state='verified' AND d.dropped_at IS NULL
			AND `+fmt.Sprintf(otherHostsSQL, "c.object_id", "$2", "$3", "$4", "$7")+`>=$5
			AND NOT EXISTS(SELECT 1 FROM object_copies r WHERE r.object_id=c.object_id AND r.state='copying' AND (r.source_store_id=c.store_id OR r.source_store_id IS NULL))
			AND NOT EXISTS(SELECT 1 FROM object_placements p WHERE p.object_id=c.object_id AND p.state='fetching' AND p.source_store_id=c.store_id)
			ORDER BY d.pack_bytes DESC,c.verified_at DESC,c.object_id LIMIT $6`, []any{s.ID, ids, roots, hosts, need, shedBatch, since}, func(rows *sql.Rows) error {
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
	}
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
		if short := -room[s.ID]; short > shedMinShort {
			if err := shed(s, minOtherHosts, short, anyTime); err != nil {
				return nil, err
			}
		}
	}
	for _, s := range all {
		if s.Class != machine.StoreDurable || !s.Drain || !me.serves(s, executorID) {
			continue
		}
		dropped, err := dropShedOrdersTx(ctx, tx, s)
		if err != nil {
			return nil, err
		}
		out = append(out, dropped...)
		// every copy whose object has a good one, read lately, on a store not
		// drained
		if err := shed(s, 1, math.MaxInt64, drainFresh(scrub)); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// ErrShedRefused: a copy that is not to be removed now.
var ErrShedRefused = errors.New("copy not to be shed")

// ClaimShed: executorID is about to remove its copy of objectID on durable
// store storeID; the record goes first, if the object still has verified,
// undamaged copies on other durable stores as declared, not drained, on
// minOtherHosts hosts (when storeID is drained, one, verified or read back
// within a scrub interval) and no replica or fetch is reading from it
// (ErrShedRefused otherwise: the files are not to be removed).
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
	served, need, since := false, minOtherHosts, anyTime
	for _, d := range declared {
		if d.Class == machine.StoreDurable && d.MovedFrom == "" && d.ID == storeID {
			served = me.serves(d, executorID)
			if d.Drain {
				need, since = 1, drainFresh(s.dataPolicy().ScrubInterval)
			}
		}
	}
	ids, roots, hosts, err := otherDurableTx(ctx, tx, declared, storeID)
	if err != nil {
		return err
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
	if err = tx.QueryRowContext(ctx, `SELECT `+fmt.Sprintf(otherHostsSQL, "$1", "$2", "$3", "$4", "$6")+`,
		EXISTS(SELECT 1 FROM object_copies WHERE object_id=$1 AND state='copying' AND (source_store_id=$5 OR source_store_id IS NULL))
		OR EXISTS(SELECT 1 FROM object_placements WHERE object_id=$1 AND state='fetching' AND source_store_id=$5)`,
		objectID, ids, roots, hosts, storeID, since).Scan(&count, &reading); err != nil {
		return err
	}
	switch {
	case dropped && !identityKept:
		return fmt.Errorf("%w: %s is dropped, but its file list is not kept yet (KeepIdentity)", ErrShedRefused, objectID)
	case !dropped && count < need:
		return fmt.Errorf("%w: %s has good copies on %d hosts besides this one, not %d", ErrShedRefused, objectID, count, need)
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
	if err = appendCoordinationEventTx(ctx, tx, "copy_shed", projectID, "object", objectID, nil, map[string]any{"store_id": storeID, "executor_id": executorID, "other_hosts": count, "dropped": dropped}); err != nil {
		return err
	}
	return tx.Commit()
}
