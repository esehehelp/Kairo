package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"time"

	"kairo/internal/machine"
)

// Evicting: a volatile store whose free space, as its executor last
// reported it, is below its floor (its declared reserve_free_bytes, else a
// tenth of its total) has things evicted until it would be above it: what
// was brought there (placements.go: it can be brought again from its
// durable copy) first, then temporal data (temporal.go: it can be made
// again by its task), each least recently used first (the later of when it
// was last handed to an attempt and when it was made or brought). Nothing is evicted that an execution not yet
// ended, or a task pending or running, names as an input (pinnedSourceTx).
// The executor serving the store claims each (evicting) as it is ordered,
// deletes it (a temporal directory only if it still holds just what its
// identity lists) and settles it: evicted, or kept with why (and not
// ordered again for evictRetryAfter).

// evictRetryAfter is how long a temporal object kept instead of evicted is
// not ordered evicted again.
const evictRetryAfter = time.Hour

// Kinds of eviction orders.
const (
	EvictPlacement = "placement"
	EvictTemporal  = "temporal"
)

// EvictionOrder is something its executor is to evict from StoreID at Root:
// an object brought there (Kind placement, under <root>/inputs/<object id>)
// or a temporal object's directory (Kind temporal: a run's output directory,
// ObjectKind output and SourceID the run, or an imported directory at
// Path).
type EvictionOrder struct {
	ObjectID   string `json:"object_id"`
	Kind       string `json:"kind"`
	ObjectKind string `json:"object_kind,omitempty"`
	SourceID   string `json:"source_id,omitempty"`
	StoreID    string `json:"store_id"`
	Root       string `json:"root"`
	Path       string `json:"path,omitempty"`
	Bytes      int64  `json:"bytes"`
}

// EvictionSettled is what an executor did with an eviction order: evicted
// it, or kept it (Reason: why it was not deleted).
type EvictionSettled struct {
	ObjectID   string `json:"object_id"`
	Kind       string `json:"kind"`
	StoreID    string `json:"store_id"`
	ExecutorID string `json:"executor_id"`
	Evicted    bool   `json:"evicted"`
	Reason     string `json:"reason,omitempty"`
}

// evictionFloor is the free space a volatile store keeps by evicting.
func evictionFloor(s machine.Store, total int64) int64 {
	if s.ReserveFreeBytes > 0 {
		return s.ReserveFreeBytes
	}
	return total / 10
}

// EvictionsToMake is what an executor is to evict now: those it was ordered
// and has not settled, then, for each volatile store it serves that is
// below its floor, enough more to bring it back above it.
func (s *Store) EvictionsToMake(ctx context.Context, executorID string) ([]EvictionOrder, error) {
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	me, err := executorInfoTx(ctx, tx, executorID)
	if err != nil {
		return nil, err
	}
	declared, err := declaredStores(ctx, tx)
	if err != nil {
		return nil, err
	}
	out := []EvictionOrder{}
	for _, d := range declared {
		if d.Class != machine.StoreVolatile || !me.serves(d, executorID) {
			continue
		}
		claimed, err := evictionsOnStoreTx(ctx, tx, d, executorID, true)
		if err != nil {
			return nil, err
		}
		out = append(out, claimed...)
		var total, free sql.NullInt64
		err = tx.QueryRowContext(ctx, `SELECT total_bytes,free_bytes FROM store_observations WHERE store_id=$1 AND usable`, d.ID).Scan(&total, &free)
		if errors.Is(err, sql.ErrNoRows) || err == nil && (!total.Valid || !free.Valid) {
			continue
		}
		if err != nil {
			return nil, err
		}
		need := evictionFloor(d, total.Int64) - free.Int64
		for _, o := range claimed {
			need -= o.Bytes
		}
		if need <= 0 {
			continue
		}
		candidates, err := evictionsOnStoreTx(ctx, tx, d, executorID, false)
		if err != nil {
			return nil, err
		}
		t := now()
		for _, o := range candidates {
			if need <= 0 {
				break
			}
			// a temporal directory only while the declaration makes it Kairo's
			if o.Kind == EvictTemporal && sourceOwnership(declared, o.ObjectKind, me.nodeID, d.ID, d.Root, o.Path, o.SourceID) != "" {
				continue
			}
			pinned, err := pinnedSourceTx(ctx, tx, o.ObjectID)
			if err != nil {
				return nil, err
			}
			if pinned != "" {
				continue
			}
			var claim sql.Result
			if o.Kind == EvictPlacement {
				claim, err = tx.ExecContext(ctx, `UPDATE object_placements SET state='evicting',updated_at=$1 WHERE object_id=$2 AND store_id=$3 AND state='ready'`, t, o.ObjectID, d.ID)
			} else {
				claim, err = tx.ExecContext(ctx, `UPDATE data_objects SET source_state='evicting',updated_at=$1 WHERE id=$2 AND source_state='present'`, t, o.ObjectID)
			}
			if err != nil {
				return nil, err
			}
			if n, _ := claim.RowsAffected(); n != 1 {
				continue
			}
			if err = appendCoordinationEventTx(ctx, tx, "eviction_ordered", nil, "object", o.ObjectID, nil, map[string]any{"kind": o.Kind, "store_id": d.ID, "bytes": o.Bytes}); err != nil {
				return nil, err
			}
			need -= o.Bytes
			out = append(out, o)
		}
	}
	return out, tx.Commit()
}

// evictionsOnStoreTx lists what is on store d that executor evicts: those
// claimed (evicting) already, or those that may be (placements ready and
// temporal objects present), in the order they go.
func evictionsOnStoreTx(ctx context.Context, tx *sql.Tx, d machine.Store, executorID string, claimed bool) ([]EvictionOrder, error) {
	placementState, sourceState := "ready", "present"
	if claimed {
		placementState, sourceState = "evicting", "evicting"
	}
	type candidate struct {
		order    EvictionOrder
		lastUsed sql.NullString
		created  string
	}
	heldSince := time.Now().Add(-evictRetryAfter)
	var placements, temporal []candidate
	err := eachRow(ctx, tx, `SELECT p.object_id,COALESCE(p.bytes,0),d.last_used_at,p.created_at FROM object_placements p JOIN data_objects d ON d.id=p.object_id
		WHERE p.store_id=$1 AND p.root=$2 AND p.executor_id=$3 AND p.state=$4`, []any{d.ID, d.Root, executorID, placementState}, func(rows *sql.Rows) error {
		c := candidate{order: EvictionOrder{Kind: EvictPlacement, StoreID: d.ID, Root: d.Root}}
		if err := rows.Scan(&c.order.ObjectID, &c.order.Bytes, &c.lastUsed, &c.created); err != nil {
			return err
		}
		placements = append(placements, c)
		return nil
	})
	if err != nil {
		return nil, err
	}
	err = eachRow(ctx, tx, `SELECT d.id,d.kind,d.source_id,d.source_path,COALESCE(d.content_bytes,0),d.last_used_at,d.created_at,d.source_reason IS NOT NULL,d.updated_at FROM data_objects d
		WHERE d.store_id=$1 AND d.root=$2 AND d.executor_id=$3 AND d.lifecycle='temporal' AND d.state='packed' AND d.source_state=$4
		AND (d.transfer_state IS NULL OR $5)`, []any{d.ID, d.Root, executorID, sourceState, claimed}, func(rows *sql.Rows) error {
		c := candidate{order: EvictionOrder{Kind: EvictTemporal, StoreID: d.ID, Root: d.Root}}
		var kept bool
		var updated string
		if err := rows.Scan(&c.order.ObjectID, &c.order.ObjectKind, &c.order.SourceID, &c.order.Path, &c.order.Bytes, &c.lastUsed, &c.created, &kept, &updated); err != nil {
			return err
		}
		if !claimed && kept {
			if at, err := time.Parse(time.RFC3339Nano, updated); err != nil || at.After(heldSince) {
				return nil // kept lately
			}
		}
		temporal = append(temporal, c)
		return nil
	})
	if err != nil {
		return nil, err
	}
	used := func(c candidate) time.Time {
		at, _ := time.Parse(time.RFC3339Nano, c.created)
		if last, err := time.Parse(time.RFC3339Nano, c.lastUsed.String); c.lastUsed.Valid && err == nil && last.After(at) {
			at = last
		}
		return at
	}
	byUse := func(list []candidate) {
		sort.SliceStable(list, func(i, j int) bool {
			a, b := used(list[i]), used(list[j])
			if !a.Equal(b) {
				return a.Before(b)
			}
			return list[i].order.ObjectID < list[j].order.ObjectID
		})
	}
	byUse(placements)
	byUse(temporal)
	out := make([]EvictionOrder, 0, len(placements)+len(temporal))
	for _, c := range append(placements, temporal...) {
		out = append(out, c.order)
	}
	return out, nil
}

// SettleEviction records what an executor did with an eviction it was
// ordered: a placement evicted is forgotten (the object may be brought there
// again); a temporal object evicted is so (its run's output directory
// deleted), and its node is to be trimmed; one kept goes back to ready or
// present (with why, not ordered again for evictRetryAfter). Settling it
// again is no error.
func (s *Store) SettleEviction(ctx context.Context, settled EvictionSettled) error {
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	t := now()
	var nodeID string
	var bytes int64
	switch settled.Kind {
	case EvictPlacement:
		var executorID, state string
		err = tx.QueryRowContext(ctx, `SELECT p.executor_id,p.state,COALESCE(p.bytes,0),x.node_id FROM object_placements p JOIN executors x ON x.id=p.executor_id
			WHERE p.object_id=$1 AND p.store_id=$2 FOR UPDATE OF p`, settled.ObjectID, settled.StoreID).Scan(&executorID, &state, &bytes, &nodeID)
		if errors.Is(err, sql.ErrNoRows) {
			return tx.Commit() // forgotten already
		}
		if err != nil {
			return err
		}
		if executorID != settled.ExecutorID {
			return fmt.Errorf("object %s on %s is executor %s's, not %s's", settled.ObjectID, settled.StoreID, executorID, settled.ExecutorID)
		}
		if state != "evicting" {
			return tx.Commit()
		}
		if settled.Evicted {
			_, err = tx.ExecContext(ctx, `DELETE FROM object_placements WHERE object_id=$1 AND store_id=$2`, settled.ObjectID, settled.StoreID)
		} else {
			_, err = tx.ExecContext(ctx, `UPDATE object_placements SET state='ready',updated_at=$1 WHERE object_id=$2 AND store_id=$3`, t, settled.ObjectID, settled.StoreID)
		}
	case EvictTemporal:
		var executorID, state, kind, sourceID, storeID string
		err = tx.QueryRowContext(ctx, `SELECT d.executor_id,d.source_state,d.kind,d.source_id,d.store_id,COALESCE(d.content_bytes,0),x.node_id FROM data_objects d JOIN executors x ON x.id=d.executor_id
			WHERE d.id=$1 FOR UPDATE OF d`, settled.ObjectID).Scan(&executorID, &state, &kind, &sourceID, &storeID, &bytes, &nodeID)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if executorID != settled.ExecutorID {
			return fmt.Errorf("object %s is executor %s's, not %s's", settled.ObjectID, executorID, settled.ExecutorID)
		}
		if state != "evicting" {
			return tx.Commit()
		}
		if settled.Evicted {
			if _, err = tx.ExecContext(ctx, `UPDATE data_objects SET source_state='evicted',source_reason=NULL,source_deleted_at=$1,updated_at=$2 WHERE id=$3`, t, t, settled.ObjectID); err != nil {
				return err
			}
			if kind == ObjectOutput {
				_, err = tx.ExecContext(ctx, `UPDATE run_outputs SET state='deleted',bytes=0,updated_at=$1 WHERE run_id=$2 AND store_id=$3`, t, sourceID, storeID)
			}
		} else {
			_, err = tx.ExecContext(ctx, `UPDATE data_objects SET source_state='present',source_reason=$1,last_used_at=$2,updated_at=$3 WHERE id=$4`, nullable(settled.Reason), t, t, settled.ObjectID)
		}
	default:
		return fmt.Errorf("an eviction is of a placement or temporal data, not %q", settled.Kind)
	}
	if err != nil {
		return err
	}
	event := "eviction_kept"
	if settled.Evicted {
		event = "evicted"
		if _, err = tx.ExecContext(ctx, `INSERT INTO node_trims(node_id,requested_at,requested_bytes,updated_at) VALUES($1,$2,$3,$4)
			ON CONFLICT(node_id) DO UPDATE SET requested_at=excluded.requested_at,requested_bytes=node_trims.requested_bytes+excluded.requested_bytes,updated_at=excluded.updated_at`,
			nodeID, t, bytes, t); err != nil {
			return err
		}
	}
	if err = appendCoordinationEventTx(ctx, tx, event, nil, "object", settled.ObjectID, nil, map[string]any{"kind": settled.Kind, "store_id": settled.StoreID, "reason": settled.Reason}); err != nil {
		return err
	}
	return tx.Commit()
}

// noteInputsReadTx marks the objects an attempt is handed as read now, so
// eviction takes the least recently read first.
func noteInputsReadTx(ctx context.Context, tx *sql.Tx, objectIDs []string) error {
	if len(objectIDs) == 0 {
		return nil
	}
	_, err := tx.ExecContext(ctx, `UPDATE data_objects SET last_used_at=$1 WHERE id=ANY($2::text[])`, now(), objectIDs)
	return err
}
