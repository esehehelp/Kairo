package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"kairo/internal/machine"
)

// StoreChange is one store whose declaration changes: From is nil for a new
// store, To for one no longer declared.
type StoreChange struct {
	Store string         `json:"store"`
	From  *machine.Store `json:"from,omitempty"`
	To    *machine.Store `json:"to,omitempty"`
}

// declaredStores are the stored store declarations, by id.
func declaredStores(ctx context.Context, q queryer) ([]machine.Store, error) {
	rows, err := q.QueryContext(ctx, `SELECT declaration_json FROM machine_stores ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []machine.Store{}
	for rows.Next() {
		var body string
		if err = rows.Scan(&body); err != nil {
			return nil, err
		}
		var s machine.Store
		if err = json.Unmarshal([]byte(body), &s); err != nil {
			return nil, fmt.Errorf("stored store declaration: %w", err)
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// NodeStores are the stores declared on a node (a node agent asks for its
// own when it starts, and again now and then).
func (s *Store) NodeStores(ctx context.Context, nodeID string) ([]machine.Store, error) {
	all, err := declaredStores(ctx, s.db)
	if err != nil {
		return nil, err
	}
	out := []machine.Store{}
	for _, store := range all {
		if store.Node == nodeID {
			out = append(out, store)
		}
	}
	return out, nil
}

// checkStoresTx checks what the declaration alone cannot: a store's executor
// is one of its node's, and its filesystem is exactly the identity of a disk
// resource registered on its node (the one its admission accounting uses).
func checkStoresTx(ctx context.Context, tx *sql.Tx, stores []machine.Store) error {
	for _, store := range stores {
		if store.Executor != "" {
			var found int
			if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM executors WHERE id=$1 AND node_id=$2`, store.Executor, store.Node).Scan(&found); err != nil {
				return err
			}
			if found == 0 {
				return fmt.Errorf("store %s: executor %s is not an executor of node %s", store.ID, store.Executor, store.Node)
			}
		}
		var found int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM resource_instances WHERE node_id=$1 AND kind='disk' AND stable_identity=$2`, store.Node, store.Filesystem).Scan(&found); err != nil {
			return err
		}
		if found == 0 {
			var disks []string
			rows, err := tx.QueryContext(ctx, `SELECT stable_identity FROM resource_instances WHERE node_id=$1 AND kind='disk' ORDER BY stable_identity`, store.Node)
			if err != nil {
				return err
			}
			for rows.Next() {
				var disk string
				if err = rows.Scan(&disk); err != nil {
					rows.Close()
					return err
				}
				disks = append(disks, disk)
			}
			rows.Close()
			if err = rows.Err(); err != nil {
				return err
			}
			known := "none"
			if len(disks) != 0 {
				known = strings.Join(disks, ", ")
			}
			return fmt.Errorf("store %s: filesystem %s is not a disk of node %s (its disks: %s)", store.ID, store.Filesystem, store.Node, known)
		}
	}
	return nil
}

// storeChangesTx compares spec's stores with the stored ones.
func storeChangesTx(ctx context.Context, tx *sql.Tx, spec machine.Spec) ([]StoreChange, int, error) {
	current, err := declaredStores(ctx, tx)
	if err != nil {
		return nil, 0, err
	}
	before := map[string]machine.Store{}
	for _, store := range current {
		before[store.ID] = store
	}
	changes := []StoreChange{}
	unchanged := 0
	for _, raw := range spec.Stores {
		to := raw.Normalized()
		from, existed := before[to.ID]
		delete(before, to.ID)
		switch {
		case !existed:
			changes = append(changes, StoreChange{Store: to.ID, To: &to})
		case sameStore(from, to):
			unchanged++
		default:
			changes = append(changes, StoreChange{Store: to.ID, From: &from, To: &to})
		}
	}
	for _, from := range before {
		changes = append(changes, StoreChange{Store: from.ID, From: &from})
	}
	sort.Slice(changes, func(i, j int) bool { return changes[i].Store < changes[j].Store })
	return changes, unchanged, nil
}

func sameStore(a, b machine.Store) bool {
	x, _ := json.Marshal(a.Normalized())
	y, _ := json.Marshal(b.Normalized())
	return bytes.Equal(x, y)
}

// applyStoresTx writes the store changes. A store no longer declared keeps
// its scratch records (and its data): nothing is deleted with a declaration.
func applyStoresTx(ctx context.Context, tx *sql.Tx, changes []StoreChange, t string) error {
	for _, change := range changes {
		if change.To == nil {
			if _, err := tx.ExecContext(ctx, `DELETE FROM machine_stores WHERE id=$1`, change.Store); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM store_observations WHERE store_id=$1`, change.Store); err != nil {
				return err
			}
		} else {
			body, err := json.Marshal(change.To)
			if err != nil {
				return err
			}
			if _, err = tx.ExecContext(ctx, `INSERT INTO machine_stores(id,node_id,executor_id,class,declaration_json,declared_at) VALUES($1,$2,$3,$4,$5,$6)
				ON CONFLICT(id) DO UPDATE SET node_id=excluded.node_id,executor_id=excluded.executor_id,class=excluded.class,declaration_json=excluded.declaration_json,declared_at=excluded.declared_at`,
				change.Store, change.To.Node, nullable(change.To.Executor), change.To.Class, string(body), t); err != nil {
				return err
			}
		}
		if err := appendCoordinationEventTx(ctx, tx, "store_declaration_changed", nil, "store", change.Store, nil, change); err != nil {
			return err
		}
	}
	return nil
}
