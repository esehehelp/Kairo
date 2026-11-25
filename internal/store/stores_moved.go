package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"kairo/internal/machine"
)

// Moving a store (Data stores, moving a store): a store declared again at
// another root with moved_from naming the root it is at is being moved. Its
// data stays recorded at the old root (so nothing reads, writes or deletes it
// there or at the new one: every decision reads only what is recorded at the
// declared root) until the executor serving it has renamed the old root to
// the new one and says so in a store observation; then every record of it is
// rebased onto the new root in one transaction and the move is over.
// Meanwhile executions naming the store are not admitted (store_moving) nor
// authorized, and its executor is told to wait (machine.Store.MoveWait) while
// anything still uses the old root.

// checkStoreMove takes from, the stored declaration of a store (existed:
// one is stored), and to, the declaration applied, and settles to's
// moved_from: a move under way stays (and the store's root cannot change
// meanwhile), one naming where the store is already says nothing, and a new
// move must name where the store is and change nothing but its root.
func checkStoreMove(from machine.Store, existed bool, to *machine.Store) error {
	key := machine.StoreRootKey
	if existed && from.MovedFrom != "" {
		if key(to.Root) != key(from.Root) {
			return fmt.Errorf("%w: store %s is being moved from %s to %s: declare another root once it has moved", ErrMachineConflict, to.ID, from.MovedFrom, from.Root)
		}
		if to.MovedFrom != "" && key(to.MovedFrom) != key(from.MovedFrom) {
			return fmt.Errorf("%w: store %s is being moved from %s, not from %s", ErrMachineConflict, to.ID, from.MovedFrom, to.MovedFrom)
		}
		to.MovedFrom = from.MovedFrom
		return nil
	}
	if to.MovedFrom == "" {
		return nil
	}
	switch {
	case !existed:
		return fmt.Errorf("store %s: moved_from %s: the store is not declared yet, so nothing of it is anywhere to move", to.ID, to.MovedFrom)
	case key(to.Root) == key(from.Root):
		to.MovedFrom = "" // there already: moved, or never anywhere else
		return nil
	case key(to.MovedFrom) != key(from.Root):
		return fmt.Errorf("store %s: moved_from %s is not where the store is (%s)", to.ID, to.MovedFrom, from.Root)
	}
	same := from
	same.Root, same.MovedFrom = to.Root, to.MovedFrom
	if !sameStore(same, *to) {
		return fmt.Errorf("store %s: a store being moved changes nothing but its root (apply its other changes before or after)", to.ID)
	}
	return nil
}

// storeMoveWait is what still uses store d where it is being moved from
// (empty: nothing, its executor may move it): executions naming it, or with
// their scratch there, holding a lease (running, about to, or lost and not
// yet reconciled: their processes may still write there); scratch being
// deleted; objects being packed, deleted, evicted or packed for transfer
// there; copies being made there; objects being brought there or evicted, or
// fetched from it.
func storeMoveWait(ctx context.Context, q queryer, d machine.Store) (string, error) {
	from := machine.StoreRootKey(d.MovedFrom)
	var parts []string
	var leased int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(DISTINCT l.id) FROM leases l JOIN execution_requests e ON e.id=l.execution_id
		WHERE l.state IN('reserved','prepared','active','releasing','revocation_requested','stale')
		AND (e.store_id=$1 OR EXISTS(SELECT 1 FROM attempts a JOIN attempt_scratch k ON k.attempt_id=a.id WHERE a.execution_id=e.id AND k.store_id=$1))`, d.ID).Scan(&leased); err != nil {
		return "", err
	}
	if leased != 0 {
		parts = append(parts, fmt.Sprintf("%d execution(s) using it hold a lease (running, starting, or lost and not reconciled)", leased))
	}
	for _, check := range []struct{ what, query string }{
		{"scratch directories being deleted", `SELECT root FROM attempt_scratch WHERE store_id=$1 AND state='deleting'`},
		{"objects being packed, deleted, evicted or packed for transfer there", `SELECT root FROM data_objects WHERE store_id=$1 AND (state='packing' OR source_state IN('deleting','evicting') OR transfer_state IN('packing','dropping'))`},
		{"copies being made there", `SELECT root FROM object_copies WHERE store_id=$1 AND state='copying'`},
		{"objects being brought there or evicted", `SELECT root FROM object_placements WHERE store_id=$1 AND state IN('fetching','evicting')`},
		{"objects being fetched from it", `SELECT source_root FROM object_placements WHERE source_store_id=$1 AND state='fetching'`},
		{"raw files being evicted there", `SELECT root FROM raw_placements WHERE store_id=$1 AND state='evicting'`},
	} {
		n := 0
		err := eachRow(ctx, q, check.query, []any{d.ID}, func(rows *sql.Rows) error {
			var root sql.NullString
			if err := rows.Scan(&root); err != nil {
				return err
			}
			if root.Valid && machine.StoreRootKey(root.String) == from {
				n++
			}
			return nil
		})
		if err != nil {
			return "", err
		}
		if n != 0 {
			parts = append(parts, fmt.Sprintf("%d %s", n, check.what))
		}
	}
	return strings.Join(parts, "; "), nil
}

// storesBeingMoved are the declared stores being moved, by id.
func storesBeingMoved(declared []machine.Store) map[string]machine.Store {
	out := map[string]machine.Store{}
	for _, d := range declared {
		if d.MovedFrom != "" {
			out[d.ID] = d
		}
	}
	return out
}

// loadStoreMoveWaits notes, on each candidate (of ids) naming a store being
// moved, that it waits for the move (store_moving).
func loadStoreMoveWaits(ctx context.Context, tx *sql.Tx, candidates []*planCandidate, ids []string) error {
	declared, err := declaredStores(ctx, tx)
	if err != nil {
		return err
	}
	moving := storesBeingMoved(declared)
	if len(moving) == 0 {
		return nil
	}
	named := map[string]string{}
	err = eachRow(ctx, tx, `SELECT id,store_id FROM execution_requests WHERE id=ANY($1::text[]) AND store_id IS NOT NULL`, []any{ids}, func(rows *sql.Rows) error {
		var id, storeID string
		if err := rows.Scan(&id, &storeID); err != nil {
			return err
		}
		named[id] = storeID
		return nil
	})
	if err != nil {
		return err
	}
	for _, c := range candidates {
		if d, ok := moving[named[c.execution.ID]]; ok {
			c.storeWait = fmt.Sprintf("store %s is being moved from %s to %s", d.ID, d.MovedFrom, d.Root)
		}
	}
	return nil
}

// storeBeingMovedTx says why an execution naming storeID is not launched now
// (empty: it may be): its store is being moved.
func storeBeingMovedTx(ctx context.Context, tx *sql.Tx, storeID string) (string, error) {
	if storeID == "" {
		return "", nil
	}
	declared, err := declaredStores(ctx, tx)
	if err != nil {
		return "", err
	}
	if d, ok := storesBeingMoved(declared)[storeID]; ok {
		return fmt.Sprintf("store %s is being moved from %s to %s", d.ID, d.MovedFrom, d.Root), nil
	}
	return "", nil
}

// completeStoreMoveTx ends the move of store declared (being moved, its
// declaration row locked): every record of it at the old root (and every
// fetch source naming it there) is rebased onto the new root, and its
// declaration no longer says moved_from.
func completeStoreMoveTx(ctx context.Context, tx *sql.Tx, declared machine.Store) error {
	from, to := declared.MovedFrom, declared.Root
	rebased := 0
	for _, table := range []struct {
		name, key, store string
		columns          []string
	}{
		{"attempt_scratch", "attempt_id", "store_id", []string{"root", "path"}},
		{"run_outputs", "run_id", "store_id", []string{"root", "path"}},
		{"data_objects", "id", "store_id", []string{"root", "source_path", "pack_path", "manifest_path"}},
		{"object_copies", "object_id||'/'||store_id", "store_id", []string{"root", "path", "manifest_path"}},
		{"object_placements", "object_id||'/'||store_id", "store_id", []string{"root", "path"}},
		{"object_placements", "object_id||'/'||store_id", "source_store_id", []string{"source_root"}},
		{"raw_placements", "sha256||'/'||store_id", "store_id", []string{"root"}},
	} {
		type change struct {
			key    string
			values []any
		}
		var changes []change
		err := eachRow(ctx, tx, `SELECT `+table.key+`,`+strings.Join(table.columns, ",")+` FROM `+table.name+` WHERE `+table.store+`=$1`, []any{declared.ID}, func(rows *sql.Rows) error {
			values := make([]sql.NullString, len(table.columns))
			dest := []any{new(string)}
			for i := range values {
				dest = append(dest, &values[i])
			}
			if err := rows.Scan(dest...); err != nil {
				return err
			}
			// the root first: a record made at another root (one this
			// store had before) is not this move's
			if !values[0].Valid || machine.StoreRootKey(values[0].String) != machine.StoreRootKey(from) {
				return nil
			}
			c := change{key: *dest[0].(*string)}
			for i, v := range values {
				switch {
				case i == 0:
					c.values = append(c.values, to)
				case !v.Valid:
					c.values = append(c.values, nil)
				default:
					// a path not under the root (an imported object's
					// source, say) stays as it is
					p, ok := machine.RebasePath(v.String, from, to)
					if !ok {
						p = v.String
					}
					c.values = append(c.values, p)
				}
			}
			changes = append(changes, c)
			return nil
		})
		if err != nil {
			return err
		}
		set := make([]string, len(table.columns))
		for i, column := range table.columns {
			set[i] = fmt.Sprintf("%s=$%d", column, i+1)
		}
		statement := `UPDATE ` + table.name + ` SET ` + strings.Join(set, ",") + fmt.Sprintf(` WHERE %s=$%d AND %s=$%d`, table.key, len(table.columns)+1, table.store, len(table.columns)+2)
		for _, c := range changes {
			if _, err := tx.ExecContext(ctx, statement, append(c.values, c.key, declared.ID)...); err != nil {
				return fmt.Errorf("rebase %s %s: %w", table.name, c.key, err)
			}
		}
		rebased += len(changes)
	}
	declared.MovedFrom, declared.MoveWait = "", ""
	body, err := json.Marshal(declared)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE machine_stores SET declaration_json=$1 WHERE id=$2`, string(body), declared.ID); err != nil {
		return err
	}
	return appendCoordinationEventTx(ctx, tx, "store_moved", nil, "store", declared.ID, nil, map[string]any{"from": from, "to": to, "records": rebased})
}

// completeStoreMove ends the move of the store obs reports moved (its
// executor renamed the old root to the declared one, MovedFrom), under the
// decision lock (the rebased records are what decisions read): only while
// the store is still being moved from there to the root obs is of, and only
// from its node.
func (s *Store) completeStoreMove(ctx context.Context, obs StoreObservation) error {
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var body string
	err = tx.QueryRowContext(ctx, `SELECT declaration_json FROM machine_stores WHERE id=$1 FOR UPDATE`, obs.StoreID).Scan(&body)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	var declared machine.Store
	if err = json.Unmarshal([]byte(body), &declared); err != nil {
		return fmt.Errorf("stored store declaration: %w", err)
	}
	if declared.MovedFrom == "" || !obs.Usable || obs.Root != declared.Root ||
		machine.StoreRootKey(obs.MovedFrom) != machine.StoreRootKey(declared.MovedFrom) {
		return nil // over already, or not this move: the observation says the rest
	}
	if wait, err := storeMoveWait(ctx, tx, declared); err != nil {
		return err
	} else if wait != "" {
		return fmt.Errorf("%w: store %s was moved from %s while %s", ErrMachineConflict, declared.ID, declared.MovedFrom, wait)
	}
	if err = completeStoreMoveTx(ctx, tx, declared); err != nil {
		return err
	}
	return tx.Commit()
}
