package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"kairo/internal/machine"
)

// A durable store's directory is the truth of what it holds (README: Durable
// inventory). Each executor serving one lists it now and then (the copies
// <root>/objects/<YYYYMM>/<object id>.tar.zst with their file lists beside
// them) and reports the listing (InventoryDurable). A copy the records do not
// have there (the database restored from an older control snapshot, the copy
// made after it) is adopted from its description, <copy>.object.json
// (AdoptCopies): the object recorded as it was described, packed, its source
// settled kept (where it is now is not known), and the copy verified. A
// verified copy recorded there before the listing began and not in it is
// marked damaged (check_problem), as a read-back that failed would mark it;
// unless nothing at all was listed, or most of what was recorded is not
// there, which is taken as the directory not being the store's (a disk not
// mounted) and changes nothing.

// DurableListing is what an executor found in a durable store's directory:
// the copies there (object id to month), listed from ListedAt on.
type DurableListing struct {
	ExecutorID string            `json:"executor_id"`
	StoreID    string            `json:"store_id"`
	Root       string            `json:"root"`
	ListedAt   string            `json:"listed_at"`
	Copies     map[string]string `json:"copies"`
}

// DurableInventory is what a listing told: the copies the records lack there
// (to adopt, with their descriptions), and the recorded ones not found.
type DurableInventory struct {
	Unknown []string `json:"unknown"`
	Missing int      `json:"missing"`
}

// inventoryMissingShare: when more than this share of the verified copies
// recorded on a store is not listed, the listing is not believed.
const inventoryMissingShare = 0.5

// checkInventoryStoreTx: executorID serves storeID, durable at root as
// declared now.
func checkInventoryStoreTx(ctx context.Context, tx *sql.Tx, executorID, storeID, root string) error {
	me, err := executorInfoTx(ctx, tx, executorID)
	if err != nil {
		return err
	}
	declared, err := declaredStores(ctx, tx)
	if err != nil {
		return err
	}
	for _, d := range declared {
		if d.ID == storeID {
			if d.Class != machine.StoreDurable || d.Root != root || d.MovedFrom != "" || !me.serves(d, executorID) {
				return fmt.Errorf("store %s is not a durable store at %s that executor %s serves", storeID, root, executorID)
			}
			return nil
		}
	}
	return fmt.Errorf("%w: store %s", ErrNotFound, storeID)
}

// InventoryDurable compares a listing of a durable store with the records.
func (s *Store) InventoryDurable(ctx context.Context, l DurableListing) (DurableInventory, error) {
	out := DurableInventory{Unknown: []string{}}
	for id, month := range l.Copies {
		if !machine.ScratchIDValid(id) || !machine.ValidMonth(month) {
			return out, fmt.Errorf("listing of %s: %q in %q is not an object's copy", l.StoreID, id, month)
		}
	}
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	if err = checkInventoryStoreTx(ctx, tx, l.ExecutorID, l.StoreID, l.Root); err != nil {
		return out, err
	}
	type recorded struct {
		state, updated string
		problem        bool
	}
	records := map[string]recorded{}
	err = eachRow(ctx, tx, `SELECT object_id,state,updated_at,check_problem IS NOT NULL FROM object_copies WHERE store_id=$1 AND root=$2`, []any{l.StoreID, l.Root}, func(rows *sql.Rows) error {
		var id string
		var r recorded
		if err := rows.Scan(&id, &r.state, &r.updated, &r.problem); err != nil {
			return err
		}
		records[id] = r
		return nil
	})
	if err != nil {
		return out, err
	}
	for id := range l.Copies {
		if _, ok := records[id]; !ok {
			out.Unknown = append(out.Unknown, id)
		}
	}
	verified := 0
	var missing []string
	for id, r := range records {
		if r.state != "verified" {
			continue
		}
		verified++
		if _, listed := l.Copies[id]; !listed && !r.problem && r.updated < l.ListedAt {
			missing = append(missing, id)
		}
	}
	switch {
	case len(missing) == 0:
	case len(l.Copies) == 0 || float64(len(missing)) > inventoryMissingShare*float64(verified):
		return out, fmt.Errorf("listing of %s at %s: %d of %d verified copies not there; not believed (is the store's disk mounted?)", l.StoreID, l.Root, len(missing), verified)
	default:
		t := now()
		for _, id := range missing {
			if _, err = tx.ExecContext(ctx, `UPDATE object_copies SET check_problem=$1,checked_at=$2,updated_at=$3 WHERE object_id=$4 AND store_id=$5 AND state='verified'`,
				"not in the store's directory (inventory at "+l.ListedAt+")", t, t, id, l.StoreID); err != nil {
				return out, err
			}
			if err = appendCoordinationEventTx(ctx, tx, "copy_damaged", nil, "object", id, nil, map[string]any{"store_id": l.StoreID, "problem": "missing", "executor_id": l.ExecutorID}); err != nil {
				return out, err
			}
		}
		out.Missing = len(missing)
	}
	return out, tx.Commit()
}

// AdoptedCopy is a copy found on a durable store that the records lack, as
// its description tells.
type AdoptedCopy struct {
	Month       string            `json:"month"`
	Description ObjectDescription `json:"description"`
}

// AdoptCopies records copies executorID found on durable store storeID at
// root that the records lack: the number recorded. A copy whose description
// is not one of Kairo's, or that differs from the object recorded, is refused
// (all of them: nothing is recorded).
func (s *Store) AdoptCopies(ctx context.Context, executorID, storeID, root string, copies []AdoptedCopy) (int, error) {
	for _, c := range copies {
		d := c.Description
		switch {
		case d.Format != DescriptionFormat:
			return 0, fmt.Errorf("object %s: description format %q, not %q", d.ObjectID, d.Format, DescriptionFormat)
		case !machine.ScratchIDValid(d.ObjectID) || !machine.ValidMonth(c.Month):
			return 0, fmt.Errorf("object %q in month %q is not an object's copy", d.ObjectID, c.Month)
		case d.Kind != ObjectOutput && d.Kind != ObjectScratch && d.Kind != ObjectImport && d.Kind != ObjectRunSnapshot:
			return 0, fmt.Errorf("object %s: unknown kind %q", d.ObjectID, d.Kind)
		case d.Lifecycle != LifecycleDurable:
			return 0, fmt.Errorf("object %s is %s: only durable objects are on durable stores", d.ObjectID, d.Lifecycle)
		case !sha256Hex.MatchString(d.PackSHA256) || !sha256Hex.MatchString(d.ManifestSHA256) || d.PackBytes <= 0:
			return 0, fmt.Errorf("object %s: its description has no pack's size and sha256s", d.ObjectID)
		case len(d.CreatedAt) < 7 || d.CreatedAt[0:4]+d.CreatedAt[5:7] != c.Month:
			return 0, fmt.Errorf("object %s made %q is not of month %s", d.ObjectID, d.CreatedAt, c.Month)
		}
	}
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	if err = checkInventoryStoreTx(ctx, tx, executorID, storeID, root); err != nil {
		return 0, err
	}
	declared, err := declaredStores(ctx, tx)
	if err != nil {
		return 0, err
	}
	rootOf := map[string]string{}
	for _, d := range declared {
		rootOf[d.ID] = d.Root
	}
	t := now()
	adopted := 0
	for _, c := range copies {
		d := c.Description
		var packSHA, listSHA sql.NullString
		var packBytes sql.NullInt64
		err = tx.QueryRowContext(ctx, `SELECT pack_sha256,manifest_sha256,pack_bytes FROM data_objects WHERE id=$1`, d.ObjectID).Scan(&packSHA, &listSHA, &packBytes)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			var projectID any
			if d.Project != nil && *d.Project != "" {
				scope, _, err := ensureScopeTx(ctx, tx, "project", nil, *d.Project)
				if err != nil {
					return 0, err
				}
				projectID = scope.ID
			}
			packRoot := rootOf[d.PackedOn]
			if packRoot == "" {
				packRoot = d.SourcePath
			}
			packedAt := t
			if d.PackedAt != nil {
				packedAt = *d.PackedAt
			}
			result, err := tx.ExecContext(ctx, `INSERT INTO data_objects(id,kind,source_id,store_id,executor_id,root,source_path,project_scope_id,state,content_bytes,files,skipped,pack_bytes,pack_sha256,manifest_sha256,
				created_at,updated_at,packed_at,source_state,source_reason,name,lifecycle)
				VALUES($1,$2,$3,$4,$5,$6,$7,$8,'packed',$9,$10,0,$11,$12,$13,$14,$15,$16,'kept',$17,$18,'durable') ON CONFLICT DO NOTHING`,
				d.ObjectID, d.Kind, d.SourceID, d.PackedOn, executorID, packRoot, d.SourcePath, projectID, d.ContentBytes, d.Files, d.PackBytes, d.PackSHA256, d.ManifestSHA256,
				d.CreatedAt, t, packedAt, "adopted from durable store "+storeID+" (its records were lost)", d.Name)
			if err != nil {
				return 0, err
			}
			if n, _ := result.RowsAffected(); n == 0 {
				return 0, fmt.Errorf("object %s: another object is recorded for its source %s on %s", d.ObjectID, d.SourceID, d.PackedOn)
			}
		case err != nil:
			return 0, err
		case packSHA.String != d.PackSHA256 || listSHA.String != d.ManifestSHA256 || packBytes.Int64 != d.PackBytes:
			return 0, fmt.Errorf("object %s: the copy on %s is not the object recorded (pack %s, %d bytes; recorded %s, %d bytes)", d.ObjectID, storeID, d.PackSHA256, d.PackBytes, packSHA.String, packBytes.Int64)
		}
		path := strings.TrimRight(root, `/\`) + "/objects/" + c.Month + "/" + d.ObjectID + ".tar.zst"
		result, err := tx.ExecContext(ctx, `INSERT INTO object_copies(object_id,store_id,executor_id,root,month,state,path,manifest_path,created_at,updated_at,verified_at,described_at)
			VALUES($1,$2,$3,$4,$5,'verified',$6,$7,$8,$9,$10,$11) ON CONFLICT(object_id,store_id) DO NOTHING`,
			d.ObjectID, storeID, executorID, root, c.Month, path, path+".files.tsv", t, t, t, t)
		if err != nil {
			return 0, err
		}
		if n, _ := result.RowsAffected(); n == 0 {
			continue
		}
		var project *string
		if err = tx.QueryRowContext(ctx, `SELECT project_scope_id FROM data_objects WHERE id=$1`, d.ObjectID).Scan(&project); err != nil {
			return 0, err
		}
		if err = appendCoordinationEventTx(ctx, tx, "copy_adopted", project, "object", d.ObjectID, nil, map[string]any{"store_id": storeID, "executor_id": executorID}); err != nil {
			return 0, err
		}
		adopted++
	}
	return adopted, tx.Commit()
}
