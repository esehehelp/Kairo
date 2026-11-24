package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"kairo/internal/machine"
)

// Dropping data (kairo data drop): what can be made again (a corpus or a mix
// a project's tools build from raw data) or fetched again from where it came
// from (raw data) is not for the durable stores to keep. Kairo keeps its
// identity only, the file list its pack had (path, bytes, sha256 and mtime of
// each file), and removes the bytes everywhere: every copy on a durable
// store, every place it was brought to, whatever room those stores have. The
// object is temporal data from then on (lifecycle temporal, dropped_at) that
// no task makes again: an execution naming it waits until the project makes
// it again or fetches it again, and imports it.
//
// The file list is on the durable stores only (<copy>.files.tsv): the
// executor serving a store with a copy is told to send it (CopyOrder
// SendList) before removing its copy, and the daemon keeps it once its sha256
// is the one recorded (KeepIdentity); no copy is removed before (ClaimShed
// refuses). Copies are removed as shed copies are (shed.go), not while a
// replica or a fetch reads them; places it was brought to are evicted
// (evictions.go), not while an input names it.
//
// An object is not dropped while an execution not yet ended, or a task
// pending or running, names it as an input (pinnedSourceTx), nor while it is
// being brought somewhere; nor is anything but packed, durable data a run
// left or that was imported.

// DropRequest selects a project's objects to drop: by id or name, by name
// prefix, or the outputs of tasks whose name has a prefix. Without Confirm
// nothing changes: the answer says what would be dropped.
type DropRequest struct {
	Project    string   `json:"project"`
	Objects    []string `json:"objects,omitempty"`
	NamePrefix string   `json:"name_prefix,omitempty"`
	TaskPrefix string   `json:"task_prefix,omitempty"`
	Reason     string   `json:"reason,omitempty"`
	Confirm    bool     `json:"confirm,omitempty"`
}

// DroppedObject is an object a drop selected: dropped now (or before), or
// refused with why; the copies and places its bytes are in.
type DroppedObject struct {
	ObjectID   string `json:"object_id"`
	Kind       string `json:"kind"`
	Name       string `json:"name,omitempty"`
	Task       string `json:"task,omitempty"`
	PackBytes  int64  `json:"pack_bytes"`
	Copies     int    `json:"copies"`
	Placements int    `json:"placements"`
	Dropped    bool   `json:"dropped"`
	Refused    string `json:"refused,omitempty"`
}

// ErrDropRefused: a drop that selects nothing, or names what is not there.
var ErrDropRefused = errors.New("data not dropped")

// DropData drops the objects req selects (or says what it would drop).
func (s *Store) DropData(ctx context.Context, req DropRequest, actor string) ([]DroppedObject, error) {
	if req.Project == "" || len(req.Objects) == 0 && req.NamePrefix == "" && req.TaskPrefix == "" {
		return nil, fmt.Errorf("%w: a drop names a project and the objects, a name prefix or a task prefix", ErrDropRefused)
	}
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var projectID string
	err = tx.QueryRowContext(ctx, `SELECT id FROM coordination_scopes WHERE kind='project' AND external_key=$1`, req.Project).Scan(&projectID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w: no project %s", ErrDropRefused, req.Project)
	}
	if err != nil {
		return nil, err
	}
	type row struct {
		DroppedObject
		state, lifecycle string
		before, listed   bool
	}
	var rows []row
	named := map[string]bool{}
	err = eachRow(ctx, tx, `SELECT d.id,d.kind,COALESCE(d.name,''),COALESCE(t.external_key,''),COALESCE(d.pack_bytes,0),d.state,d.lifecycle,d.dropped_at IS NOT NULL,
		d.manifest IS NOT NULL,(SELECT COUNT(*) FROM object_copies c WHERE c.object_id=d.id),(SELECT COUNT(*) FROM object_placements p WHERE p.object_id=d.id)
		FROM data_objects d LEFT JOIN execution_requests e ON d.kind='output' AND e.id=d.source_id LEFT JOIN coordination_scopes t ON t.id=e.task_scope_id
		WHERE d.project_scope_id=$1 AND (d.id=ANY($2::text[]) OR d.name=ANY($2::text[]) OR $3<>'' AND starts_with(d.name,$3) OR $4<>'' AND d.kind='output' AND starts_with(t.external_key,$4))
		ORDER BY d.created_at,d.id`, []any{projectID, req.Objects, req.NamePrefix, req.TaskPrefix}, func(r *sql.Rows) error {
		var x row
		if err := r.Scan(&x.ObjectID, &x.Kind, &x.Name, &x.Task, &x.PackBytes, &x.state, &x.lifecycle, &x.before, &x.listed, &x.Copies, &x.Placements); err != nil {
			return err
		}
		named[x.ObjectID], named[x.Name] = true, true
		rows = append(rows, x)
		return nil
	})
	if err != nil {
		return nil, err
	}
	var missing []string
	for _, o := range req.Objects {
		if !named[o] {
			missing = append(missing, o)
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("%w: project %s has no object %s", ErrDropRefused, req.Project, strings.Join(missing, ", "))
	}
	t := now()
	out := make([]DroppedObject, 0, len(rows))
	for _, x := range rows {
		o := x.DroppedObject
		switch {
		case x.before:
			o.Dropped = true
		case x.Kind != ObjectImport && x.Kind != ObjectOutput:
			o.Refused = "a " + x.Kind + ", not data a run left or that was imported"
		case x.lifecycle != LifecycleDurable || x.state != "packed":
			o.Refused = x.lifecycle + " data " + x.state + ", not packed durable data"
		}
		if o.Refused == "" && !o.Dropped {
			why, err := pinnedSourceTx(ctx, tx, o.ObjectID)
			if err != nil {
				return nil, err
			}
			if why != "" {
				o.Refused = "in use: " + why
			}
		}
		if o.Refused == "" && !o.Dropped {
			var fetching, verified int
			if err := tx.QueryRowContext(ctx, `SELECT (SELECT COUNT(*) FROM object_placements WHERE object_id=$1 AND state='fetching'),
				(SELECT COUNT(*) FROM object_copies WHERE object_id=$1 AND state='verified' AND check_problem IS NULL)`, o.ObjectID).Scan(&fetching, &verified); err != nil {
				return nil, err
			}
			switch {
			case fetching > 0:
				o.Refused = "being brought to a node"
			case !x.listed && verified == 0:
				o.Refused = "no good copy to keep its file list from"
			}
		}
		if o.Refused == "" && !o.Dropped && req.Confirm {
			if _, err := tx.ExecContext(ctx, `UPDATE data_objects SET lifecycle='temporal',dropped_at=$1,updated_at=$2 WHERE id=$3 AND dropped_at IS NULL`, t, t, o.ObjectID); err != nil {
				return nil, err
			}
			if err := appendCoordinationEventTx(ctx, tx, "data_dropped", &projectID, "object", o.ObjectID, nil, map[string]any{
				"actor": actor, "reason": req.Reason, "pack_bytes": o.PackBytes, "copies": o.Copies, "placements": o.Placements}); err != nil {
				return nil, err
			}
			o.Dropped = true
		}
		out = append(out, o)
	}
	if !req.Confirm {
		return out, nil
	}
	return out, tx.Commit()
}

// dropShedOrdersTx are the copies of dropped objects on durable store s
// (served by the executor asking) to remove now: not one a replica or a
// fetch reads; each saying whether the file list beside it is to be sent
// first (the object's identity not kept yet).
func dropShedOrdersTx(ctx context.Context, tx *sql.Tx, s machine.Store) ([]CopyOrder, error) {
	var out []CopyOrder
	err := eachRow(ctx, tx, `SELECT c.object_id,c.month,c.root,COALESCE(d.pack_bytes,0),d.manifest IS NULL FROM object_copies c JOIN data_objects d ON d.id=c.object_id
		WHERE c.store_id=$1 AND c.root=$2 AND c.state='verified' AND d.dropped_at IS NOT NULL
		AND NOT EXISTS(SELECT 1 FROM object_copies r WHERE r.object_id=c.object_id AND r.state='copying' AND (r.source_store_id=c.store_id OR r.source_store_id IS NULL))
		AND NOT EXISTS(SELECT 1 FROM object_placements p WHERE p.object_id=c.object_id AND p.state='fetching' AND p.source_store_id=c.store_id)
		ORDER BY d.dropped_at,c.object_id LIMIT $3`, []any{s.ID, s.Root, shedBatch}, func(rows *sql.Rows) error {
		o := CopyOrder{StoreID: s.ID, Shed: true, Drop: true}
		if err := rows.Scan(&o.ObjectID, &o.Month, &o.Root, &o.PackBytes, &o.SendList); err != nil {
			return err
		}
		out = append(out, o)
		return nil
	})
	return out, err
}

// ErrIdentityRefused: a file list sent for a dropped object that is not the
// one recorded for it.
var ErrIdentityRefused = errors.New("file list not kept")

// KeepIdentity keeps the file list an executor read beside its copy of a
// dropped object on durable store storeID, as the object's identity, if its
// sha256 is the one recorded for the object's pack's list. Keeping it again
// is no error.
func (s *Store) KeepIdentity(ctx context.Context, executorID, objectID, storeID, list string) error {
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var dropped, kept bool
	var listSHA string
	var projectID *string
	err = tx.QueryRowContext(ctx, `SELECT d.dropped_at IS NOT NULL,d.manifest IS NOT NULL,COALESCE(d.manifest_sha256,''),d.project_scope_id FROM object_copies c JOIN data_objects d ON d.id=c.object_id
		WHERE c.object_id=$1 AND c.store_id=$2 FOR UPDATE OF d`, objectID, storeID).Scan(&dropped, &kept, &listSHA, &projectID)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: no copy of %s on %s", ErrIdentityRefused, objectID, storeID)
	}
	if err != nil {
		return err
	}
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
		served = served || d.ID == storeID && d.Class == machine.StoreDurable && me.serves(d, executorID)
	}
	sum := sha256.Sum256([]byte(list))
	switch {
	case !served:
		return fmt.Errorf("%w: executor %s does not serve durable store %s", ErrIdentityRefused, executorID, storeID)
	case !dropped:
		return fmt.Errorf("%w: %s is not dropped", ErrIdentityRefused, objectID)
	case kept:
		return tx.Commit()
	case len(list) > maxManifestBytes:
		return fmt.Errorf("%w: a file list of %d bytes is more than the %d kept", ErrIdentityRefused, len(list), maxManifestBytes)
	case hex.EncodeToString(sum[:]) != listSHA:
		return fmt.Errorf("%w: the file list beside the copy of %s on %s is not the one its pack was made with", ErrIdentityRefused, objectID, storeID)
	}
	if _, err = tx.ExecContext(ctx, `UPDATE data_objects SET manifest=$1,updated_at=$2 WHERE id=$3`, list, now(), objectID); err != nil {
		return err
	}
	if err = appendCoordinationEventTx(ctx, tx, "identity_kept", projectID, "object", objectID, nil, map[string]any{"store_id": storeID, "executor_id": executorID, "manifest_sha256": listSHA}); err != nil {
		return err
	}
	return tx.Commit()
}

// dropEvictionsTx claims for eviction what was brought to volatile store d
// (served by executorID) of dropped objects, whatever room d has: not what an
// input names.
func dropEvictionsTx(ctx context.Context, tx *sql.Tx, d machine.Store, executorID string) ([]EvictionOrder, error) {
	var candidates []EvictionOrder
	err := eachRow(ctx, tx, `SELECT p.object_id,COALESCE(p.bytes,0) FROM object_placements p JOIN data_objects o ON o.id=p.object_id
		WHERE p.store_id=$1 AND p.root=$2 AND p.executor_id=$3 AND p.state='ready' AND o.dropped_at IS NOT NULL ORDER BY p.object_id`, []any{d.ID, d.Root, executorID}, func(rows *sql.Rows) error {
		o := EvictionOrder{Kind: EvictPlacement, StoreID: d.ID, Root: d.Root}
		if err := rows.Scan(&o.ObjectID, &o.Bytes); err != nil {
			return err
		}
		candidates = append(candidates, o)
		return nil
	})
	if err != nil {
		return nil, err
	}
	t := now()
	var out []EvictionOrder
	for _, o := range candidates {
		pinned, err := pinnedSourceTx(ctx, tx, o.ObjectID)
		if err != nil {
			return nil, err
		}
		if pinned != "" {
			continue
		}
		if err := checkTransition(recPlacement, "ready", "evicting"); err != nil {
			return nil, err
		}
		claim, err := tx.ExecContext(ctx, `UPDATE object_placements SET state='evicting',updated_at=$1 WHERE object_id=$2 AND store_id=$3 AND state='ready'`, t, o.ObjectID, d.ID)
		if err != nil {
			return nil, err
		}
		if n, _ := claim.RowsAffected(); n != 1 {
			continue
		}
		if err = appendCoordinationEventTx(ctx, tx, "eviction_ordered", nil, "object", o.ObjectID, nil, map[string]any{"kind": o.Kind, "store_id": d.ID, "bytes": o.Bytes, "dropped": true}); err != nil {
			return nil, err
		}
		out = append(out, o)
	}
	return out, nil
}
