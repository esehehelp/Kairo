package store

import (
	"context"
	"database/sql"
	"fmt"

	"kairo/internal/machine"
)

// Bringing temporal data to another node: temporal data is not copied to a
// durable store (temporal.go), so an execution waiting on a node for
// temporal data present on another node (its directory still there) has it
// brought from there. The executor holding it packs it for the move
// (transfer_state packing, PackOrder.Transfer): the pack must hold exactly
// what the object's identity lists, or it is not recorded (the directory
// changed since). Once packed (packed), it is brought as a durable object
// is (placements.go: unpacked under <root>/inputs/<object id> of the node,
// read in place there, evicted first when room runs short), fetched from
// the agent of the node that packed it. Once no execution waits for it on
// a node lacking it and none is being brought, the pack is removed
// (dropping, PackOrder.Drop; its identity's file list stays). Meanwhile the
// directory is not evicted. Temporal data present nowhere is made again
// instead (regenerate.go).

// The transfer states (data_objects.transfer_state).
const (
	transferPacking  = "packing"
	transferPacked   = "packed"
	transferDropping = "dropping"
)

// bringTemporalTx brings temporal object objectID to node when its
// directory is present where it was made: it orders it packed for the move,
// or, packed, brought to node. ok is false when it is not present (made
// again instead).
func bringTemporalTx(ctx context.Context, tx *sql.Tx, declared []machine.Store, objectID, node string) (bool, error) {
	var state, lifecycle, sourceState string
	var transfer, projectID sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT state,lifecycle,source_state,transfer_state,project_scope_id FROM data_objects WHERE id=$1 FOR UPDATE`, objectID).
		Scan(&state, &lifecycle, &sourceState, &transfer, &projectID)
	if err != nil {
		return false, err
	}
	if lifecycle != LifecycleTemporal || state != "packed" || sourceState != "present" {
		return false, nil
	}
	switch transfer.String {
	case "":
		if _, err = tx.ExecContext(ctx, `UPDATE data_objects SET transfer_state=$1,updated_at=$2 WHERE id=$3`, transferPacking, now(), objectID); err != nil {
			return false, err
		}
		var project *string
		if projectID.Valid {
			project = &projectID.String
		}
		return true, appendCoordinationEventTx(ctx, tx, "transfer_ordered", project, "object", objectID, nil, map[string]any{"node": node})
	case transferPacked:
		_, err = orderFetchTx(ctx, tx, declared, objectID, node)
		return true, err
	}
	return true, nil // being packed, or its pack being removed (packed again after)
}

// dropTransfersTx orders removed the transfer packs no execution waits for
// any more (wanted lists those that one does) and none is being brought
// from.
func dropTransfersTx(ctx context.Context, tx *sql.Tx, wanted map[string]bool) error {
	var packed []string
	err := eachRow(ctx, tx, `SELECT d.id FROM data_objects d WHERE d.transfer_state=$1
		AND NOT EXISTS(SELECT 1 FROM object_placements p WHERE p.object_id=d.id AND p.state='fetching')`, []any{transferPacked}, func(rows *sql.Rows) error {
		var id string
		if err := rows.Scan(&id); err != nil {
			return err
		}
		if !wanted[id] {
			packed = append(packed, id)
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, id := range packed {
		if _, err = tx.ExecContext(ctx, `UPDATE data_objects SET transfer_state=$1,updated_at=$2 WHERE id=$3 AND transfer_state=$4`, transferDropping, now(), id, transferPacked); err != nil {
			return err
		}
	}
	return nil
}

// recordTransferTx records a pack made for the move of a temporal object
// (made.Transfer), or its removal (made.Dropped). Recording either again is
// no error.
func recordTransferTx(ctx context.Context, tx *sql.Tx, made PackMade, root string, projectID sql.NullString) error {
	var lifecycle string
	var transfer, listSHA, packSHA sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT lifecycle,transfer_state,manifest_sha256,pack_sha256 FROM data_objects WHERE id=$1`, made.ObjectID).
		Scan(&lifecycle, &transfer, &listSHA, &packSHA); err != nil {
		return err
	}
	if lifecycle != LifecycleTemporal {
		return fmt.Errorf("object %s is %s: only temporal data is packed for a move", made.ObjectID, lifecycle)
	}
	var project *string
	if projectID.Valid {
		project = &projectID.String
	}
	t := now()
	if made.Dropped {
		if transfer.String != transferDropping {
			return tx.Commit() // removed already (or packed again since: not this removal's)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE data_objects SET transfer_state=NULL,pack_path=NULL,pack_bytes=0,pack_sha256=NULL,updated_at=$1 WHERE id=$2`, t, made.ObjectID); err != nil {
			return err
		}
		if err := appendCoordinationEventTx(ctx, tx, "transfer_dropped", project, "object", made.ObjectID, nil, nil); err != nil {
			return err
		}
		return tx.Commit()
	}
	switch {
	case transfer.String == transferPacked && packSHA.String == made.PackSHA256:
		return tx.Commit()
	case transfer.String != transferPacking:
		return fmt.Errorf("object %s is not to be packed for a move (%s)", made.ObjectID, transfer.String)
	case made.ManifestSHA256 != listSHA.String:
		return fmt.Errorf("object %s changed since it was identified: its pack lists other files (sha256 %s, identity %s)", made.ObjectID, made.ManifestSHA256, listSHA.String)
	case !machine.IsPackPath(made.PackPath, root, made.ObjectID, ""):
		return fmt.Errorf("pack %s of object %s is not <root>/packs/<object id>.tar.zst under %s", made.PackPath, made.ObjectID, root)
	case !sha256Hex.MatchString(made.PackSHA256):
		return fmt.Errorf("pack of object %s: its sha256 is not 64 hex digits", made.ObjectID)
	case made.PackBytes <= 0:
		return fmt.Errorf("pack of object %s: impossible size", made.ObjectID)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE data_objects SET transfer_state=$1,pack_path=$2,pack_bytes=$3,pack_sha256=$4,updated_at=$5 WHERE id=$6`,
		transferPacked, made.PackPath, made.PackBytes, made.PackSHA256, t, made.ObjectID); err != nil {
		return err
	}
	if err := appendCoordinationEventTx(ctx, tx, "transfer_packed", project, "object", made.ObjectID, nil, map[string]any{"pack_bytes": made.PackBytes, "pack_sha256": made.PackSHA256}); err != nil {
		return err
	}
	return tx.Commit()
}

// transferWaitTx says why an execution on node waits for temporal object
// objectID being brought there (ok false: it is not being brought).
func transferWaitTx(ctx context.Context, q queryer, objectID, node string) (string, bool, error) {
	var state, sourceState string
	var transfer sql.NullString
	var from sql.NullString
	err := q.QueryRowContext(ctx, `SELECT d.state,d.source_state,d.transfer_state,x.node_id FROM data_objects d LEFT JOIN executors x ON x.id=d.executor_id WHERE d.id=$1`, objectID).
		Scan(&state, &sourceState, &transfer, &from)
	if err != nil || state != "packed" || sourceState != "present" {
		return "", false, err
	}
	switch transfer.String {
	case transferPacked:
		return "temporal data, being brought to " + node + " from " + from.String, true, nil
	case transferDropping:
		return "temporal data, to be packed again on " + from.String + " and brought to " + node, true, nil
	}
	return "temporal data, being packed on " + from.String + " to be brought to " + node, true, nil
}
