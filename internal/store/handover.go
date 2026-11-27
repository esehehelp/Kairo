package store

import (
	"context"
	"database/sql"
)

// handOverRetiredDataTx gives the data work of node's disabled executors to
// the executor that serves its store now (storeServerTx): what Kairo still
// has to do with what they wrote (pack it, remove its source once copied,
// pack it again to bring it, keep or settle a scratch, land an output).
// That work is found by executor, so data written by an executor retired for
// another (a host executor for a container one) would wait for it forever.
// Only records with work left move; finished ones keep their writer. It
// returns how many moved.
func handOverRetiredDataTx(ctx context.Context, tx *sql.Tx, nodeID string) (int64, error) {
	declared, err := declaredStores(ctx, tx)
	if err != nil {
		return 0, err
	}
	const retired = `(SELECT id FROM executors WHERE node_id=$1 AND NOT enabled)`
	statements := []string{
		`UPDATE data_objects SET executor_id=$2 WHERE store_id=$3 AND executor_id IN ` + retired + `
			AND (state='packing' OR state='packed' AND (source_state IN('present','deleting') OR transfer_state IS NOT NULL))`,
		`UPDATE run_outputs SET executor_id=$2 WHERE store_id=$3 AND executor_id IN ` + retired + ` AND state='active'`,
		`UPDATE attempt_scratch SET executor_id=$2 WHERE store_id=$3 AND executor_id IN ` + retired + `
			AND settled_at IS NULL AND state IN('active','missing','deleting','kept')`,
		`UPDATE output_landings SET executor_id=$2 WHERE store_id=$3 AND executor_id IN ` + retired + ` AND state NOT IN('done','failed')`,
	}
	var moved int64
	for _, s := range declared {
		if s.Node != nodeID {
			continue
		}
		server, found, err := storeServerTx(ctx, tx, s)
		if err != nil {
			return moved, err
		}
		if !found {
			continue // no one serves it now: it waits for one
		}
		for _, q := range statements {
			result, err := tx.ExecContext(ctx, q, nodeID, server.id, s.ID)
			if err != nil {
				return moved, err
			}
			n, _ := result.RowsAffected()
			moved += n
		}
	}
	if moved > 0 {
		if err := appendCoordinationEventTx(ctx, tx, "data_handed_over", nil, "node", nodeID, nil, map[string]any{"records": moved}); err != nil {
			return moved, err
		}
	}
	return moved, nil
}
