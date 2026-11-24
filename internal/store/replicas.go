package store

import (
	"context"
	"database/sql"
	"strings"

	"kairo/internal/machine"
)

// Replicas: a durable object is kept on every durable store, not on one
// only, so that losing a store (a disk deleted or failed) loses none of what
// Kairo keeps. Once an object has its first verified copy, each other
// durable store without a copy of it takes one if it has room above the
// reserve and shedding margin and has not shed that object in the last
// 24 hours. The smallest objects go first, a few at a time (replicaBatch).
// An object nowhere durable yet goes before any replica (CopiesToMake).
// A replica is read from a durable copy on the node of the executor making
// it (the pack may be gone by then), else from one on another node, through
// the pack server of the agent that made that copy (pack_addr), with a
// token for that copy (object_copies.source_*).

// replicaBatch bounds the replicas one CopiesToMake orders.
const replicaBatch = 50

// orderReplicasTx orders, for executor me, replicas of objects with a
// verified copy on a durable store it serves onto the durable stores it serves
// without a copy of them (targets in order, room as copyRoomTx says, less
// the shedding margin and what it orders).
func orderReplicasTx(ctx context.Context, tx *sql.Tx, me executorInfo, executorID string, targets []machine.Store, room map[string]int64, t string) error {
	var eligible []machine.Store
	var eligibleIDs []string
	for _, s := range targets {
		if !me.serves(s, executorID) {
			continue
		}
		if room[s.ID] == unknownRoom || room[s.ID] >= shedMinShort {
			eligible = append(eligible, s)
			eligibleIDs = append(eligibleIDs, s.ID)
		}
	}
	if len(eligible) == 0 {
		return nil // nowhere here to replicate to
	}
	type candidate struct {
		id, created, projectID string
		bytes                  int64
		blocked                map[string]bool
	}
	var candidates []candidate
	err := eachRow(ctx, tx, `SELECT d.id,d.created_at,COALESCE(d.project_scope_id,''),COALESCE(d.pack_bytes,0),COALESCE(b.stores,'')
		FROM data_objects d CROSS JOIN LATERAL (
			SELECT string_agg(store_id,',') AS stores, COUNT(*) AS n FROM (
				SELECT c.store_id FROM object_copies c WHERE c.object_id=d.id AND c.store_id=ANY($1::text[])
				UNION
				SELECT e.payload_json::jsonb->>'store_id' FROM coordination_events e WHERE e.event_type='copy_shed' AND e.aggregate_id=d.id
				AND e.payload_json::jsonb->>'store_id'=ANY($1::text[]) AND e.created_at::timestamptz>$3::text::timestamptz-INTERVAL '24 hours'
			) blocked
		) b
		WHERE d.state='packed' AND d.lifecycle='durable' AND b.n<$2
		AND EXISTS(SELECT 1 FROM object_copies v WHERE v.object_id=d.id AND v.state='verified' AND v.check_problem IS NULL)
		ORDER BY d.pack_bytes,d.id LIMIT 2000`, []any{eligibleIDs, len(eligible), t}, func(rows *sql.Rows) error {
		var c candidate
		var blocked string
		if err := rows.Scan(&c.id, &c.created, &c.projectID, &c.bytes, &blocked); err != nil {
			return err
		}
		c.blocked = map[string]bool{}
		for _, id := range strings.Split(blocked, ",") {
			c.blocked[id] = true
		}
		candidates = append(candidates, c)
		return nil
	})
	if err != nil {
		return err
	}
	ordered := 0
	for _, c := range candidates {
		if ordered >= replicaBatch {
			break
		}
		for _, target := range eligible {
			available := room[target.ID]
			if available != unknownRoom {
				// The margin limits replicas, not the room shedding sees.
				available -= shedMinShort
			}
			if c.blocked[target.ID] || available < c.bytes {
				continue
			}
			month := c.created[0:4] + c.created[5:7]
			if err := checkMade(recCopy, "copying"); err != nil {
				return err
			}
			result, err := tx.ExecContext(ctx, `INSERT INTO object_copies(object_id,store_id,executor_id,root,month,state,created_at,updated_at) VALUES($1,$2,$3,$4,$5,'copying',$6,$7)
				ON CONFLICT(object_id,store_id) DO NOTHING`, c.id, target.ID, executorID, target.Root, month, t, t)
			if err != nil {
				return err
			}
			if n, _ := result.RowsAffected(); n == 0 {
				continue
			}
			if room[target.ID] != unknownRoom {
				room[target.ID] -= c.bytes
			}
			var project *string
			if c.projectID != "" {
				project = &c.projectID
			}
			if err = appendCoordinationEventTx(ctx, tx, "copy_ordered", project, "object", c.id, nil, map[string]any{"store_id": target.ID, "executor_id": executorID, "replica": true}); err != nil {
				return err
			}
			ordered++
		}
	}
	return nil
}

// remoteCopyTx is a verified copy of objectID on a durable store as declared
// that executor me does not serve, read through the pack server of whoever
// serves that store now (storeServerTx): where a replica is read when no copy
// is on me's node.
func remoteCopyTx(ctx context.Context, tx *sql.Tx, me executorInfo, executorID, objectID string, byID map[string]machine.Store) (CopySource, string, bool, error) {
	type copyRow struct{ storeID, root, month string }
	var rows []copyRow
	err := eachRow(ctx, tx, `SELECT c.store_id,c.root,c.month FROM object_copies c
		WHERE c.object_id=$1 AND c.state='verified' AND c.check_problem IS NULL ORDER BY c.store_id`, []any{objectID}, func(r *sql.Rows) error {
		var c copyRow
		if err := r.Scan(&c.storeID, &c.root, &c.month); err != nil {
			return err
		}
		rows = append(rows, c)
		return nil
	})
	if err != nil {
		return CopySource{}, "", false, err
	}
	for _, c := range rows {
		d, ok := byID[c.storeID]
		if !ok || d.Class != machine.StoreDurable || d.Root != c.root || d.MovedFrom != "" || me.serves(d, executorID) {
			continue
		}
		server, found, err := storeServerTx(ctx, tx, d)
		if err != nil {
			return CopySource{}, "", false, err
		}
		if found {
			return CopySource{StoreID: c.storeID, Root: c.root, Month: c.month, Addr: server.attrs["pack_addr"]}, server.nodeID, true, nil
		}
	}
	return CopySource{}, "", false, nil
}

// localCopiesTx is, for each object executor me is copying, a verified copy
// of it on a durable store me serves at its declared root: where a replica of
// it is read.
func localCopiesTx(ctx context.Context, tx *sql.Tx, me executorInfo, executorID string, byID map[string]machine.Store) (map[string]CopySource, error) {
	out := map[string]CopySource{}
	err := eachRow(ctx, tx, `SELECT c.object_id,c.store_id,c.root,c.month FROM object_copies c
		WHERE c.state='verified' AND c.object_id IN (SELECT object_id FROM object_copies WHERE executor_id=$1 AND state='copying')
		ORDER BY c.object_id,c.store_id`, []any{executorID}, func(rows *sql.Rows) error {
		var objectID, storeID, root, month string
		if err := rows.Scan(&objectID, &storeID, &root, &month); err != nil {
			return err
		}
		if d, ok := byID[storeID]; ok && d.Class == machine.StoreDurable && d.Root == root && d.MovedFrom == "" && me.serves(d, executorID) {
			if _, have := out[objectID]; !have {
				out[objectID] = CopySource{Local: true, StoreID: storeID, Root: root, Month: month}
			}
		}
		return nil
	})
	return out, err
}
