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
// durable store without a copy of it takes one, as it has room for (its
// last report, less its reserve and the copies being made there), smallest
// objects first, a few at a time (replicaBatch); an object nowhere durable
// yet is always copied before any replica (CopiesToMake). A replica is read
// from a durable copy on the node of the executor making it (the pack may be
// gone by then), else from one on another node, through the pack server of
// the agent that made that copy (pack_addr), with a token for that copy
// (object_copies.source_*): so every durable store, on whatever node, holds
// every durable object.

// replicaBatch bounds the replicas one CopiesToMake orders.
const replicaBatch = 50

// orderReplicasTx orders, for executor me, replicas of objects with a
// verified copy on a durable store it serves onto the durable stores it serves
// without a copy of them (targets in order, room as copyRoomTx says, less
// what it orders).
func orderReplicasTx(ctx context.Context, tx *sql.Tx, me executorInfo, executorID string, targets []machine.Store, room map[string]int64, t string) error {
	var served []machine.Store
	var servedIDs []string
	for _, s := range targets {
		if me.serves(s, executorID) {
			served = append(served, s)
			servedIDs = append(servedIDs, s.ID)
		}
	}
	if len(served) < 2 {
		return nil // nowhere here to replicate between
	}
	type candidate struct {
		id, created, projectID string
		bytes                  int64
		on                     map[string]bool
	}
	var candidates []candidate
	err := eachRow(ctx, tx, `SELECT d.id,d.created_at,COALESCE(d.project_scope_id,''),COALESCE(d.pack_bytes,0),
		COALESCE((SELECT string_agg(c.store_id,',') FROM object_copies c WHERE c.object_id=d.id),'')
		FROM data_objects d WHERE d.state='packed' AND d.lifecycle='durable'
		AND EXISTS(SELECT 1 FROM object_copies v JOIN executors x ON x.id=v.executor_id WHERE v.object_id=d.id AND v.state='verified' AND v.check_problem IS NULL
			AND (v.store_id=ANY($1::text[]) OR COALESCE(x.attributes_json::jsonb->>'pack_addr','')<>''))
		AND (SELECT COUNT(*) FROM object_copies c WHERE c.object_id=d.id AND c.store_id=ANY($1::text[]))<$2
		ORDER BY d.pack_bytes,d.id LIMIT 2000`, []any{servedIDs, len(served)}, func(rows *sql.Rows) error {
		var c candidate
		var on string
		if err := rows.Scan(&c.id, &c.created, &c.projectID, &c.bytes, &on); err != nil {
			return err
		}
		c.on = map[string]bool{}
		for _, id := range strings.Split(on, ",") {
			c.on[id] = true
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
		for _, target := range served {
			if c.on[target.ID] || room[target.ID] < c.bytes {
				continue
			}
			month := c.created[0:4] + c.created[5:7]
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
// that executor me does not serve, made by an executor whose node's pack
// server serves it (its pack_addr): where a replica is read when no copy is
// on me's node.
func remoteCopyTx(ctx context.Context, tx *sql.Tx, me executorInfo, executorID, objectID string, byID map[string]machine.Store) (CopySource, string, bool, error) {
	var src CopySource
	var node string
	found := false
	err := eachRow(ctx, tx, `SELECT c.store_id,c.root,c.month,x.node_id,COALESCE(x.attributes_json::jsonb->>'pack_addr','') FROM object_copies c JOIN executors x ON x.id=c.executor_id
		WHERE c.object_id=$1 AND c.state='verified' AND c.check_problem IS NULL ORDER BY c.store_id`, []any{objectID}, func(rows *sql.Rows) error {
		var storeID, root, month, nodeID, addr string
		if err := rows.Scan(&storeID, &root, &month, &nodeID, &addr); err != nil {
			return err
		}
		d, ok := byID[storeID]
		if found || addr == "" || !ok || d.Class != machine.StoreDurable || d.Root != root || d.MovedFrom != "" || d.Node != nodeID || me.serves(d, executorID) {
			return nil
		}
		src, node, found = CopySource{StoreID: storeID, Root: root, Month: month, Addr: addr}, nodeID, true
		return nil
	})
	return src, node, found, err
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
