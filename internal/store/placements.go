package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"kairo/internal/machine"
)

// An input not on the node an execution runs on is brought there
// (inputs.go): its data object is fetched from a verified copy on a durable
// store (or, while it is still there, from the pack where it was made) and
// unpacked under <root>/inputs/<object id> on a volatile store of the node
// (of those an executor of the node serves, the one
// with the most room for the pack and unpacked tree, the first by id among
// equals; a fetch still waiting for room moves to the one with the most room:
// fetchRoomTx), by that executor, which checks the pack against its size and sha256 and each
// file against the pack's file list (object_placements: fetching, then
// ready). A fetch is ordered for each input an execution waiting on a node
// lacks there (orderInputFetches), or by kairo data fetch. What is brought
// stays: it is read in place by every later execution on that node that
// names it.

// FetchOrder is an object its executor is to bring to the volatile store
// StoreID at Root, unpacked under <root>/inputs/<object id>, from the pack
// read from Source (a durable copy when Source.Month is set): the pack must
// be PackBytes long with PackSHA256, its file list ManifestSHA256.
type FetchOrder struct {
	ObjectID       string     `json:"object_id"`
	StoreID        string     `json:"store_id"`
	Root           string     `json:"root"`
	PackBytes      int64      `json:"pack_bytes"`
	ContentBytes   int64      `json:"content_bytes"`
	PackSHA256     string     `json:"pack_sha256"`
	ManifestSHA256 string     `json:"manifest_sha256"`
	Source         CopySource `json:"source"`
}

// FetchMade is an object an executor brought: where it is unpacked and the
// bytes it holds.
type FetchMade struct {
	ObjectID   string `json:"object_id"`
	StoreID    string `json:"store_id"`
	ExecutorID string `json:"executor_id"`
	Path       string `json:"path"`
	Bytes      int64  `json:"bytes"`
}

// fetchTokenLife is how long a fetch's token can be used.
const fetchTokenLife = 12 * time.Hour

// inputDirOf is <root>/inputs/<object id>, spelled with the root's own
// separators.
func inputDirOf(root, objectID string) string {
	sep := "/"
	if !strings.HasPrefix(root, "/") && strings.Contains(root, `\`) {
		sep = `\`
	}
	return strings.TrimRight(root, `/\`) + sep + "inputs" + sep + objectID
}

// fetchRoomTx is what each volatile store of stores can still take for a
// fetch: its free bytes as last reported (usable), less its
// reserve_free_bytes (at least minFetchHeadroom) and the pack and unpacked
// tree of each pending fetch except objectID's own; unknownRoom for one
// never reported.
func fetchRoomTx(ctx context.Context, q queryer, stores []machine.Store, objectID string) (map[string]int64, error) {
	room := map[string]int64{}
	for _, s := range stores {
		var free sql.NullInt64
		err := q.QueryRowContext(ctx, `SELECT free_bytes FROM store_observations WHERE store_id=$1 AND usable`, s.ID).Scan(&free)
		if errors.Is(err, sql.ErrNoRows) || err == nil && !free.Valid {
			room[s.ID] = unknownRoom
			continue
		}
		if err != nil {
			return nil, err
		}
		var fetching int64
		if err = q.QueryRowContext(ctx, `SELECT COALESCE(SUM(COALESCE(d.pack_bytes,0)+COALESCE(d.content_bytes,0)),0)::bigint FROM object_placements p JOIN data_objects d ON d.id=p.object_id
			WHERE p.store_id=$1 AND p.state='fetching' AND p.object_id<>$2`, s.ID, objectID).Scan(&fetching); err != nil {
			return nil, err
		}
		reserve := s.ReserveFreeBytes
		if reserve < minFetchHeadroom {
			reserve = minFetchHeadroom
		}
		room[s.ID] = free.Int64 - reserve - fetching
	}
	return room, nil
}

// fetchMoveAfter: a fetch not ready this long after it was ordered, on a
// store without room for it, is moved (moveWaitingFetchesTx).
const fetchMoveAfter = time.Hour

// minFetchHeadroom is what a fetch leaves free on a store declaring no
// reserve (the agent's own check: executor/pack.go headroom).
const minFetchHeadroom = 1 << 30

// placementStoreTx is the volatile store of a node objects are brought to,
// and the executor that brings them: the first store by id that an executor
// of the node serves.
func placementStoreTx(ctx context.Context, q queryer, declared []machine.Store, nodeID string) (machine.Store, string, bool, error) {
	stores, err := placementStoresTx(ctx, q, declared, nodeID)
	if err != nil || len(stores) == 0 {
		return machine.Store{}, "", false, err
	}
	return stores[0].store, stores[0].executor, true, nil
}

// placementStoreWithRoomTx is the store of placementStoresTx with the most
// room (fetchRoomTx), the first by id among equals: the volatile stores of a
// node are one pool, none preferred for its id (user 2026-11-16: 「Volatileに
// node以外の区別はしない方が正しい」; bringing to the first by id with room
// filled a system drive that sorts first).
func placementStoreWithRoomTx(ctx context.Context, q queryer, declared []machine.Store, nodeID, objectID string) (machine.Store, string, bool, error) {
	stores, err := placementStoresTx(ctx, q, declared, nodeID)
	if err != nil || len(stores) == 0 {
		return machine.Store{}, "", false, err
	}
	all := make([]machine.Store, len(stores))
	for i, s := range stores {
		all[i] = s.store
	}
	room, err := fetchRoomTx(ctx, q, all, objectID)
	if err != nil {
		return machine.Store{}, "", false, err
	}
	best := stores[0]
	for _, s := range stores[1:] {
		if room[s.store.ID] > room[best.store.ID] {
			best = s
		}
	}
	return best.store, best.executor, true, nil
}

type placementStore struct {
	store    machine.Store
	executor string
}

// placementStoresTx are the volatile stores of a node objects can be brought
// to, by id, each with the executor that brings them: one of the node
// serving it.
func placementStoresTx(ctx context.Context, q queryer, declared []machine.Store, nodeID string) ([]placementStore, error) {
	var executorIDs []string
	err := eachRow(ctx, q, `SELECT id FROM executors WHERE node_id=$1 ORDER BY id`, []any{nodeID}, func(rows *sql.Rows) error {
		var id string
		if err := rows.Scan(&id); err != nil {
			return err
		}
		executorIDs = append(executorIDs, id)
		return nil
	})
	if err != nil {
		return nil, err
	}
	stores := make([]machine.Store, 0, len(declared))
	for _, d := range declared {
		if d.Node == nodeID && d.Class == machine.StoreVolatile {
			stores = append(stores, d)
		}
	}
	sort.Slice(stores, func(i, j int) bool { return stores[i].ID < stores[j].ID })
	var out []placementStore
	for _, d := range stores {
		for _, executorID := range executorIDs {
			info, err := executorInfoTx(ctx, q, executorID)
			if err != nil {
				return nil, err
			}
			if info.serves(d, executorID) {
				out = append(out, placementStore{d, executorID})
				break
			}
		}
	}
	return out, nil
}

// moveWaitingFetchesTx moves each fetch executorID is to make whose store
// has no room for its pack and unpacked tree to the other volatile store
// of the node it serves with the most room, if one has room for it (event
// fetch_moved): a fetch waiting
// for room it will not get would keep its execution waiting for good. Only
// a fetch ordered fetchMoveAfter ago or more is moved: the agent checks the
// room before it writes anything, but the daemon cannot tell a fetch it
// refused from one it is writing (that store's free bytes fall as it
// writes), and one being written is done well within that.
func moveWaitingFetchesTx(ctx context.Context, tx *sql.Tx, me executorInfo, executorID string, declared []machine.Store, byID map[string]machine.Store) error {
	type waiting struct {
		objectID, storeID, projectID string
		bytes                        int64
	}
	var fetches []waiting
	err := eachRow(ctx, tx, `SELECT p.object_id,p.store_id,COALESCE(d.project_scope_id,''),COALESCE(d.pack_bytes,0)+COALESCE(d.content_bytes,0) FROM object_placements p JOIN data_objects d ON d.id=p.object_id
		WHERE p.executor_id=$1 AND p.state='fetching' AND p.created_at::timestamptz<$2::text::timestamptz ORDER BY p.created_at,p.object_id`, []any{executorID, time.Now().UTC().Add(-fetchMoveAfter).Format(time.RFC3339Nano)}, func(rows *sql.Rows) error {
		var w waiting
		if err := rows.Scan(&w.objectID, &w.storeID, &w.projectID, &w.bytes); err != nil {
			return err
		}
		fetches = append(fetches, w)
		return nil
	})
	if err != nil || len(fetches) == 0 {
		return err
	}
	stores, err := placementStoresTx(ctx, tx, declared, me.nodeID)
	if err != nil {
		return err
	}
	var mine []machine.Store
	for _, s := range stores {
		if s.executor == executorID {
			mine = append(mine, s.store)
		}
	}
	for _, w := range fetches {
		room, err := fetchRoomTx(ctx, tx, mine, w.objectID)
		if err != nil {
			return err
		}
		if _, declaredHere := byID[w.storeID]; declaredHere && room[w.storeID] >= w.bytes {
			continue
		}
		// the other store with the most room for it (one pool: placementStoreWithRoomTx)
		var to []machine.Store
		for _, s := range mine {
			if s.ID == w.storeID || room[s.ID] < w.bytes {
				continue
			}
			if len(to) == 0 || room[s.ID] > room[to[0].ID] {
				to = []machine.Store{s}
			}
		}
		for _, s := range to {
			t := now()
			result, err := tx.ExecContext(ctx, `UPDATE object_placements SET store_id=$1,root=$2,path=$3,token_hash=NULL,token_expires_at=NULL,updated_at=$4
				WHERE object_id=$5 AND store_id=$6 AND executor_id=$7 AND state='fetching'
				AND NOT EXISTS(SELECT 1 FROM object_placements o WHERE o.object_id=$5 AND o.store_id=$1)`, s.ID, s.Root, inputDirOf(s.Root, w.objectID), t, w.objectID, w.storeID, executorID)
			if err != nil {
				return err
			}
			if n, _ := result.RowsAffected(); n == 1 {
				var project *string
				if w.projectID != "" {
					project = &w.projectID
				}
				if err = appendCoordinationEventTx(ctx, tx, "fetch_moved", project, "object", w.objectID, nil, map[string]any{"from": w.storeID, "to": s.ID, "bytes": w.bytes}); err != nil {
					return err
				}
			}
			break
		}
	}
	return nil
}

// orderFetchTx orders an object brought to a node, unless it is there or
// being brought already (or the node has no store to bring it to).
func orderFetchTx(ctx context.Context, tx *sql.Tx, declared []machine.Store, objectID, nodeID string) (bool, error) {
	var there bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM object_placements p JOIN executors x ON x.id=p.executor_id WHERE p.object_id=$1 AND x.node_id=$2)`, objectID, nodeID).Scan(&there); err != nil || there {
		return false, err
	}
	s, executorID, ok, err := placementStoreWithRoomTx(ctx, tx, declared, nodeID, objectID)
	if err != nil || !ok {
		return false, err
	}
	t := now()
	if err := checkMade(recPlacement, "fetching"); err != nil {
		return false, err
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO object_placements(object_id,store_id,executor_id,root,path,state,created_at,updated_at) VALUES($1,$2,$3,$4,$5,'fetching',$6,$7)
		ON CONFLICT(object_id,store_id) DO NOTHING`, objectID, s.ID, executorID, s.Root, inputDirOf(s.Root, objectID), t, t)
	if err != nil {
		return false, err
	}
	if n, _ := result.RowsAffected(); n == 0 {
		return false, nil
	}
	var projectID sql.NullString
	if err = tx.QueryRowContext(ctx, `SELECT project_scope_id FROM data_objects WHERE id=$1`, objectID).Scan(&projectID); err != nil {
		return false, err
	}
	var project *string
	if projectID.Valid {
		project = &projectID.String
	}
	return true, appendCoordinationEventTx(ctx, tx, "fetch_ordered", project, "object", objectID, nil, map[string]any{"node": nodeID, "store_id": s.ID, "executor_id": executorID})
}

// FetchRequest asks for data objects of a project brought to a node (kairo
// data fetch): by name (the newest packed with it) or by id.
type FetchRequest struct {
	Project string   `json:"project"`
	Node    string   `json:"node"`
	Data    []string `json:"data,omitempty"`
	Objects []string `json:"objects,omitempty"`
}

// FetchedObject is an object a fetch request names and whether a fetch was
// ordered for it now (false: there or on its way already).
type FetchedObject struct {
	ObjectID string `json:"object_id"`
	Ordered  bool   `json:"ordered"`
}

// FetchData orders the objects a request names brought to its node.
func (s *Store) FetchData(ctx context.Context, req FetchRequest) ([]FetchedObject, error) {
	if len(req.Data)+len(req.Objects) == 0 {
		return nil, fmt.Errorf("%w: a fetch names data or objects", ErrImportRefused)
	}
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var projectID string
	err = tx.QueryRowContext(ctx, `SELECT id FROM coordination_scopes WHERE kind='project' AND external_key=$1`, req.Project).Scan(&projectID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w: unknown project %q", ErrImportRefused, req.Project)
	}
	if err != nil {
		return nil, err
	}
	declared, err := declaredStores(ctx, tx)
	if err != nil {
		return nil, err
	}
	if _, _, ok, err := placementStoreTx(ctx, tx, declared, req.Node); err != nil || !ok {
		if err == nil {
			err = fmt.Errorf("%w: node %q has no volatile store an executor brings data to", ErrImportRefused, req.Node)
		}
		return nil, err
	}
	var ids []string
	for _, name := range req.Data {
		var id string
		err := tx.QueryRowContext(ctx, `SELECT id FROM data_objects WHERE project_scope_id=$1 AND name=$2 AND state='packed' ORDER BY packed_at DESC,id DESC LIMIT 1`, projectID, name).Scan(&id)
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: the project has no packed data %q", ErrImportRefused, name)
		}
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	for _, id := range req.Objects {
		var found bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM data_objects WHERE project_scope_id=$1 AND id=$2 AND state='packed')`, projectID, id).Scan(&found); err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("%w: the project has no packed object %q", ErrImportRefused, id)
		}
		ids = append(ids, id)
	}
	out := make([]FetchedObject, 0, len(ids))
	for _, id := range ids {
		ordered, err := orderFetchTx(ctx, tx, declared, id, req.Node)
		if err != nil {
			return nil, err
		}
		out = append(out, FetchedObject{ObjectID: id, Ordered: ordered})
	}
	return out, tx.Commit()
}

// PlacementStatus is the state of a fetch on a node. Its path is only
// available once the object is ready to read.
type PlacementStatus struct {
	State   string `json:"state"`
	Path    string `json:"path,omitempty"`
	Problem string `json:"problem,omitempty"`
}

// DataPlacement reads the placement on a node and checks whether a pending
// fetch still has a pack to read. Executors retry errors that may clear.
func (s *Store) DataPlacement(ctx context.Context, objectID, node string) (PlacementStatus, error) {
	var status PlacementStatus
	var objectState, lifecycle, transfer, sourceState string
	var verifiedCopy bool
	err := s.db.QueryRowContext(ctx, `SELECT d.state,d.lifecycle,COALESCE(d.transfer_state,''),d.source_state,
		EXISTS(SELECT 1 FROM object_copies c WHERE c.object_id=d.id AND c.state='verified' AND c.check_problem IS NULL),
		COALESCE(p.state,''),COALESCE(CASE WHEN p.state='ready' THEN p.path END,'')
		FROM data_objects d LEFT JOIN LATERAL (
			SELECT p.state,p.path FROM object_placements p JOIN executors x ON x.id=p.executor_id
			WHERE p.object_id=d.id AND x.node_id=$2
			ORDER BY CASE p.state WHEN 'ready' THEN 0 WHEN 'fetching' THEN 1 ELSE 2 END,p.store_id LIMIT 1
		) p ON true WHERE d.id=$1`, objectID, node).Scan(&objectState, &lifecycle, &transfer, &sourceState, &verifiedCopy, &status.State, &status.Path)
	if errors.Is(err, sql.ErrNoRows) {
		return status, fmt.Errorf("%w: object %s", ErrNotFound, objectID)
	}
	if err != nil || status.State == "ready" {
		return status, err
	}
	switch {
	case objectState == "abandoned":
		return PlacementStatus{State: "abandoned", Problem: "the object was abandoned"}, nil
	case objectState != "packed":
		status.Problem = "the object is not packed"
	case status.State == "":
		status.Problem = "no placement was ordered or it was removed"
	case status.State == "evicting":
		status.Problem = "the placement is being evicted"
	case lifecycle == LifecycleTemporal && transfer != transferPacking && transfer != transferPacked:
		status.Problem = "the object has no transfer pack"
	case sourceState != "present" && sourceState != SourceDeleting && !verifiedCopy:
		status.Problem = "there is no pack to fetch"
	}
	if status.Problem != "" {
		status.State = "failed"
	}
	return status, nil
}

// FetchesToMake is what an executor is to bring now: its placements being
// fetched whose object is packed, each with where to read the pack: a
// verified durable copy or the pack where it was made, on a store this
// executor serves (Local) before another node's agent (with a token issued
// now, the fetch's earlier one no longer working).
func (s *Store) FetchesToMake(ctx context.Context, executorID string) ([]FetchOrder, error) {
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
	byID := map[string]machine.Store{}
	for _, d := range declared {
		byID[d.ID] = d
	}
	if err = moveWaitingFetchesTx(ctx, tx, me, executorID, declared, byID); err != nil {
		return nil, err
	}
	type row struct {
		order                                          FetchOrder
		packExecutor, packStore, packRoot, sourceState string
	}
	var rows []row
	err = eachRow(ctx, tx, `SELECT p.object_id,p.store_id,p.root,d.pack_bytes,COALESCE(d.content_bytes,0),d.pack_sha256,d.manifest_sha256,d.executor_id,d.store_id,d.root,d.source_state
		FROM object_placements p JOIN data_objects d ON d.id=p.object_id WHERE p.executor_id=$1 AND p.state='fetching' AND d.state='packed' AND (d.lifecycle='durable' OR d.transfer_state='packed') ORDER BY p.created_at,p.object_id`, []any{executorID}, func(r *sql.Rows) error {
		var x row
		if err := r.Scan(&x.order.ObjectID, &x.order.StoreID, &x.order.Root, &x.order.PackBytes, &x.order.ContentBytes, &x.order.PackSHA256, &x.order.ManifestSHA256,
			&x.packExecutor, &x.packStore, &x.packRoot, &x.sourceState); err != nil {
			return err
		}
		rows = append(rows, x)
		return nil
	})
	if err != nil {
		return nil, err
	}
	type source struct {
		src  CopySource
		node string
	}
	expires := time.Now().UTC().Add(fetchTokenLife).Format(time.RFC3339Nano)
	out := []FetchOrder{}
	for _, x := range rows {
		if d, ok := byID[x.order.StoreID]; !ok || d.Class != machine.StoreVolatile || d.Root != x.order.Root || !me.serves(d, executorID) {
			continue // brought only to the store as it is still declared, by an executor serving it
		}
		var local, remote []source
		type copyRow struct{ storeID, root, month, copier string }
		var copyRows []copyRow
		err = eachRow(ctx, tx, `SELECT c.store_id,c.root,c.month,c.executor_id FROM object_copies c WHERE c.object_id=$1 AND c.state='verified' AND c.check_problem IS NULL ORDER BY c.store_id`,
			[]any{x.order.ObjectID}, func(r *sql.Rows) error {
				var c copyRow
				if err := r.Scan(&c.storeID, &c.root, &c.month, &c.copier); err != nil {
					return err
				}
				copyRows = append(copyRows, c)
				return nil
			})
		if err != nil {
			return nil, err
		}
		for _, c := range copyRows {
			d, ok := byID[c.storeID]
			if !ok || d.Class != machine.StoreDurable || d.Root != c.root {
				continue
			}
			if me.serves(d, executorID) {
				local = append(local, source{src: CopySource{Local: true, StoreID: c.storeID, Root: c.root, Month: c.month}})
				continue
			}
			// read through whoever serves the copy's store now
			info, found, err := storeServerTx(ctx, tx, d)
			if err != nil {
				return nil, err
			}
			if found {
				remote = append(remote, source{src: CopySource{StoreID: c.storeID, Root: c.root, Month: c.month, Addr: info.attrs["pack_addr"]}, node: info.nodeID})
			}
		}
		// the pack where it was made, until its source is settled (it goes then)
		if d, ok := byID[x.packStore]; ok && d.Root == x.packRoot && (x.sourceState == "present" || x.sourceState == SourceDeleting) {
			if me.serves(d, executorID) {
				local = append(local, source{src: CopySource{Local: true, StoreID: x.packStore, Root: x.packRoot}})
			} else if info, found, err := storeServerTx(ctx, tx, d); err != nil {
				return nil, err
			} else if found {
				remote = append(remote, source{src: CopySource{StoreID: x.packStore, Root: x.packRoot, Addr: info.attrs["pack_addr"]}, node: info.nodeID})
			}
		}
		o := x.order
		switch {
		case len(local) > 0:
			o.Source = local[0].src
			// A local read needs no token, but it still keeps its copy from being shed.
			if _, err = tx.ExecContext(ctx, `UPDATE object_placements SET token_hash=NULL,token_expires_at=NULL,source_store_id=$1,source_root=$2,source_month=$3,source_node_id=$4,updated_at=$5
				WHERE object_id=$6 AND store_id=$7`, o.Source.StoreID, o.Source.Root, nullable(o.Source.Month), me.nodeID, now(), o.ObjectID, o.StoreID); err != nil {
				return nil, err
			}
		case len(remote) > 0:
			chosen := remote[0]
			for _, r := range remote {
				if r.src.Addr != "" {
					chosen = r
					break
				}
			}
			o.Source = chosen.src
			if o.Source.Addr != "" {
				token, hash, err := newSecret("kairo_fetch_")
				if err != nil {
					return nil, err
				}
				if _, err = tx.ExecContext(ctx, `UPDATE object_placements SET token_hash=$1,token_expires_at=$2,source_store_id=$3,source_root=$4,source_month=$5,source_node_id=$6,updated_at=$7
					WHERE object_id=$8 AND store_id=$9`, hash, expires, o.Source.StoreID, o.Source.Root, nullable(o.Source.Month), chosen.node, now(), o.ObjectID, o.StoreID); err != nil {
					return nil, err
				}
				o.Source.Token = token
			}
		default:
			continue // nowhere to read it from now
		}
		out = append(out, o)
	}
	return out, tx.Commit()
}

// RecordFetch records an object an executor brought, unpacked at
// <root>/inputs/<object id> of the store it was ordered to. Recording it
// again is no error.
func (s *Store) RecordFetch(ctx context.Context, made FetchMade) error {
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var executorID, root, state string
	var projectID sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT p.executor_id,p.root,p.state,d.project_scope_id FROM object_placements p JOIN data_objects d ON d.id=p.object_id
		WHERE p.object_id=$1 AND p.store_id=$2 FOR UPDATE OF p`, made.ObjectID, made.StoreID).Scan(&executorID, &root, &state, &projectID)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	switch {
	case executorID != made.ExecutorID:
		return fmt.Errorf("object %s is executor %s's to bring to %s, not %s's", made.ObjectID, executorID, made.StoreID, made.ExecutorID)
	case machine.StoreRootKey(made.Path) != machine.StoreRootKey(inputDirOf(root, made.ObjectID)):
		return fmt.Errorf("object %s brought to %s, not <root>/inputs/<object id> under %s", made.ObjectID, made.Path, root)
	case made.Bytes < 0:
		return fmt.Errorf("object %s: impossible size", made.ObjectID)
	}
	if state != "fetching" {
		return tx.Commit() // ready, or being evicted
	}
	t := now()
	if err := checkTransition(recPlacement, state, "ready"); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE object_placements SET state='ready',bytes=$1,token_hash=NULL,token_expires_at=NULL,ready_at=$2,updated_at=$3 WHERE object_id=$4 AND store_id=$5`,
		made.Bytes, t, t, made.ObjectID, made.StoreID); err != nil {
		return err
	}
	var project *string
	if projectID.Valid {
		project = &projectID.String
	}
	if err = appendCoordinationEventTx(ctx, tx, "fetched", project, "object", made.ObjectID, nil, map[string]any{"store_id": made.StoreID, "path": made.Path, "bytes": made.Bytes}); err != nil {
		return err
	}
	return tx.Commit()
}

// orderInputFetches orders brought, for every execution waiting on a node
// (its executor selector naming one), each input it lacks there that is a
// packed object.
func (s *Store) orderInputFetches(ctx context.Context) error {
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	type waiting struct{ id, project, selector, inputs string }
	var found []waiting
	err = eachRow(ctx, tx, `SELECT id,project_scope_id,executor_selector_json,inputs_json FROM execution_requests WHERE state='waiting' AND inputs_json IS NOT NULL`, nil, func(rows *sql.Rows) error {
		var w waiting
		if err := rows.Scan(&w.id, &w.project, &w.selector, &w.inputs); err != nil {
			return err
		}
		found = append(found, w)
		return nil
	})
	if err != nil || len(found) == 0 {
		return err
	}
	declared, err := declaredStores(ctx, tx)
	if err != nil {
		return err
	}
	for _, w := range found {
		node := selectorNode(w.selector)
		if node == "" {
			continue
		}
		inputs, err := decodeInputs(w.inputs)
		if err != nil {
			continue
		}
		for _, in := range inputs {
			src, err := inputSourceTx(ctx, tx, declared, w.project, in)
			if err != nil {
				return err
			}
			if src.on(node) || src.objectID == "" || !src.packed {
				continue
			}
			if _, err = orderFetchTx(ctx, tx, declared, src.objectID, node); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}
