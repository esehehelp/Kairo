package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"kairo/internal/id"
	"kairo/internal/machine"
)

// Landing a run's output directory where an execution is to run (README:
// Data stores, landing a run's output). Every attempt of a run finds what
// its run wrote in its output directory (/kairo/output in its container): a
// continuation finds its checkpoint there. The directory is on a volatile
// store of one node. An execution that needs it on another node has it
// brought there first (an output landing, output_landings):
//
//   - move: a continuation the planner sends to another node than the one
//     holding its run's directory, where it is expected to end sooner
//     (outputhome.go). The directory is packed where it is, as an object of
//     kind output-move (temporal: never copied to a durable store; its
//     transfer_state is its pack's), and the executor of the store it goes
//     to fetches the pack, checks it against its file list and unpacks it as
//     <root>/outputs/<run id> there (placed). From then on that is the run's
//     directory: the one it left is moved (run_outputs; moved it stays, and
//     active again should the run land back there), and the agent there
//     removes it with the pack (done).
//   - checkpoint: a rerun from a checkpoint (task rerun --from-checkpoint)
//     is a run of its own, and its checkpoint is in the output of the run
//     that published it. Once that run's output object is packed it is
//     brought the same way (from a verified durable copy, or from its pack
//     while that is still where it was made), unpacked as the rerun's output
//     directory on the node the rerun is planned onto (done once placed).
//
// A landing not placed by its deadline (landingDeadline), or whose execution
// ends first, is given up (failed): the continuation of a move then runs
// where the directory is; a rerun waits, saying why (output_unavailable).

// ObjectOutputMove is the kind of a run's output directory packed to be
// moved to another node.
const ObjectOutputMove = "output-move"

// The states (output_landings.state) and reasons of a landing.
const (
	landingWaiting  = "waiting"  // a checkpoint's: the run's output not packed yet
	landingPacking  = "packing"  // a move's: the directory being packed where it is
	landingBringing = "bringing" // being fetched and unpacked where it goes
	landingPlaced   = "placed"   // a move's, in place: what it left there still to remove
	landingDone     = "done"
	landingFailed   = "failed"

	landingMove       = "move"
	landingCheckpoint = "checkpoint"
)

// landingLive is the states of a landing not placed yet.
const landingLive = `('waiting','packing','bringing')`

// moveSourceID is the source id of a move's object: the run, and the
// continuation it moves for (a run may move from the same store again).
func moveSourceID(runID, executionID string) string { return runID + "~" + executionID }

// runOfMove is the run a move's source id names.
func runOfMove(sourceID string) string {
	run, _, _ := strings.Cut(sourceID, "~")
	return run
}

// landingDeadline is how long a landing of bytes may take before it is
// given up: an hour, or four times bringing it at 50 MB/s when longer.
func landingDeadline(bytes int64) time.Duration {
	return max(time.Hour, 4*time.Duration(float64(max(bytes, 0))/50e6*float64(time.Second)))
}

// outputHome is where a run's output directory is: active on a volatile
// store, with its executor, node and last measured size.
type outputHome struct {
	runID, storeID, executorID, nodeID, root, path string
	bytes                                          int64
}

// runOutputHomeTx is where runID's output directory is now (its latest
// active one on a store still declared as it was made); ok is false when it
// has none.
func runOutputHomeTx(ctx context.Context, q queryer, declared []machine.Store, runID string) (outputHome, bool, error) {
	h := outputHome{runID: runID}
	err := q.QueryRowContext(ctx, `SELECT o.store_id,o.executor_id,x.node_id,o.root,o.path,COALESCE(o.bytes,0) FROM run_outputs o JOIN executors x ON x.id=o.executor_id
		WHERE o.run_id=$1 AND o.state='active' ORDER BY o.updated_at DESC LIMIT 1`, runID).Scan(&h.storeID, &h.executorID, &h.nodeID, &h.root, &h.path, &h.bytes)
	if errors.Is(err, sql.ErrNoRows) {
		return h, false, nil
	}
	if err != nil {
		return h, false, err
	}
	return h, outputOwnership(declared, h.nodeID, h.storeID, h.root, h.path, runID) == "", nil
}

// landingStoreTx is the volatile store of node a landing goes to, and the
// executor that brings it: the store the execution names (if a store of the
// node an executor of it serves), else the one with the most room (one
// pool, placementStoreWithRoomTx).
func landingStoreTx(ctx context.Context, q queryer, declared []machine.Store, node, named, objectID string) (machine.Store, string, bool, error) {
	if named == "" {
		return placementStoreWithRoomTx(ctx, q, declared, node, objectID)
	}
	stores, err := placementStoresTx(ctx, q, declared, node)
	if err != nil {
		return machine.Store{}, "", false, err
	}
	for _, s := range stores {
		if s.store.ID == named {
			return s.store, s.executor, true, nil
		}
	}
	return machine.Store{}, "", false, nil
}

// orderOutputMoveTx orders the output directory of a continuation's run
// moved from home to node to: packed where it is (an output-move object),
// then brought (a move landing). estimate is the planner's reasons, kept
// with it. ok is false when node has no volatile store to bring it to.
func orderOutputMoveTx(ctx context.Context, tx *sql.Tx, executionID, projectID string, home outputHome, to string, estimate any) (bool, error) {
	declared, err := declaredStores(ctx, tx)
	if err != nil {
		return false, err
	}
	var named sql.NullString
	if err = tx.QueryRowContext(ctx, `SELECT store_id FROM execution_requests WHERE id=$1`, executionID).Scan(&named); err != nil {
		return false, err
	}
	objectID := id.New("obj")
	target, executorID, ok, err := landingStoreTx(ctx, tx, declared, to, named.String, objectID)
	if err != nil || !ok {
		return false, err
	}
	for _, c := range []struct {
		r  lifecycleRecord
		to string
	}{{recObject, "packing"}, {recSource, SourceKept}, {recTransfer, transferPacking}} {
		if err := checkTransition(c.r, noRecord, c.to); err != nil {
			return false, err
		}
	}
	body, err := json.Marshal(estimate)
	if err != nil {
		return false, err
	}
	t := now()
	if _, err = tx.ExecContext(ctx, `INSERT INTO data_objects(id,kind,source_id,store_id,executor_id,root,source_path,project_scope_id,state,lifecycle,source_state,transfer_state,created_at,updated_at)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,'packing',$9,$10,$11,$12,$13)`, objectID, ObjectOutputMove, moveSourceID(home.runID, executionID), home.storeID, home.executorID,
		home.root, home.path, projectID, LifecycleTemporal, SourceKept, transferPacking, t, t); err != nil {
		return false, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO output_landings(execution_id,run_id,reason,from_run_id,from_store_id,object_id,node_id,store_id,executor_id,state,bytes,estimate_json,created_at,updated_at)
		VALUES($1,$2,$3,$2,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`, executionID, home.runID, landingMove, home.storeID, objectID, to, target.ID, executorID,
		landingPacking, home.bytes, string(body), t, t); err != nil {
		return false, err
	}
	if err = appendCoordinationEventTx(ctx, tx, "pack_ordered", &projectID, "object", objectID, nil, map[string]any{"kind": ObjectOutputMove, "source_id": moveSourceID(home.runID, executionID), "store_id": home.storeID, "path": home.path}); err != nil {
		return false, err
	}
	return true, appendCoordinationEventTx(ctx, tx, "output_move_ordered", &projectID, "execution", executionID, nil, map[string]any{
		"run_id": home.runID, "from_node": home.nodeID, "from_store": home.storeID, "to_node": to, "to_store": target.ID, "bytes": home.bytes, "object_id": objectID, "estimate": json.RawMessage(body)})
}

// movePackedTx records a move's pack made (RecordPack): it is to be brought
// now.
func movePackedTx(ctx context.Context, tx *sql.Tx, objectID string, project *string) error {
	if err := checkTransition(recTransfer, transferPacking, transferPacked); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE data_objects SET transfer_state=$1 WHERE id=$2 AND transfer_state=$3`, transferPacked, objectID, transferPacking); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE output_landings SET state=$1,updated_at=$2 WHERE object_id=$3 AND state=$4`, landingBringing, now(), objectID, landingPacking)
	if err != nil {
		return err
	}
	if n, _ := result.RowsAffected(); n == 0 {
		return nil // given up meanwhile: its pack goes (dropTransfersTx)
	}
	return appendCoordinationEventTx(ctx, tx, "output_move_packed", project, "object", objectID, nil, nil)
}

// landingFetchOrdersTx is what executorID is to bring as runs' output
// directories now (FetchesToMake): each landing being brought by it, from
// where its object's pack can be read (as for a fetch), with a token for a
// read from another node's agent.
func landingFetchOrdersTx(ctx context.Context, tx *sql.Tx, me executorInfo, executorID string, byID map[string]machine.Store) ([]FetchOrder, error) {
	type row struct {
		order                                    FetchOrder
		executionID, kind, sourceState, transfer string
		packStore, packRoot                      string
	}
	var rows []row
	err := eachRow(ctx, tx, `SELECT l.execution_id,l.run_id,l.object_id,l.store_id,d.pack_bytes,COALESCE(d.content_bytes,0),d.pack_sha256,d.manifest_sha256,
		d.kind,d.source_state,COALESCE(d.transfer_state,''),d.store_id,d.root
		FROM output_landings l JOIN data_objects d ON d.id=l.object_id
		WHERE l.executor_id=$1 AND l.state=$2 AND d.state='packed' ORDER BY l.created_at,l.execution_id`, []any{executorID, landingBringing}, func(r *sql.Rows) error {
		var x row
		if err := r.Scan(&x.executionID, &x.order.RunID, &x.order.ObjectID, &x.order.StoreID, &x.order.PackBytes, &x.order.ContentBytes, &x.order.PackSHA256, &x.order.ManifestSHA256,
			&x.kind, &x.sourceState, &x.transfer, &x.packStore, &x.packRoot); err != nil {
			return err
		}
		rows = append(rows, x)
		return nil
	})
	if err != nil {
		return nil, err
	}
	expires := time.Now().UTC().Add(fetchTokenLife).Format(time.RFC3339Nano)
	var out []FetchOrder
	for _, x := range rows {
		d, ok := byID[x.order.StoreID]
		if !ok || d.Class != machine.StoreVolatile || !me.serves(d, executorID) {
			continue // brought only to the store as it is still declared, by an executor serving it
		}
		x.order.Root = d.Root
		// a move's pack is read where it was made, while it is there; a
		// run's output from a verified durable copy, or its pack until its
		// source is settled (as for a fetch)
		var sources []CopySource
		addPack := x.kind == ObjectOutputMove && x.transfer == transferPacked || x.kind != ObjectOutputMove && (x.sourceState == SourcePresent || x.sourceState == SourceDeleting)
		if x.kind != ObjectOutputMove {
			err := eachRow(ctx, tx, `SELECT c.store_id,c.root,c.month FROM object_copies c WHERE c.object_id=$1 AND c.state='verified' AND c.check_problem IS NULL ORDER BY c.store_id`,
				[]any{x.order.ObjectID}, func(r *sql.Rows) error {
					var src CopySource
					if err := r.Scan(&src.StoreID, &src.Root, &src.Month); err != nil {
						return err
					}
					if s, ok := byID[src.StoreID]; ok && s.Class == machine.StoreDurable && s.Root == src.Root {
						sources = append(sources, src)
					}
					return nil
				})
			if err != nil {
				return nil, err
			}
		}
		if s, ok := byID[x.packStore]; addPack && ok && s.Root == x.packRoot {
			sources = append(sources, CopySource{StoreID: x.packStore, Root: x.packRoot})
		}
		var chosen *CopySource
		var chosenNode string
		for i := range sources {
			s := byID[sources[i].StoreID]
			if me.serves(s, executorID) {
				sources[i].Local = true
				chosen, chosenNode = &sources[i], me.nodeID
				break
			}
		}
		if chosen == nil {
			for i := range sources {
				info, found, err := storeServerTx(ctx, tx, byID[sources[i].StoreID])
				if err != nil {
					return nil, err
				}
				if found && info.attrs["pack_addr"] != "" {
					sources[i].Addr = info.attrs["pack_addr"]
					chosen, chosenNode = &sources[i], info.nodeID
					break
				}
			}
		}
		if chosen == nil {
			continue // nowhere to read it from now
		}
		o := x.order
		o.Source = *chosen
		hash := any(nil)
		if !o.Source.Local {
			token, h, err := newSecret("kairo_fetch_")
			if err != nil {
				return nil, err
			}
			o.Source.Token, hash = token, h
		}
		tokenExpires := any(nil)
		if hash != nil {
			tokenExpires = expires
		}
		if _, err = tx.ExecContext(ctx, `UPDATE output_landings SET token_hash=$1,token_expires_at=$2,source_store_id=$3,source_root=$4,source_month=$5,source_node_id=$6,updated_at=$7
			WHERE execution_id=$8 AND state=$9`, hash, tokenExpires, o.Source.StoreID, o.Source.Root, nullable(o.Source.Month), chosenNode, now(), x.executionID, landingBringing); err != nil {
			return nil, err
		}
		out = append(out, o)
	}
	return out, nil
}

// authorizeLandingRead is AuthorizePackRead for a landing's fetch: the pack
// (or durable copy) it was issued to read on nodeID.
func authorizeLandingRead(ctx context.Context, q queryer, nodeID, objectID, token string) (PackRead, string, error) {
	var read PackRead
	var month sql.NullString
	var expires string
	err := q.QueryRowContext(ctx, `SELECT l.source_store_id,l.source_root,l.source_month,l.token_expires_at FROM output_landings l
		WHERE l.object_id=$1 AND l.token_hash=$2 AND l.state=$3 AND l.source_node_id=$4`, objectID, hashToken(token), landingBringing, nodeID).Scan(&read.StoreID, &read.Root, &month, &expires)
	read.Month = month.String
	return read, expires, err
}

// recordLandingTx records a run's output directory an executor brought for
// a landing (RecordFetch with a run), unpacked at <root>/outputs/<run id> of
// the store it was ordered to: the run's directory there from now on
// (run_outputs, active, no attempt yet). A move's directory where it was is
// moved, to be removed with the move's pack (done once it is); a
// checkpoint's landing is done.
func recordLandingTx(ctx context.Context, tx *sql.Tx, made FetchMade) error {
	var executionID, reason, state, executorID, objectID, projectID string
	var fromStore, storeID sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT l.execution_id,l.reason,l.state,COALESCE(l.executor_id,''),COALESCE(l.object_id,''),l.from_store_id,l.store_id,e.project_scope_id
		FROM output_landings l JOIN execution_requests e ON e.id=l.execution_id
		WHERE l.run_id=$1 AND l.object_id=$2 AND l.store_id=$3 ORDER BY l.created_at DESC LIMIT 1 FOR UPDATE OF l`, made.RunID, made.ObjectID, made.StoreID).
		Scan(&executionID, &reason, &state, &executorID, &objectID, &fromStore, &storeID, &projectID)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	declared, err := declaredStores(ctx, tx)
	if err != nil {
		return err
	}
	var root, nodeID string
	for _, d := range declared {
		if d.ID == made.StoreID {
			root, nodeID = d.Root, d.Node
		}
	}
	switch {
	case executorID != made.ExecutorID:
		return fmt.Errorf("run %s's output is executor %s's to bring to %s, not %s's", made.RunID, executorID, made.StoreID, made.ExecutorID)
	case root == "" || !machine.IsOutputDir(made.Path, root, made.RunID):
		return fmt.Errorf("run %s's output brought to %s, not <root>/outputs/<run id> under %s", made.RunID, made.Path, root)
	case made.Bytes < 0:
		return fmt.Errorf("run %s's output: impossible size", made.RunID)
	}
	if state != landingBringing {
		return nil // placed already, or given up (its directory is then counted as unrecorded)
	}
	if why := outputOwnership(declared, nodeID, made.StoreID, root, made.Path, made.RunID); why != "" {
		return fmt.Errorf("output directory of run %s refused: %s", made.RunID, why)
	}
	t := now()
	var existing string
	err = tx.QueryRowContext(ctx, `SELECT state FROM run_outputs WHERE run_id=$1 AND store_id=$2 FOR UPDATE`, made.RunID, made.StoreID).Scan(&existing)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		if err := checkMade(recRunOutput, "active"); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO run_outputs(run_id,store_id,executor_id,root,path,state,bytes,created_at,updated_at) VALUES($1,$2,$3,$4,$5,'active',$6,$7,$8)`,
			made.RunID, made.StoreID, made.ExecutorID, root, made.Path, made.Bytes, t, t); err != nil {
			return err
		}
	case err != nil:
		return err
	default:
		// the run moved away from this store before (its directory removed
		// since: a move is made only once the run's last one is done)
		if err := checkTransition(recRunOutput, existing, "active"); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE run_outputs SET executor_id=$1,root=$2,path=$3,state='active',bytes=$4,missing_at=NULL,updated_at=$5 WHERE run_id=$6 AND store_id=$7`,
			made.ExecutorID, root, made.Path, made.Bytes, t, made.RunID, made.StoreID); err != nil {
			return err
		}
	}
	next := landingDone
	if reason == landingMove {
		next = landingPlaced
		if err := checkRowsTransitionTx(ctx, tx, recRunOutput, "run_outputs", "run_id=$1 AND store_id=$2 AND state='active'", "moved", made.RunID, fromStore.String); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE run_outputs SET state='moved',updated_at=$1 WHERE run_id=$2 AND store_id=$3 AND state='active'`, t, made.RunID, fromStore.String); err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(ctx, `UPDATE output_landings SET state=$1,bytes=$2,token_hash=NULL,token_expires_at=NULL,updated_at=$3 WHERE execution_id=$4`, next, made.Bytes, t, executionID); err != nil {
		return err
	}
	return appendCoordinationEventTx(ctx, tx, "output_landed", &projectID, "execution", executionID, nil, map[string]any{
		"run_id": made.RunID, "reason": reason, "object_id": made.ObjectID, "store_id": made.StoreID, "path": made.Path, "bytes": made.Bytes})
}

// moveDropsSourceTx reports whether removing a move's pack (PacksToMake)
// removes the directory it moved too: once placed where it went (the
// directory left behind is moved).
func moveDropsSourceTx(ctx context.Context, q queryer, objectID string) (bool, error) {
	var placed bool
	err := q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM output_landings l JOIN data_objects d ON d.id=l.object_id
		JOIN run_outputs o ON o.run_id=l.run_id AND o.store_id=d.store_id
		WHERE l.object_id=$1 AND l.state=$2 AND o.state='moved')`, objectID, landingPlaced).Scan(&placed)
	return placed, err
}

// moveDroppedTx records a move's pack removed (recordTransferTx), and with
// it the directory it moved (made.SourceRemoved): that holds nothing now
// (it stays moved), and the move is done.
func moveDroppedTx(ctx context.Context, tx *sql.Tx, made PackMade, project *string) error {
	if !made.SourceRemoved {
		return nil
	}
	var sourceID, storeID string
	if err := tx.QueryRowContext(ctx, `SELECT source_id,store_id FROM data_objects WHERE id=$1`, made.ObjectID).Scan(&sourceID, &storeID); err != nil {
		return err
	}
	runID, t := runOfMove(sourceID), now()
	if _, err := tx.ExecContext(ctx, `UPDATE run_outputs SET bytes=0,updated_at=$1 WHERE run_id=$2 AND store_id=$3 AND state='moved'`, t, runID, storeID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE output_landings SET state=$1,updated_at=$2 WHERE object_id=$3 AND state=$4`, landingDone, t, made.ObjectID, landingPlaced); err != nil {
		return err
	}
	return appendCoordinationEventTx(ctx, tx, "output_move_done", project, "object", made.ObjectID, nil, map[string]any{"run_id": runID, "store_id": storeID})
}

// launchStoreTx is the store an attempt's scratch, output and cache go on:
// the one its execution names, else the store holding its run's output
// directory (landed there, or written there by an earlier attempt) when the
// launching executor serves it, else "" (the executor's first).
func launchStoreTx(ctx context.Context, q queryer, named sql.NullString, runID, executorID string) (string, error) {
	if named.Valid && named.String != "" {
		return named.String, nil
	}
	declared, err := declaredStores(ctx, q)
	if err != nil {
		return "", err
	}
	home, ok, err := runOutputHomeTx(ctx, q, declared, runID)
	if err != nil || !ok {
		return "", err
	}
	me, err := executorInfoTx(ctx, q, executorID)
	if err != nil {
		return "", err
	}
	for _, d := range declared {
		if d.ID == home.storeID && d.Class == machine.StoreVolatile && me.serves(d, executorID) {
			return d.ID, nil
		}
	}
	return "", nil
}

// reconcileOutputLandings makes the landings of reruns from a checkpoint,
// orders brought the ones whose output is packed now, and gives up the ones
// past their deadline or whose execution ended first.
func (s *Store) reconcileOutputLandings(ctx context.Context) error {
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	declared, err := declaredStores(ctx, tx)
	if err != nil {
		return err
	}
	// Reruns from a checkpoint another run published: their run's output
	// starts as that run's.
	type rerun struct{ executionID, projectID, selector, commandID string }
	var reruns []rerun
	err = eachRow(ctx, tx, `SELECT e.id,e.project_scope_id,e.executor_selector_json,e.input_checkpoint_command_id FROM execution_requests e
		WHERE e.state='waiting' AND e.input_checkpoint_command_id IS NOT NULL
		AND NOT EXISTS(SELECT 1 FROM output_landings l WHERE l.execution_id=e.id) ORDER BY e.submitted_at,e.id`, nil, func(rows *sql.Rows) error {
		var r rerun
		if err := rows.Scan(&r.executionID, &r.projectID, &r.selector, &r.commandID); err != nil {
			return err
		}
		reruns = append(reruns, r)
		return nil
	})
	if err != nil {
		return err
	}
	t := now()
	for _, r := range reruns {
		runID, err := runIDTx(ctx, tx, r.executionID)
		if err != nil {
			return err
		}
		var published string
		if err = tx.QueryRowContext(ctx, `SELECT execution_id FROM commands WHERE id=$1`, r.commandID).Scan(&published); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			return err
		}
		fromRun, err := runIDTx(ctx, tx, published)
		if err != nil {
			return err
		}
		node := selectorNode(r.selector)
		if fromRun == runID || node == "" {
			continue // a continuation of its run: its directory is its run's
		}
		if _, has, err := runOutputHomeTx(ctx, tx, declared, runID); err != nil || has {
			if err != nil {
				return err
			}
			continue
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO output_landings(execution_id,run_id,reason,from_run_id,node_id,state,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`,
			r.executionID, runID, landingCheckpoint, fromRun, node, landingWaiting, t, t); err != nil {
			return err
		}
		if err = appendCoordinationEventTx(ctx, tx, "output_landing_ordered", &r.projectID, "execution", r.executionID, nil, map[string]any{"run_id": runID, "from_run_id": fromRun, "node": node, "reason": landingCheckpoint}); err != nil {
			return err
		}
	}
	// Waiting for the output of the run that published the checkpoint:
	// brought once packed.
	type waiting struct{ executionID, fromRun, node, named string }
	var waits []waiting
	err = eachRow(ctx, tx, `SELECT l.execution_id,l.from_run_id,l.node_id,COALESCE(e.store_id,'') FROM output_landings l JOIN execution_requests e ON e.id=l.execution_id WHERE l.state=$1`,
		[]any{landingWaiting}, func(rows *sql.Rows) error {
			var w waiting
			if err := rows.Scan(&w.executionID, &w.fromRun, &w.node, &w.named); err != nil {
				return err
			}
			waits = append(waits, w)
			return nil
		})
	if err != nil {
		return err
	}
	for _, w := range waits {
		var objectID string
		var bytes int64
		err := tx.QueryRowContext(ctx, `SELECT id,COALESCE(pack_bytes,0)+COALESCE(content_bytes,0) FROM data_objects WHERE kind='output' AND source_id=$1 AND state='packed' AND lifecycle='durable' ORDER BY packed_at DESC,id DESC LIMIT 1`, w.fromRun).Scan(&objectID, &bytes)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return err
		}
		target, executorID, ok, err := landingStoreTx(ctx, tx, declared, w.node, w.named, objectID)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		if _, err = tx.ExecContext(ctx, `UPDATE output_landings SET object_id=$1,store_id=$2,executor_id=$3,bytes=$4,state=$5,updated_at=$6 WHERE execution_id=$7 AND state=$8`,
			objectID, target.ID, executorID, bytes, landingBringing, t, w.executionID, landingWaiting); err != nil {
			return err
		}
	}
	// Given up: past the deadline, or its execution ended before it landed.
	type live struct {
		executionID, projectID, state, executionState, created string
		bytes                                                  int64
	}
	var lives []live
	err = eachRow(ctx, tx, `SELECT l.execution_id,e.project_scope_id,l.state,e.state,l.created_at,COALESCE(l.bytes,0) FROM output_landings l JOIN execution_requests e ON e.id=l.execution_id
		WHERE l.state IN `+landingLive, nil, func(rows *sql.Rows) error {
		var l live
		if err := rows.Scan(&l.executionID, &l.projectID, &l.state, &l.executionState, &l.created, &l.bytes); err != nil {
			return err
		}
		lives = append(lives, l)
		return nil
	})
	if err != nil {
		return err
	}
	for _, l := range lives {
		problem := ""
		created, err := time.Parse(time.RFC3339Nano, l.created)
		switch {
		case l.executionState != "waiting":
			problem = "its execution is " + l.executionState + ": not brought for it"
		case err == nil && l.state != landingWaiting && time.Since(created) > landingDeadline(l.bytes):
			problem = fmt.Sprintf("not brought within %s of when it was ordered", formatAbout(landingDeadline(l.bytes)))
		}
		if problem == "" {
			continue
		}
		if _, err = tx.ExecContext(ctx, `UPDATE output_landings SET state=$1,problem=$2,token_hash=NULL,token_expires_at=NULL,updated_at=$3 WHERE execution_id=$4 AND state=$5`,
			landingFailed, problem, t, l.executionID, l.state); err != nil {
			return err
		}
		if err = appendCoordinationEventTx(ctx, tx, "output_landing_failed", &l.projectID, "execution", l.executionID, nil, map[string]any{"problem": problem, "state": l.state}); err != nil {
			return err
		}
	}
	return tx.Commit()
}
