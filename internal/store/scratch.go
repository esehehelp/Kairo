package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"kairo/internal/machine"
)

// An attempt's scratch is a directory its executor makes on one of its
// volatile stores before the launch, <root>/scratch/<attempt id>, handed to
// the attempt as KAIRO_SCRATCH. It is recorded here (attempt_scratch) with
// the root and path it was made at, once checked against the store's
// declaration. Once the attempt has quiesced or was lost its executor
// settles it: a successful attempt's (exit 0 without a signal, or one that
// suspended with a published checkpoint) is deleted; a failed or lost
// attempt's is kept, empty or not (P1b moves kept scratch to a durable store;
// until then it stays where it is). A scratch whose path is no longer the
// scratch directory of a volatile store as declared now (the store was
// dropped, moved or made durable, or another store now holds the path) is
// left untouched.

// AttemptScratch is an attempt's scratch directory as its executor made it.
type AttemptScratch struct {
	AttemptID  string `json:"attempt_id"`
	LeaseID    string `json:"lease_id"`
	Epoch      int64  `json:"epoch"`
	StoreID    string `json:"store_id"`
	ExecutorID string `json:"executor_id"`
	Root       string `json:"root"`
	Path       string `json:"path"`
}

// What an executor is to do with the scratch of an ended attempt
// (ScratchSettlement.Action).
const (
	// ScratchDelete: the attempt succeeded; the scratch is deleted.
	ScratchDelete = "delete"
	// ScratchKeep: the attempt failed or was lost; the scratch is kept as it
	// is (its size measured once).
	ScratchKeep = "keep"
	// ScratchLeave: the path is no longer the declared scratch directory of
	// a volatile store; the scratch is kept and not even read.
	ScratchLeave = "leave"
)

// ScratchSettlement is a scratch its executor is to settle now, what to do
// with it (Action), and why.
type ScratchSettlement struct {
	AttemptID string `json:"attempt_id"`
	StoreID   string `json:"store_id"`
	Root      string `json:"root"`
	Path      string `json:"path"`
	Action    string `json:"action"`
	Reason    string `json:"reason"`
}

// ScratchSettled is what an executor did with a scratch: State "deleted" or
// "kept", with its Bytes when known, and a Reason that replaces the rules'
// when given (a deletion the executor refused, say).
type ScratchSettled struct {
	AttemptID string `json:"attempt_id"`
	State     string `json:"state"`
	Bytes     *int64 `json:"bytes,omitempty"`
	Reason    string `json:"reason,omitempty"`
}

// ErrScratchKept: a scratch the rules keep was to be recorded deleted.
var ErrScratchKept = errors.New("the scratch is kept, not deleted")

// scratchOwnership says why path is not, as declared now, the scratch
// directory of attemptID on the volatile store storeID of node nodeID with
// root root ("" when it is): the store must still be declared, volatile, on
// that node and at that root; the path must be <root>/scratch/<attempt id>;
// and no other store of the node may hold it or lie inside it.
func scratchOwnership(declared []machine.Store, nodeID, storeID, root, path, attemptID string) string {
	var store *machine.Store
	for i := range declared {
		if declared[i].ID == storeID {
			store = &declared[i]
		}
	}
	switch {
	case store == nil:
		return "store " + storeID + " is no longer declared"
	case store.Class != machine.StoreVolatile:
		return "store " + storeID + " is no longer volatile"
	case store.Node != nodeID:
		return "store " + storeID + " is no longer on node " + nodeID
	case machine.StoreRootKey(store.Root) != machine.StoreRootKey(root):
		return "store " + storeID + " has moved from " + root + " to " + store.Root
	case !machine.IsScratchDir(path, root, attemptID):
		return path + " is not the attempt's scratch directory under " + root
	}
	for _, other := range declared {
		if other.ID != storeID && other.Node == nodeID && machine.RootsOverlap(path, other.Root) {
			return path + " overlaps store " + other.ID + " (" + other.Class + ", " + other.Root + ")"
		}
	}
	return ""
}

// RecordAttemptScratch records the scratch an executor made for an attempt
// it is launching (authorized, not started): on a volatile store of the
// attempt's node that is in the executor's namespace, at the store's declared
// root, and at <root>/scratch/<attempt id>. Repeating the same record is no
// error.
func (s *Store) RecordAttemptScratch(ctx context.Context, scratch AttemptScratch) error {
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = validateEpochTx(ctx, tx, scratch.AttemptID, scratch.LeaseID, scratch.Epoch); err != nil {
		return err
	}
	var attemptState, executorID, executionID, nodeID string
	if err = tx.QueryRowContext(ctx, `SELECT a.state,a.executor_id,a.execution_id,x.node_id FROM attempts a JOIN executors x ON x.id=a.executor_id WHERE a.id=$1`, scratch.AttemptID).Scan(&attemptState, &executorID, &executionID, &nodeID); err != nil {
		return err
	}
	var existing string
	err = tx.QueryRowContext(ctx, `SELECT path FROM attempt_scratch WHERE attempt_id=$1`, scratch.AttemptID).Scan(&existing)
	if err == nil {
		if existing != scratch.Path {
			return fmt.Errorf("attempt %s already has scratch %s", scratch.AttemptID, existing)
		}
		return tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if attemptState != "authorized" {
		return fmt.Errorf("%w: attempt %s is %s, not about to launch", ErrStaleEpoch, scratch.AttemptID, attemptState)
	}
	if executorID != scratch.ExecutorID {
		return fmt.Errorf("attempt %s is not executor %s's", scratch.AttemptID, scratch.ExecutorID)
	}
	declared, err := declaredStores(ctx, tx)
	if err != nil {
		return err
	}
	var store *machine.Store
	for i := range declared {
		if declared[i].ID == scratch.StoreID {
			store = &declared[i]
		}
	}
	if store == nil {
		return fmt.Errorf("%w: store %s", ErrNotFound, scratch.StoreID)
	}
	if store.Executor != "" && store.Executor != scratch.ExecutorID {
		return fmt.Errorf("store %s is in executor %s's namespace, not %s's", store.ID, store.Executor, scratch.ExecutorID)
	}
	if why := scratchOwnership(declared, nodeID, scratch.StoreID, scratch.Root, scratch.Path, scratch.AttemptID); why != "" {
		return fmt.Errorf("scratch of attempt %s refused: %s", scratch.AttemptID, why)
	}
	t := now()
	if _, err = tx.ExecContext(ctx, `INSERT INTO attempt_scratch(attempt_id,store_id,executor_id,root,path,state,created_at,updated_at) VALUES($1,$2,$3,$4,$5,'active',$6,$7)`,
		scratch.AttemptID, scratch.StoreID, scratch.ExecutorID, scratch.Root, scratch.Path, t, t); err != nil {
		return err
	}
	projectID, err := executionProjectIDTx(ctx, tx, executionID)
	if err != nil {
		return err
	}
	if err = appendCoordinationEventTx(ctx, tx, "scratch_created", &projectID, "attempt", scratch.AttemptID, &scratch.Epoch, map[string]any{"store_id": scratch.StoreID, "path": scratch.Path}); err != nil {
		return err
	}
	return tx.Commit()
}

const reasonLost = "attempt lost"

// scratchOutcome is how an attempt ended, as its scratch is settled by.
type scratchOutcome struct {
	attemptState  string
	terminalCause sql.NullString
	exitCode      sql.NullInt64
	exitSignal    sql.NullString
	continuation  sql.NullString
}

// scratchOutcomeColumns are the columns scanned into a scratchOutcome
// (attempts a, execution_requests e).
const scratchOutcomeColumns = `a.state,e.terminal_cause,a.exit_code,a.exit_signal,a.continuation_ref`

func (o *scratchOutcome) targets() []any {
	return []any{&o.attemptState, &o.terminalCause, &o.exitCode, &o.exitSignal, &o.continuation}
}

// disposition is whether an ended attempt's scratch is deleted, and why: a
// lost attempt's is kept (its processes may still run; one reconciled to
// quiesced since is known by its execution's terminal cause), a successful
// one's (a suspend that published its checkpoint, or exit 0 without a
// signal) deleted, any other kept, empty or not.
func (o scratchOutcome) disposition() (bool, string) {
	switch {
	case o.attemptState == "lost" || o.terminalCause.String == "executor_identity_lost":
		return false, reasonLost
	case o.continuation.Valid:
		return true, "suspended with a published checkpoint"
	case !o.exitCode.Valid:
		return false, "no exit (the launch did not run)"
	case o.exitSignal.Valid && o.exitSignal.String != "":
		return false, "signal " + o.exitSignal.String
	case o.exitCode.Int64 != 0:
		return false, fmt.Sprintf("exit %d", o.exitCode.Int64)
	}
	return true, "exit 0"
}

// scratchAction is what is to be done with a scratch now, and why: deleted
// or kept by its attempt's outcome, left untouched when the declaration no
// longer makes its path a volatile store's scratch directory.
func scratchAction(declared []machine.Store, nodeID, storeID, root, path, attemptID string, o scratchOutcome) (string, string) {
	remove, reason := o.disposition()
	if why := scratchOwnership(declared, nodeID, storeID, root, path, attemptID); why != "" {
		return ScratchLeave, reason + "; not touched: " + why
	}
	if remove {
		return ScratchDelete, reason
	}
	return ScratchKeep, reason
}

// ScratchToSettle is the unsettled scratch (active, or missing before it was
// settled) of an executor's attempts that have quiesced or were lost, each
// with what is to be done with it.
func (s *Store) ScratchToSettle(ctx context.Context, executorID string) ([]ScratchSettlement, error) {
	tx, err := s.beginRead(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	declared, err := declaredStores(ctx, tx)
	if err != nil {
		return nil, err
	}
	out := []ScratchSettlement{}
	err = eachRow(ctx, tx, `SELECT k.attempt_id,k.store_id,k.root,k.path,x.node_id,`+scratchOutcomeColumns+`
		FROM attempt_scratch k JOIN attempts a ON a.id=k.attempt_id JOIN execution_requests e ON e.id=a.execution_id JOIN executors x ON x.id=k.executor_id
		WHERE k.executor_id=$1 AND k.state IN('active','missing') AND k.settled_at IS NULL AND a.state IN('quiesced','lost') ORDER BY k.created_at,k.attempt_id`, []any{executorID}, func(rows *sql.Rows) error {
		var k ScratchSettlement
		var nodeID string
		var o scratchOutcome
		if err := rows.Scan(append([]any{&k.AttemptID, &k.StoreID, &k.Root, &k.Path, &nodeID}, o.targets()...)...); err != nil {
			return err
		}
		k.Action, k.Reason = scratchAction(declared, nodeID, k.StoreID, k.Root, k.Path, k.AttemptID, o)
		out = append(out, k)
		return nil
	})
	return out, err
}

// SettleScratch records what an executor did with an attempt's scratch:
// "deleted" is taken only when the scratch is to be deleted now
// (ScratchDelete), "kept" always. Settling it again the same way is no
// error.
func (s *Store) SettleScratch(ctx context.Context, settled ScratchSettled) error {
	if settled.State != "deleted" && settled.State != "kept" {
		return fmt.Errorf("scratch is settled deleted or kept, not %q", settled.State)
	}
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var current, storeID, root, path, nodeID, executionID string
	var settledAt sql.NullString
	var epoch int64
	var o scratchOutcome
	err = tx.QueryRowContext(ctx, `SELECT k.state,k.settled_at,k.store_id,k.root,k.path,x.node_id,a.execution_id,a.coordination_epoch,`+scratchOutcomeColumns+`
		FROM attempt_scratch k JOIN attempts a ON a.id=k.attempt_id JOIN execution_requests e ON e.id=a.execution_id JOIN executors x ON x.id=k.executor_id
		WHERE k.attempt_id=$1`, settled.AttemptID).Scan(append([]any{&current, &settledAt, &storeID, &root, &path, &nodeID, &executionID, &epoch}, o.targets()...)...)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if current == settled.State {
		return tx.Commit()
	}
	unsettled := current == "active" || current == "missing" && !settledAt.Valid
	if !unsettled || o.attemptState != "quiesced" && o.attemptState != "lost" {
		return fmt.Errorf("%w: scratch %s of an attempt %s cannot be settled", ErrStaleEpoch, current, o.attemptState)
	}
	declared, err := declaredStores(ctx, tx)
	if err != nil {
		return err
	}
	action, reason := scratchAction(declared, nodeID, storeID, root, path, settled.AttemptID, o)
	if settled.State == "deleted" && action != ScratchDelete {
		return fmt.Errorf("%w (%s)", ErrScratchKept, reason)
	}
	if settled.Reason != "" {
		reason = settled.Reason
	}
	t := now()
	if _, err = tx.ExecContext(ctx, `UPDATE attempt_scratch SET state=$1,bytes=COALESCE($2,bytes),reason=$3,settled_at=$4,updated_at=$5,missing_at=NULL WHERE attempt_id=$6`,
		settled.State, settled.Bytes, reason, t, t, settled.AttemptID); err != nil {
		return err
	}
	projectID, err := executionProjectIDTx(ctx, tx, executionID)
	if err != nil {
		return err
	}
	if err = appendCoordinationEventTx(ctx, tx, "scratch_"+settled.State, &projectID, "attempt", settled.AttemptID, &epoch, map[string]any{"reason": reason, "bytes": settled.Bytes}); err != nil {
		return err
	}
	return tx.Commit()
}
