package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// An attempt's scratch is a directory its executor makes on one of its
// volatile stores before the launch, <root>/scratch/<attempt id>, handed to
// the attempt as KAIRO_SCRATCH. It is recorded here (attempt_scratch) with
// the root and path it was made at. Once the attempt has quiesced or was
// lost its executor settles it: a successful attempt's (exit 0 without a
// signal, or one that suspended with a published checkpoint) is deleted; a
// failed or lost attempt's is kept (P1b moves kept scratch to a durable
// store; until then it stays where it is).

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

// ScratchSettlement is a scratch its executor is to settle now: delete it
// (Delete), or keep it. Reason says why. A kept scratch that holds nothing
// but its marker is deleted all the same, unless its attempt was lost (Lost:
// its processes may still be writing there).
type ScratchSettlement struct {
	AttemptID string `json:"attempt_id"`
	StoreID   string `json:"store_id"`
	Root      string `json:"root"`
	Path      string `json:"path"`
	Delete    bool   `json:"delete"`
	Lost      bool   `json:"lost,omitempty"`
	Reason    string `json:"reason"`
}

// ScratchSettled is what an executor did with a scratch: State "deleted"
// (Empty: a scratch the rules keep, deleted because it held nothing but its
// marker) or "kept", with its Bytes when known, and a Reason that replaces
// the rules' when given (a deletion the executor refused, say).
type ScratchSettled struct {
	AttemptID string `json:"attempt_id"`
	State     string `json:"state"`
	Empty     bool   `json:"empty,omitempty"`
	Bytes     *int64 `json:"bytes,omitempty"`
	Reason    string `json:"reason,omitempty"`
}

// ErrScratchKept: a scratch the rules keep was to be recorded deleted.
var ErrScratchKept = errors.New("the scratch of a failed or lost attempt is kept, not deleted")

// RecordAttemptScratch records the scratch an executor made for an attempt
// it is launching (authorized, not started), on a volatile store of the
// attempt's node. Repeating the same record is no error.
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
	var class, storeNode string
	if err = tx.QueryRowContext(ctx, `SELECT class,node_id FROM machine_stores WHERE id=$1`, scratch.StoreID).Scan(&class, &storeNode); errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: store %s", ErrNotFound, scratch.StoreID)
	} else if err != nil {
		return err
	}
	if class != "volatile" || storeNode != nodeID {
		return fmt.Errorf("store %s is not a volatile store of node %s", scratch.StoreID, nodeID)
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

// scratchDisposition is whether an ended attempt's scratch is deleted, and
// why: a lost attempt's is kept (its processes may still run; one
// reconciled to quiesced since is known by its execution's terminal
// cause), a successful one's (a suspend that published its checkpoint, or
// exit 0 without a signal) deleted, any other kept.
func scratchDisposition(attemptState string, terminalCause sql.NullString, exitCode sql.NullInt64, exitSignal, continuation sql.NullString) (bool, string) {
	switch {
	case attemptState == "lost" || terminalCause.String == "executor_identity_lost":
		return false, reasonLost
	case continuation.Valid:
		return true, "suspended with a published checkpoint"
	case !exitCode.Valid:
		return false, "no exit (the launch did not run)"
	case exitSignal.Valid && exitSignal.String != "":
		return false, "signal " + exitSignal.String
	case exitCode.Int64 != 0:
		return false, fmt.Sprintf("exit %d", exitCode.Int64)
	}
	return true, "exit 0"
}

// ScratchToSettle is the active scratch of an executor's attempts that have
// quiesced or were lost, each with what is to be done with it.
func (s *Store) ScratchToSettle(ctx context.Context, executorID string) ([]ScratchSettlement, error) {
	out := []ScratchSettlement{}
	err := eachRow(ctx, s.db, `SELECT k.attempt_id,k.store_id,k.root,k.path,a.state,e.terminal_cause,a.exit_code,a.exit_signal,a.continuation_ref
		FROM attempt_scratch k JOIN attempts a ON a.id=k.attempt_id JOIN execution_requests e ON e.id=a.execution_id
		WHERE k.executor_id=$1 AND k.state='active' AND a.state IN('quiesced','lost') ORDER BY k.created_at,k.attempt_id`, []any{executorID}, func(rows *sql.Rows) error {
		var k ScratchSettlement
		var state string
		var exitCode sql.NullInt64
		var cause, exitSignal, continuation sql.NullString
		if err := rows.Scan(&k.AttemptID, &k.StoreID, &k.Root, &k.Path, &state, &cause, &exitCode, &exitSignal, &continuation); err != nil {
			return err
		}
		k.Delete, k.Reason = scratchDisposition(state, cause, exitCode, exitSignal, continuation)
		k.Lost = k.Reason == reasonLost
		out = append(out, k)
		return nil
	})
	return out, err
}

// SettleScratch records what an executor did with an attempt's scratch.
// "deleted" is taken only when the rules delete it, or (Empty) when it held
// nothing but its marker and its attempt was not lost. Settling it again
// the same way is no error.
func (s *Store) SettleScratch(ctx context.Context, settled ScratchSettled) error {
	if settled.State != "deleted" && settled.State != "kept" {
		return fmt.Errorf("scratch is settled deleted or kept, not %q", settled.State)
	}
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var current, attemptState, executionID string
	var exitCode sql.NullInt64
	var cause, exitSignal, continuation sql.NullString
	var epoch int64
	err = tx.QueryRowContext(ctx, `SELECT k.state,a.state,e.terminal_cause,a.exit_code,a.exit_signal,a.continuation_ref,a.execution_id,a.coordination_epoch
		FROM attempt_scratch k JOIN attempts a ON a.id=k.attempt_id JOIN execution_requests e ON e.id=a.execution_id
		WHERE k.attempt_id=$1`, settled.AttemptID).Scan(&current, &attemptState, &cause, &exitCode, &exitSignal, &continuation, &executionID, &epoch)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if current == settled.State {
		return tx.Commit()
	}
	if current != "active" || attemptState != "quiesced" && attemptState != "lost" {
		return fmt.Errorf("%w: scratch %s of an attempt %s cannot be settled", ErrStaleEpoch, current, attemptState)
	}
	remove, reason := scratchDisposition(attemptState, cause, exitCode, exitSignal, continuation)
	if settled.State == "deleted" && !remove {
		if !settled.Empty || reason == reasonLost {
			return fmt.Errorf("%w (%s)", ErrScratchKept, reason)
		}
		reason = "empty; " + reason
	}
	if settled.Reason != "" {
		reason = settled.Reason
	}
	t := now()
	if _, err = tx.ExecContext(ctx, `UPDATE attempt_scratch SET state=$1,bytes=COALESCE($2,bytes),reason=$3,settled_at=$4,updated_at=$5 WHERE attempt_id=$6`,
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
