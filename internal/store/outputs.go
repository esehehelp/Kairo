package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"kairo/internal/machine"
)

// A run's output directory is where its attempts write what is to be kept:
// <root>/outputs/<run id> on a volatile store of the executor launching the
// attempt, handed to it as KAIRO_OUTPUT_DIR. A run is an execution with the
// continuations that follow it (runIDTx), so a run suspended and continued on
// the same store finds what it wrote there before, and a rerun or another
// Task never writes into another run's directory. It is recorded here
// (run_outputs) once checked against the store's declaration, and measured
// by its executor's reports. P1b's later steps pack it, move it to a durable
// store and delete it; until then it stays where it is.

// RunOutput is a run's output directory as an executor made it (or found it
// made) for an attempt it is launching.
type RunOutput struct {
	AttemptID  string `json:"attempt_id"`
	LeaseID    string `json:"lease_id"`
	Epoch      int64  `json:"epoch"`
	RunID      string `json:"run_id"`
	StoreID    string `json:"store_id"`
	ExecutorID string `json:"executor_id"`
	Root       string `json:"root"`
	Path       string `json:"path"`
}

// runIDTx is the run executionID belongs to: for a Task's execution, the
// latest of the Task's executions up to it that is not a continuation (its
// initial execution or the rerun it continues); for a direct execution, the
// execution itself. A gang rank other than 0 belongs to its leader's run, as
// <run>-rank<r>.
func runIDTx(ctx context.Context, q queryer, executionID string) (string, error) {
	base, suffix := executionID, ""
	var leader string
	var rank int
	err := q.QueryRowContext(ctx, `SELECT g.leader_execution_id,gm.rank FROM gang_members gm JOIN execution_gangs g ON g.id=gm.gang_id WHERE gm.execution_id=$1`, executionID).Scan(&leader, &rank)
	switch {
	case err == nil && rank > 0:
		base, suffix = leader, fmt.Sprintf("-rank%d", rank)
	case err != nil && !errors.Is(err, sql.ErrNoRows):
		return "", err
	}
	var start string
	err = q.QueryRowContext(ctx, `SELECT s.execution_id FROM orchestration_task_executions o
		JOIN orchestration_task_executions s ON s.task_scope_id=o.task_scope_id AND s.execution_ordinal<=o.execution_ordinal AND s.reason<>'continuation'
		WHERE o.execution_id=$1 ORDER BY s.execution_ordinal DESC LIMIT 1`, base).Scan(&start)
	switch {
	case err == nil:
		base = start
	case !errors.Is(err, sql.ErrNoRows):
		return "", err
	}
	return base + suffix, nil
}

// outputOwnership says why path is not, as declared now, the output
// directory of runID on the volatile store storeID of node nodeID with root
// root ("" when it is): scratchOwnership's rules, for <root>/outputs/<run id>.
func outputOwnership(declared []machine.Store, nodeID, storeID, root, path, runID string) string {
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
	case store.Root != root:
		return "store " + storeID + " has moved from " + root + " to " + store.Root
	case !machine.IsOutputDir(path, root, runID):
		return path + " is not the run's output directory under " + root
	}
	for _, other := range declared {
		if other.ID != storeID && other.Node == nodeID && machine.RootsOverlap(path, other.Root) {
			return path + " overlaps store " + other.ID + " (" + other.Class + ", " + other.Root + ")"
		}
	}
	return ""
}

// RecordRunOutput records the output directory an executor made, or found
// made, for an attempt it is launching (authorized, not started): the
// attempt's run (runIDTx), on a volatile store of the attempt's node in the
// executor's namespace, at the store's declared root, at
// <root>/outputs/<run id>. The run's directory on that store is recorded
// once; each later attempt of the run that gets it is noted on it. Repeating
// the same record is no error.
func (s *Store) RecordRunOutput(ctx context.Context, out RunOutput) error {
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = validateEpochTx(ctx, tx, out.AttemptID, out.LeaseID, out.Epoch); err != nil {
		return err
	}
	var attemptState, executorID, executionID, nodeID string
	if err = tx.QueryRowContext(ctx, `SELECT a.state,a.executor_id,a.execution_id,x.node_id FROM attempts a JOIN executors x ON x.id=a.executor_id WHERE a.id=$1`, out.AttemptID).Scan(&attemptState, &executorID, &executionID, &nodeID); err != nil {
		return err
	}
	runID, err := runIDTx(ctx, tx, executionID)
	if err != nil {
		return err
	}
	if runID != out.RunID {
		return fmt.Errorf("attempt %s runs in run %s, not %s", out.AttemptID, runID, out.RunID)
	}
	var existing, lastAttempt string
	err = tx.QueryRowContext(ctx, `SELECT path,last_attempt_id FROM run_outputs WHERE run_id=$1 AND store_id=$2`, out.RunID, out.StoreID).Scan(&existing, &lastAttempt)
	found := err == nil
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if found && existing != out.Path {
		return fmt.Errorf("run %s already has output directory %s on store %s", out.RunID, existing, out.StoreID)
	}
	if found && lastAttempt == out.AttemptID {
		return tx.Commit()
	}
	if attemptState != "authorized" {
		return fmt.Errorf("%w: attempt %s is %s, not about to launch", ErrStaleEpoch, out.AttemptID, attemptState)
	}
	if executorID != out.ExecutorID {
		return fmt.Errorf("attempt %s is not executor %s's", out.AttemptID, out.ExecutorID)
	}
	declared, err := declaredStores(ctx, tx)
	if err != nil {
		return err
	}
	var store *machine.Store
	for i := range declared {
		if declared[i].ID == out.StoreID {
			store = &declared[i]
		}
	}
	if store == nil {
		return fmt.Errorf("%w: store %s", ErrNotFound, out.StoreID)
	}
	if store.Executor != "" && store.Executor != out.ExecutorID {
		return fmt.Errorf("store %s is in executor %s's namespace, not %s's", store.ID, store.Executor, out.ExecutorID)
	}
	if why := outputOwnership(declared, nodeID, out.StoreID, out.Root, out.Path, out.RunID); why != "" {
		return fmt.Errorf("output directory of run %s refused: %s", out.RunID, why)
	}
	t := now()
	if found {
		// Made again (or found) for this attempt: there again if it was missing.
		if _, err = tx.ExecContext(ctx, `UPDATE run_outputs SET last_attempt_id=$1,state='active',missing_at=NULL,updated_at=$2 WHERE run_id=$3 AND store_id=$4`, out.AttemptID, t, out.RunID, out.StoreID); err != nil {
			return err
		}
	} else if _, err = tx.ExecContext(ctx, `INSERT INTO run_outputs(run_id,store_id,executor_id,root,path,state,last_attempt_id,created_at,updated_at) VALUES($1,$2,$3,$4,$5,'active',$6,$7,$8)`,
		out.RunID, out.StoreID, out.ExecutorID, out.Root, out.Path, out.AttemptID, t, t); err != nil {
		return err
	}
	projectID, err := executionProjectIDTx(ctx, tx, executionID)
	if err != nil {
		return err
	}
	event := "output_created"
	if found {
		event = "output_reused"
	}
	if err = appendCoordinationEventTx(ctx, tx, event, &projectID, "attempt", out.AttemptID, &out.Epoch, map[string]any{"run_id": out.RunID, "store_id": out.StoreID, "path": out.Path}); err != nil {
		return err
	}
	return tx.Commit()
}
