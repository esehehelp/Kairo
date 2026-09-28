package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

// quarantineProject declares one task; checkpointable decides how a node
// quarantine stops it (suspend vs terminate).
func quarantineProject(t *testing.T, store *Store, checkpointable bool) {
	t.Helper()
	cp := "false"
	if checkpointable {
		cp = "true"
	}
	parsed := projectSpec(t, `schema_version = 1
[project]
name = "orchestrated"
[[tasks]]
name = "job"
queue = "work"
depends_on = []
policy = "RunToCompletion@v1"
[tasks.execution]
schema_version = 2
argv = ["python", "job.py"]
cwd = "/work"
checkpointable = `+cp+`
preemptible = `+cp+`
[tasks.execution.executor]
labels = { environment = "wsl2" }
[[tasks.execution.exclusive]]
kind = "gpu"
count = 1
`)
	ctx := context.Background()
	if _, err := store.ApplyProject(ctx, parsed); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
}

func launchNext(t *testing.T, store *Store) *Launch {
	t.Helper()
	ctx := context.Background()
	reservation, err := store.ReserveNext(ctx, "executor")
	if err != nil || reservation == nil {
		t.Fatalf("reserve: %+v %v", reservation, err)
	}
	if err = store.MarkLeasePrepared(ctx, reservation.Lease.ID, reservation.Lease.CoordinationEpoch); err != nil {
		t.Fatal(err)
	}
	launch, err := store.AuthorizeLaunch(ctx, reservation)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.ActivateLaunch(ctx, launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch, launch.AuthorizationToken, 100, "launcher:100"); err != nil {
		t.Fatal(err)
	}
	return launch
}

func finishLaunch(t *testing.T, store *Store, launch *Launch, exitCode int) {
	t.Helper()
	ctx := context.Background()
	epoch := launch.Lease.CoordinationEpoch
	if err := store.RecordTerminal(ctx, launch.Attempt.ID, launch.Lease.ID, epoch, exitCode, ""); err != nil {
		t.Fatal(err)
	}
	refreshSchedulerGPU(t, store)
	if err := store.FinalizeQuiescence(ctx, launch.Attempt.ID, launch.Lease.ID, epoch); err != nil {
		t.Fatal(err)
	}
}

func TestQuarantinedNodeReservesNothingUntilReleased(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	quarantineProject(t, store, false)
	ctx := context.Background()
	if _, err := store.QuarantineNode(ctx, "node", "tester", "maintenance"); err != nil {
		t.Fatal(err)
	}
	if r, err := store.ReserveNext(ctx, "executor"); err != nil || r != nil {
		t.Fatalf("quarantined node reserved %+v %v", r, err)
	}
	if c, err := store.EnsurePriorityPreemption(ctx, "executor"); err != nil || len(c) != 0 {
		t.Fatalf("quarantined node preempted %v %v", c, err)
	}
	if err := store.ReleaseNodeQuarantine(ctx, "node", "tester"); err != nil {
		t.Fatal(err)
	}
	if r, err := store.ReserveNext(ctx, "executor"); err != nil || r == nil {
		t.Fatalf("released node did not reserve: %v", err)
	}
}

func TestAllNodeQuarantineCoversEveryNode(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	quarantineProject(t, store, false)
	ctx := context.Background()
	if _, err := store.QuarantineNode(ctx, AllNodes, "tester", "all"); err != nil {
		t.Fatal(err)
	}
	if r, err := store.ReserveNext(ctx, "executor"); err != nil || r != nil {
		t.Fatalf("all-node quarantine let the node reserve %+v %v", r, err)
	}
	if _, err := store.QuarantineNode(ctx, "missing-node", "tester", "x"); err != ErrNotFound {
		t.Fatalf("unknown node: %v", err)
	}
}

func TestQuarantineTerminatesNonCheckpointableAndRestartsTheTask(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	quarantineProject(t, store, false)
	ctx := context.Background()
	launch := launchNext(t, store)
	if _, err := store.QuarantineNode(ctx, "node", "tester", "maintenance"); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcileNodeQuarantines(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcileNodeQuarantines(ctx); err != nil { // idempotent
		t.Fatal(err)
	}
	terms, err := store.ListQuarantineTerminations(ctx, "executor")
	if err != nil || len(terms) != 1 || terms[0].AttemptID != launch.Attempt.ID || terms[0].PID == nil || *terms[0].PID != 100 {
		t.Fatalf("terminations: %+v %v", terms, err)
	}
	status, err := store.ListNodeQuarantines(ctx)
	if err != nil || len(status) != 1 || status[0].LiveAttempts != 1 || status[0].PendingTerminations != 1 {
		t.Fatalf("status: %+v %v", status, err)
	}
	// the executor kills the process tree: the launcher exits non-zero
	if err = store.MarkQuarantineTerminated(ctx, launch.Attempt.ID); err != nil {
		t.Fatal(err)
	}
	finishLaunch(t, store, launch, 1)
	if err = store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	project, err := store.GetProjectStatus(ctx, "orchestrated")
	if err != nil {
		t.Fatal(err)
	}
	task := project.Tasks[0]
	if task.State == "failed" || len(task.Executions) != 2 || task.Executions[1].Reason != "quarantine_restart" {
		t.Fatalf("terminated task was not restarted: %+v", task)
	}
	second, err := store.GetExecution(ctx, task.Executions[1].ExecutionID)
	if err != nil || second.InputContinuationRef != nil {
		t.Fatalf("restart must start from scratch: %+v %v", second, err)
	}
	status, _ = store.ListNodeQuarantines(ctx)
	if status[0].LiveAttempts != 0 || status[0].PendingTerminations != 0 {
		t.Fatalf("quarantine not settled: %+v", status)
	}
	// still quarantined: the restart waits
	if r, err := store.ReserveNext(ctx, "executor"); err != nil || r != nil {
		t.Fatalf("restart ran during quarantine %+v %v", r, err)
	}
	if err = store.ReleaseNodeQuarantine(ctx, "node", "tester"); err != nil {
		t.Fatal(err)
	}
	if r, err := store.ReserveNext(ctx, "executor"); err != nil || r == nil || r.Execution.ID != second.ID {
		t.Fatalf("restart not reserved after release: %+v %v", r, err)
	}
}

func TestQuarantineSuspendsCheckpointableAndContinues(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	quarantineProject(t, store, true)
	ctx := context.Background()
	launch := launchNext(t, store)
	if _, err := store.QuarantineNode(ctx, "node", "tester", "maintenance"); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcileNodeQuarantines(ctx); err != nil {
		t.Fatal(err)
	}
	if terms, _ := store.ListQuarantineTerminations(ctx, "executor"); len(terms) != 0 {
		t.Fatalf("checkpointable attempt must not be terminated: %+v", terms)
	}
	var commandID, origin string
	if err := store.db.QueryRowContext(ctx, `SELECT id,origin FROM commands WHERE attempt_id=?`, launch.Attempt.ID).Scan(&commandID, &origin); err != nil || origin != "node_quarantine" {
		t.Fatalf("suspend: %s %s %v", commandID, origin, err)
	}
	epoch := launch.Lease.CoordinationEpoch
	for _, phase := range []string{"accepted", "checkpointing"} {
		if err := store.AckCommand(ctx, launch.Attempt.ID, launch.Lease.ID, epoch, commandID, phase, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.AckCommand(ctx, launch.Attempt.ID, launch.Lease.ID, epoch, commandID, "checkpointed", json.RawMessage(`{"continuation_ref":"ckpt://7"}`)); err != nil {
		t.Fatal(err)
	}
	finishLaunch(t, store, launch, 75)
	if err := store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	project, _ := store.GetProjectStatus(ctx, "orchestrated")
	if ex := project.Tasks[0].Executions; len(ex) != 2 || ex[1].Reason != "continuation" {
		t.Fatalf("no continuation: %+v", project.Tasks[0])
	}
}

func TestOpenWidensChecksOfAnOlderDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "old.db")
	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	st.Close()
	// rewind the two CHECKs to their pre-quarantine form
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range []struct{ table, from, to string }{
		{"commands", "origin IN('scope_pause','priority_preemption','node_quarantine')", "origin IN('scope_pause','priority_preemption')"},
		{"orchestration_decisions", "reason IN('initial','continuation','quarantine_restart')", "reason IN('initial','continuation')"},
	} {
		if err = widenCheck(db, w.table, w.from, w.to); err != nil { // same procedure, narrowing
			t.Fatal(err)
		}
	}
	db.Close()
	st, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	for _, table := range []string{"commands", "orchestration_decisions"} {
		var ddl string
		if err = st.db.QueryRow(`SELECT sql FROM sqlite_master WHERE name=?`, table).Scan(&ddl); err != nil {
			t.Fatal(err)
		}
		if want := map[string]string{"commands": "node_quarantine", "orchestration_decisions": "quarantine_restart"}[table]; !contains(ddl, want) {
			t.Fatalf("%s not widened: %s", table, ddl)
		}
	}
	var indexes int
	if err = st.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name IN('one_live_suspend_per_attempt','commands_attempt_idx','orchestration_decisions_state_idx')`).Scan(&indexes); err != nil || indexes != 3 {
		t.Fatalf("indexes lost: %d %v", indexes, err)
	}
}

func contains(s, sub string) bool {
	return len(sub) == 0 || (len(s) >= len(sub) && (s == sub || indexOf(s, sub) >= 0))
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func taskStatus(t *testing.T, store *Store) TaskStatus {
	t.Helper()
	ctx := context.Background()
	if err := store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	project, err := store.GetProjectStatus(ctx, "orchestrated")
	if err != nil {
		t.Fatal(err)
	}
	return project.Tasks[0]
}

// quarantineAndTerminate quarantines the node and settles the termination of
// the running attempt as the executor would: signalled or found gone.
func quarantineAndTerminate(t *testing.T, store *Store, launch *Launch, signalled bool, exitCode int) {
	t.Helper()
	ctx := context.Background()
	if _, err := store.QuarantineNode(ctx, "node", "tester", "maintenance"); err != nil && err != ErrNotFound {
		t.Fatal(err)
	}
	if err := store.ReconcileNodeQuarantines(ctx); err != nil {
		t.Fatal(err)
	}
	if signalled {
		if _, err := store.MarkQuarantineSignalled(ctx, launch.Attempt.ID); err != nil {
			t.Fatal(err)
		}
	}
	epoch := launch.Lease.CoordinationEpoch
	if err := store.RecordTerminal(ctx, launch.Attempt.ID, launch.Lease.ID, epoch, exitCode, ""); err != nil {
		t.Fatal(err)
	}
	refreshSchedulerGPU(t, store)
	// the lease stays held until the executor has seen the whole tree gone
	if err := store.FinalizeQuiescence(ctx, launch.Attempt.ID, launch.Lease.ID, epoch); err == nil {
		t.Fatal("quiescence finalized with a pending quarantine termination")
	}
	if c, err := store.ListQuiescenceCandidates(ctx, "executor"); err != nil || len(c) != 0 {
		t.Fatalf("quiescence candidate with a pending quarantine termination: %+v %v", c, err)
	}
	if err := store.MarkQuarantineTerminated(ctx, launch.Attempt.ID); err != nil {
		t.Fatal(err)
	}
	if err := store.FinalizeQuiescence(ctx, launch.Attempt.ID, launch.Lease.ID, epoch); err != nil {
		t.Fatal(err)
	}
}

// A launcher that exits 0 on SIGTERM was still cut off: the task runs again.
func TestSignalledAttemptExitingZeroIsRestartedNotSucceeded(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	quarantineProject(t, store, false)
	quarantineAndTerminate(t, store, launchNext(t, store), true, 0)
	task := taskStatus(t, store)
	if task.State == "succeeded" || len(task.Executions) != 2 || task.Executions[1].Reason != "quarantine_restart" {
		t.Fatalf("signalled attempt counted as success: %+v", task)
	}
}

// An attempt that finished by itself before the executor signalled it keeps
// its result.
func TestAttemptFinishedBeforeSignalKeepsItsSuccess(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	quarantineProject(t, store, false)
	quarantineAndTerminate(t, store, launchNext(t, store), false, 0)
	if task := taskStatus(t, store); task.State != "succeeded" || len(task.Executions) != 1 {
		t.Fatalf("finished attempt not succeeded: %+v", task)
	}
}

// A checkpointable attempt that rejects the node_quarantine suspend is
// terminated instead of being asked again every tick, and runs again from
// the continuation it started with.
func TestRejectedQuarantineSuspendFallsBackToTermination(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	quarantineProject(t, store, true)
	ctx := context.Background()
	// first attempt checkpoints for a scope pause, so the second starts from ckpt://7
	first := launchNext(t, store)
	commandID, err := store.EnqueueSuspend(ctx, first.Attempt.ID, "scope_pause", "test")
	if err != nil {
		t.Fatal(err)
	}
	epoch := first.Lease.CoordinationEpoch
	if err = store.AckCommand(ctx, first.Attempt.ID, first.Lease.ID, epoch, commandID, "checkpointed", json.RawMessage(`{"continuation_ref":"ckpt://7"}`)); err != nil {
		t.Fatal(err)
	}
	finishLaunch(t, store, first, 75)
	if task := taskStatus(t, store); len(task.Executions) != 2 {
		t.Fatalf("no continuation: %+v", task)
	}
	second := launchNext(t, store)
	if second.InputContinuationRef == nil || *second.InputContinuationRef != "ckpt://7" {
		t.Fatalf("second attempt input: %v", second.InputContinuationRef)
	}

	if _, err = store.QuarantineNode(ctx, "node", "tester", "maintenance"); err != nil {
		t.Fatal(err)
	}
	if err = store.ReconcileNodeQuarantines(ctx); err != nil {
		t.Fatal(err)
	}
	if err = store.db.QueryRowContext(ctx, `SELECT id FROM commands WHERE attempt_id=? AND origin='node_quarantine'`, second.Attempt.ID).Scan(&commandID); err != nil {
		t.Fatal(err)
	}
	epoch = second.Lease.CoordinationEpoch
	if err = store.AckCommand(ctx, second.Attempt.ID, second.Lease.ID, epoch, commandID, "rejected", nil); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err = store.ReconcileNodeQuarantines(ctx); err != nil {
			t.Fatal(err)
		}
	}
	var commands int
	if err = store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM commands WHERE attempt_id=?`, second.Attempt.ID).Scan(&commands); err != nil || commands != 1 {
		t.Fatalf("suspend re-requested after rejection: %d %v", commands, err)
	}
	terms, err := store.ListQuarantineTerminations(ctx, "executor")
	if err != nil || len(terms) != 1 || terms[0].AttemptID != second.Attempt.ID {
		t.Fatalf("rejected attempt not terminated: %+v %v", terms, err)
	}
	quarantineAndTerminate(t, store, second, true, 143)
	task := taskStatus(t, store)
	if len(task.Executions) != 3 || task.Executions[2].Reason != "quarantine_restart" {
		t.Fatalf("not restarted: %+v", task)
	}
	third, err := store.GetExecution(ctx, task.Executions[2].ExecutionID)
	if err != nil || third.InputContinuationRef == nil || *third.InputContinuationRef != "ckpt://7" {
		t.Fatalf("restart lost the continuation it started from: %+v %v", third, err)
	}
}

// Releasing withdraws what has not reached the attempt yet; what has, runs its course.
func TestReleaseWithdrawsUnstartedTerminationsAndSuspends(t *testing.T) {
	ctx := context.Background()
	t.Run("termination", func(t *testing.T) {
		store := openCoordinationStore(t)
		setupSchedulerInventory(t, store)
		quarantineProject(t, store, false)
		launch := launchNext(t, store)
		if _, err := store.QuarantineNode(ctx, "node", "tester", "x"); err != nil {
			t.Fatal(err)
		}
		if err := store.ReconcileNodeQuarantines(ctx); err != nil {
			t.Fatal(err)
		}
		if err := store.ReleaseNodeQuarantine(ctx, "node", "tester"); err != nil {
			t.Fatal(err)
		}
		if terms, err := store.ListQuarantineTerminations(ctx, "executor"); err != nil || len(terms) != 0 {
			t.Fatalf("termination survived release: %+v %v", terms, err)
		}
		if _, err := store.MarkQuarantineSignalled(ctx, launch.Attempt.ID); err != ErrNotFound {
			t.Fatalf("withdrawn termination could still be signalled: %v", err)
		}
		// signalled before the release: kept
		if _, err := store.QuarantineNode(ctx, "node", "tester", "x"); err != nil {
			t.Fatal(err)
		}
		if err := store.ReconcileNodeQuarantines(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := store.MarkQuarantineSignalled(ctx, launch.Attempt.ID); err != nil {
			t.Fatal(err)
		}
		if err := store.ReleaseNodeQuarantine(ctx, "node", "tester"); err != nil {
			t.Fatal(err)
		}
		if terms, err := store.ListQuarantineTerminations(ctx, "executor"); err != nil || len(terms) != 1 || terms[0].SignalledAt == nil {
			t.Fatalf("signalled termination withdrawn: %+v %v", terms, err)
		}
	})
	t.Run("still covered", func(t *testing.T) {
		store := openCoordinationStore(t)
		setupSchedulerInventory(t, store)
		quarantineProject(t, store, false)
		launchNext(t, store)
		for _, node := range []string{"node", AllNodes} {
			if _, err := store.QuarantineNode(ctx, node, "tester", "x"); err != nil {
				t.Fatal(err)
			}
		}
		if err := store.ReconcileNodeQuarantines(ctx); err != nil {
			t.Fatal(err)
		}
		if err := store.ReleaseNodeQuarantine(ctx, "node", "tester"); err != nil {
			t.Fatal(err)
		}
		if terms, _ := store.ListQuarantineTerminations(ctx, "executor"); len(terms) != 1 {
			t.Fatalf("termination withdrawn while the all-node quarantine still covers the node: %+v", terms)
		}
	})
	t.Run("suspend", func(t *testing.T) {
		store := openCoordinationStore(t)
		setupSchedulerInventory(t, store)
		quarantineProject(t, store, true)
		launch := launchNext(t, store)
		if _, err := store.QuarantineNode(ctx, "node", "tester", "x"); err != nil {
			t.Fatal(err)
		}
		if err := store.ReconcileNodeQuarantines(ctx); err != nil {
			t.Fatal(err)
		}
		if err := store.ReleaseNodeQuarantine(ctx, "node", "tester"); err != nil {
			t.Fatal(err)
		}
		epoch := launch.Lease.CoordinationEpoch
		if commands, err := store.PollCommands(ctx, launch.Attempt.ID, launch.Lease.ID, epoch); err != nil || len(commands) != 0 {
			t.Fatalf("withdrawn suspend delivered: %+v %v", commands, err)
		}
		// quarantined again: a fresh suspend, delivered before the next release, stands
		if _, err := store.QuarantineNode(ctx, "node", "tester", "x"); err != nil {
			t.Fatal(err)
		}
		if err := store.ReconcileNodeQuarantines(ctx); err != nil {
			t.Fatal(err)
		}
		if terms, _ := store.ListQuarantineTerminations(ctx, "executor"); len(terms) != 0 {
			t.Fatalf("withdrawn suspend taken for a rejection: %+v", terms)
		}
		commands, err := store.PollCommands(ctx, launch.Attempt.ID, launch.Lease.ID, epoch)
		if err != nil || len(commands) != 1 {
			t.Fatalf("no fresh suspend: %+v %v", commands, err)
		}
		if err = store.ReleaseNodeQuarantine(ctx, "node", "tester"); err != nil {
			t.Fatal(err)
		}
		if again, err := store.PollCommands(ctx, launch.Attempt.ID, launch.Lease.ID, epoch); err != nil || len(again) != 1 || again[0].ID != commands[0].ID {
			t.Fatalf("delivered suspend withdrawn: %+v %v", again, err)
		}
	})
}

// An attempt that moves on between the query and the suspend is not an error.
func TestEnqueueSuspendRefusalIsRecognizable(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	quarantineProject(t, store, true)
	launch := launchNext(t, store)
	finishLaunch(t, store, launch, 0)
	_, err := store.EnqueueSuspend(context.Background(), launch.Attempt.ID, "node_quarantine", "x")
	var refusal notSuspendableError
	if !errors.As(err, &refusal) || err.Error() != "attempt is not suspendable from state quiesced" {
		t.Fatalf("refusal: %v", err)
	}
}

func TestQuarantineNodeReturnsItsOwnRecord(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	ctx := context.Background()
	if _, err := store.QuarantineNode(ctx, AllNodes, "a", "everything"); err != nil {
		t.Fatal(err)
	}
	q, err := store.QuarantineNode(ctx, "node", "b", "one")
	if err != nil || q.NodeID != "node" || q.Actor != "b" || q.Reason != "one" {
		t.Fatalf("record: %+v %v", q, err)
	}
	// idempotent: the first record is kept
	if q, err = store.QuarantineNode(ctx, "node", "c", "two"); err != nil || q.Actor != "b" {
		t.Fatalf("second call: %+v %v", q, err)
	}
}

func TestOpenAddsSignalledAtToAnOlderDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "old.db")
	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	st.Close()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`ALTER TABLE quarantine_terminations DROP COLUMN signalled_at`); err != nil {
		t.Fatal(err)
	}
	db.Close()
	st, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	var n int
	if err = st.db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('quarantine_terminations') WHERE name='signalled_at'`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("signalled_at not added: %d %v", n, err)
	}
}
