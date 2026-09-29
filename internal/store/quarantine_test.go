package store

import (
	"context"
	"database/sql"
	"encoding/json"
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
		{"orchestration_decisions", "reason IN('initial','continuation','quarantine_restart','gang_restart')", "reason IN('initial','continuation')"},
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
		if table == "orchestration_decisions" && !contains(ddl, "gang_restart") {
			t.Fatalf("%s not widened for gangs: %s", table, ddl)
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
