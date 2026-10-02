package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

// forceProject declares "job" and "after" (which depends on job).
func forceProject(t *testing.T, store *Store, checkpointable bool) {
	t.Helper()
	cp := "false"
	if checkpointable {
		cp = "true"
	}
	execution := `[tasks.execution]
schema_version = 2
argv = ["python", "job.py"]
cwd = "/work"
checkpointable = ` + cp + `
preemptible = ` + cp + `
[tasks.execution.executor]
labels = { environment = "wsl2" }
[[tasks.execution.exclusive]]
kind = "gpu"
count = 1
`
	parsed := projectSpec(t, `schema_version = 1
[project]
name = "orchestrated"
[[tasks]]
name = "job"
queue = "work"
depends_on = []
policy = "RunToCompletion@v1"
`+execution+`
[[tasks]]
name = "after"
queue = "work"
depends_on = ["job"]
policy = "RunToCompletion@v1"
`+execution)
	ctx := context.Background()
	if _, err := store.ApplyProject(ctx, parsed); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
}

func taskScopeID(t *testing.T, store *Store, project, queue, task string) string {
	t.Helper()
	set, err := store.GetScopeByPath(context.Background(), ScopePath{Project: project, Queue: queue, Task: task})
	if err != nil {
		t.Fatal(err)
	}
	return set.Task.ID
}

func reconcilePauses(t *testing.T, store *Store) {
	t.Helper()
	if err := store.ReconcilePauseOperations(context.Background(), true); err != nil {
		t.Fatal(err)
	}
}

func projectTasks(t *testing.T, store *Store, project string) map[string]TaskStatus {
	t.Helper()
	ctx := context.Background()
	if err := store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	status, err := store.GetProjectStatus(ctx, project)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]TaskStatus{}
	for _, task := range status.Tasks {
		out[task.Name] = task
	}
	return out
}

func TestForcePauseTerminatesARunningTaskAndStopsIt(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	forceProject(t, store, false)
	ctx := context.Background()
	launch := launchNext(t, store)
	scope := taskScopeID(t, store, "orchestrated", "work", "job")
	op, err := store.ForcePauseScope(ctx, scope, "tester", "force-1", ForceStopSpec{GraceSeconds: 5, Reason: "wrong config"})
	if err != nil || op.Force == nil || op.Force.GraceSeconds != 5 {
		t.Fatalf("force pause: %+v %v", op, err)
	}
	// idempotent by request ID, and the request ID cannot change meaning
	if again, err := store.ForcePauseScope(ctx, scope, "tester", "force-1", ForceStopSpec{GraceSeconds: 5, Reason: "wrong config"}); err != nil || again.ID != op.ID {
		t.Fatalf("replay: %+v %v", again, err)
	}
	if _, err := store.ForcePauseScope(ctx, scope, "tester", "force-1", ForceStopSpec{GraceSeconds: 9, Reason: "wrong config"}); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("changed grace under the same request: %v", err)
	}
	if _, err := store.PauseScope(ctx, scope, "tester", "force-1"); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("plain pause under a forced request: %v", err)
	}
	if _, err := store.ForcePauseScope(ctx, scope, "tester", "bad", ForceStopSpec{GraceSeconds: -1}); err == nil {
		t.Fatal("negative grace accepted")
	}
	reconcilePauses(t, store)
	reconcilePauses(t, store) // idempotent
	orders, err := store.ForceStopOrders(ctx, "executor")
	if err != nil || len(orders) != 1 || orders[0].AttemptID != launch.Attempt.ID || orders[0].State != "pending" || orders[0].PID == nil || *orders[0].PID != 100 || orders[0].KillAfterSeconds != 5 {
		t.Fatalf("orders: %+v %v", orders, err)
	}
	if o, _ := store.ForceStopOrders(ctx, "other-executor"); len(o) != 0 {
		t.Fatalf("order delivered to another executor: %+v", o)
	}
	operation, _ := store.GetPauseOperation(ctx, op.ID)
	targets, _ := store.ListPauseTargets(ctx, op.ID)
	if operation.State != "quiescing" || len(targets) != 1 || targets[0].State != "quiescing" {
		t.Fatalf("operation=%+v targets=%+v", operation, targets)
	}
	// the executor sends the graceful stop; the grace period counts down
	if err = store.AckForceStop(ctx, launch.Attempt.ID, "signalled", json.RawMessage(`{"executor_id":"executor"}`)); err != nil {
		t.Fatal(err)
	}
	orders, _ = store.ForceStopOrders(ctx, "executor")
	if len(orders) != 1 || orders[0].State != "signalled" || orders[0].KillAfterSeconds > 5 || orders[0].KillAfterSeconds < 4 {
		t.Fatalf("signalled order: %+v", orders)
	}
	// the tree is killed: the launcher exits non-zero, the executor confirms
	if err = store.RecordTerminal(ctx, launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch, 1, ""); err != nil {
		t.Fatal(err)
	}
	if err = store.AckForceStop(ctx, launch.Attempt.ID, "terminated", json.RawMessage(`{"killed":true}`)); err != nil {
		t.Fatal(err)
	}
	if err = store.AckForceStop(ctx, launch.Attempt.ID, "signalled", nil); err != nil { // never moves back
		t.Fatal(err)
	}
	if orders, _ = store.ForceStopOrders(ctx, "executor"); len(orders) != 0 {
		t.Fatalf("terminated order still delivered: %+v", orders)
	}
	// not stopped before the lease is released
	if tasks := projectTasks(t, store, "orchestrated"); tasks["job"].State != "running" {
		t.Fatalf("task ended before quiescence: %+v", tasks["job"])
	}
	refreshSchedulerGPU(t, store)
	if err = store.FinalizeQuiescence(ctx, launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch); err != nil {
		t.Fatal(err)
	}
	reconcilePauses(t, store)
	if operation, _ = store.GetPauseOperation(ctx, op.ID); operation.State != "quiesced" {
		t.Fatalf("operation not quiesced: %+v", operation)
	}
	execution, err := store.GetExecution(ctx, launch.Execution.ID)
	if err != nil || execution.TerminalCause == nil || *execution.TerminalCause != "force_stopped" {
		t.Fatalf("terminal cause: %+v %v", execution, err)
	}
	projectTasks(t, store, "orchestrated") // a dependant is evaluated on the next pass
	tasks := projectTasks(t, store, "orchestrated")
	job := tasks["job"]
	if job.State != "stopped" || len(job.Executions) != 1 || job.Executions[0].TerminalCause == nil || *job.Executions[0].TerminalCause != "force_stopped" {
		t.Fatalf("job: %+v", job)
	}
	var result map[string]any
	if err = json.Unmarshal(job.Result, &result); err != nil || result["reason"] != "force stopped" || result["actor"] != "tester" || result["stop_reason"] != "wrong config" || result["pause_operation_id"] != op.ID {
		t.Fatalf("result: %s %v", job.Result, err)
	}
	if tasks["after"].State != "blocked" {
		t.Fatalf("dependant of a stopped task: %+v", tasks["after"])
	}
	stops, err := store.ListForceStops(ctx, op.ID)
	if err != nil || len(stops) != 1 || stops[0].State != "terminated" || stops[0].TerminatedAt == nil || !contains(string(stops[0].Detail), `"killed":true`) {
		t.Fatalf("force stops: %+v %v", stops, err)
	}
	var events int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM coordination_events WHERE event_type IN('force_stop_requested','force_stop_signalled','force_stop_terminated')`).Scan(&events); err != nil || events != 3 {
		t.Fatalf("events: %d %v", events, err)
	}
	// the task stays stopped: nothing is planned after it
	if tasks = projectTasks(t, store, "orchestrated"); len(tasks["job"].Executions) != 1 {
		t.Fatalf("stopped task ran again: %+v", tasks["job"])
	}
}

func TestForcePauseDoesNotWaitForACheckpoint(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	forceProject(t, store, true)
	ctx := context.Background()
	launch := launchNext(t, store)
	scope := taskScopeID(t, store, "orchestrated", "work", "job")
	// an ordinary pause: the suspend is accepted, then the job hangs
	if _, err := store.PauseScope(ctx, scope, "tester", "pause-1"); err != nil {
		t.Fatal(err)
	}
	reconcilePauses(t, store)
	var commandID string
	if err := store.db.QueryRow(`SELECT id FROM commands WHERE attempt_id=?`, launch.Attempt.ID).Scan(&commandID); err != nil {
		t.Fatal(err)
	}
	epoch := launch.Lease.CoordinationEpoch
	if err := store.AckCommand(ctx, launch.Attempt.ID, launch.Lease.ID, epoch, commandID, "accepted", nil); err != nil {
		t.Fatal(err)
	}
	op, err := store.ForcePauseScope(ctx, scope, "tester", "force-1", ForceStopSpec{GraceSeconds: 0})
	if err != nil {
		t.Fatal(err)
	}
	reconcilePauses(t, store)
	orders, _ := store.ForceStopOrders(ctx, "executor")
	if len(orders) != 1 || orders[0].KillAfterSeconds != 0 {
		t.Fatalf("hung checkpointing attempt got no force stop: %+v", orders)
	}
	// even a checkpoint published during the grace period does not continue the task
	if err = store.AckCommand(ctx, launch.Attempt.ID, launch.Lease.ID, epoch, commandID, "checkpointed", json.RawMessage(`{"continuation_ref":"ckpt://1"}`)); err != nil {
		t.Fatal(err)
	}
	finishLaunch(t, store, launch, 0)
	if err = store.AckForceStop(ctx, launch.Attempt.ID, "terminated", nil); err != nil {
		t.Fatal(err)
	}
	reconcilePauses(t, store)
	if operation, _ := store.GetPauseOperation(ctx, op.ID); operation.State != "quiesced" {
		t.Fatalf("operation: %+v", operation)
	}
	job := projectTasks(t, store, "orchestrated")["job"]
	if job.State != "stopped" || len(job.Executions) != 1 {
		t.Fatalf("forced checkpointable task continued: %+v", job)
	}
}

func TestForcePauseWithdrawsWhatHasNotStarted(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	forceProject(t, store, false)
	ctx := context.Background()
	scope := taskScopeID(t, store, "orchestrated", "work", "job")
	op, err := store.ForcePauseScope(ctx, scope, "tester", "force-1", ForceStopSpec{GraceSeconds: 30})
	if err != nil {
		t.Fatal(err)
	}
	reconcilePauses(t, store)
	if operation, _ := store.GetPauseOperation(ctx, op.ID); operation.State != "quiesced" {
		t.Fatalf("operation: %+v", operation)
	}
	stops, _ := store.ListForceStops(ctx, op.ID)
	if len(stops) != 1 || stops[0].State != "withdrawn" {
		t.Fatalf("force stops: %+v", stops)
	}
	execution, _ := store.GetExecution(ctx, stops[0].ExecutionID)
	if execution.State != "terminal" || *execution.TerminalCause != "withdrawn_before_start" {
		t.Fatalf("execution: %+v", execution)
	}
	projectTasks(t, store, "orchestrated") // a dependant is evaluated on the next pass
	tasks := projectTasks(t, store, "orchestrated")
	if tasks["job"].State != "stopped" || tasks["after"].State != "blocked" {
		t.Fatalf("tasks: %+v", tasks)
	}
}

func TestForcePauseRevokesAReservedLease(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	forceProject(t, store, false)
	ctx := context.Background()
	reservation, err := store.ReserveNext(ctx, "executor")
	if err != nil || reservation == nil {
		t.Fatalf("reserve: %v", err)
	}
	scope := taskScopeID(t, store, "orchestrated", "work", "job")
	op, err := store.ForcePauseScope(ctx, scope, "tester", "force-1", ForceStopSpec{GraceSeconds: 30})
	if err != nil {
		t.Fatal(err)
	}
	reconcilePauses(t, store)
	if operation, _ := store.GetPauseOperation(ctx, op.ID); operation.State != "quiescing" {
		t.Fatalf("operation before the executor released: %+v", operation)
	}
	// the executor finds its reservation revoked and releases it
	refreshSchedulerGPU(t, store)
	if err = store.ReleaseReservation(ctx, reservation.Lease.ID, reservation.Lease.CoordinationEpoch, "revoked"); err != nil {
		t.Fatal(err)
	}
	reconcilePauses(t, store)
	if operation, _ := store.GetPauseOperation(ctx, op.ID); operation.State != "quiesced" {
		t.Fatalf("operation: %+v", operation)
	}
	if tasks := projectTasks(t, store, "orchestrated"); tasks["job"].State != "stopped" {
		t.Fatalf("tasks: %+v", tasks)
	}
}

func TestForceStopNotPickedUpIsReported(t *testing.T) {
	saved := ForceStopPickupTimeout
	ForceStopPickupTimeout = 50 * time.Millisecond
	t.Cleanup(func() { ForceStopPickupTimeout = saved })
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	forceProject(t, store, false)
	ctx := context.Background()
	launch := launchNext(t, store)
	scope := taskScopeID(t, store, "orchestrated", "work", "job")
	op, err := store.ForcePauseScope(ctx, scope, "tester", "force-1", ForceStopSpec{GraceSeconds: 30})
	if err != nil {
		t.Fatal(err)
	}
	reconcilePauses(t, store)
	time.Sleep(60 * time.Millisecond)
	blocker := func() string {
		t.Helper()
		reconcilePauses(t, store)
		targets, _ := store.ListPauseTargets(ctx, op.ID)
		operation, _ := store.GetPauseOperation(ctx, op.ID)
		if len(targets) != 1 {
			t.Fatalf("targets: %+v", targets)
		}
		if targets[0].BlockerReason == nil {
			return targets[0].State + "/" + operation.State
		}
		return *targets[0].BlockerReason + "/" + operation.State
	}
	// nothing has polled: the executor is not reachable
	if got := blocker(); got != "executor_unreachable/blocked" {
		t.Fatalf("blocker = %s", got)
	}
	// an agent that polls for quiescence but never for force stops is too old
	if _, err = store.ListQuiescenceCandidates(ctx, "executor"); err != nil {
		t.Fatal(err)
	}
	if got := blocker(); got != "agent_lacks_force_stop/blocked" {
		t.Fatalf("blocker = %s", got)
	}
	// once an executor picks it up, the target converges again
	if err = store.AckForceStop(ctx, launch.Attempt.ID, "signalled", nil); err != nil {
		t.Fatal(err)
	}
	if got := blocker(); got != "quiescing/quiescing" {
		t.Fatalf("after pickup = %s", got)
	}
}

func TestForcePauseStopsEveryGangRank(t *testing.T) {
	store := openCoordinationStore(t)
	setupGangInventory(t, store)
	gangProject(t, store, true)
	ctx := context.Background()
	rank0, rank1 := launchGang(t, store)
	scope := taskScopeID(t, store, "gangs", "training", "ddp")
	op, err := store.ForcePauseScope(ctx, scope, "tester", "force-1", ForceStopSpec{GraceSeconds: 30})
	if err != nil {
		t.Fatal(err)
	}
	reconcilePauses(t, store)
	for _, r := range []gangRankLaunch{rank0, rank1} {
		orders, _ := store.ForceStopOrders(ctx, r.executor)
		if len(orders) != 1 || orders[0].AttemptID != r.launch.Attempt.ID {
			t.Fatalf("%s orders: %+v", r.executor, orders)
		}
	}
	// rank 1 dies first; one-fate reconciliation must not add its own termination
	finishGangRank(t, store, rank1, 137)
	if err = store.ReconcileGangs(ctx); err != nil {
		t.Fatal(err)
	}
	var gangTerms int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM gang_terminations`).Scan(&gangTerms); err != nil || gangTerms != 0 {
		t.Fatalf("gang terminations: %d %v", gangTerms, err)
	}
	finishGangRank(t, store, rank0, 1)
	for _, r := range []gangRankLaunch{rank0, rank1} {
		if err = store.AckForceStop(ctx, r.launch.Attempt.ID, "terminated", nil); err != nil {
			t.Fatal(err)
		}
	}
	reconcilePauses(t, store)
	if operation, _ := store.GetPauseOperation(ctx, op.ID); operation.State != "quiesced" {
		t.Fatalf("operation: %+v", operation)
	}
	task := gangTaskState(t, store)
	if task.State != "stopped" || len(task.Executions) != 1 {
		t.Fatalf("gang task: %+v", task)
	}
}

func TestOpenWidensTaskStatesOfAnOlderDatabase(t *testing.T) {
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
	if err = widenCheck(db, "orchestration_tasks", "state IN('pending','running','succeeded','failed','blocked','stopped')", "state IN('pending','running','succeeded','failed','blocked')"); err != nil {
		t.Fatal(err)
	}
	db.Close()
	st, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	var ddl string
	if err = st.db.QueryRow(`SELECT sql FROM sqlite_master WHERE name='orchestration_tasks'`).Scan(&ddl); err != nil || !contains(ddl, "'stopped'") {
		t.Fatalf("not widened: %s %v", ddl, err)
	}
	var tables int
	if err = st.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN('force_stops','force_stop_operations')`).Scan(&tables); err != nil || tables != 2 {
		t.Fatalf("force stop tables: %d %v", tables, err)
	}
}

func TestForcePauseOfAPlacedGangReleasesEveryLease(t *testing.T) {
	store := openCoordinationStore(t)
	setupGangInventory(t, store)
	gangProject(t, store, false)
	ctx := context.Background()
	// rank 1's executor received its lease; rank 0's lease is placed but undelivered
	rb, err := store.ReserveNext(ctx, "exec-b")
	if err != nil || rb == nil || rb.Gang == nil {
		t.Fatalf("exec-b reservation: %+v %v", rb, err)
	}
	scope := taskScopeID(t, store, "gangs", "training", "ddp")
	op, err := store.ForcePauseScope(ctx, scope, "tester", "force-1", ForceStopSpec{GraceSeconds: 30})
	if err != nil {
		t.Fatal(err)
	}
	reconcilePauses(t, store)
	if err = store.ReconcileGangs(ctx); err != nil {
		t.Fatal(err)
	}
	refreshGangGPUs(t, store)
	if err = store.ReleaseReservation(ctx, rb.Lease.ID, rb.Lease.CoordinationEpoch, "revoked"); err != nil {
		t.Fatal(err)
	}
	reconcilePauses(t, store)
	var live int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM leases WHERE state!='released'`).Scan(&live); err != nil || live != 0 {
		t.Fatalf("leases left: %d %v", live, err)
	}
	if operation, _ := store.GetPauseOperation(ctx, op.ID); operation.State != "quiesced" {
		t.Fatalf("operation: %+v", operation)
	}
	if task := gangTaskState(t, store); task.State != "stopped" {
		t.Fatalf("gang task: %+v", task)
	}
}
