package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// cpuProject declares prepare -> train -> report without GPUs: nothing is
// admitted without an executor, so executions stay waiting.
func cpuProject(t *testing.T, store *Store) {
	t.Helper()
	task := func(name, depends string) string {
		return `
[[tasks]]
name = "` + name + `"
queue = "work"
depends_on = [` + depends + `]
policy = "RunToCompletion@v1"
[tasks.execution]
schema_version = 2
argv = ["python", "` + name + `.py"]
cwd = "/work"
checkpointable = false
preemptible = false
[tasks.execution.executor]
labels = { environment = "wsl2" }
`
	}
	parsed := projectSpec(t, `schema_version = 1
[project]
name = "orchestrated"
`+task("prepare", "")+task("train", `"prepare"`)+task("report", `"train"`))
	if _, err := store.ApplyProject(context.Background(), parsed); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcileProjectTasks(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func taskConflictError(t *testing.T, err error, want string) {
	t.Helper()
	if !errors.Is(err, ErrTaskConflict) || !strings.Contains(err.Error(), want) {
		t.Fatalf("error %v, want a task conflict with %q", err, want)
	}
}

func TestCancelPendingTaskWithdrawsAndBlocksDependents(t *testing.T) {
	store := openCoordinationStore(t)
	cpuProject(t, store)
	ctx := context.Background()
	prepare := projectTasks(t, store, "orchestrated")["prepare"]
	if prepare.State != "running" || len(prepare.Executions) != 1 {
		t.Fatalf("prepare: %+v", prepare)
	}
	executionID := prepare.Executions[0].ExecutionID

	dry, err := store.CancelTask(ctx, "orchestrated", "prepare", "tester", true)
	if err != nil || !dry.DryRun || len(dry.Withdrawn) != 1 || dry.Withdrawn[0] != executionID {
		t.Fatalf("dry run: %+v %v", dry, err)
	}
	if execution, _ := store.GetExecution(ctx, executionID); execution.State != "waiting" {
		t.Fatalf("dry run withdrew: %+v", execution)
	}
	if tasks := projectTasks(t, store, "orchestrated"); tasks["prepare"].State != "running" {
		t.Fatalf("dry run changed the task: %+v", tasks["prepare"])
	}

	result, err := store.CancelTask(ctx, "orchestrated", "prepare", "tester", false)
	if err != nil || result.State != "cancelled" || len(result.Withdrawn) != 1 || result.Withdrawn[0] != executionID {
		t.Fatalf("cancel: %+v %v", result, err)
	}
	execution, _ := store.GetExecution(ctx, executionID)
	if execution.State != "terminal" || *execution.TerminalCause != "withdrawn_before_start" {
		t.Fatalf("execution: %+v", execution)
	}
	events, err := store.ListEvents(ctx, EventFilter{Limit: 1000})
	if err != nil {
		t.Fatal(err)
	}
	var withdrawnEvent, cancelledEvent bool
	for _, e := range events {
		withdrawnEvent = withdrawnEvent || (e.EventType == "execution_withdrawn" && e.AggregateID == executionID)
		cancelledEvent = cancelledEvent || e.EventType == "task_cancelled"
	}
	if !withdrawnEvent || !cancelledEvent {
		t.Fatalf("events: withdrawn=%v cancelled=%v", withdrawnEvent, cancelledEvent)
	}
	projectTasks(t, store, "orchestrated")
	tasks := projectTasks(t, store, "orchestrated")
	if tasks["prepare"].State != "cancelled" || tasks["train"].State != "blocked" || tasks["report"].State != "blocked" {
		t.Fatalf("tasks: %+v", tasks)
	}
	var reason map[string]any
	if err = json.Unmarshal(tasks["prepare"].Result, &reason); err != nil || reason["reason"] != "cancelled" || reason["actor"] != "tester" {
		t.Fatalf("result: %s", tasks["prepare"].Result)
	}

	// Cancelling again is idempotent and reports the same withdrawals.
	again, err := store.CancelTask(ctx, "orchestrated", "prepare", "tester", false)
	if err != nil || len(again.Withdrawn) != 1 || again.Withdrawn[0] != executionID {
		t.Fatalf("repeated cancel: %+v %v", again, err)
	}
	// A blocked task is cancelled without anything to withdraw.
	blocked, err := store.CancelTask(ctx, "orchestrated", "train", "tester", false)
	if err != nil || len(blocked.Withdrawn) != 0 {
		t.Fatalf("cancel blocked: %+v %v", blocked, err)
	}
	if _, err = store.CancelTask(ctx, "orchestrated", "missing", "tester", false); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown task: %v", err)
	}
}

func TestCancelRefusesAStartedExecutionAndEndedTasks(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	ctx := context.Background()
	if _, err := store.ApplyProject(ctx, projectSpec(t, orchestrationManifest())); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	launch := launchNext(t, store)
	_, err := store.CancelTask(ctx, "orchestrated", "prepare", "tester", false)
	taskConflictError(t, err, "execution "+launch.Execution.ID+" of orchestrated/data/prepare is running (started); pause the task first, or end it with task pause --force")
	finishLaunch(t, store, launch, 0)
	projectTasks(t, store, "orchestrated")
	_, err = store.CancelTask(ctx, "orchestrated", "prepare", "tester", false)
	taskConflictError(t, err, "task prepare already ended (succeeded)")
}

func TestCancelWithdrawsAnAuthorizedExecutionAndRevokesItsLease(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	ctx := context.Background()
	if _, err := store.ApplyProject(ctx, projectSpec(t, orchestrationManifest())); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	reservation, err := store.ReserveNext(ctx, "executor")
	if err != nil || reservation == nil {
		t.Fatalf("reserve: %+v %v", reservation, err)
	}
	if err = store.MarkLeasePrepared(ctx, reservation.Lease.ID, reservation.Lease.CoordinationEpoch); err != nil {
		t.Fatal(err)
	}
	if _, err = store.AuthorizeLaunch(ctx, reservation); err != nil {
		t.Fatal(err)
	}
	result, err := store.CancelTask(ctx, "orchestrated", "prepare", "tester", false)
	if err != nil || len(result.Withdrawn) != 1 || result.Withdrawn[0] != reservation.Execution.ID {
		t.Fatalf("cancel: %+v %v", result, err)
	}
	var lease, authorization string
	if err = store.db.QueryRow(`SELECT state FROM leases WHERE id=?`, reservation.Lease.ID).Scan(&lease); err != nil || lease != "revocation_requested" {
		t.Fatalf("lease: %s %v", lease, err)
	}
	if err = store.db.QueryRow(`SELECT state FROM launch_authorizations WHERE lease_id=?`, reservation.Lease.ID).Scan(&authorization); err != nil || authorization != "revoked" {
		t.Fatalf("authorization: %s %v", authorization, err)
	}
}

func TestCancelWithdrawsEveryGangRank(t *testing.T) {
	store := openCoordinationStore(t)
	setupGangInventory(t, store)
	gangProject(t, store, false)
	result, err := store.CancelTask(context.Background(), "gangs", "ddp", "tester", false)
	if err != nil || len(result.Withdrawn) != 2 {
		t.Fatalf("cancel: %+v %v", result, err)
	}
	if task := gangTaskState(t, store); task.State != "cancelled" {
		t.Fatalf("task: %+v", task)
	}
}

// The reconciler reads a planned decision, then submits it; a cancel can land
// on either side of the submission. A cancel committed first abandons the
// decision, which is then never submitted; a cancel committed after finds the
// execution live and withdraws it. Either way the task stays cancelled.
func TestCancelRacingTheReconcilerNeverRevivesTheTask(t *testing.T) {
	for _, cancelAfterSubmit := range []bool{false, true} {
		store := openCoordinationStore(t)
		ctx := context.Background()
		if _, err := store.ApplyProject(ctx, projectSpec(t, orchestrationManifest())); err != nil {
			t.Fatal(err)
		}
		if err := store.planProjectTasks(ctx); err != nil {
			t.Fatal(err)
		}
		decisions, err := store.pendingOrchestrationDecisions(ctx)
		if err != nil || len(decisions) != 1 {
			t.Fatalf("decisions: %+v %v", decisions, err)
		}
		cancel := func() CancelResult {
			result, err := store.CancelTask(ctx, "orchestrated", "prepare", "tester", false)
			if err != nil {
				t.Fatal(err)
			}
			return result
		}
		var result CancelResult
		if !cancelAfterSubmit {
			result = cancel()
		}
		if err = store.submitOrchestrationDecision(ctx, decisions[0]); err != nil {
			t.Fatal(err)
		}
		if cancelAfterSubmit {
			result = cancel()
		}
		// A retried submission of the decision read before is harmless.
		if err = store.submitOrchestrationDecision(ctx, decisions[0]); err != nil {
			t.Fatal(err)
		}
		executions, err := store.ListExecutions(ctx, ExecutionFilter{})
		if err != nil {
			t.Fatal(err)
		}
		tasks := projectTasks(t, store, "orchestrated")
		var state string
		if err = store.db.QueryRow(`SELECT state FROM orchestration_decisions WHERE decision_key=?`, decisions[0].DecisionKey).Scan(&state); err != nil {
			t.Fatal(err)
		}
		if !cancelAfterSubmit {
			if len(executions) != 0 || len(result.Withdrawn) != 0 || state != "abandoned" || tasks["prepare"].State != "cancelled" || len(tasks["prepare"].Executions) != 0 {
				t.Fatalf("cancel before submit: executions %+v, cancel %+v, decision %s, task %+v", executions, result, state, tasks["prepare"])
			}
		} else {
			if len(executions) != 1 || len(result.Withdrawn) != 1 || result.Withdrawn[0] != executions[0].ID || state != "submitted" || tasks["prepare"].State != "cancelled" || len(tasks["prepare"].Executions) != 1 {
				t.Fatalf("cancel after submit: executions %+v, cancel %+v, decision %s, task %+v", executions, result, state, tasks["prepare"])
			}
			if executions[0].State != "terminal" || *executions[0].TerminalCause != "withdrawn_before_start" {
				t.Fatalf("cancel after submit: execution %+v", executions[0])
			}
		}
		if pending, _ := store.pendingOrchestrationDecisions(ctx); len(pending) != 0 {
			t.Fatalf("planned again: %+v", pending)
		}
	}
}

func TestWithdrawnExecutionCancelsTheTask(t *testing.T) {
	store := openCoordinationStore(t)
	cpuProject(t, store)
	ctx := context.Background()
	executionID := projectTasks(t, store, "orchestrated")["prepare"].Executions[0].ExecutionID
	if err := store.WithdrawExecution(ctx, executionID); err != nil {
		t.Fatal(err)
	}
	tasks := projectTasks(t, store, "orchestrated")
	var result map[string]any
	if tasks["prepare"].State != "cancelled" || json.Unmarshal(tasks["prepare"].Result, &result) != nil || result["reason"] != "withdrawn before start" || result["execution_id"] != executionID {
		t.Fatalf("prepare: %+v %s", tasks["prepare"], tasks["prepare"].Result)
	}
	if tasks = projectTasks(t, store, "orchestrated"); tasks["train"].State != "blocked" {
		t.Fatalf("train: %+v", tasks["train"])
	}
}

func TestOpenMigratesAnOlderDatabaseForCancelAndRerun(t *testing.T) {
	path := filepath.Join(t.TempDir(), "old.db")
	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	cpuProject(t, st)
	st.Close()
	// Rewind to the schema before cancel and rerun, with two historic failures.
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range []struct{ table, from, to string }{
		{"orchestration_tasks", "state IN('pending','running','succeeded','failed','blocked','stopped','cancelled')", "state IN('pending','running','succeeded','failed','blocked','stopped')"},
		{"orchestration_decisions", "reason IN('initial','continuation','quarantine_restart','gang_restart','rerun')", "reason IN('initial','continuation','quarantine_restart','gang_restart')"},
		{"orchestration_decisions", "state IN('planned','submitted','abandoned')", "state IN('planned','submitted')"},
	} {
		if err = widenCheck(db, w.table, w.from, w.to); err != nil {
			t.Fatal(err)
		}
	}
	for _, stmt := range []string{
		`DROP TABLE orchestration_task_reruns`,
		`ALTER TABLE orchestration_tasks DROP COLUMN rerun_ordinal`,
		`UPDATE orchestration_tasks SET state='failed',result_json='{"execution_id":"exe_old","reason":"execution terminal before attempt","terminal_cause":"withdrawn_before_start"}' WHERE name='prepare'`,
		`UPDATE orchestration_tasks SET state='failed',result_json='{"execution_id":"exe_other","reason":"execution terminal before attempt","terminal_cause":"admission_fail"}' WHERE name='train'`,
	} {
		if _, err = db.Exec(stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	db.Close()
	for range 2 { // the migration is idempotent
		if st, err = Open(path); err != nil {
			t.Fatal(err)
		}
		status, err := st.GetProjectStatus(context.Background(), "orchestrated")
		if err != nil {
			t.Fatal(err)
		}
		if status.Tasks[0].Name != "prepare" || status.Tasks[0].State != "cancelled" || string(status.Tasks[0].Result) != `{"reason":"withdrawn before start","execution_id":"exe_old","migrated":true}` {
			t.Fatalf("prepare: %+v %s", status.Tasks[0], status.Tasks[0].Result)
		}
		if status.Tasks[2].Name != "train" || status.Tasks[2].State != "failed" {
			t.Fatalf("train: %+v", status.Tasks[2])
		}
		for _, w := range []struct{ table, want string }{{"orchestration_tasks", "'cancelled'"}, {"orchestration_decisions", "'abandoned'"}, {"orchestration_decisions", "'rerun'"}} {
			var ddl string
			if err = st.db.QueryRow(`SELECT sql FROM sqlite_master WHERE name=?`, w.table).Scan(&ddl); err != nil || !strings.Contains(ddl, w.want) {
				t.Fatalf("%s not widened: %s %v", w.table, ddl, err)
			}
		}
		for _, index := range []string{"orchestration_task_dependents_idx", "executions_task_idx"} {
			var n int
			if err = st.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name=?`, index).Scan(&n); err != nil || n != 1 {
				t.Fatalf("index %s: %d %v", index, n, err)
			}
		}
		st.Close()
	}
}

func TestRerunFailedTaskRunsANewExecutionAndItsDependents(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	ctx := context.Background()
	if _, err := store.ApplyProject(ctx, projectSpec(t, orchestrationManifest())); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	first := completeNextOrchestrationExecution(t, store, 9)
	projectTasks(t, store, "orchestrated")
	if tasks := projectTasks(t, store, "orchestrated"); tasks["prepare"].State != "failed" || tasks["train"].State != "blocked" {
		t.Fatalf("tasks: %+v", tasks)
	}
	for _, name := range []string{"train"} {
		// train is blocked: rerunning it alone waits for prepare again.
		if _, err := store.RerunTask(ctx, "orchestrated", name, "tester", "rerun-train"); err != nil {
			t.Fatal(err)
		}
	}
	if tasks := projectTasks(t, store, "orchestrated"); tasks["train"].State != "blocked" || len(tasks["train"].Executions) != 0 {
		t.Fatalf("train ran with a failed dependency: %+v", tasks["train"])
	}
	result, err := store.RerunTask(ctx, "orchestrated", "prepare", "tester", "rerun-1")
	if err != nil || result.State != "pending" || result.Ordinal != 1 || len(result.Unblocked) != 1 || result.Unblocked[0] != "train" {
		t.Fatalf("rerun: %+v %v", result, err)
	}
	again, err := store.RerunTask(ctx, "orchestrated", "prepare", "tester", "rerun-1")
	if err != nil || again.Ordinal != 1 || len(again.Unblocked) != 1 {
		t.Fatalf("repeated rerun: %+v %v", again, err)
	}
	tasks := projectTasks(t, store, "orchestrated")
	prepare := tasks["prepare"]
	if prepare.State != "running" || len(prepare.Executions) != 2 || prepare.Executions[1].Ordinal != 1 || prepare.Executions[1].Reason != "rerun" {
		t.Fatalf("prepare: %+v", prepare)
	}
	if tasks["train"].State != "pending" {
		t.Fatalf("train: %+v", tasks["train"])
	}
	var trigger string
	if err = store.db.QueryRow(`SELECT trigger_json FROM orchestration_decisions WHERE reason='rerun' AND execution_id=?`, prepare.Executions[1].ExecutionID).Scan(&trigger); err != nil || trigger != `{"predecessor_execution_id":"`+first.ID+`","request_id":"rerun-1"}` {
		t.Fatalf("trigger: %s %v", trigger, err)
	}
	completeNextOrchestrationExecution(t, store, 0)
	if tasks = projectTasks(t, store, "orchestrated"); tasks["prepare"].State != "succeeded" || tasks["train"].State != "running" {
		t.Fatalf("after rerun: %+v", tasks)
	}
	// train's own rerun ordinal (0) is its first execution.
	if train := tasks["train"]; len(train.Executions) != 1 || train.Executions[0].Reason != "rerun" || train.Executions[0].Ordinal != 0 {
		t.Fatalf("train: %+v", train)
	}
	completeNextOrchestrationExecution(t, store, 0)
	if tasks = projectTasks(t, store, "orchestrated"); tasks["train"].State != "succeeded" {
		t.Fatalf("train: %+v", tasks["train"])
	}
	var reruns int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM orchestration_task_reruns`).Scan(&reruns); err != nil || reruns != 2 {
		t.Fatalf("reruns: %d %v", reruns, err)
	}
}

func TestRerunUnblocksTransitiveDependents(t *testing.T) {
	store := openCoordinationStore(t)
	cpuProject(t, store)
	ctx := context.Background()
	if _, err := store.CancelTask(ctx, "orchestrated", "prepare", "tester", false); err != nil {
		t.Fatal(err)
	}
	projectTasks(t, store, "orchestrated")
	if tasks := projectTasks(t, store, "orchestrated"); tasks["report"].State != "blocked" {
		t.Fatalf("report: %+v", tasks["report"])
	}
	result, err := store.RerunTask(ctx, "orchestrated", "prepare", "", "")
	if err != nil || result.Ordinal != 1 || strings.Join(result.Unblocked, ",") != "report,train" {
		t.Fatalf("rerun: %+v %v", result, err)
	}
	tasks := projectTasks(t, store, "orchestrated")
	if tasks["prepare"].State != "running" || len(tasks["prepare"].Executions) != 2 || tasks["train"].State != "pending" || tasks["report"].State != "pending" {
		t.Fatalf("tasks: %+v", tasks)
	}
}

func TestRerunRefusesLiveTasks(t *testing.T) {
	store := openCoordinationStore(t)
	cpuProject(t, store)
	ctx := context.Background()
	_, err := store.RerunTask(ctx, "orchestrated", "prepare", "tester", "")
	taskConflictError(t, err, "task prepare is running; only failed, stopped, cancelled or blocked tasks can be rerun")
	_, err = store.RerunTask(ctx, "orchestrated", "train", "tester", "")
	taskConflictError(t, err, "task train is pending; only")
	if _, err = store.CancelTask(ctx, "orchestrated", "prepare", "tester", false); err != nil {
		t.Fatal(err)
	}
	// An execution submitted into the task's scope by hand keeps it from rerunning.
	manual, _, err := store.SubmitExecution(ctx, ExecutionSpec{ClientRequestID: "manual", Scope: ScopePath{Project: "orchestrated", Queue: "work", Task: "prepare"}, Argv: []string{"x"}, CWD: "/"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.RerunTask(ctx, "orchestrated", "prepare", "tester", "")
	taskConflictError(t, err, "execution "+manual.ID+" of orchestrated/work/prepare is not terminal (waiting)")
}

func TestRerunOfAFailedGangRunsTheWholeGangAgain(t *testing.T) {
	store := openCoordinationStore(t)
	setupGangInventory(t, store)
	gangProject(t, store, false)
	rank0, rank1 := launchGang(t, store)
	finishGangRank(t, store, rank0, 3)
	finishGangRank(t, store, rank1, 0)
	if task := gangTaskState(t, store); task.State != "failed" {
		t.Fatalf("gang: %+v", task)
	}
	if _, err := store.RerunTask(context.Background(), "gangs", "ddp", "tester", ""); err != nil {
		t.Fatal(err)
	}
	task := gangTaskState(t, store)
	if task.State != "running" || len(task.Executions) != 2 || task.Executions[1].Reason != "rerun" {
		t.Fatalf("gang: %+v", task)
	}
	detail, err := store.GetTaskDetail(context.Background(), "gangs", "ddp")
	if err != nil || len(detail.Executions) != 2 {
		t.Fatalf("detail: %+v %v", detail, err)
	}
	old := detail.Executions[0]
	if len(old.Gang) != 2 || old.Gang[1].NodeID == nil || *old.Gang[1].NodeID != "node-b" || old.Gang[0].AttemptID == nil {
		t.Fatalf("old gang: %+v", old.Gang)
	}
	if fresh := detail.Executions[1]; len(fresh.Gang) != 2 || fresh.Gang[0].State != "waiting" || len(fresh.Attempts) != 0 {
		t.Fatalf("new gang: %+v", fresh)
	}
	if len(old.Attempts) != 1 || old.Attempts[0].NodeID == nil || *old.Attempts[0].NodeID != "node-a" {
		t.Fatalf("leader attempt: %+v", old.Attempts)
	}
}

func TestProjectStatusFilters(t *testing.T) {
	store := openCoordinationStore(t)
	cpuProject(t, store)
	ctx := context.Background()
	before := time.Now().Add(-time.Second)
	if _, err := store.CancelTask(ctx, "orchestrated", "prepare", "tester", false); err != nil {
		t.Fatal(err)
	}
	projectTasks(t, store, "orchestrated")
	projectTasks(t, store, "orchestrated") // prepare cancelled; train and report blocked
	names := func(filter TaskFilter) string {
		t.Helper()
		status, err := store.QueryProjectStatus(ctx, "orchestrated", filter)
		if err != nil {
			t.Fatal(err)
		}
		if status.StateCounts["cancelled"] != 1 || status.StateCounts["blocked"] != 2 {
			t.Fatalf("state counts: %+v", status.StateCounts)
		}
		var out []string
		for _, task := range status.Tasks {
			out = append(out, task.Name)
			if task.CreatedAt == "" || task.UpdatedAt == "" {
				t.Fatalf("timestamps: %+v", task)
			}
			if task.Name == "train" && (len(task.DependsOn) != 1 || task.DependsOn[0] != "prepare") {
				t.Fatalf("train depends on: %+v", task.DependsOn)
			}
			if task.Name == "prepare" && (len(task.Executions) != 1 || task.Executions[0].SubmittedAt == "" || task.Executions[0].TerminalAt == nil) {
				t.Fatalf("prepare executions: %+v", task.Executions)
			}
		}
		return strings.Join(out, ",")
	}
	for _, c := range []struct {
		filter TaskFilter
		want   string
	}{
		{TaskFilter{}, "prepare,report,train"},
		{TaskFilter{Names: []string{"train", "prepare", "missing"}}, "prepare,train"},
		{TaskFilter{Prefix: "tr"}, "train"},
		{TaskFilter{States: []string{"blocked"}}, "report,train"},
		{TaskFilter{States: []string{"blocked", "cancelled"}, Prefix: "re"}, "report"},
		{TaskFilter{Live: true}, ""},
		{TaskFilter{Since: before}, "prepare,report,train"},
		{TaskFilter{Since: time.Now().Add(time.Hour)}, ""},
	} {
		if got := names(c.filter); got != c.want {
			t.Fatalf("%+v: %q, want %q", c.filter, got, c.want)
		}
	}
	if _, err := store.QueryProjectStatus(ctx, "orchestrated", TaskFilter{States: []string{"bogus"}}); err == nil {
		t.Fatal("unknown state accepted")
	}
	if _, err := store.QueryProjectStatus(ctx, "missing", TaskFilter{}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown project: %v", err)
	}
}

func TestTaskDetail(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	ctx := context.Background()
	if _, err := store.ApplyProject(ctx, projectSpec(t, orchestrationManifest())); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	launch := launchNext(t, store)
	if err := store.SetAttemptLogPaths(ctx, launch.Attempt.ID, launch.Lease.CoordinationEpoch, "logs/a.stdout.log", "logs/a.stderr.log"); err != nil {
		t.Fatal(err)
	}
	if err := store.Heartbeat(ctx, launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch, json.RawMessage(`{"unit":"step","current":3,"total":10,"detail":{"loss":1.5}}`)); err != nil {
		t.Fatal(err)
	}
	detail, err := store.GetTaskDetail(ctx, "orchestrated", "prepare")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(detail.Spec), `"name":"prepare"`) || len(detail.Dependents) != 1 || detail.Dependents[0] != (TaskRef{"train", "pending"}) || len(detail.Dependencies) != 0 {
		t.Fatalf("detail: %+v", detail)
	}
	if len(detail.Gates) != 3 || detail.Gates[0] != (TaskGate{Scope: "project", State: "open"}) || detail.Gates[2] != (TaskGate{Scope: "task", State: "open"}) {
		t.Fatalf("gates: %+v", detail.Gates)
	}
	if len(detail.Executions) != 1 || len(detail.Executions[0].Attempts) != 1 {
		t.Fatalf("executions: %+v", detail.Executions)
	}
	execution := detail.Executions[0]
	if execution.AttemptID == nil || *execution.AttemptID != launch.Attempt.ID || execution.ExecutorID == nil || *execution.ExecutorID != "executor" || execution.StartedAt == nil || execution.AttemptStartedAt == nil {
		t.Fatalf("execution: %+v", execution.TaskExecution)
	}
	if string(execution.Progress) != `{"unit":"step","current":3,"total":10}` {
		t.Fatalf("progress: %s", execution.Progress)
	}
	attempt := execution.Attempts[0]
	if attempt.NodeID == nil || *attempt.NodeID != "node" || attempt.StdoutPath == nil || *attempt.StdoutPath != "logs/a.stdout.log" || !strings.Contains(string(attempt.Progress), "loss") {
		t.Fatalf("attempt: %+v", attempt)
	}
	if execution.Waiting != nil || execution.Gang != nil {
		t.Fatalf("a started execution waits: %+v", execution)
	}
	if attempt.LogProblems != nil || detail.Paused != nil {
		t.Fatalf("problems or pause of a healthy task: %+v %+v", attempt.LogProblems, detail.Paused)
	}
	// Log problems reported while it runs are shown on its attempt.
	for _, p := range []LogProblem{
		{Problem: LogForwardFailed, Stream: "stderr", Detail: "agent API 500: no space left on device"},
		{Problem: LogForwardFailed, Stream: "stderr", Detail: "", Resolved: true},
		{Problem: LogSpaceExhausted, Detail: "/srv/kairo has 0 B free"},
	} {
		if err := store.RecordAttemptLogProblem(ctx, launch.Attempt.ID, p); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.RecordAttemptLogProblem(ctx, launch.Attempt.ID, LogProblem{Problem: "disk_on_fire"}); err == nil {
		t.Fatal("unknown log problem accepted")
	}
	if err := store.RecordAttemptLogProblem(ctx, "att_missing", LogProblem{Problem: LogSpaceLow}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("problem of an unknown attempt: %v", err)
	}
	if detail, err = store.GetTaskDetail(ctx, "orchestrated", "prepare"); err != nil {
		t.Fatal(err)
	}
	problems := detail.Executions[0].Attempts[0].LogProblems
	if len(problems) != 2 || problems[0].Problem != LogForwardFailed || problems[0].Detail != "agent API 500: no space left on device" || !problems[0].Resolved || problems[0].ResolvedAt == nil || problems[0].Occurrences != 2 ||
		problems[1].Problem != LogSpaceExhausted || problems[1].Resolved {
		t.Fatalf("log problems: %+v", problems)
	}
	events, err := store.ListEvents(ctx, EventFilter{})
	if err != nil {
		t.Fatal(err)
	}
	var logEvents []string
	for _, e := range events {
		if e.AggregateID == launch.Attempt.ID && strings.HasPrefix(e.EventType, "attempt_log_problem") {
			logEvents = append(logEvents, e.EventType)
		}
	}
	if strings.Join(logEvents, ",") != "attempt_log_problem,attempt_log_problem_resolved,attempt_log_problem" {
		t.Fatalf("log problem events: %v", logEvents)
	}

	// train waits behind its own closed task gate once submitted.
	finishLaunch(t, store, launch, 0)
	if _, err = store.PauseScope(ctx, taskScopeID(t, store, "orchestrated", "training", "train"), "tester", "pause-train"); err != nil {
		t.Fatal(err)
	}
	projectTasks(t, store, "orchestrated")
	if detail, err = store.GetTaskDetail(ctx, "orchestrated", "train"); err != nil {
		t.Fatal(err)
	}
	if len(detail.Dependencies) != 1 || detail.Dependencies[0] != (TaskRef{"prepare", "succeeded"}) || detail.Gates[2].Scope != "task" || detail.Gates[2].State != "closed" || detail.Gates[2].ClosedBy != "tester" || detail.Gates[2].ClosedAt == "" {
		t.Fatalf("train: %+v", detail)
	}
	if len(detail.Executions) != 1 || len(detail.Executions[0].Waiting) != 1 || detail.Executions[0].Waiting[0].ReasonCode != "scope_gate_closed" || detail.Executions[0].Waiting[0].Scope != "task" ||
		string(detail.Executions[0].Waiting[0].Detail) != `{"closed_by":"tester"}` || detail.Executions[0].Waiting[0].CreatedAt != detail.Gates[2].ClosedAt {
		t.Fatalf("waiting: %+v", detail.Executions)
	}
	// The task stays running, and is shown paused by its task gate.
	if detail.State != "running" || detail.Paused == nil || *detail.Paused != detail.Gates[2] {
		t.Fatalf("paused: %s %+v", detail.State, detail.Paused)
	}
	// A closed project gate is reported for every live task, unless an inner
	// one is closed too; a second pause of a closed gate is not its closer.
	if _, err = store.PauseScope(ctx, taskScopeID(t, store, "orchestrated", "training", "train"), "someone-else", "pause-train-again"); err != nil {
		t.Fatal(err)
	}
	project, err := store.GetScopeByPath(ctx, ScopePath{Project: "orchestrated"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.PauseScope(ctx, project.Project.ID, "operator", "pause-project"); err != nil {
		t.Fatal(err)
	}
	status, err := store.GetProjectStatus(ctx, "orchestrated")
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range status.Tasks {
		want := "project operator"
		switch task.Name {
		case "train":
			want = "task tester"
		case "prepare": // succeeded: nothing to hold back
			if task.Paused != nil {
				t.Fatalf("ended task paused: %+v", task.Paused)
			}
			continue
		}
		if task.Paused == nil || task.Paused.Scope+" "+task.Paused.ClosedBy != want {
			t.Fatalf("%s paused: %+v, want %s", task.Name, task.Paused, want)
		}
	}
	if _, err = store.GetTaskDetail(ctx, "orchestrated", "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown task: %v", err)
	}
}
