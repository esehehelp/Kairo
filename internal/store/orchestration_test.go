package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"kairo/internal/orchestration"
)

func projectSpec(t *testing.T, source string) orchestration.Validated {
	t.Helper()
	parsed, err := orchestration.Parse([]byte(source))
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

func orchestrationManifest() string {
	return `schema_version = 1
[project]
name = "orchestrated"

[[tasks]]
name = "prepare"
queue = "data"
depends_on = []
policy = "RunToCompletion@v1"
[tasks.execution]
schema_version = 2
argv = ["python", "prepare.py"]
cwd = "/work"
checkpointable = false
preemptible = false
[tasks.execution.executor]
labels = { environment = "wsl2" }
[[tasks.execution.exclusive]]
kind = "gpu"
count = 1

[[tasks]]
name = "train"
queue = "training"
depends_on = ["prepare"]
policy = "RunToCompletion@v1"
[tasks.execution]
schema_version = 2
argv = ["python", "train.py"]
cwd = "/work"
checkpointable = true
preemptible = true
[tasks.execution.executor]
labels = { environment = "wsl2" }
[[tasks.execution.exclusive]]
kind = "gpu"
count = 1
`
}

func completeNextOrchestrationExecution(t *testing.T, store *Store, exitCode int) ExecutionRequest {
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
	if err = store.RecordTerminal(ctx, launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch, exitCode, ""); err != nil {
		t.Fatal(err)
	}
	refreshSchedulerGPU(t, store)
	if err = store.FinalizeQuiescence(ctx, launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch); err != nil {
		t.Fatal(err)
	}
	return launch.Execution
}

func TestProjectTaskDAGReconcilesIntoImmutableExecutions(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	ctx := context.Background()
	spec := projectSpec(t, orchestrationManifest())
	idempotent, err := store.ApplyProject(ctx, spec)
	if err != nil || idempotent {
		t.Fatalf("apply: idempotent=%v err=%v", idempotent, err)
	}
	if err = store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	status, err := store.GetProjectStatus(ctx, "orchestrated")
	if err != nil {
		t.Fatal(err)
	}
	if status.Tasks[0].Name != "prepare" || status.Tasks[0].State != "running" || len(status.Tasks[0].Executions) != 1 {
		t.Fatalf("prepare was not started: %+v", status)
	}
	if status.Tasks[1].Name != "train" || status.Tasks[1].State != "pending" || len(status.Tasks[1].Executions) != 0 {
		t.Fatalf("dependency was bypassed: %+v", status)
	}
	first := completeNextOrchestrationExecution(t, store, 0)
	if err = store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	status, err = store.GetProjectStatus(ctx, "orchestrated")
	if err != nil {
		t.Fatal(err)
	}
	if status.Tasks[0].State != "succeeded" || status.Tasks[1].State != "running" || len(status.Tasks[1].Executions) != 1 {
		t.Fatalf("DAG did not advance: %+v", status)
	}
	if first.TaskScopeID == nil || status.Tasks[0].Executions[0].ExecutionID != first.ID {
		t.Fatal("logical task was not durably bound to its execution")
	}
}

func TestFailedTaskBlocksDependents(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	ctx := context.Background()
	if _, err := store.ApplyProject(ctx, projectSpec(t, orchestrationManifest())); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	completeNextOrchestrationExecution(t, store, 9)
	if err := store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	status, err := store.GetProjectStatus(ctx, "orchestrated")
	if err != nil {
		t.Fatal(err)
	}
	if status.Tasks[0].State != "failed" || status.Tasks[1].State != "blocked" || len(status.Tasks[1].Executions) != 0 {
		t.Fatalf("failure did not block dependent: %+v", status)
	}
}

func TestRunToCompletionResumesCheckpointedExecution(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	ctx := context.Background()
	parsed := projectSpec(t, `schema_version = 1
[project]
name = "orchestrated"
[[tasks]]
name = "active-train"
queue = "training"
depends_on = []
policy = "RunToCompletion@v1"
[tasks.execution]
schema_version = 2
argv = ["python", "train.py"]
cwd = "/work"
checkpointable = true
preemptible = true
[tasks.execution.executor]
labels = { environment = "wsl2" }
[[tasks.execution.exclusive]]
kind = "gpu"
count = 1
`)
	if _, err := store.ApplyProject(ctx, parsed); err != nil {
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
	launch, err := store.AuthorizeLaunch(ctx, reservation)
	if err != nil {
		t.Fatal(err)
	}
	epoch := launch.Lease.CoordinationEpoch
	if err = store.ActivateLaunch(ctx, launch.Attempt.ID, launch.Lease.ID, epoch, launch.AuthorizationToken, 100, "launcher:100"); err != nil {
		t.Fatal(err)
	}
	commandID, err := store.EnqueueSuspend(ctx, launch.Attempt.ID, "priority_preemption", "test")
	if err != nil {
		t.Fatal(err)
	}
	for _, phase := range []string{"accepted", "checkpointing"} {
		if err = store.AckCommand(ctx, launch.Attempt.ID, launch.Lease.ID, epoch, commandID, phase, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err = store.AckCommand(ctx, launch.Attempt.ID, launch.Lease.ID, epoch, commandID, "checkpointed", json.RawMessage(`{"continuation_ref":"checkpoint://step-5"}`)); err != nil {
		t.Fatal(err)
	}
	if err = store.RecordTerminal(ctx, launch.Attempt.ID, launch.Lease.ID, epoch, 75, ""); err != nil {
		t.Fatal(err)
	}
	refreshSchedulerGPU(t, store)
	if err = store.FinalizeQuiescence(ctx, launch.Attempt.ID, launch.Lease.ID, epoch); err != nil {
		t.Fatal(err)
	}
	if err = store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	status, err := store.GetProjectStatus(ctx, "orchestrated")
	if err != nil {
		t.Fatal(err)
	}
	if len(status.Tasks) != 1 || len(status.Tasks[0].Executions) != 2 || status.Tasks[0].Executions[1].Reason != "continuation" {
		t.Fatalf("continuation was not submitted: %+v", status)
	}
	second, err := store.GetExecution(ctx, status.Tasks[0].Executions[1].ExecutionID)
	if err != nil || second.InputContinuationRef == nil || *second.InputContinuationRef != "checkpoint://step-5" {
		t.Fatalf("opaque continuation was not carried: %+v %v", second, err)
	}
}

func TestTaskDeclarationIsImmutable(t *testing.T) {
	store := openCoordinationStore(t)
	ctx := context.Background()
	source := orchestrationManifest()
	if _, err := store.ApplyProject(ctx, projectSpec(t, source)); err != nil {
		t.Fatal(err)
	}
	changed := strings.Replace(source, "priority = 0", "priority = 1", 1)
	if changed == source {
		changed = strings.Replace(source, "cwd = \"/work\"", "cwd = \"/other\"", 1)
	}
	if _, err := store.ApplyProject(ctx, projectSpec(t, changed)); !errors.Is(err, ErrOrchestrationConflict) {
		t.Fatalf("changed task declaration error=%v", err)
	}
}
