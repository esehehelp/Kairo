package store

import (
	"context"
	"testing"
)

// The attempt a quarantine terminates may exit and quiesce before its
// executor records the termination carried out: the task is still run again
// from the start, not failed.
func TestQuarantineRestartDoesNotDependOnWhenTheTerminationIsRecorded(t *testing.T) {
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
	// The launcher exits and quiesces first; the termination is still pending.
	finishLaunch(t, store, launch, 1)
	if err := store.ReconcileProjectTasks(ctx); err != nil {
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
	if err = store.MarkQuarantineTerminated(ctx, launch.Attempt.ID); err != nil {
		t.Fatal(err)
	}
}
