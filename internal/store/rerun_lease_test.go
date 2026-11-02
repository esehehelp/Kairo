package store

import (
	"context"
	"testing"
)

// A task whose attempt was lost (its executor restarted) keeps a stale lease
// whose processes may still run: it is rerun only once that lease is
// reconciled, never alongside them.
func TestRerunWaitsUntilALostAttemptsLeaseIsReleased(t *testing.T) {
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
	if count, err := store.MarkExecutorUnknown(ctx, "executor"); err != nil || count != 1 {
		t.Fatalf("mark unknown: %d %v", count, err)
	}
	if tasks := projectTasks(t, store, "orchestrated"); tasks["prepare"].State != "blocked" {
		t.Fatalf("prepare: %+v", tasks["prepare"])
	}
	_, err := store.RerunTask(ctx, "orchestrated", "prepare", "tester", "")
	taskConflictError(t, err, "execution "+launch.Execution.ID+" of orchestrated/data/prepare still holds lease "+launch.Lease.ID+" (stale)")
	refreshSchedulerGPU(t, store)
	if err = store.ReconcileResource(ctx, "gpu-0", true); err != nil {
		t.Fatal(err)
	}
	if result, err := store.RerunTask(ctx, "orchestrated", "prepare", "tester", ""); err != nil || result.State != "pending" {
		t.Fatalf("rerun after reconcile: %+v %v", result, err)
	}
}
