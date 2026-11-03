package store

import (
	"context"
	"testing"
)

// An attempt whose registered processes exited but whose lease is still held
// (processes it left run on) is reached by a forced pause: its executor gets
// a force stop order for the attempt's tree.
func TestForcePauseReachesAnExitedAttemptThatStillHoldsItsLease(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	forceProject(t, store, false)
	ctx := context.Background()
	launch := launchNext(t, store)
	if err := store.RecordTerminal(ctx, launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch, 0, ""); err != nil {
		t.Fatal(err)
	}
	scope := taskScopeID(t, store, "orchestrated", "work", "job")
	if _, err := store.ForcePauseScope(ctx, scope, "tester", "force-left", ForceStopSpec{GraceSeconds: 1}); err != nil {
		t.Fatal(err)
	}
	reconcilePauses(t, store)
	orders, err := store.ForceStopOrders(ctx, "executor")
	if err != nil || len(orders) != 1 || orders[0].AttemptID != launch.Attempt.ID {
		t.Fatalf("orders: %+v %v", orders, err)
	}
}

// A quarantine reaches such an attempt too (a termination of what is left),
// and the attempt, which exited by itself before, is not restarted for it.
func TestQuarantineReachesAnExitedAttemptThatStillHoldsItsLease(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	quarantineProject(t, store, true)
	ctx := context.Background()
	launch := launchNext(t, store)
	if err := store.RecordTerminal(ctx, launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch, 3, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := store.QuarantineNode(ctx, "node", "tester", "maintenance"); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcileNodeQuarantines(ctx); err != nil {
		t.Fatal(err)
	}
	terms, err := store.ListQuarantineTerminations(ctx, "executor")
	if err != nil || len(terms) != 1 || terms[0].AttemptID != launch.Attempt.ID {
		t.Fatalf("terminations: %+v %v", terms, err)
	}
	if err = store.MarkQuarantineTerminated(ctx, launch.Attempt.ID); err != nil {
		t.Fatal(err)
	}
	refreshSchedulerGPU(t, store)
	if err = store.FinalizeQuiescence(ctx, launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch); err != nil {
		t.Fatal(err)
	}
	if err = store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	project, err := store.GetProjectStatus(ctx, "orchestrated")
	if err != nil {
		t.Fatal(err)
	}
	if task := project.Tasks[0]; task.State != "failed" || len(task.Executions) != 1 {
		t.Fatalf("an attempt that failed before the quarantine was restarted for it: %+v", task)
	}
}
