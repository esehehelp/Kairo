package store

import (
	"context"
	"testing"
)

// A waiter that is not checkpointable itself (a bench) preempts a running,
// checkpointable and preemptible lease of lower priority holding its GPU.
func TestANonCheckpointableWaiterPreemptsALowerPriorityServe(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	startPreemptionVictim(t, store, "executor", true, -100)
	spec := schedulerExecutionSpec("bench-waiter")
	spec.Priority = -50
	spec.Checkpointable, spec.Preemptible = false, false
	if _, _, err := store.SubmitExecution(context.Background(), spec); err != nil {
		t.Fatal(err)
	}
	commandIDs, err := store.EnsurePriorityPreemption(context.Background(), "executor")
	if err != nil || len(commandIDs) != 1 {
		t.Fatalf("preemption commands=%v err=%v", commandIDs, err)
	}
}
