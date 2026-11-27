package store

import (
	"context"
	"errors"
	"testing"
)

// A gang one of whose ranks never ran (its launch failed after the barrier,
// so it has no exit) did not succeed because its leader exited 0: it is run
// again from the start.
func TestGangWithARankThatNeverRanIsNotSucceeded(t *testing.T) {
	store := openCoordinationStore(t)
	setupGangInventory(t, store)
	gangProject(t, store, false)
	ctx := context.Background()
	rb, err := store.ReserveNext(ctx, "exec-b")
	if err != nil || rb == nil || rb.Gang == nil {
		t.Fatalf("exec-b reservation: %+v %v", rb, err)
	}
	if err = store.MarkLeasePrepared(ctx, rb.Lease.ID, rb.Lease.CoordinationEpoch); err != nil {
		t.Fatal(err)
	}
	if _, err = store.AuthorizeLaunch(ctx, rb); !errors.Is(err, ErrGangNotReady) {
		t.Fatalf("rank 1 launched alone: %v", err)
	}
	ra, err := store.ReserveNext(ctx, "exec-a")
	if err != nil || ra == nil || ra.Gang == nil {
		t.Fatalf("exec-a reservation: %+v %v", ra, err)
	}
	if err = store.MarkLeasePrepared(ctx, ra.Lease.ID, ra.Lease.CoordinationEpoch); err != nil {
		t.Fatal(err)
	}
	leader, err := store.AuthorizeLaunch(ctx, ra)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.AuthorizeLaunch(ctx, rb); err != nil {
		t.Fatal(err)
	}
	if err = store.ActivateLaunch(ctx, leader.Attempt.ID, leader.Lease.ID, leader.Lease.CoordinationEpoch, leader.AuthorizationToken, 200, "launcher"); err != nil {
		t.Fatal(err)
	}
	// Rank 1's launch fails before activation: its lease is released, its
	// attempt quiesced without an exit.
	refreshGangGPUs(t, store)
	if err = store.ReleaseReservation(ctx, rb.Lease.ID, rb.Lease.CoordinationEpoch, "launch failed"); err != nil {
		t.Fatal(err)
	}
	finishGangRank(t, store, gangRankLaunch{"exec-a", leader}, 0)
	task := gangTaskState(t, store)
	if task.State == "succeeded" {
		t.Fatalf("a gang with a rank that never ran succeeded: %+v", task)
	}
	if len(task.Executions) != 2 || task.Executions[1].Reason != "gang_restart" {
		t.Fatalf("not run again from the start: %+v", task)
	}
}
