package store

import (
	"context"
	"testing"
)

func TestExecutorRestartMakesOrphanReservationStale(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	ctx := context.Background()
	if _, _, err := store.SubmitExecution(ctx, schedulerExecutionSpec("orphan-reservation")); err != nil {
		t.Fatal(err)
	}
	reservation, err := store.ReserveNext(ctx, "executor")
	if err != nil || reservation == nil {
		t.Fatalf("reserve: %+v %v", reservation, err)
	}

	count, err := store.MarkExecutorUnknown(ctx, "executor")
	if err != nil || count != 1 {
		t.Fatalf("mark executor unknown: count=%d err=%v", count, err)
	}
	var state string
	var epoch int64
	if err = store.db.QueryRow(`SELECT state,coordination_epoch FROM leases WHERE id=?`, reservation.Lease.ID).Scan(&state, &epoch); err != nil {
		t.Fatal(err)
	}
	if state != "stale" || epoch != reservation.Lease.CoordinationEpoch+1 {
		t.Fatalf("orphan lease was not fenced: state=%s epoch=%d", state, epoch)
	}
	if next, reserveErr := store.ReserveNext(ctx, "executor"); reserveErr != nil || next != nil {
		t.Fatalf("stale orphan permitted duplicate reservation: %+v %v", next, reserveErr)
	}

	// Fresh provider evidence can reconcile the stale coordination fact. The
	// request never spawned a process, so it remains eligible at a new epoch.
	refreshSchedulerGPU(t, store)
	if err = store.ReconcileResource(ctx, "gpu-0"); err != nil {
		t.Fatal(err)
	}
	next, err := store.ReserveNext(ctx, "executor")
	if err != nil || next == nil || next.Lease.CoordinationEpoch != epoch+1 {
		t.Fatalf("request did not recover at a new epoch: %+v %v", next, err)
	}
}

func TestLostAttemptRequiresExplicitAbsenceEvenWithoutProcessRow(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	ctx := context.Background()
	if _, _, err := store.SubmitExecution(ctx, schedulerExecutionSpec("spawn-before-activation")); err != nil {
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
	if count, markErr := store.MarkExecutorUnknown(ctx, "executor"); markErr != nil || count != 1 {
		t.Fatalf("mark unknown: count=%d err=%v", count, markErr)
	}
	if err = store.ReconcileResource(ctx, "gpu-0", true); err == nil {
		t.Fatal("stale lease accepted an observation from before identity loss")
	}
	refreshSchedulerGPU(t, store)
	if err = store.ReconcileResource(ctx, "gpu-0"); err == nil {
		t.Fatal("lost attempt without a registered process was released without explicit absence confirmation")
	}
	if err = store.ReconcileResource(ctx, "gpu-0", true); err != nil {
		t.Fatal(err)
	}
}
