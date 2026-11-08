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
	if err = store.db.QueryRow(`SELECT state,coordination_epoch FROM leases WHERE id=$1`, reservation.Lease.ID).Scan(&state, &epoch); err != nil {
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

// A restarted agent that still runs an attempt (its shim alive with the pid
// and identity it was activated with) adopts it: the attempt, its lease and
// execution stay as they were. A claim whose identity differs (the pid
// reused) is not believed: that attempt is lost.
func TestARestartedAgentAdoptsTheAttemptsItStillRuns(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	ctx := context.Background()
	launch := startRunningExecution(t, store, "adopted")
	claim := AdoptedAttempt{AttemptID: launch.Attempt.ID, ExecutorID: "executor", PID: 4321, ProcessIdentity: "win:4321:start:1"}
	stale, adopted, err := store.ReregisterExecutor(ctx, "executor", []AdoptedAttempt{claim})
	if err != nil || stale != 0 || len(adopted) != 1 || adopted[0] != launch.Attempt.ID {
		t.Fatalf("adopt: stale=%d adopted=%v err=%v", stale, adopted, err)
	}
	var attemptState, leaseState, executionState string
	var epoch int64
	if err = store.db.QueryRow(`SELECT a.state,l.state,l.coordination_epoch,e.state FROM attempts a JOIN leases l ON l.attempt_id=a.id JOIN execution_requests e ON e.id=a.execution_id WHERE a.id=$1`, launch.Attempt.ID).Scan(&attemptState, &leaseState, &epoch, &executionState); err != nil {
		t.Fatal(err)
	}
	if attemptState != "running" || leaseState != "active" || epoch != launch.Lease.CoordinationEpoch || executionState == "terminal" {
		t.Fatalf("adopted attempt changed: %s %s %d %s", attemptState, leaseState, epoch, executionState)
	}
	var events int
	_ = store.db.QueryRow(`SELECT COUNT(*) FROM coordination_events WHERE event_type='attempt_adopted' AND aggregate_id=$1`, launch.Attempt.ID).Scan(&events)
	if events != 1 {
		t.Fatalf("attempt_adopted events: %d", events)
	}
	// the exit it then records is taken as usual
	if err = store.RecordTerminal(ctx, launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch, 0, ""); err != nil {
		t.Fatalf("exit of the adopted attempt: %v", err)
	}
	refreshSchedulerGPU(t, store)
	if err = store.FinalizeQuiescence(ctx, launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch); err != nil {
		t.Fatal(err)
	}

	otherLaunch := startRunningExecution(t, store, "pid-reused")
	claim = AdoptedAttempt{AttemptID: otherLaunch.Attempt.ID, ExecutorID: "executor", PID: 4321, ProcessIdentity: "win:4321:start:2"}
	stale, adopted, err = store.ReregisterExecutor(ctx, "executor", []AdoptedAttempt{claim})
	if err != nil || stale != 1 || len(adopted) != 0 {
		t.Fatalf("a claim with another identity: stale=%d adopted=%v err=%v", stale, adopted, err)
	}
	if err = store.db.QueryRow(`SELECT state FROM execution_requests WHERE id=$1`, otherLaunch.Attempt.ExecutionID).Scan(&executionState); err != nil || executionState != "terminal" {
		t.Fatalf("not adopted, not lost: %s %v", executionState, err)
	}
}

// startRunningExecution submits an execution named name and runs it on
// executor (pid 4321, identity win:4321:start:1).
func startRunningExecution(t *testing.T, store *Store, name string) *Launch {
	t.Helper()
	ctx := context.Background()
	if _, _, err := store.SubmitExecution(ctx, schedulerExecutionSpec(name)); err != nil {
		t.Fatal(err)
	}
	reservation, err := store.ReserveNext(ctx, "executor")
	if err != nil || reservation == nil {
		t.Fatalf("reserve %s: %+v %v", name, reservation, err)
	}
	if err = store.MarkLeasePrepared(ctx, reservation.Lease.ID, reservation.Lease.CoordinationEpoch); err != nil {
		t.Fatal(err)
	}
	launch, err := store.AuthorizeLaunch(ctx, reservation)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.ActivateLaunch(ctx, launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch, launch.AuthorizationToken, 4321, "win:4321:start:1"); err != nil {
		t.Fatal(err)
	}
	return launch
}
