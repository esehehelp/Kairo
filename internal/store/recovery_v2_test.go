package store

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
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

// An attempt lost before its activation never started its command (its shim
// waits for the go-ahead sent after the activation): its stale lease needs
// no process-absence confirmation, and the daemon releases it by itself once
// its resources are observed again (issue #19). An activated one still needs
// the operator's confirmation.
func TestALostAttemptThatNeverStartedIsReleasedWithoutAnOperator(t *testing.T) {
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
	if n, err := store.ReconcileSafeStaleLeases(ctx); err != nil || n != 0 {
		t.Fatalf("released before an observation since it went stale: %d %v", n, err)
	}
	refreshSchedulerGPU(t, store)
	if n, err := store.ReconcileSafeStaleLeases(ctx); err != nil || n != 1 {
		t.Fatalf("released %d %v, want 1", n, err)
	}
	var state string
	if err = store.db.QueryRowContext(ctx, `SELECT state FROM leases WHERE id=$1`, reservation.Lease.ID).Scan(&state); err != nil || state != "released" {
		t.Fatalf("lease %s %v", state, err)
	}
	assertReconcileReason(t, store, reservation.Lease.ID, "never started")
	if n, err := store.ReconcileSafeStaleLeases(ctx); err != nil || n != 0 {
		t.Fatalf("released again: %d %v", n, err)
	}
}

func TestReconcileActivatedAttemptAfterNodeRegistration(t *testing.T) {
	for _, tc := range []struct {
		name          string
		bootFromStart time.Duration
		wantReboot    bool
		manual        bool
	}{
		{name: "unknown"},
		{name: "before-start", bootFromStart: -time.Minute},
		{name: "within-margin", bootFromStart: time.Minute},
		{name: "at-margin", bootFromStart: 2 * time.Minute},
		{name: "after-margin", bootFromStart: 2*time.Minute + time.Second, wantReboot: true},
		{name: "manual-after-reboot", bootFromStart: 3 * time.Minute, wantReboot: true, manual: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := openCoordinationStore(t)
			setupSchedulerInventory(t, store)
			ctx := t.Context()
			launch := startRunningExecution(t, store, tc.name)
			// Use different offsets so comparing the timestamp text would fail.
			started := time.Now().Add(-time.Hour).Truncate(time.Second).In(time.FixedZone("JST", 9*3600))
			if _, err := store.testExec(`UPDATE attempts SET started_at=$1 WHERE id=$2`, started.Format(time.RFC3339Nano), launch.Attempt.ID); err != nil {
				t.Fatal(err)
			}
			if tc.bootFromStart != 0 {
				bootedAt := started.Add(tc.bootFromStart).UTC().Format(time.RFC3339Nano)
				if err := store.UpsertNode(ctx, Node{ID: "node", Name: "node", OS: "windows", Architecture: "amd64", Enabled: true, BootedAt: &bootedAt}); err != nil {
					t.Fatal(err)
				}
			}
			if n, _, err := store.ReregisterExecutor(ctx, "executor", nil); err != nil || n != 1 {
				t.Fatalf("register executor: %d stale leases, %v", n, err)
			}
			if n, err := store.ReconcileSafeStaleLeases(ctx); err != nil || n != 0 {
				t.Fatalf("released before fresh observations: %d %v", n, err)
			}
			refreshSchedulerGPU(t, store)
			if !tc.wantReboot {
				if n, err := store.ReconcileSafeStaleLeases(ctx); err != nil || n != 0 {
					t.Fatalf("released without proof of reboot: %d %v", n, err)
				}
				if err := store.ReconcileResource(ctx, "gpu-0"); err == nil || !strings.Contains(err.Error(), "requires explicit process-absence confirmation") {
					t.Fatalf("reconcile without confirmation: %v", err)
				}
				if err := store.ReconcileResource(ctx, "gpu-0", true); err != nil {
					t.Fatal(err)
				}
				return
			}
			if tc.manual {
				if err := store.ReconcileResource(ctx, "gpu-0"); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := store.ObserveClaim(ctx, ExternalClaim{ID: "own-claim", ResourceID: "gpu-0", ClaimKind: "external_process", Evidence: json.RawMessage(`{"attempt_id":"` + launch.Attempt.ID + `"}`)}); err != nil {
					t.Fatal(err)
				}
				if n, err := store.ReconcileSafeStaleLeases(ctx); err != nil || n != 0 {
					t.Fatalf("released with a claim of the attempt: %d %v", n, err)
				}
				if err := store.ClearClaim(ctx, "own-claim"); err != nil {
					t.Fatal(err)
				}
				if n, err := store.ReconcileSafeStaleLeases(ctx); err != nil || n != 1 {
					t.Fatalf("released after reboot: %d %v, want 1", n, err)
				}
			}
			assertReconcileReason(t, store, launch.Lease.ID, "node rebooted since it started")
		})
	}
}

func assertReconcileReason(t *testing.T, store *Store, leaseID, want string) {
	t.Helper()
	var reason string
	if err := store.db.QueryRow(`SELECT payload_json::jsonb->>'reason' FROM coordination_events WHERE event_type='stale_lease_reconciled' AND aggregate_id=$1`, leaseID).Scan(&reason); err != nil || reason != want {
		t.Fatalf("reconcile reason = %q (%v), want %q", reason, err, want)
	}
}

// A claim on the device that is not the lost attempt's (another program's
// process, unattributed activity) does not hold its stale lease; a process
// of the attempt still seen there does (issue #20).
func TestReconcileIsHeldOnlyByTheLostAttemptsOwnClaims(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	ctx := context.Background()
	if _, _, err := store.SubmitExecution(ctx, schedulerExecutionSpec("lost-with-claims")); err != nil {
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
	if count, markErr := store.MarkExecutorUnknown(ctx, "executor"); markErr != nil || count != 1 {
		t.Fatalf("mark unknown: count=%d err=%v", count, markErr)
	}
	refreshSchedulerGPU(t, store)
	foreign := "pid:77:start:1"
	for _, c := range []ExternalClaim{
		{ID: "claim-foreign", ResourceID: "gpu-0", ClaimKind: "external_process", ProcessIdentity: &foreign, Evidence: json.RawMessage(`{"pid":"77"}`)},
		{ID: "claim-unattributed", ResourceID: "gpu-0", ClaimKind: "unattributed_activity", Evidence: json.RawMessage(`{"reason":"significant_vram_without_attributed_process"}`)},
		{ID: "claim-its-own", ResourceID: "gpu-0", ClaimKind: "external_process", ProcessIdentity: &foreign, Evidence: json.RawMessage(`{"pid":"78","attempt_id":"` + launch.Attempt.ID + `"}`)},
	} {
		if err = store.ObserveClaim(ctx, c); err != nil {
			t.Fatal(err)
		}
	}
	if err = store.ReconcileResource(ctx, "gpu-0", true); err == nil || !strings.Contains(err.Error(), launch.Attempt.ID) {
		t.Fatalf("released while a process of the lost attempt is seen: %v", err)
	}
	if _, err = store.testExec(`UPDATE external_claims SET cleared_at=$1 WHERE id='claim-its-own'`, now()); err != nil {
		t.Fatal(err)
	}
	if err = store.ReconcileResource(ctx, "gpu-0", true); err != nil {
		t.Fatalf("held by claims that are not the attempt's: %v", err)
	}
	var state string
	if err = store.db.QueryRowContext(ctx, `SELECT state FROM leases WHERE id=$1`, reservation.Lease.ID).Scan(&state); err != nil || state != "released" {
		t.Fatalf("lease %s %v", state, err)
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
