package store

import (
	"context"
	"errors"
	"testing"
)

// An AuthorizeLaunch asked again (its answer was lost) hands over the same
// attempt with new tokens; only the new authorization activates, and once it
// is consumed a repeat is refused.
func TestAuthorizeLaunchAskedAgainReissuesTheUnconsumedAuthorization(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	ctx := context.Background()
	if _, _, err := store.SubmitExecution(ctx, schedulerExecutionSpec("reauthorize")); err != nil {
		t.Fatal(err)
	}
	reservation, err := store.ReserveNext(ctx, "executor")
	if err != nil || reservation == nil {
		t.Fatalf("reserve: %+v %v", reservation, err)
	}
	if err = store.MarkLeasePrepared(ctx, reservation.Lease.ID, reservation.Lease.CoordinationEpoch); err != nil {
		t.Fatal(err)
	}
	first, err := store.AuthorizeLaunch(ctx, reservation)
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.AuthorizeLaunch(ctx, reservation)
	if err != nil {
		t.Fatalf("repeated authorization: %v", err)
	}
	if second.Attempt.ID != first.Attempt.ID || second.AuthorizationID != first.AuthorizationID ||
		second.AuthorizationToken == first.AuthorizationToken || second.WorkerToken == first.WorkerToken {
		t.Fatalf("reissue: first %+v second %+v", first, second)
	}
	epoch := second.Lease.CoordinationEpoch
	if err = store.ActivateLaunch(ctx, first.Attempt.ID, first.Lease.ID, epoch, first.AuthorizationToken, 100, "launcher:100"); err == nil {
		t.Fatal("the replaced authorization token activated the launch")
	}
	if _, err = store.AuthenticateWorkerToken(ctx, first.WorkerToken); err == nil {
		t.Fatal("the replaced worker token still authenticates")
	}
	if err = store.ActivateLaunch(ctx, second.Attempt.ID, second.Lease.ID, epoch, second.AuthorizationToken, 100, "launcher:100"); err != nil {
		t.Fatal(err)
	}
	if _, err = store.AuthenticateWorkerToken(ctx, second.WorkerToken); err != nil {
		t.Fatalf("the reissued worker token: %v", err)
	}
	if _, err = store.AuthorizeLaunch(ctx, reservation); !errors.Is(err, ErrExecutionStarted) {
		t.Fatalf("authorization repeated after activation: %v", err)
	}
}

// An ActivateLaunch asked again with the same launcher is answered the same;
// another process is refused.
func TestActivateLaunchAskedAgainIsAnsweredTheSame(t *testing.T) {
	store, _, launch := startCooperativeExecution(t, "reactivate")
	ctx := context.Background()
	epoch := launch.Lease.CoordinationEpoch
	if err := store.ActivateLaunch(ctx, launch.Attempt.ID, launch.Lease.ID, epoch, launch.AuthorizationToken, 100, "launcher:100"); err != nil {
		t.Fatalf("repeated activation: %v", err)
	}
	if err := store.ActivateLaunch(ctx, launch.Attempt.ID, launch.Lease.ID, epoch, launch.AuthorizationToken, 101, "launcher:101"); err == nil {
		t.Fatal("a second process activated the attempt")
	}
	var processes int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM attempt_processes WHERE attempt_id=$1`, launch.Attempt.ID).Scan(&processes); err != nil || processes != 1 {
		t.Fatalf("launcher registrations: %d %v", processes, err)
	}
}

// The leases an executor holds that never reached a running attempt are
// listed for it (for a lost answer); a running one, or another executor's, is
// not.
func TestUnlaunchedLeasesListReservationsOnly(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	ctx := context.Background()
	if _, _, err := store.SubmitExecution(ctx, schedulerExecutionSpec("unlaunched")); err != nil {
		t.Fatal(err)
	}
	reservation, err := store.ReserveNext(ctx, "executor")
	if err != nil || reservation == nil {
		t.Fatalf("reserve: %+v %v", reservation, err)
	}
	leases, err := store.ListUnlaunchedLeases(ctx, "executor", 0)
	if err != nil || len(leases) != 1 || leases[0].Lease.ID != reservation.Lease.ID || leases[0].AttemptID != "" || len(leases[0].Resources) != 1 {
		t.Fatalf("reserved: %+v %v", leases, err)
	}
	if leases, err = store.ListUnlaunchedLeases(ctx, "executor", 1<<40); err != nil || len(leases) != 0 {
		t.Fatalf("a young lease listed: %+v %v", leases, err)
	}
	if leases, err = store.ListUnlaunchedLeases(ctx, "other", 0); err != nil || len(leases) != 0 {
		t.Fatalf("another executor's: %+v %v", leases, err)
	}
	if err = store.MarkLeasePrepared(ctx, reservation.Lease.ID, reservation.Lease.CoordinationEpoch); err != nil {
		t.Fatal(err)
	}
	launch, err := store.AuthorizeLaunch(ctx, reservation)
	if err != nil {
		t.Fatal(err)
	}
	if leases, err = store.ListUnlaunchedLeases(ctx, "executor", 0); err != nil || len(leases) != 1 || leases[0].AttemptID != launch.Attempt.ID {
		t.Fatalf("authorized: %+v %v", leases, err)
	}
	if err = store.ActivateLaunch(ctx, launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch, launch.AuthorizationToken, 100, "launcher:100"); err != nil {
		t.Fatal(err)
	}
	if leases, err = store.ListUnlaunchedLeases(ctx, "executor", 0); err != nil || len(leases) != 0 {
		t.Fatalf("running: %+v %v", leases, err)
	}
}
