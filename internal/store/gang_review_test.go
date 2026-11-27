package store

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// A member that loses its lease after another member was launched, before
// its own launch, ends as a launch that failed: the launched member is
// stopped and the gang is restarted, not left waiting for a member that can
// never be placed alone.
func TestAMemberLostBeforeItsLaunchRestartsTheGang(t *testing.T) {
	store := openCoordinationStore(t)
	setupGangInventory(t, store)
	gangProject(t, store, false)
	ctx := context.Background()
	rb, err := store.ReserveNext(ctx, "exec-b")
	if err != nil || rb == nil || rb.Gang == nil {
		t.Fatalf("exec-b: %+v %v", rb, err)
	}
	ra, err := store.ReserveNext(ctx, "exec-a")
	if err != nil || ra == nil || ra.Gang == nil {
		t.Fatalf("exec-a: %+v %v", ra, err)
	}
	for _, r := range []*Reservation{ra, rb} {
		if err := store.MarkLeasePrepared(ctx, r.Lease.ID, r.Lease.CoordinationEpoch); err != nil {
			t.Fatal(err)
		}
	}
	leader, err := store.AuthorizeLaunch(ctx, ra)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ActivateLaunch(ctx, leader.Attempt.ID, leader.Lease.ID, leader.Lease.CoordinationEpoch, leader.AuthorizationToken, 200, "launcher"); err != nil {
		t.Fatal(err)
	}
	// exec-b's agent comes back without rank 1's reservation
	if _, err := store.MarkExecutorUnknown(ctx, "exec-b"); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcileGangs(ctx); err != nil {
		t.Fatal(err)
	}
	if terms, _ := store.ListQuarantineTerminations(ctx, "exec-a"); len(terms) != 1 {
		t.Fatalf("the launched member not stopped: %+v", terms)
	}
	var cause string
	if err := store.db.QueryRow(`SELECT COALESCE(terminal_cause,'') FROM execution_requests WHERE id=$1`, rb.Execution.ID).Scan(&cause); err != nil || !strings.HasPrefix(cause, "launch_aborted") {
		t.Fatalf("the unlaunched member: %q %v", cause, err)
	}
	if err := store.MarkQuarantineTerminated(ctx, leader.Attempt.ID); err != nil {
		t.Fatal(err)
	}
	finishGangRank(t, store, gangRankLaunch{"exec-a", leader}, 137)
	// once the lost member's lease is released (never by time alone)
	if err := store.ReconcileResource(ctx, "gpu-b", true); err != nil {
		t.Fatal(err)
	}
	if task := gangTaskState(t, store); len(task.Executions) != 2 || task.Executions[1].Reason != "gang_restart" {
		t.Fatalf("not restarted: %+v", task)
	}
}

// A member that took a suspend but published nothing is stopped when a peer
// is lost (the round can no longer end); one that published is not.
func TestAMemberWaitingOnASuspendIsStoppedWhenAPeerIsLost(t *testing.T) {
	store := openCoordinationStore(t)
	setupGangInventory(t, store)
	gangProject(t, store, true)
	ctx := context.Background()
	rank0, rank1 := launchGang(t, store)
	if _, err := store.EnqueueSuspend(ctx, rank1.launch.Attempt.ID, "scope_pause", "paused"); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcileGangs(ctx); err != nil {
		t.Fatal(err)
	}
	l := rank0.launch
	cmd := liveCommand(t, store, rank0, "suspend")
	for _, phase := range []string{"accepted", "checkpointing"} {
		if err := store.AckCommand(ctx, l.Attempt.ID, l.Lease.ID, l.Lease.CoordinationEpoch, cmd, phase, nil); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.MarkExecutorUnknown(ctx, "exec-b"); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcileGangs(ctx); err != nil {
		t.Fatal(err)
	}
	if terms, _ := store.ListQuarantineTerminations(ctx, "exec-a"); len(terms) != 1 || terms[0].AttemptID != l.Attempt.ID {
		t.Fatalf("the member waiting on the suspend not stopped: %+v", terms)
	}
}

// Withdrawing a member of a gang that has not started withdraws every
// member; a gang is not rerun from a checkpoint.
func TestAGangIsWithdrawnWholeAndNotRerunFromACheckpoint(t *testing.T) {
	store := openCoordinationStore(t)
	setupGangInventory(t, store)
	gangProject(t, store, true)
	ctx := context.Background()
	var leader string
	if err := store.db.QueryRow(`SELECT leader_execution_id FROM execution_gangs`).Scan(&leader); err != nil {
		t.Fatal(err)
	}
	if err := store.WithdrawExecution(ctx, leader); err != nil {
		t.Fatal(err)
	}
	var waiting int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM execution_requests WHERE state='waiting'`).Scan(&waiting); err != nil || waiting != 0 {
		t.Fatalf("members still waiting: %d %v", waiting, err)
	}
	if err := store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	_, err := store.RerunTaskWith(ctx, "gangs", "ddp", "tester", "rerun-1", RerunOptions{FromCheckpoint: "latest"})
	var conflict taskConflict
	if !errors.As(err, &conflict) || !strings.Contains(err.Error(), "is a gang") {
		t.Fatalf("a gang rerun from a checkpoint: %v", err)
	}
}
