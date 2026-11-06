package store

import (
	"context"
	"errors"
	"testing"
)

// A poll with nothing to deliver is answered from a read; a command its worker never
// acknowledges is still counted on every delivery, but its delivery history
// stops growing a row per poll.
func TestPollCommandsAnswersEmptyPollsAndBoundsDeliveryHistory(t *testing.T) {
	store, _, launch := startCooperativeExecution(t, "poll-bounded")
	ctx := context.Background()
	attempt, lease, epoch := launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch
	for i := 0; i < 3; i++ {
		commands, err := store.PollCommands(ctx, attempt, lease, epoch)
		if err != nil || len(commands) != 0 {
			t.Fatalf("empty poll: %+v %v", commands, err)
		}
	}
	if _, err := store.PollCommands(ctx, attempt, lease, epoch+1); err == nil {
		t.Fatal("a poll at another epoch was answered")
	}
	commandID, err := store.EnqueueSuspend(ctx, attempt, "scope_pause", "test")
	if err != nil {
		t.Fatal(err)
	}
	const polls = deliveryHistory + 15
	for i := 1; i <= polls; i++ {
		commands, err := store.PollCommands(ctx, attempt, lease, epoch)
		if err != nil || len(commands) != 1 || commands[0].ID != commandID || commands[0].DeliveryCount != i {
			t.Fatalf("poll %d: %+v %v", i, commands, err)
		}
	}
	var deliveries int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM command_deliveries WHERE command_id=$1`, commandID).Scan(&deliveries); err != nil {
		t.Fatal(err)
	}
	if deliveries != deliveryHistory {
		t.Fatalf("delivery history has %d rows after %d polls, want %d", deliveries, polls, deliveryHistory)
	}
}

// A worker that calls between its launch and its activation (the executor
// has started it but not yet recorded it running) is answered, not fenced:
// no command, its heartbeat and its process recorded. Once its attempt has
// ended it is fenced.
func TestAWorkerCallingBeforeItsActivationIsNotFenced(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	ctx := context.Background()
	if _, _, err := store.SubmitExecution(ctx, schedulerExecutionSpec("early")); err != nil {
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
	attempt, lease, epoch := launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch
	if commands, err := store.PollCommands(ctx, attempt, lease, epoch); err != nil || len(commands) != 0 {
		t.Fatalf("poll before activation: %+v %v", commands, err)
	}
	if err := store.Heartbeat(ctx, attempt, lease, epoch, []byte(`{"phase":"loading"}`)); err != nil {
		t.Fatalf("heartbeat before activation: %v", err)
	}
	if err := store.RegisterProcess(ctx, attempt, lease, epoch, 0, 101, "proc:101:starttime:7"); err != nil {
		t.Fatalf("process before activation: %v", err)
	}
	if _, err := store.PollCommands(ctx, attempt, lease, epoch+1); err == nil {
		t.Fatal("a poll at another epoch was answered")
	}
	if err = store.ActivateLaunch(ctx, attempt, lease, epoch, launch.AuthorizationToken, 100, "launcher:100"); err != nil {
		t.Fatal(err)
	}
	if commands, err := store.PollCommands(ctx, attempt, lease, epoch); err != nil || len(commands) != 0 {
		t.Fatalf("poll once running: %+v %v", commands, err)
	}
	if err = store.RecordTerminal(ctx, attempt, lease, epoch, 0, ""); err != nil {
		t.Fatal(err)
	}
	if err = store.MarkAttemptProcessExited(ctx, attempt, "worker", 0, "proc:101:starttime:7"); err != nil {
		t.Fatal(err)
	}
	refreshSchedulerGPU(t, store)
	if err = store.FinalizeQuiescence(ctx, attempt, lease, epoch); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PollCommands(ctx, attempt, lease, epoch); !errors.Is(err, ErrStaleEpoch) {
		t.Fatalf("poll after the end: %v", err)
	}
	if err := store.Heartbeat(ctx, attempt, lease, epoch, nil); !errors.Is(err, ErrStaleEpoch) {
		t.Fatalf("heartbeat after the end: %v", err)
	}
}
