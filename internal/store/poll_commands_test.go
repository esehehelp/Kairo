package store

import (
	"context"
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
