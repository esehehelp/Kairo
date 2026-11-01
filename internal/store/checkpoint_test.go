package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
)

// A checkpoint command leaves the attempt running and its lease active, its
// checkpoint is not the continuation, one is in flight at a time, and it does
// not stand in for a suspend.
func TestCheckpointCommandLeavesTheAttemptRunning(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	ctx := context.Background()
	if _, err := store.ApplyProject(ctx, projectSpec(t, orchestrationManifest())); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	_, err := store.CheckpointTask(ctx, "orchestrated", "prepare", "tester")
	taskConflictError(t, err, "has no running attempt to checkpoint")
	prepare := launchNext(t, store)
	_, err = store.CheckpointTask(ctx, "orchestrated", "prepare", "tester")
	taskConflictError(t, err, "execution "+prepare.Execution.ID+" of orchestrated/data/prepare is not checkpointable")
	finishLaunch(t, store, prepare, 0)
	projectTasks(t, store, "orchestrated")
	train := launchNext(t, store)
	attempt, lease, epoch := train.Attempt.ID, train.Lease.ID, train.Lease.CoordinationEpoch

	first, err := store.CheckpointTask(ctx, "orchestrated", "train", "tester")
	if err != nil || len(first.Commands) != 1 {
		t.Fatalf("checkpoint: %+v %v", first, err)
	}
	again, err := store.CheckpointTask(ctx, "orchestrated", "train", "tester")
	if err != nil || len(again.Commands) != 1 || again.Commands[0] != first.Commands[0] {
		t.Fatalf("a second request while one is in flight: %+v %v", again, err)
	}
	commandID := first.Commands[0]
	commands, err := store.PollCommands(ctx, attempt, lease, epoch)
	if err != nil || len(commands) != 1 || commands[0].Kind != "checkpoint" || commands[0].Origin != "operator_checkpoint" {
		t.Fatalf("poll: %+v %v", commands, err)
	}
	running := func(when string) {
		t.Helper()
		var attemptState, leaseState string
		var continuation sql.NullString
		if err := store.db.QueryRow(`SELECT a.state,a.continuation_ref,l.state FROM attempts a JOIN leases l ON l.attempt_id=a.id WHERE a.id=$1`, attempt).Scan(&attemptState, &continuation, &leaseState); err != nil {
			t.Fatal(err)
		}
		if attemptState != "running" || leaseState != "active" || continuation.Valid {
			t.Fatalf("%s: attempt %s lease %s continuation %v", when, attemptState, leaseState, continuation)
		}
	}
	for _, phase := range []string{"accepted", "checkpointing"} {
		if err = store.AckCommand(ctx, attempt, lease, epoch, commandID, phase, nil); err != nil {
			t.Fatal(err)
		}
		running(phase)
	}
	if err = store.AckCommand(ctx, attempt, lease, epoch, commandID, "checkpointed", json.RawMessage(`{}`)); err == nil {
		t.Fatal("a checkpointed acknowledgement without continuation_ref was accepted")
	}
	if err = store.AckCommand(ctx, attempt, lease, epoch, commandID, "checkpointed", json.RawMessage(`{"continuation_ref":"checkpoint://step-200"}`)); err != nil {
		t.Fatal(err)
	}
	running("checkpointed")
	var payloadJSON string
	if err = store.db.QueryRow(`SELECT payload_json FROM coordination_events WHERE event_type='checkpoint_saved' AND aggregate_id=$1`, attempt).Scan(&payloadJSON); err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err = json.Unmarshal([]byte(payloadJSON), &payload); err != nil || payload["checkpoint_ref"] != "checkpoint://step-200" || payload["command_id"] != commandID {
		t.Fatalf("checkpoint_saved: %s %v", payloadJSON, err)
	}
	var published int
	if err = store.db.QueryRow(`SELECT count(*) FROM coordination_events WHERE event_type='checkpoint_published' AND aggregate_id=$1`, attempt).Scan(&published); err != nil || published != 0 {
		t.Fatalf("a checkpoint was published as the continuation: %d %v", published, err)
	}
	command, err := store.GetCommand(ctx, commandID)
	if err != nil || command.State != "checkpointed" || command.Kind != "checkpoint" {
		t.Fatalf("command: %+v %v", command, err)
	}
	if commands, err = store.PollCommands(ctx, attempt, lease, epoch); err != nil || len(commands) != 0 {
		t.Fatalf("a checkpointed checkpoint was redelivered: %+v %v", commands, err)
	}

	// The next request is a new command, and a suspend is still its own.
	next, err := store.CheckpointTask(ctx, "orchestrated", "train", "tester")
	if err != nil || len(next.Commands) != 1 || next.Commands[0] == commandID {
		t.Fatalf("next checkpoint: %+v %v", next, err)
	}
	suspendID, err := store.EnqueueSuspend(ctx, attempt, "scope_pause", "test")
	if err != nil || suspendID == commandID || suspendID == next.Commands[0] {
		t.Fatalf("suspend: %s %v", suspendID, err)
	}
	if id, origin, _, err := latestAttemptCommand(ctx, mustTx(t, store), attempt); err != nil || id != suspendID || origin != "scope_pause" {
		t.Fatalf("latest suspend: %s %s %v", id, origin, err)
	}
}

func mustTx(t *testing.T, store *Store) *sql.Tx {
	t.Helper()
	tx, err := store.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	return tx
}
