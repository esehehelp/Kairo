package store

import (
	"context"
	"encoding/json"
	"testing"
)

func startCooperativeExecution(t *testing.T, requestID string) (*Store, ExecutionRequest, *Launch) {
	t.Helper()
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	ctx := context.Background()
	execution, _, err := store.SubmitExecution(ctx, schedulerExecutionSpec(requestID))
	if err != nil {
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
	if err = store.ActivateLaunch(ctx, launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch, launch.AuthorizationToken, 100, "launcher:100"); err != nil {
		t.Fatal(err)
	}
	return store, execution, launch
}

func TestSuspendCheckpointExitAndQuiescenceAreDistinctFacts(t *testing.T) {
	store, execution, launch := startCooperativeExecution(t, "suspend-order")
	ctx := context.Background()
	epoch := launch.Lease.CoordinationEpoch
	if err := store.RegisterProcess(ctx, launch.Attempt.ID, launch.Lease.ID, epoch, 0, 200, "worker:200"); err != nil {
		t.Fatal(err)
	}
	if err := store.RegisterProcess(ctx, launch.Attempt.ID, launch.Lease.ID, epoch, 0, 201, "worker:201"); err == nil {
		t.Fatal("live worker rank identity was overwritten")
	}
	commandID, err := store.EnqueueSuspend(ctx, launch.Attempt.ID, "scope_pause", "test")
	if err != nil {
		t.Fatal(err)
	}
	for wantDelivery := 1; wantDelivery <= 2; wantDelivery++ {
		commands, pollErr := store.PollCommands(ctx, launch.Attempt.ID, launch.Lease.ID, epoch)
		if pollErr != nil || len(commands) != 1 || commands[0].ID != commandID || commands[0].DeliveryCount != wantDelivery {
			t.Fatalf("poll %d: commands=%+v err=%v", wantDelivery, commands, pollErr)
		}
	}
	for _, phase := range []string{"accepted", "checkpointing"} {
		if err = store.AckCommand(ctx, launch.Attempt.ID, launch.Lease.ID, epoch, commandID, phase, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err = store.AckCommand(ctx, launch.Attempt.ID, launch.Lease.ID, epoch, commandID, "checkpointed", json.RawMessage(`{"continuation_ref":"checkpoint://first"}`)); err != nil {
		t.Fatal(err)
	}
	if commands, pollErr := store.PollCommands(ctx, launch.Attempt.ID, launch.Lease.ID, epoch); pollErr != nil || len(commands) != 0 {
		t.Fatalf("checkpointed command was redelivered: %+v %v", commands, pollErr)
	}
	// A duplicate terminal ACK is recorded for audit but cannot replace the
	// already-published opaque continuation.
	if err = store.AckCommand(ctx, launch.Attempt.ID, launch.Lease.ID, epoch, commandID, "checkpointed", json.RawMessage(`{"continuation_ref":"checkpoint://different"}`)); err != nil {
		t.Fatal(err)
	}
	var attemptState, leaseState, continuation string
	if err = store.db.QueryRow(`SELECT a.state,l.state,a.continuation_ref FROM attempts a JOIN leases l ON l.attempt_id=a.id WHERE a.id=?`, launch.Attempt.ID).Scan(&attemptState, &leaseState, &continuation); err != nil {
		t.Fatal(err)
	}
	if attemptState != "quiescing" || leaseState != "releasing" || continuation != "checkpoint://first" {
		t.Fatalf("checkpoint facts collapsed or changed: attempt=%s lease=%s continuation=%s", attemptState, leaseState, continuation)
	}

	if err = store.RecordTerminal(ctx, launch.Attempt.ID, launch.Lease.ID, epoch, 75, ""); err != nil {
		t.Fatal(err)
	}
	terminal, err := store.GetExecution(ctx, execution.ID)
	if err != nil || terminal.State != "terminal" || terminal.TerminalCause == nil || *terminal.TerminalCause != "process_exit" {
		t.Fatalf("raw terminal fact missing: %+v %v", terminal, err)
	}
	if err = store.FinalizeQuiescence(ctx, launch.Attempt.ID, launch.Lease.ID, epoch); err == nil {
		t.Fatal("launcher exit falsely implied distributed quiescence")
	}
	if err = store.MarkAttemptProcessExited(ctx, launch.Attempt.ID, "worker", 0, "worker:200"); err != nil {
		t.Fatal(err)
	}
	if err = store.FinalizeQuiescence(ctx, launch.Attempt.ID, launch.Lease.ID, epoch); err == nil {
		t.Fatal("observation from before process exit proved quiescence")
	}
	refreshSchedulerGPU(t, store)
	if err = store.FinalizeQuiescence(ctx, launch.Attempt.ID, launch.Lease.ID, epoch); err != nil {
		t.Fatal(err)
	}
	if err = store.db.QueryRow(`SELECT a.state,l.state FROM attempts a JOIN leases l ON l.attempt_id=a.id WHERE a.id=?`, launch.Attempt.ID).Scan(&attemptState, &leaseState); err != nil {
		t.Fatal(err)
	}
	if attemptState != "quiesced" || leaseState != "released" {
		t.Fatalf("quiescence did not release lease: attempt=%s lease=%s", attemptState, leaseState)
	}
}

func TestFinalizeQuiescenceHonorsUnattributedActivityPolicy(t *testing.T) {
	for _, tc := range []struct {
		name      string
		policy    string
		wantError bool
	}{
		{name: "allowed", policy: "allow"},
		{name: "waiting", policy: "wait", wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := openCoordinationStore(t)
			setupSchedulerInventory(t, store)
			ctx := context.Background()
			spec := schedulerExecutionSpec("release-with-unattributed-" + tc.name)
			spec.Exclusive[0].OnUnattributedActivity = tc.policy
			_, _, err := store.SubmitExecution(ctx, spec)
			if err != nil {
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
			epoch := launch.Lease.CoordinationEpoch
			if err = store.ActivateLaunch(ctx, launch.Attempt.ID, launch.Lease.ID, epoch, launch.AuthorizationToken, 100, "launcher:100"); err != nil {
				t.Fatal(err)
			}
			if err = store.RecordTerminal(ctx, launch.Attempt.ID, launch.Lease.ID, epoch, 1, ""); err != nil {
				t.Fatal(err)
			}
			refreshSchedulerGPU(t, store)
			if err = store.ObserveClaim(ctx, ExternalClaim{ID: "desktop", ResourceID: "gpu-0", ClaimKind: "unattributed_activity"}); err != nil {
				t.Fatal(err)
			}
			err = store.FinalizeQuiescence(ctx, launch.Attempt.ID, launch.Lease.ID, epoch)
			if tc.wantError && err == nil {
				t.Fatal("forbidden unattributed activity released lease")
			}
			if !tc.wantError && err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestExitCodesRemainUnclassifiedAndNeverResubmit(t *testing.T) {
	for _, exitCode := range []int{0, 1, 75} {
		store, execution, launch := startCooperativeExecution(t, "exit-code-"+string(rune('a'+exitCode%26)))
		if err := store.RecordTerminal(context.Background(), launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch, exitCode, ""); err != nil {
			t.Fatal(err)
		}
		value, err := store.GetExecution(context.Background(), execution.ID)
		if err != nil || value.State != "terminal" || value.TerminalCause == nil || *value.TerminalCause != "process_exit" {
			t.Fatalf("exit %d was classified: %+v err=%v", exitCode, value, err)
		}
		var requests int
		if err = store.db.QueryRow(`SELECT COUNT(*) FROM execution_requests`).Scan(&requests); err != nil || requests != 1 {
			t.Fatalf("exit %d caused resubmission: count=%d err=%v", exitCode, requests, err)
		}
	}
}

func TestDelayedCheckpointAckCannotRegressExitedAttempt(t *testing.T) {
	store, _, launch := startCooperativeExecution(t, "delayed-checkpoint")
	ctx := context.Background()
	epoch := launch.Lease.CoordinationEpoch
	commandID, err := store.EnqueueSuspend(ctx, launch.Attempt.ID, "scope_pause", "test")
	if err != nil {
		t.Fatal(err)
	}
	if err = store.RecordTerminal(ctx, launch.Attempt.ID, launch.Lease.ID, epoch, 75, ""); err != nil {
		t.Fatal(err)
	}
	if err = store.AckCommand(ctx, launch.Attempt.ID, launch.Lease.ID, epoch, commandID, "checkpointed", json.RawMessage(`{"continuation_ref":"checkpoint://late"}`)); err != nil {
		t.Fatal(err)
	}
	var attemptState, leaseState, continuation string
	if err = store.db.QueryRow(`SELECT a.state,l.state,a.continuation_ref FROM attempts a JOIN leases l ON l.attempt_id=a.id WHERE a.id=?`, launch.Attempt.ID).Scan(&attemptState, &leaseState, &continuation); err != nil {
		t.Fatal(err)
	}
	if attemptState != "exited" || leaseState != "releasing" || continuation != "checkpoint://late" {
		t.Fatalf("late checkpoint regressed state: attempt=%s lease=%s continuation=%s", attemptState, leaseState, continuation)
	}
	refreshSchedulerGPU(t, store)
	if err = store.FinalizeQuiescence(ctx, launch.Attempt.ID, launch.Lease.ID, epoch); err != nil {
		t.Fatal(err)
	}
}

func TestDelayedRejectedAckCannotReactivateExitedLease(t *testing.T) {
	store, _, launch := startCooperativeExecution(t, "delayed-rejected")
	ctx := context.Background()
	epoch := launch.Lease.CoordinationEpoch
	commandID, err := store.EnqueueSuspend(ctx, launch.Attempt.ID, "scope_pause", "test")
	if err != nil {
		t.Fatal(err)
	}
	if err = store.AckCommand(ctx, launch.Attempt.ID, launch.Lease.ID, epoch, commandID, "accepted", nil); err != nil {
		t.Fatal(err)
	}
	if err = store.RecordTerminal(ctx, launch.Attempt.ID, launch.Lease.ID, epoch, 1, ""); err != nil {
		t.Fatal(err)
	}
	if err = store.AckCommand(ctx, launch.Attempt.ID, launch.Lease.ID, epoch, commandID, "rejected", json.RawMessage(`{"reason":"late"}`)); err != nil {
		t.Fatal(err)
	}
	var attemptState, leaseState string
	if err = store.db.QueryRow(`SELECT a.state,l.state FROM attempts a JOIN leases l ON l.attempt_id=a.id WHERE a.id=?`, launch.Attempt.ID).Scan(&attemptState, &leaseState); err != nil {
		t.Fatal(err)
	}
	if attemptState != "exited" || leaseState != "releasing" {
		t.Fatalf("late rejection regressed state: attempt=%s lease=%s", attemptState, leaseState)
	}
	refreshSchedulerGPU(t, store)
	if err = store.FinalizeQuiescence(ctx, launch.Attempt.ID, launch.Lease.ID, epoch); err != nil {
		t.Fatal(err)
	}
}
