package store

import (
	"context"
	"errors"
	"testing"

	"kairo/internal/machine"
)

// authorizeScratchTest submits an execution of its own task and takes it to
// an authorized launch on "executor" (the one GPU must be free).
func authorizeScratchTest(t *testing.T, store *Store, name string) *Launch {
	t.Helper()
	ctx := context.Background()
	spec := schedulerExecutionSpec("scratch-" + name)
	spec.Scope.Task = "scratch-" + name
	if _, _, err := store.SubmitExecution(ctx, spec); err != nil {
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
	return launch
}

func scratchOf(launch *Launch, storeID string) AttemptScratch {
	return AttemptScratch{AttemptID: launch.Attempt.ID, LeaseID: launch.Lease.ID, Epoch: launch.Lease.CoordinationEpoch, StoreID: storeID, ExecutorID: "executor",
		Root: "D:/kairo", Path: "D:/kairo/scratch/" + launch.Attempt.ID}
}

// startWithScratch authorizes a launch, records its scratch on d-scratch
// and activates it.
func startWithScratch(t *testing.T, store *Store, name string) *Launch {
	t.Helper()
	ctx := context.Background()
	launch := authorizeScratchTest(t, store, name)
	if err := store.RecordAttemptScratch(ctx, scratchOf(launch, "d-scratch")); err != nil {
		t.Fatal(err)
	}
	if err := store.ActivateLaunch(ctx, launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch, launch.AuthorizationToken, 100, "launcher:100"); err != nil {
		t.Fatal(err)
	}
	return launch
}

func scratchState(t *testing.T, store *Store, attemptID string) (state string, bytes *int64, reason *string) {
	t.Helper()
	if err := store.db.QueryRow(`SELECT state,bytes,reason FROM attempt_scratch WHERE attempt_id=$1`, attemptID).Scan(&state, &bytes, &reason); err != nil {
		t.Fatal(err)
	}
	return state, bytes, reason
}

// A scratch is recorded only for an attempt about to launch, on a volatile
// store of its node; recording it again is no error.
func TestRecordAttemptScratchChecksTheLaunch(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	addTestDisk(t, store, "node", "D:")
	declareTestStore(t, store,
		machine.Store{ID: "d-scratch", Class: machine.StoreVolatile, Node: "node", Root: "D:/kairo", Filesystem: "D:"},
		machine.Store{ID: "d-durable", Class: machine.StoreDurable, Node: "node", Root: "D:/durable", Filesystem: "D:"},
	)
	ctx := context.Background()
	launch := authorizeScratchTest(t, store, "checks")
	scratch := scratchOf(launch, "d-scratch")

	durable := scratch
	durable.StoreID = "d-durable"
	if err := store.RecordAttemptScratch(ctx, durable); err == nil {
		t.Fatal("scratch recorded on a durable store")
	}
	undeclared := scratch
	undeclared.StoreID = "nowhere"
	if err := store.RecordAttemptScratch(ctx, undeclared); !errors.Is(err, ErrNotFound) {
		t.Fatalf("an undeclared store: %v", err)
	}
	stale := scratch
	stale.Epoch++
	if err := store.RecordAttemptScratch(ctx, stale); !errors.Is(err, ErrStaleEpoch) {
		t.Fatalf("a stale epoch: %v", err)
	}
	other := scratch
	other.ExecutorID = "another"
	if err := store.RecordAttemptScratch(ctx, other); err == nil {
		t.Fatal("scratch recorded by an executor the attempt is not of")
	}
	if err := store.RecordAttemptScratch(ctx, scratch); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordAttemptScratch(ctx, scratch); err != nil {
		t.Fatalf("recorded again: %v", err)
	}
	moved := scratch
	moved.Path = "D:/kairo/scratch/elsewhere"
	if err := store.RecordAttemptScratch(ctx, moved); err == nil {
		t.Fatal("a second scratch for the attempt was recorded")
	}
	if state, _, _ := scratchState(t, store, launch.Attempt.ID); state != "active" {
		t.Fatalf("state %s", state)
	}

	// once running, no scratch is made for it any more
	if err := store.ActivateLaunch(ctx, launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch, launch.AuthorizationToken, 100, "launcher:100"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.testExec(`DELETE FROM attempt_scratch WHERE attempt_id=$1`, launch.Attempt.ID); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordAttemptScratch(ctx, scratch); !errors.Is(err, ErrStaleEpoch) {
		t.Fatalf("scratch recorded for a running attempt: %v", err)
	}
}

// An ended attempt's scratch is settled by its outcome: deleted after exit 0
// without a signal or a suspend that published its checkpoint, kept after a
// failure (exit code, signal) or a loss. A kept one that held only its
// marker may be deleted, unless its attempt was lost.
func TestScratchIsSettledByTheAttemptsOutcome(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	addTestDisk(t, store, "node", "D:")
	declareTestStore(t, store, machine.Store{ID: "d-scratch", Class: machine.StoreVolatile, Node: "node", Root: "D:/kairo", Filesystem: "D:"})
	ctx := context.Background()

	settle := func(want map[string]ScratchSettlement) {
		t.Helper()
		got, err := store.ScratchToSettle(ctx, "executor")
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != len(want) {
			t.Fatalf("to settle: %+v, want %+v", got, want)
		}
		for _, k := range got {
			w, ok := want[k.AttemptID]
			if !ok || k.Delete != w.Delete || k.Lost != w.Lost || k.Reason != w.Reason || k.StoreID != "d-scratch" || k.Root != "D:/kairo" || k.Path != "D:/kairo/scratch/"+k.AttemptID {
				t.Fatalf("settlement %+v, want %+v", k, w)
			}
		}
	}

	// exit 0: deleted; while it runs (or has exited but not quiesced) there
	// is nothing to settle
	ok := startWithScratch(t, store, "ok")
	settle(nil)
	if err := store.RecordTerminal(ctx, ok.Attempt.ID, ok.Lease.ID, ok.Lease.CoordinationEpoch, 0, ""); err != nil {
		t.Fatal(err)
	}
	settle(nil)
	refreshSchedulerGPU(t, store)
	if err := store.FinalizeQuiescence(ctx, ok.Attempt.ID, ok.Lease.ID, ok.Lease.CoordinationEpoch); err != nil {
		t.Fatal(err)
	}
	settle(map[string]ScratchSettlement{ok.Attempt.ID: {Delete: true, Reason: "exit 0"}})
	if err := store.SettleScratch(ctx, ScratchSettled{AttemptID: ok.Attempt.ID, State: "deleted"}); err != nil {
		t.Fatal(err)
	}
	if err := store.SettleScratch(ctx, ScratchSettled{AttemptID: ok.Attempt.ID, State: "deleted"}); err != nil {
		t.Fatalf("settled again: %v", err)
	}
	if state, _, reason := scratchState(t, store, ok.Attempt.ID); state != "deleted" || reason == nil || *reason != "exit 0" {
		t.Fatalf("ok: %s %v", state, reason)
	}
	settle(nil)

	// exit 1: kept; it cannot be recorded deleted (unless empty)
	failed := startWithScratch(t, store, "failed")
	finishLaunch(t, store, failed, 1)
	settle(map[string]ScratchSettlement{failed.Attempt.ID: {Reason: "exit 1"}})
	if err := store.SettleScratch(ctx, ScratchSettled{AttemptID: failed.Attempt.ID, State: "deleted"}); !errors.Is(err, ErrScratchKept) {
		t.Fatalf("a failed attempt's scratch deleted: %v", err)
	}
	size := int64(4096)
	if err := store.SettleScratch(ctx, ScratchSettled{AttemptID: failed.Attempt.ID, State: "kept", Bytes: &size}); err != nil {
		t.Fatal(err)
	}
	if state, bytes, reason := scratchState(t, store, failed.Attempt.ID); state != "kept" || bytes == nil || *bytes != 4096 || reason == nil || *reason != "exit 1" {
		t.Fatalf("failed: %s %v %v", state, bytes, reason)
	}
	if err := store.SettleScratch(ctx, ScratchSettled{AttemptID: failed.Attempt.ID, State: "deleted", Empty: true}); err == nil {
		t.Fatal("a kept scratch was settled again")
	}

	// killed by a signal: kept; empty, it may go
	signalled := startWithScratch(t, store, "signalled")
	if err := store.RecordTerminal(ctx, signalled.Attempt.ID, signalled.Lease.ID, signalled.Lease.CoordinationEpoch, -1, "SIGKILL"); err != nil {
		t.Fatal(err)
	}
	refreshSchedulerGPU(t, store)
	if err := store.FinalizeQuiescence(ctx, signalled.Attempt.ID, signalled.Lease.ID, signalled.Lease.CoordinationEpoch); err != nil {
		t.Fatal(err)
	}
	settle(map[string]ScratchSettlement{signalled.Attempt.ID: {Reason: "signal SIGKILL"}})
	if err := store.SettleScratch(ctx, ScratchSettled{AttemptID: signalled.Attempt.ID, State: "deleted", Empty: true}); err != nil {
		t.Fatal(err)
	}
	if state, _, reason := scratchState(t, store, signalled.Attempt.ID); state != "deleted" || reason == nil || *reason != "empty; signal SIGKILL" {
		t.Fatalf("signalled: %s %v", state, reason)
	}

	// suspended with a published checkpoint: deleted, whatever its exit
	suspended := startWithScratch(t, store, "suspended")
	finishLaunch(t, store, suspended, 143)
	if _, err := store.testExec(`UPDATE attempts SET continuation_ref='checkpoint://step-200' WHERE id=$1`, suspended.Attempt.ID); err != nil {
		t.Fatal(err)
	}
	settle(map[string]ScratchSettlement{suspended.Attempt.ID: {Delete: true, Reason: "suspended with a published checkpoint"}})
	if err := store.SettleScratch(ctx, ScratchSettled{AttemptID: suspended.Attempt.ID, State: "deleted"}); err != nil {
		t.Fatal(err)
	}

	// an active scratch's bytes are counted as active, a kept one's as kept
	active := startWithScratch(t, store, "active")
	total, free := int64(100_000), int64(50_000)
	if err := store.RecordStoreObservation(ctx, StoreObservation{StoreID: "d-scratch", ExecutorID: "executor", Usable: true, TotalBytes: &total, FreeBytes: &free,
		Scratch: map[string]int64{active.Attempt.ID: 300, failed.Attempt.ID: 5000}}); err != nil {
		t.Fatal(err)
	}
	if status, err := store.ListStoreStatus(ctx); err != nil || status[0].ScratchActiveBytes != 300 || status[0].ScratchKeptBytes != 5000 || *status[0].ExternalBytes != 50_000-5300 {
		t.Fatalf("classes: %+v %v", status, err)
	}
	finishLaunch(t, store, active, 0)
	if err := store.SettleScratch(ctx, ScratchSettled{AttemptID: active.Attempt.ID, State: "deleted"}); err != nil {
		t.Fatal(err)
	}

	// lost (its executor's identity went): kept, even empty
	lost := startWithScratch(t, store, "lost")
	if n, err := store.MarkExecutorUnknown(ctx, "executor"); err != nil || n != 1 {
		t.Fatalf("lost: %d %v", n, err)
	}
	settle(map[string]ScratchSettlement{lost.Attempt.ID: {Lost: true, Reason: "attempt lost"}})
	if err := store.SettleScratch(ctx, ScratchSettled{AttemptID: lost.Attempt.ID, State: "deleted", Empty: true}); !errors.Is(err, ErrScratchKept) {
		t.Fatalf("a lost attempt's scratch deleted: %v", err)
	}
	// still lost after it is reconciled to quiesced
	if _, err := store.testExec(`UPDATE attempts SET state='quiesced' WHERE id=$1`, lost.Attempt.ID); err != nil {
		t.Fatal(err)
	}
	settle(map[string]ScratchSettlement{lost.Attempt.ID: {Lost: true, Reason: "attempt lost"}})
	if err := store.SettleScratch(ctx, ScratchSettled{AttemptID: lost.Attempt.ID, State: "kept", Reason: "kept: lost"}); err != nil {
		t.Fatal(err)
	}
	settle(nil)

	// the kept ones are listed on their store, with their task
	status, err := store.ListStoreStatus(ctx)
	if err != nil || len(status) != 1 {
		t.Fatalf("status: %+v %v", status, err)
	}
	kept := status[0].Kept
	if len(kept) != 2 || kept[0].AttemptID != failed.Attempt.ID || kept[0].Task != "scheduler-project/train/scratch-failed" || kept[0].Path != "D:/kairo/scratch/"+failed.Attempt.ID ||
		*kept[0].Reason != "exit 1" || *kept[0].Bytes != 5000 || kept[1].AttemptID != lost.Attempt.ID || *kept[1].Reason != "kept: lost" {
		t.Fatalf("kept: %+v", kept)
	}
}

// scratch_tmpdir is part of the execution's digest (absent, the digest is
// as before) and reaches its launch, a reissued one too.
func TestScratchTmpdirReachesTheLaunch(t *testing.T) {
	_, _, plain, err := normalizeExecutionSpec(testExecutionSpec("pin"))
	if err != nil {
		t.Fatal(err)
	}
	withTmpdir := testExecutionSpec("pin")
	withTmpdir.ScratchTmpdir = true
	if _, _, digest, err := normalizeExecutionSpec(withTmpdir); err != nil || digest == plain {
		t.Fatalf("scratch_tmpdir left out of the digest: %s %v", digest, err)
	}

	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	ctx := context.Background()
	spec := schedulerExecutionSpec("tmpdir")
	spec.ScratchTmpdir = true
	if _, _, err = store.SubmitExecution(ctx, spec); err != nil {
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
	if err != nil || !launch.ScratchTmpdir {
		t.Fatalf("launch: %+v %v", launch, err)
	}
	if again, err := store.AuthorizeLaunch(ctx, reservation); err != nil || !again.ScratchTmpdir {
		t.Fatalf("reissued launch: %+v %v", again, err)
	}
}
