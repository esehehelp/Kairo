package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
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

// settleExpect checks what the executor is told to settle now: the actions
// and reasons by attempt (a reason ending in "..." is a prefix).
func settleExpect(t *testing.T, store *Store, want map[string]ScratchSettlement) {
	t.Helper()
	got, err := store.ScratchToSettle(context.Background(), "executor")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("to settle: %+v, want %+v", got, want)
	}
	for _, k := range got {
		w, ok := want[k.AttemptID]
		reasonOK := k.Reason == w.Reason || strings.HasSuffix(w.Reason, "...") && strings.HasPrefix(k.Reason, strings.TrimSuffix(w.Reason, "..."))
		if !ok || k.Action != w.Action || !reasonOK || k.StoreID != "d-scratch" || k.Root != "D:/kairo" || k.Path != "D:/kairo/scratch/"+k.AttemptID {
			t.Fatalf("settlement %+v, want %+v", k, w)
		}
	}
}

// A scratch is recorded only for an attempt about to launch, on a volatile
// store of its node in its executor's namespace, at the store's declared
// root and exactly at <root>/scratch/<attempt id>; recording it again is no
// error.
func TestRecordAttemptScratchChecksTheLaunch(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	addTestDisk(t, store, "node", "D:")
	ctx := context.Background()
	if err := store.UpsertExecutor(ctx, Executor{ID: "wsl", NodeID: "node", Kind: "wsl2", Attributes: json.RawMessage(`{"environment":"wsl2"}`), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	declareTestStore(t, store,
		machine.Store{ID: "d-scratch", Class: machine.StoreVolatile, Node: "node", Root: "D:/kairo", Filesystem: "D:"},
		machine.Store{ID: "d-durable", Class: machine.StoreDurable, Node: "node", Root: "D:/durable", Filesystem: "D:"},
		machine.Store{ID: "w-scratch", Class: machine.StoreVolatile, Node: "node", Executor: "wsl", Root: "/var/tmp/kairo", Filesystem: "D:"},
	)
	launch := authorizeScratchTest(t, store, "checks")
	scratch := scratchOf(launch, "d-scratch")

	refused := func(what string, change func(*AttemptScratch), want string) {
		t.Helper()
		bad := scratch
		change(&bad)
		if err := store.RecordAttemptScratch(ctx, bad); err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("%s: %v, want %q", what, err, want)
		}
	}
	refused("on a durable store", func(s *AttemptScratch) {
		s.StoreID, s.Root, s.Path = "d-durable", "D:/durable", "D:/durable/scratch/"+s.AttemptID
	}, "no longer volatile")
	refused("on another executor's store", func(s *AttemptScratch) {
		s.StoreID, s.Root, s.Path = "w-scratch", "/var/tmp/kairo", "/var/tmp/kairo/scratch/"+s.AttemptID
	}, "in executor wsl's namespace")
	refused("at another root", func(s *AttemptScratch) { s.Root, s.Path = "D:/other", "D:/other/scratch/"+s.AttemptID }, "has moved from D:/other to D:/kairo")
	refused("inside the scratch", func(s *AttemptScratch) { s.Path += "/inner" }, "is not the attempt's scratch directory")
	refused("out of the root", func(s *AttemptScratch) { s.Path = "D:/kairo/scratch/../../durable" }, "is not the attempt's scratch directory")
	refused("another attempt's", func(s *AttemptScratch) { s.Path = "D:/kairo/scratch/att_other" }, "is not the attempt's scratch directory")
	refused("by an executor the attempt is not of", func(s *AttemptScratch) { s.ExecutorID = "another" }, "is not executor another's")
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
	refused("at a drive-relative path", func(s *AttemptScratch) { s.Path = "D:kairo/scratch/" + s.AttemptID }, "is not the attempt's scratch directory")
	// the root is spelled as declared (another spelling is taken as another
	// root); the path may be spelled otherwise
	refused("at the root spelled otherwise", func(s *AttemptScratch) { s.Root = `d:\KAIRO\` }, `has moved from d:\KAIRO\ to D:/kairo`)
	spelled := scratch
	spelled.Path = `D:\kairo\scratch\` + scratch.AttemptID
	if err := store.RecordAttemptScratch(ctx, spelled); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordAttemptScratch(ctx, spelled); err != nil {
		t.Fatalf("recorded again: %v", err)
	}
	if err := store.RecordAttemptScratch(ctx, scratch); err == nil {
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

// An ended attempt's scratch is settled by its outcome, reached through the
// real transitions: deleted after exit 0 without a signal or a suspend that
// published its checkpoint; kept after a failure (exit code, signal, a
// launch that never ran) or a loss, empty or not.
func TestScratchIsSettledByTheAttemptsOutcome(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	addTestDisk(t, store, "node", "D:")
	declareTestStore(t, store, machine.Store{ID: "d-scratch", Class: machine.StoreVolatile, Node: "node", Root: "D:/kairo", Filesystem: "D:"})
	ctx := context.Background()
	kept := func(attemptID, reason string) {
		t.Helper()
		if err := store.SettleScratch(ctx, ScratchSettled{AttemptID: attemptID, State: ScratchDeleting}); !errors.Is(err, ErrScratchKept) {
			t.Fatalf("a kept scratch was claimed for deletion: %v", err)
		}
		if err := store.SettleScratch(ctx, ScratchSettled{AttemptID: attemptID, State: "kept"}); err != nil {
			t.Fatal(err)
		}
		if state, _, got := scratchState(t, store, attemptID); state != "kept" || got == nil || *got != reason {
			t.Fatalf("%s: %s %v, want kept %q", attemptID, state, got, reason)
		}
	}

	// exit 0: deleted; while it runs (or has exited but not quiesced) there
	// is nothing to settle
	ok := startWithScratch(t, store, "ok")
	settleExpect(t, store, nil)
	if err := store.RecordTerminal(ctx, ok.Attempt.ID, ok.Lease.ID, ok.Lease.CoordinationEpoch, 0, ""); err != nil {
		t.Fatal(err)
	}
	settleExpect(t, store, nil)
	refreshSchedulerGPU(t, store)
	if err := store.FinalizeQuiescence(ctx, ok.Attempt.ID, ok.Lease.ID, ok.Lease.CoordinationEpoch); err != nil {
		t.Fatal(err)
	}
	settleExpect(t, store, map[string]ScratchSettlement{ok.Attempt.ID: {Action: ScratchDelete, Reason: "exit 0"}})
	if err := store.SettleScratch(ctx, ScratchSettled{AttemptID: ok.Attempt.ID, State: "deleted"}); err == nil {
		t.Fatal("a scratch recorded deleted before it was claimed")
	}
	settleDeleted(t, store, ok.Attempt.ID)
	if err := store.SettleScratch(ctx, ScratchSettled{AttemptID: ok.Attempt.ID, State: "deleted"}); err != nil {
		t.Fatalf("settled again: %v", err)
	}
	if state, _, reason := scratchState(t, store, ok.Attempt.ID); state != "deleted" || reason == nil || *reason != "exit 0" {
		t.Fatalf("ok: %s %v", state, reason)
	}
	settleExpect(t, store, nil)

	// exit 1: kept, with its size
	failed := startWithScratch(t, store, "failed")
	finishLaunch(t, store, failed, 1)
	settleExpect(t, store, map[string]ScratchSettlement{failed.Attempt.ID: {Action: ScratchKeep, Reason: "exit 1"}})
	size := int64(4096)
	if err := store.SettleScratch(ctx, ScratchSettled{AttemptID: failed.Attempt.ID, State: "kept", Bytes: &size}); err != nil {
		t.Fatal(err)
	}
	if state, bytes, reason := scratchState(t, store, failed.Attempt.ID); state != "kept" || bytes == nil || *bytes != 4096 || reason == nil || *reason != "exit 1" {
		t.Fatalf("failed: %s %v %v", state, bytes, reason)
	}
	if err := store.SettleScratch(ctx, ScratchSettled{AttemptID: failed.Attempt.ID, State: "deleted"}); err == nil {
		t.Fatal("a kept scratch was settled again")
	}

	// killed by a signal: kept, though it held nothing but its marker
	signalled := startWithScratch(t, store, "signalled")
	if err := store.RecordTerminal(ctx, signalled.Attempt.ID, signalled.Lease.ID, signalled.Lease.CoordinationEpoch, -1, "SIGKILL"); err != nil {
		t.Fatal(err)
	}
	refreshSchedulerGPU(t, store)
	if err := store.FinalizeQuiescence(ctx, signalled.Attempt.ID, signalled.Lease.ID, signalled.Lease.CoordinationEpoch); err != nil {
		t.Fatal(err)
	}
	settleExpect(t, store, map[string]ScratchSettlement{signalled.Attempt.ID: {Action: ScratchKeep, Reason: "signal SIGKILL"}})
	kept(signalled.Attempt.ID, "signal SIGKILL")

	// a launch given up before it ran (its reservation released): kept too
	aborted := authorizeScratchTest(t, store, "aborted")
	if err := store.RecordAttemptScratch(ctx, scratchOf(aborted, "d-scratch")); err != nil {
		t.Fatal(err)
	}
	refreshSchedulerGPU(t, store)
	if err := store.ReleaseReservation(ctx, aborted.Lease.ID, aborted.Lease.CoordinationEpoch, "launch failed before activation"); err != nil {
		t.Fatal(err)
	}
	settleExpect(t, store, map[string]ScratchSettlement{aborted.Attempt.ID: {Action: ScratchKeep, Reason: "no exit (the launch did not run)"}})
	kept(aborted.Attempt.ID, "no exit (the launch did not run)")

	// suspended by a pause, with its checkpoint published: deleted, whatever
	// its exit
	suspended := startWithScratch(t, store, "suspended")
	scope := taskScopeID(t, store, "scheduler-project", "train", "scratch-suspended")
	if _, err := store.PauseScope(ctx, scope, "tester", "pause-scratch"); err != nil {
		t.Fatal(err)
	}
	reconcilePauses(t, store)
	var commandID string
	if err := store.db.QueryRow(`SELECT id FROM commands WHERE attempt_id=$1`, suspended.Attempt.ID).Scan(&commandID); err != nil {
		t.Fatal(err)
	}
	epoch := suspended.Lease.CoordinationEpoch
	for _, phase := range []string{"accepted", "checkpointing"} {
		if err := store.AckCommand(ctx, suspended.Attempt.ID, suspended.Lease.ID, epoch, commandID, phase, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.AckCommand(ctx, suspended.Attempt.ID, suspended.Lease.ID, epoch, commandID, "checkpointed", json.RawMessage(`{"continuation_ref":"checkpoint://step-200"}`)); err != nil {
		t.Fatal(err)
	}
	finishLaunch(t, store, suspended, 143)
	settleExpect(t, store, map[string]ScratchSettlement{suspended.Attempt.ID: {Action: ScratchDelete, Reason: "suspended with a published checkpoint"}})
	settleDeleted(t, store, suspended.Attempt.ID)

	// an active scratch's bytes are counted as active, a kept one's as kept
	active := startWithScratch(t, store, "active")
	total, free := int64(100_000), int64(50_000)
	if err := store.RecordStoreObservation(ctx, StoreObservation{StoreID: "d-scratch", ExecutorID: "executor", Root: "D:/kairo", Usable: true, TotalBytes: &total, FreeBytes: &free,
		Scratch: map[string]int64{active.Attempt.ID: 300, failed.Attempt.ID: 5000}}); err != nil {
		t.Fatal(err)
	}
	if status, err := store.ListStoreStatus(ctx); err != nil || status[0].ScratchActiveBytes != 300 || status[0].ScratchKeptBytes != 5000 || *status[0].ExternalBytes != 50_000-5300 {
		t.Fatalf("classes: %+v %v", status, err)
	}
	finishLaunch(t, store, active, 0)
	settleDeleted(t, store, active.Attempt.ID)

	// lost (its executor's identity went): kept, also once it is reconciled
	// to quiesced
	lost := startWithScratch(t, store, "lost")
	if n, err := store.MarkExecutorUnknown(ctx, "executor"); err != nil || n != 1 {
		t.Fatalf("lost: %d %v", n, err)
	}
	settleExpect(t, store, map[string]ScratchSettlement{lost.Attempt.ID: {Action: ScratchKeep, Reason: "attempt lost"}})
	refreshSchedulerGPU(t, store)
	if err := store.ReconcileResource(ctx, "gpu-0", true); err != nil {
		t.Fatal(err)
	}
	if state, _ := attemptStateOf(t, store, lost.Attempt.ID); state != "quiesced" {
		t.Fatalf("reconciled attempt is %s", state)
	}
	settleExpect(t, store, map[string]ScratchSettlement{lost.Attempt.ID: {Action: ScratchKeep, Reason: "attempt lost"}})
	if err := store.SettleScratch(ctx, ScratchSettled{AttemptID: lost.Attempt.ID, State: ScratchDeleting}); !errors.Is(err, ErrScratchKept) {
		t.Fatalf("a lost attempt's scratch deleted: %v", err)
	}
	if err := store.SettleScratch(ctx, ScratchSettled{AttemptID: lost.Attempt.ID, State: "kept", Reason: "kept: lost"}); err != nil {
		t.Fatal(err)
	}
	settleExpect(t, store, nil)

	// the kept ones are listed on their store, with their task
	status, err := store.ListStoreStatus(ctx)
	if err != nil || len(status) != 1 {
		t.Fatalf("status: %+v %v", status, err)
	}
	reasons := map[string]string{}
	for _, k := range status[0].Kept {
		reasons[k.AttemptID] = *k.Reason
	}
	want := map[string]string{failed.Attempt.ID: "exit 1", signalled.Attempt.ID: "signal SIGKILL", aborted.Attempt.ID: "no exit (the launch did not run)", lost.Attempt.ID: "kept: lost"}
	if len(reasons) != len(want) {
		t.Fatalf("kept: %+v", status[0].Kept)
	}
	for id, reason := range want {
		if reasons[id] != reason {
			t.Fatalf("kept %s: %q, want %q", id, reasons[id], reason)
		}
	}
	if k := status[0].Kept[0]; k.AttemptID != failed.Attempt.ID || k.Task != "scheduler-project/train/scratch-failed" || k.Path != "D:/kairo/scratch/"+failed.Attempt.ID || *k.Bytes != 5000 {
		t.Fatalf("kept: %+v", k)
	}
}

func attemptStateOf(t *testing.T, store *Store, attemptID string) (string, error) {
	t.Helper()
	var state string
	err := store.db.QueryRow(`SELECT state FROM attempts WHERE id=$1`, attemptID).Scan(&state)
	if err != nil {
		t.Fatal(err)
	}
	return state, err
}

// A successful attempt's scratch is deleted only while the declaration still
// makes its path the scratch directory of that volatile store: once the
// store is moved, made durable or dropped, the scratch is left untouched
// (kept, never deleted) until the declaration says so again.
func TestScratchIsLeftAloneWhenTheDeclarationChanged(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	addTestDisk(t, store, "node", "D:")
	volatile := machine.Store{ID: "d-scratch", Class: machine.StoreVolatile, Node: "node", Root: "D:/kairo", Filesystem: "D:"}
	declareTestStore(t, store, volatile)
	ctx := context.Background()
	ok := startWithScratch(t, store, "ok")
	finishLaunch(t, store, ok, 0)
	settleExpect(t, store, map[string]ScratchSettlement{ok.Attempt.ID: {Action: ScratchDelete, Reason: "exit 0"}})

	moved := volatile
	moved.Root = "D:/moved"
	durable := volatile
	durable.Class = machine.StoreDurable
	for _, c := range []struct {
		stores []machine.Store
		why    string
	}{
		{[]machine.Store{moved}, "store d-scratch has moved from D:/kairo to D:/moved"},
		{[]machine.Store{durable}, "store d-scratch is no longer volatile"},
		{[]machine.Store{{ID: "d-other", Class: machine.StoreDurable, Node: "node", Root: "D:/kairo", Filesystem: "D:"}}, "store d-scratch is no longer declared"},
	} {
		declareTestStore(t, store, c.stores...)
		settleExpect(t, store, map[string]ScratchSettlement{ok.Attempt.ID: {Action: ScratchLeave, Reason: "exit 0; not touched: " + c.why}})
		if err := store.SettleScratch(ctx, ScratchSettled{AttemptID: ok.Attempt.ID, State: ScratchDeleting}); !errors.Is(err, ErrScratchKept) {
			t.Fatalf("claimed for deletion although %s: %v", c.why, err)
		}
	}
	// declared as it was, it is deleted after all
	declareTestStore(t, store, volatile)
	settleExpect(t, store, map[string]ScratchSettlement{ok.Attempt.ID: {Action: ScratchDelete, Reason: "exit 0"}})
	settleDeleted(t, store, ok.Attempt.ID)
}

// settleDeleted claims a scratch for deletion and records it deleted, as its
// executor does.
func settleDeleted(t *testing.T, store *Store, attemptID string) {
	t.Helper()
	ctx := context.Background()
	for _, state := range []string{ScratchDeleting, ScratchDeleting, "deleted"} {
		if err := store.SettleScratch(ctx, ScratchSettled{AttemptID: attemptID, State: state}); err != nil {
			t.Fatalf("settle %s %s: %v", attemptID, state, err)
		}
	}
}

// Once its executor has claimed a scratch for deletion, the declaration
// cannot change under it: a machine declaration under which it would not be
// deleted (its store moved, made durable or dropped) is refused until it is
// settled, deleted or kept; one that leaves it as it is goes through. A
// claimed scratch stays to be deleted.
func TestADeclarationCannotChangeUnderADeletion(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	addTestDisk(t, store, "node", "D:")
	volatile := machine.Store{ID: "d-scratch", Class: machine.StoreVolatile, Node: "node", Root: "D:/kairo", Filesystem: "D:"}
	declareTestStore(t, store, volatile)
	ctx := context.Background()
	ok := startWithScratch(t, store, "ok")
	finishLaunch(t, store, ok, 0)
	if err := store.SettleScratch(ctx, ScratchSettled{AttemptID: ok.Attempt.ID, State: ScratchDeleting}); err != nil {
		t.Fatal(err)
	}
	if state, _, _ := scratchState(t, store, ok.Attempt.ID); state != ScratchDeleting {
		t.Fatalf("claimed: %s", state)
	}
	moved, durable := volatile, volatile
	moved.Root = "D:/moved"
	durable.Class = machine.StoreDurable
	for _, stores := range [][]machine.Store{{moved}, {durable}, {}} {
		if _, err := store.ApplyMachines(ctx, storesSpec(stores...), false); !errors.Is(err, ErrMachineConflict) || !strings.Contains(err.Error(), "scratch being deleted") {
			t.Fatalf("declared %+v under a deletion: %v", stores, err)
		}
		if _, err := store.ApplyMachines(ctx, storesSpec(stores...), true); !errors.Is(err, ErrMachineConflict) {
			t.Fatalf("dry run of %+v under a deletion: %v", stores, err)
		}
	}
	declareTestStore(t, store, volatile, machine.Store{ID: "d-durable", Class: machine.StoreDurable, Node: "node", Root: "D:/durable", Filesystem: "D:"})
	settleExpect(t, store, map[string]ScratchSettlement{ok.Attempt.ID: {Action: ScratchDelete, Reason: "exit 0"}})
	if err := store.SettleScratch(ctx, ScratchSettled{AttemptID: ok.Attempt.ID, State: "deleted"}); err != nil {
		t.Fatal(err)
	}
	declareTestStore(t, store, durable)

	// a claimed scratch whose deletion was refused for safety is kept
	declareTestStore(t, store, volatile)
	refused := startWithScratch(t, store, "refused")
	finishLaunch(t, store, refused, 0)
	if err := store.SettleScratch(ctx, ScratchSettled{AttemptID: refused.Attempt.ID, State: ScratchDeleting}); err != nil {
		t.Fatal(err)
	}
	if err := store.SettleScratch(ctx, ScratchSettled{AttemptID: refused.Attempt.ID, State: "kept", Reason: "exit 0; not deleted: a link"}); err != nil {
		t.Fatal(err)
	}
	if state, _, reason := scratchState(t, store, refused.Attempt.ID); state != "kept" || reason == nil || *reason != "exit 0; not deleted: a link" {
		t.Fatalf("refused deletion: %s %v", state, reason)
	}
	declareTestStore(t, store, durable)
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
