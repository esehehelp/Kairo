package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"kairo/internal/machine"
)

// authorizeNext reserves the next execution on "executor" and authorizes its
// launch.
func authorizeNext(t *testing.T, store *Store) *Launch {
	t.Helper()
	ctx := context.Background()
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
	return launch
}

// A run is an execution with the continuations that follow it: a
// continuation is in its predecessor's run, a rerun starts a run of its own,
// a direct execution is its own run, and a gang rank has its leader's run
// with its rank.
func TestARunIsAnExecutionAndItsContinuations(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	ctx := context.Background()
	parsed := projectSpec(t, `schema_version = 1
[project]
name = "runs"
[[tasks]]
name = "train"
queue = "training"
depends_on = []
policy = "RunToCompletion@v1"
[tasks.execution]
schema_version = 2
argv = ["python", "train.py"]
cwd = "/work"
checkpointable = true
preemptible = true
[tasks.execution.executor]
labels = { environment = "wsl2" }
[[tasks.execution.exclusive]]
kind = "gpu"
count = 1
`)
	if _, err := store.ApplyProject(ctx, parsed); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	finish := func(launch *Launch, exitCode int, continuation string) {
		t.Helper()
		epoch := launch.Lease.CoordinationEpoch
		if err := store.ActivateLaunch(ctx, launch.Attempt.ID, launch.Lease.ID, epoch, launch.AuthorizationToken, 100, "launcher:100"); err != nil {
			t.Fatal(err)
		}
		if continuation != "" {
			commandID, err := store.EnqueueSuspend(ctx, launch.Attempt.ID, "priority_preemption", "test")
			if err != nil {
				t.Fatal(err)
			}
			for _, phase := range []string{"accepted", "checkpointing"} {
				if err = store.AckCommand(ctx, launch.Attempt.ID, launch.Lease.ID, epoch, commandID, phase, nil); err != nil {
					t.Fatal(err)
				}
			}
			if err = store.AckCommand(ctx, launch.Attempt.ID, launch.Lease.ID, epoch, commandID, "checkpointed", json.RawMessage(`{"continuation_ref":"`+continuation+`"}`)); err != nil {
				t.Fatal(err)
			}
		}
		if err := store.RecordTerminal(ctx, launch.Attempt.ID, launch.Lease.ID, epoch, exitCode, ""); err != nil {
			t.Fatal(err)
		}
		refreshSchedulerGPU(t, store)
		if err := store.FinalizeQuiescence(ctx, launch.Attempt.ID, launch.Lease.ID, epoch); err != nil {
			t.Fatal(err)
		}
		if err := store.ReconcileProjectTasks(ctx); err != nil {
			t.Fatal(err)
		}
	}

	ended := func(runID string, want bool) {
		t.Helper()
		if got, err := runEndedTx(ctx, store.db, runID); err != nil || got != want {
			t.Fatalf("run %s ended: %v %v, want %v", runID, got, err, want)
		}
	}
	first := authorizeNext(t, store)
	if first.RunID != first.Execution.ID {
		t.Fatalf("the initial execution's run is %q, want its own id %s", first.RunID, first.Execution.ID)
	}
	ended(first.RunID, false)
	finish(first, 75, "checkpoint://step-5")
	ended(first.RunID, false) // its continuation waits
	continued := authorizeNext(t, store)
	if continued.Execution.ID == first.Execution.ID || continued.RunID != first.Execution.ID {
		t.Fatalf("the continuation %s is in run %q, want %s", continued.Execution.ID, continued.RunID, first.Execution.ID)
	}
	finish(continued, 1, "")
	ended(first.RunID, true) // the Task failed
	if _, err := store.RerunTask(ctx, "runs", "train", "tester", "rerun-1"); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	ended(first.RunID, true) // pending again, but a later run was made
	rerun := authorizeNext(t, store)
	if rerun.RunID != rerun.Execution.ID || rerun.RunID == first.Execution.ID {
		t.Fatalf("the rerun %s is in run %q, want a run of its own", rerun.Execution.ID, rerun.RunID)
	}
	ended(rerun.RunID, false)
	finish(rerun, 75, "checkpoint://step-9")
	again := authorizeNext(t, store)
	if again.RunID != rerun.Execution.ID {
		t.Fatalf("the rerun's continuation is in run %q, want %s", again.RunID, rerun.Execution.ID)
	}
	ended(rerun.RunID, false)
	ended(rerun.RunID+"-rank1", false)
	ended("exe_unknown", false)

	// a direct execution, and the ranks of a gang
	spec := schedulerExecutionSpec("direct")
	direct, _, err := store.SubmitExecution(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	if run, err := runIDTx(ctx, store.db, direct.ID); err != nil || run != direct.ID {
		t.Fatalf("a direct execution's run: %q %v", run, err)
	}
	ganged := schedulerExecutionSpec("ganged")
	ganged.Gang = &GangSpec{Size: 2, Ranks: []GangRank{{Rank: 1, Argv: []string{"x"}}}}
	leader, _, err := store.SubmitExecution(ctx, ganged)
	if err != nil {
		t.Fatal(err)
	}
	var member string
	if err = store.db.QueryRow(`SELECT gm.execution_id FROM gang_members gm JOIN execution_gangs g ON g.id=gm.gang_id WHERE g.leader_execution_id=$1 AND gm.rank=1`, leader.ID).Scan(&member); err != nil {
		t.Fatal(err)
	}
	if run, err := runIDTx(ctx, store.db, leader.ID); err != nil || run != leader.ID {
		t.Fatalf("a gang leader's run: %q %v", run, err)
	}
	if run, err := runIDTx(ctx, store.db, member); err != nil || run != leader.ID+"-rank1" || !machine.ScratchIDValid(run) {
		t.Fatalf("rank 1's run: %q %v", run, err)
	}
}

func outputOf(launch *Launch, storeID string) RunOutput {
	return RunOutput{AttemptID: launch.Attempt.ID, LeaseID: launch.Lease.ID, Epoch: launch.Lease.CoordinationEpoch, RunID: launch.RunID,
		StoreID: storeID, ExecutorID: "executor", Root: "D:/kairo", Path: "D:/kairo/outputs/" + launch.RunID}
}

func outputState(t *testing.T, store *Store, runID, storeID string) (state, lastAttempt string, bytes *int64) {
	t.Helper()
	if err := store.db.QueryRow(`SELECT state,last_attempt_id,bytes FROM run_outputs WHERE run_id=$1 AND store_id=$2`, runID, storeID).Scan(&state, &lastAttempt, &bytes); err != nil {
		t.Fatal(err)
	}
	return state, lastAttempt, bytes
}

// A run's output directory is recorded only as <root>/outputs/<run id> on a
// volatile store of the attempt's executor, for an attempt about to launch
// in that run; each report measures it, and a whole scan that no longer
// finds it marks it missing.
func TestRecordRunOutputChecksTheLaunchAndReportsMeasureIt(t *testing.T) {
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
	launch := authorizeScratchTest(t, store, "outputs")
	if launch.RunID != launch.Execution.ID {
		t.Fatalf("run %q", launch.RunID)
	}
	out := outputOf(launch, "d-scratch")
	refused := func(what string, change func(*RunOutput), want string) {
		t.Helper()
		bad := out
		change(&bad)
		if err := store.RecordRunOutput(ctx, bad); err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("%s: %v, want %q", what, err, want)
		}
	}
	refused("of another run", func(o *RunOutput) { o.RunID, o.Path = "exe_other", "D:/kairo/outputs/exe_other" }, "runs in run "+launch.RunID)
	refused("on a durable store", func(o *RunOutput) {
		o.StoreID, o.Root, o.Path = "d-durable", "D:/durable", "D:/durable/outputs/"+o.RunID
	}, "no longer volatile")
	refused("on another executor's store", func(o *RunOutput) {
		o.StoreID, o.Root, o.Path = "w-scratch", "/var/tmp/kairo", "/var/tmp/kairo/outputs/"+o.RunID
	}, "in executor wsl's namespace")
	refused("at another root", func(o *RunOutput) { o.Root, o.Path = "D:/other", "D:/other/outputs/"+o.RunID }, "has moved from D:/other to D:/kairo")
	refused("inside the directory", func(o *RunOutput) { o.Path += "/inner" }, "is not the run's output directory")
	refused("in the scratch", func(o *RunOutput) { o.Path = "D:/kairo/scratch/" + o.RunID }, "is not the run's output directory")
	refused("by an executor the attempt is not of", func(o *RunOutput) { o.ExecutorID = "another" }, "is not executor another's")
	stale := out
	stale.Epoch++
	if err := store.RecordRunOutput(ctx, stale); !errors.Is(err, ErrStaleEpoch) {
		t.Fatalf("a stale epoch: %v", err)
	}
	if err := store.RecordRunOutput(ctx, out); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordRunOutput(ctx, out); err != nil {
		t.Fatalf("recorded again: %v", err)
	}
	other := out
	other.Path = `D:\kairo\outputs\` + out.RunID + `\..\x`
	if err := store.RecordRunOutput(ctx, other); err == nil {
		t.Fatal("another directory for the run on the store was recorded")
	}
	if state, last, _ := outputState(t, store, out.RunID, "d-scratch"); state != "active" || last != launch.Attempt.ID {
		t.Fatalf("state %s, last attempt %s", state, last)
	}

	// reports measure it; bytes found without a record count as outputs too
	gib := int64(1 << 30)
	total, free := 100*gib, 40*gib
	report := func(outputs map[string]int64, scanned bool) {
		t.Helper()
		obs := StoreObservation{StoreID: "d-scratch", ExecutorID: "executor", Root: "D:/kairo", Usable: true, TotalBytes: &total, FreeBytes: &free,
			Scratch: map[string]int64{}, ScratchScanned: true, Outputs: outputs, OutputsScanned: scanned}
		if err := store.RecordStoreObservation(ctx, obs); err != nil {
			t.Fatal(err)
		}
	}
	report(map[string]int64{out.RunID: 5 * gib, "exe_unrecorded": gib}, true)
	if state, _, bytes := outputState(t, store, out.RunID, "d-scratch"); state != "active" || bytes == nil || *bytes != 5*gib {
		t.Fatalf("after a report: %s %v", state, bytes)
	}
	status := func() StoreStatus {
		t.Helper()
		stores, err := store.ListStoreStatus(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, s := range stores {
			if s.ID == "d-scratch" {
				return s
			}
		}
		t.Fatal("d-scratch not listed")
		return StoreStatus{}
	}
	if s := status(); s.OutputBytes != 6*gib || s.Outputs != 1 || s.ExternalBytes == nil || *s.ExternalBytes != 54*gib {
		t.Fatalf("status: outputs %d (%d), external %v", s.OutputBytes, s.Outputs, s.ExternalBytes)
	}
	// found but not measured this time: the bytes measured before stay
	report(map[string]int64{out.RunID: -1}, true)
	if _, _, bytes := outputState(t, store, out.RunID, "d-scratch"); bytes == nil || *bytes != 5*gib {
		t.Fatalf("unmeasured: %v", bytes)
	}
	// a scan cut short takes nothing as missing; a whole one does, once the
	// record is older than the scan
	report(map[string]int64{}, false)
	if state, _, _ := outputState(t, store, out.RunID, "d-scratch"); state != "active" {
		t.Fatalf("after a partial scan: %s", state)
	}
	report(map[string]int64{}, true)
	if state, _, _ := outputState(t, store, out.RunID, "d-scratch"); state != "active" {
		t.Fatalf("missing though recorded during the scan: %s", state)
	}
	if _, err := store.testExec(`UPDATE run_outputs SET updated_at='2026-01-01T00:00:00Z' WHERE run_id=$1`, out.RunID); err != nil {
		t.Fatal(err)
	}
	report(map[string]int64{}, true)
	if state, _, bytes := outputState(t, store, out.RunID, "d-scratch"); state != "missing" || bytes == nil || *bytes != 0 {
		t.Fatalf("after a whole scan without it: %s %v", state, bytes)
	}
	if s := status(); s.Outputs != 0 || s.OutputBytes != 0 {
		t.Fatalf("status with it missing: %d (%d)", s.OutputBytes, s.Outputs)
	}
	// found again: active again
	report(map[string]int64{out.RunID: 2 * gib}, true)
	if state, _, bytes := outputState(t, store, out.RunID, "d-scratch"); state != "active" || *bytes != 2*gib {
		t.Fatalf("found again: %s %v", state, *bytes)
	}

	// once running, no output directory is recorded for it any more
	if err := store.ActivateLaunch(ctx, launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch, launch.AuthorizationToken, 100, "launcher:100"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.testExec(`DELETE FROM run_outputs WHERE run_id=$1`, out.RunID); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordRunOutput(ctx, out); !errors.Is(err, ErrStaleEpoch) {
		t.Fatalf("an output directory recorded for a running attempt: %v", err)
	}
}
