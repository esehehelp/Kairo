package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"slices"
	"strconv"
	"testing"

	"kairo/internal/pgtest"
)

func TestBenchMigrationKeepsAPopulatedDatabase(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	// The schema before benches were tasks, with runs of every state.
	if err = CreateSchemaAt(ctx, tx, 1); err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		`INSERT INTO coordination_scopes VALUES('s-project','project',NULL,'kairo','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')`,
		`INSERT INTO execution_requests(id,project_scope_id,client_request_id,spec_digest,state,argv_json,cwd,submitted_at) VALUES('e1','s-project','r1','d1','terminal','["run"]','/w','2026-01-01T00:00:00Z')`,
		`INSERT INTO bench_runs(id,host_id,device_id,class,kinds_json,node_id,execution_id,state,requested_by,created_at,references_json,valid) VALUES
		 ('b-some','h','GPU-1','gpu','["gemm","copy"]','n1','e1','invalid','t','1','{"gemm":{"valid":false},"copy":{"valid":true}}',FALSE),
		 ('b-none','h','GPU-1','gpu','["gemm"]','n1','e1','invalid','t','2','{"gemm":{"valid":false}}',FALSE),
		 ('b-null','h','GPU-1','gpu','["gemm"]','n1','e1','invalid','t','3','null',FALSE),
		 ('b-ok','h','GPU-1','gpu','["gemm"]','n1','e1','finished','t','4','{"gemm":{"valid":true}}',TRUE),
		 ('b-live','h','GPU-1','gpu','["gemm"]','n1','e1','running','t','5',NULL,NULL)`,
	} {
		if _, err = tx.ExecContext(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	store, err := Open(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	want := map[string]string{"b-some": BenchPartial, "b-none": BenchInvalid, "b-null": BenchInvalid, "b-ok": BenchFinished, "b-live": BenchRunning}
	runs, err := store.ListBenchRuns(ctx, "h", "", false)
	if err != nil || len(runs) != len(want) {
		t.Fatalf("runs: %+v %v", runs, err)
	}
	for _, run := range runs {
		if run.State != want[run.ID] {
			t.Fatalf("%s is %s, want %s", run.ID, run.State, want[run.ID])
		}
	}
	// A run from before has no task: it fails once its execution ended.
	if err = store.ReconcileBenchRuns(ctx); err != nil {
		t.Fatal(err)
	}
	if run, _ := store.GetBenchRun(ctx, "b-live"); run.State != BenchFailed {
		t.Fatalf("a run of before: %+v", run)
	}
}

// quiesce ends an attempt that exited with exitCode and releases its lease.
func quiesce(t *testing.T, store *Store, launch *Launch, exitCode int) {
	t.Helper()
	ctx := context.Background()
	if err := store.RecordTerminal(ctx, launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch, exitCode, ""); err != nil {
		t.Fatal(err)
	}
	pollShared(t, store, false, false)
	if err := store.FinalizeQuiescence(ctx, launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch); err != nil {
		t.Fatal(err)
	}
}

func TestAGPUBenchIsPreemptedAndItsContinuationCarriesOnTheRun(t *testing.T) {
	store := openCoordinationStore(t)
	setupHost(t, store)
	ctx := context.Background()
	benchNode(t, store, "a", true)
	cpuPoll(t, store, "a", 8000)
	plan, err := store.CreateBenchRun(ctx, BenchRequest{HostID: "h", DeviceID: "GPU-S", Kinds: []string{"gemm", "copy"}})
	if err != nil {
		t.Fatal(err)
	}
	first := launchOn(t, store, "exec-a")
	epoch := first.Lease.CoordinationEpoch
	if err = store.StartBenchRun(ctx, first.Attempt.ID, BenchStart{DriverBuild: "test"}); err != nil {
		t.Fatal(err)
	}
	if _, err = store.SetBenchSetting(ctx, first.Attempt.ID, first.Lease.ID, DeviceSettings{GraphicsClockMaxMHz: intp(1800)}); err != nil {
		t.Fatal(err)
	}
	// gemm is done; copy is half measured.
	gemm := BenchReference{Unit: "flop", WorkPerSecond: []float64{6e13, 6e13, 6e13}, Mean: 6e13, Valid: true}
	points := []BenchPoint{{Seq: 0, Kind: "gemm", Phase: PhaseReference, Valid: true}, {Seq: 1, Kind: "gemm", Phase: PhaseSweep, Valid: true}, {Seq: 2, Kind: "copy", Phase: PhaseReference, Valid: true}}
	if err = store.AddBenchPoints(ctx, first.Attempt.ID, points[:2]); err != nil {
		t.Fatal(err)
	}
	if err = store.RecordBenchKind(ctx, first.Attempt.ID, "gemm", gemm); err != nil {
		t.Fatal(err)
	}
	if err = store.AddBenchPoints(ctx, first.Attempt.ID, points[2:]); err != nil {
		t.Fatal(err)
	}
	// Work of a higher priority needs the GPU: the bench is suspended.
	if _, _, err = store.SubmitExecution(ctx, sharedSpec("train", "a", 0)); err != nil {
		t.Fatal(err)
	}
	if reservation, err := store.ReserveNext(ctx, "exec-a"); err != nil || reservation != nil {
		t.Fatalf("reserved the GPU the bench holds: %+v %v", reservation, err)
	}
	commands, err := store.EnsurePriorityPreemption(ctx, "exec-a")
	if err != nil || len(commands) != 1 {
		t.Fatalf("preemption commands=%v err=%v", commands, err)
	}
	var attemptID string
	if err = store.db.QueryRow(`SELECT attempt_id FROM commands WHERE id=$1`, commands[0]).Scan(&attemptID); err != nil || attemptID != first.Attempt.ID {
		t.Fatalf("suspend for %s, want the bench's %s (%v)", attemptID, first.Attempt.ID, err)
	}
	ref := `{"run":"` + plan.Run.ID + `","done":["gemm"]}`
	for _, phase := range []string{"accepted", "checkpointing", "checkpointed"} {
		if err = store.AckCommand(ctx, first.Attempt.ID, first.Lease.ID, epoch, commands[0], phase, json.RawMessage(`{"continuation_ref":`+strconv.Quote(ref)+`}`)); err != nil {
			t.Fatal(err)
		}
	}
	quiesce(t, store, first, 0)
	// Its setting went with its lease.
	if hardware, _ := store.ListHardware(ctx, "h"); hardware[0].Devices[0].Effective != nil {
		t.Fatalf("the bench setting outlived its lease: %+v", hardware[0].Devices[0].Effective)
	}
	if err = store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	if err = store.ReconcileBenchRuns(ctx); err != nil {
		t.Fatal(err)
	}
	// The continuation carries on the run: gemm stays, copy's points go.
	run, err := store.GetBenchRun(ctx, plan.Run.ID)
	if err != nil || run.State != BenchWaiting || run.ExecutionID == plan.Run.ExecutionID || len(run.Points) != 2 || run.Points[1].Kind != "gemm" || !run.References["gemm"].Valid {
		t.Fatalf("suspended run: %+v %v", run, err)
	}
	continuation, err := store.GetExecution(ctx, run.ExecutionID)
	if err != nil || continuation.InputContinuationRef == nil || *continuation.InputContinuationRef != ref || !continuation.Preemptible {
		t.Fatalf("continuation: %+v %v", continuation, err)
	}
	if eventCount(t, store, "hardware_bench_continued") != 1 || eventCount(t, store, "hardware_bench_invalid") != 0 {
		t.Fatal("events of the continuation")
	}
	// The work that preempted it runs first, then the continuation.
	train := launchOn(t, store, "exec-a")
	if train.Lease.ExecutionID == run.ExecutionID {
		t.Fatal("the bench went before the work that preempted it")
	}
	quiesce(t, store, train, 0)
	second := launchOn(t, store, "exec-a")
	if second.Lease.ExecutionID != run.ExecutionID {
		t.Fatalf("launched %s, want the continuation %s", second.Lease.ExecutionID, run.ExecutionID)
	}
	status, err := store.BenchStatusOf(ctx, second.Attempt.ID, second.Lease.ID)
	if err != nil || status.Run.ID != plan.Run.ID || status.NextSeq != 2 || !slices.Equal(status.Run.doneKinds(), []string{"gemm"}) {
		t.Fatalf("status of the continuation: %+v %v", status, err)
	}
	if err = store.StartBenchRun(ctx, second.Attempt.ID, BenchStart{DriverBuild: "test"}); err != nil {
		t.Fatal(err)
	}
	if eventCount(t, store, "hardware_bench_started") != 1 || eventCount(t, store, "hardware_bench_resumed") != 1 {
		t.Fatal("a resumed run is not started again")
	}
	// It sets the device again, under its own lease.
	if result, err := store.SetBenchSetting(ctx, second.Attempt.ID, second.Lease.ID, DeviceSettings{GraphicsClockMaxMHz: intp(1800)}); err != nil || result.Effective == nil || result.Effective.Owner.Ref != second.Lease.ID {
		t.Fatalf("setting of the continuation: %+v %v", result, err)
	}
	if err = store.AddBenchPoints(ctx, second.Attempt.ID, []BenchPoint{{Seq: 2, Kind: "copy", Phase: PhaseReference, Valid: true}}); err != nil {
		t.Fatal(err)
	}
	copyRef := BenchReference{Unit: "byte", WorkPerSecond: []float64{8e11, 8e11, 8e11}, Mean: 8e11, Valid: true}
	if err = store.FinishBenchRun(ctx, second.Attempt.ID, second.Lease.ID, BenchFinish{References: map[string]BenchReference{"copy": copyRef}, Valid: true}); err != nil {
		t.Fatal(err)
	}
	quiesce(t, store, second, 0)
	if err = store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	if err = store.ReconcileBenchRuns(ctx); err != nil {
		t.Fatal(err)
	}
	// Suspended and resumed, the run ends valid.
	run, err = store.GetBenchRun(ctx, plan.Run.ID)
	if err != nil || run.State != BenchFinished || run.Valid == nil || !*run.Valid || len(run.References) != 2 || len(run.Points) != 3 {
		t.Fatalf("resumed run: %+v %v", run, err)
	}
	if project, _ := store.GetProjectStatus(ctx, BenchProject); project.Tasks[0].State != "succeeded" || len(project.Tasks[0].Executions) != 2 {
		t.Fatalf("bench task: %+v", project.Tasks[0])
	}
	// Both executions are the run's (the policy sees their leases as a bench's).
	var n int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM bench_run_executions WHERE run_id=$1`, plan.Run.ID).Scan(&n); err != nil || n != 2 {
		t.Fatalf("run executions: %d %v", n, err)
	}
}

func TestAForceStoppedBenchFailsWithoutItsResults(t *testing.T) {
	store := openCoordinationStore(t)
	setupHost(t, store)
	ctx := context.Background()
	benchNode(t, store, "a", true)
	cpuPoll(t, store, "a", 8000)
	plan, err := store.CreateBenchRun(ctx, BenchRequest{HostID: "h", DeviceID: "GPU-S", Kinds: []string{"copy", "gemm"}})
	if err != nil {
		t.Fatal(err)
	}
	launch := launchOn(t, store, "exec-a")
	if err = store.StartBenchRun(ctx, launch.Attempt.ID, BenchStart{DriverBuild: "test"}); err != nil {
		t.Fatal(err)
	}
	if err = store.RecordBenchKind(ctx, launch.Attempt.ID, "copy", BenchReference{Unit: "byte", Mean: 8e11, Valid: true}); err != nil {
		t.Fatal(err)
	}
	scope := taskScopeID(t, store, BenchProject, BenchQueue, plan.Run.ID)
	if _, err = store.ForcePauseScope(ctx, scope, "operator", "force-1", ForceStopSpec{GraceSeconds: 30, Reason: "test"}); err != nil {
		t.Fatal(err)
	}
	reconcilePauses(t, store)
	if err = store.AckForceStop(ctx, launch.Attempt.ID, "signalled", nil); err != nil {
		t.Fatal(err)
	}
	if err = store.ReconcileBenchRuns(ctx); err != nil {
		t.Fatal(err)
	}
	if run, _ := store.GetBenchRun(ctx, plan.Run.ID); run.State != BenchRunning {
		t.Fatalf("the run of a task being stopped ended: %+v", run)
	}
	// The driver leaves at its stop signal; the task is stopped once its
	// lease is released, and the run fails.
	quiesce(t, store, launch, 0)
	if err = store.AckForceStop(ctx, launch.Attempt.ID, "terminated", nil); err != nil {
		t.Fatal(err)
	}
	reconcilePauses(t, store)
	if err = store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	if err = store.ReconcileBenchRuns(ctx); err != nil {
		t.Fatal(err)
	}
	run, err := store.GetBenchRun(ctx, plan.Run.ID)
	if err != nil || run.State != BenchFailed || run.Reason == nil || *run.Reason != "the bench execution ended without its results (force_stopped)" || eventCount(t, store, "hardware_bench_invalid") != 1 {
		t.Fatalf("force-stopped run: %+v %v", run, err)
	}
	if project, _ := store.GetProjectStatus(ctx, BenchProject); project.Tasks[0].State != "stopped" {
		t.Fatalf("bench task: %+v", project.Tasks[0])
	}
}
