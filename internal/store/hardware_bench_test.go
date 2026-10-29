package store

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"kairo/internal/machine"
)

// benchNode gives node the bench programs of an agent's [bench] table.
func benchNode(t *testing.T, store *Store, node string, gpu bool) {
	t.Helper()
	bench := map[string]any{"kairo": "/opt/kairo/kairo"}
	if gpu {
		bench["gpu"] = "/opt/kairo/kairo-bench-gpu"
	}
	attributes, _ := json.Marshal(map[string]any{"bench": bench})
	if err := store.UpsertNode(context.Background(), Node{ID: node, Name: node, OS: "linux", Architecture: "amd64", Attributes: attributes, Enabled: true}); err != nil {
		t.Fatal(err)
	}
}

// cpuPoll: node's host provider reports its CPU (millis) and RAM.
func cpuPoll(t *testing.T, store *Store, node string, millis int64) {
	t.Helper()
	ctx := context.Background()
	if err := store.UpsertProvider(ctx, node+"-host", node, "host", nil); err != nil {
		t.Fatal(err)
	}
	stamp := time.Now().UTC()
	ram := int64(64 << 30)
	resources := []ResourceInstance{
		{ID: node + "-cpu", NodeID: node, ProviderID: node + "-host", Kind: "cpu", StableIdentity: "logical-cpu", Binding: json.RawMessage(`{}`)},
		{ID: node + "-ram", NodeID: node, ProviderID: node + "-host", Kind: "ram", StableIdentity: "physical-memory", Binding: json.RawMessage(`{}`)},
	}
	observe := func(id string, total int64) Observation {
		return Observation{ResourceID: id, ObservedAt: stamp.Format(time.RFC3339Nano), ValidUntil: stamp.Add(time.Minute).Format(time.RFC3339Nano), TotalBytes: &total, FreeBytes: &total}
	}
	if err := store.ApplyObservationBatch(ctx, node+"-host", resources, []Observation{observe(node+"-cpu", millis), observe(node+"-ram", ram)}, nil); err != nil {
		t.Fatal(err)
	}
}

// launchOn reserves, prepares and activates the next execution placed on
// executor.
func launchOn(t *testing.T, store *Store, executor string) *Launch {
	t.Helper()
	ctx := context.Background()
	reservation, err := store.ReserveNext(ctx, executor)
	if err != nil || reservation == nil {
		t.Fatalf("reserve on %s: %+v %v", executor, reservation, err)
	}
	if err = store.MarkLeasePrepared(ctx, reservation.Lease.ID, reservation.Lease.CoordinationEpoch); err != nil {
		t.Fatal(err)
	}
	launch, err := store.AuthorizeLaunch(ctx, reservation)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.ActivateLaunch(ctx, launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch, launch.AuthorizationToken, 4321, executor+":4321:start:1"); err != nil {
		t.Fatal(err)
	}
	return launch
}

func TestGPUBenchIsAnExclusiveLowestPriorityExecutionOfItsOwnRun(t *testing.T) {
	store := openCoordinationStore(t)
	setupHost(t, store)
	ctx := context.Background()
	req := BenchRequest{HostID: "h", DeviceID: "GPU-S", Kinds: []string{"gemm", "pcie"}, Actor: "operator"}
	// Neither node reports a GPU bench binary yet.
	if _, err := store.CreateBenchRun(ctx, req); err == nil || !strings.Contains(err.Error(), "GPU bench binary") {
		t.Fatalf("no bench binary: %v", err)
	}
	benchNode(t, store, "a", true)
	benchNode(t, store, "b", false)
	cpuPoll(t, store, "a", 8000)
	cpuPoll(t, store, "b", 8000)
	for _, c := range []struct {
		req  BenchRequest
		want string
	}{
		{BenchRequest{HostID: "h", DeviceID: "GPU-S", Kinds: []string{"cpu-compute"}}, "not a gpu bench kind"},
		{BenchRequest{HostID: "nowhere", DeviceID: "GPU-S"}, "not declared"},
		{BenchRequest{HostID: "h", DeviceID: "GPU-S", Node: "b"}, "GPU bench binary"},
		{BenchRequest{HostID: "h", DeviceID: "GPU-S", Node: "x"}, "not on host"},
	} {
		if _, err := store.CreateBenchRun(ctx, c.req); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("%+v: %v, want %q", c.req, err, c.want)
		}
	}
	dry := req
	dry.DryRun = true
	plan, err := store.CreateBenchRun(ctx, dry)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"gemm", "pcie-h2d", "pcie-d2h", "pcie-h2d-latency", "pcie-d2h-latency"}
	e := plan.Execution
	if !slices.Equal(plan.Run.Kinds, want) || plan.Run.NodeID != "a" || e.Priority != BenchPriority || e.Scope.Project != BenchProject || e.Scope.Queue != BenchQueue || e.Scope.Task != plan.Run.ID ||
		len(e.Exclusive) != 1 || e.Exclusive[0].Device != "GPU-S" || !e.Preemptible || !e.Checkpointable || strings.Join(e.Argv, " ") != "/opt/kairo/kairo hardware bench-run --run "+plan.Run.ID ||
		e.CWD != "/opt/kairo" || !strings.Contains(string(e.ExecutorSelector), `"node":"a"`) {
		t.Fatalf("plan: %+v", plan)
	}
	if runs, _ := store.ListBenchRuns(ctx, "h", "", false); len(runs) != 0 {
		t.Fatalf("a dry run created %+v", runs)
	}
	if status, err := store.GetProjectStatus(ctx, BenchProject); err == nil && len(status.Tasks) != 0 {
		t.Fatalf("a dry run declared %+v", status.Tasks)
	}
	plan, err = store.CreateBenchRun(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	// The run is a task of project kairo, its first execution submitted.
	project, err := store.GetProjectStatus(ctx, BenchProject)
	if err != nil || len(project.Tasks) != 1 || project.Tasks[0].Name != plan.Run.ID || project.Tasks[0].Queue != BenchQueue || project.Tasks[0].State != "running" ||
		len(project.Tasks[0].Executions) != 1 || project.Tasks[0].Executions[0].ExecutionID != plan.Run.ExecutionID || project.Tasks[0].Executions[0].Reason != "initial" {
		t.Fatalf("bench task: %+v %v", project, err)
	}
	if execution, err := store.GetExecution(ctx, plan.Run.ExecutionID); err != nil || !execution.Checkpointable || !execution.Preemptible || execution.Priority != BenchPriority {
		t.Fatalf("bench execution: %+v %v", execution, err)
	}
	// Reconciling the task plans nothing more.
	if err = store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	if project, _ = store.GetProjectStatus(ctx, BenchProject); len(project.Tasks[0].Executions) != 1 {
		t.Fatalf("reconciled bench task: %+v", project.Tasks[0])
	}
	if _, err = store.CreateBenchRun(ctx, req); !errors.Is(err, ErrBenchConflict) {
		t.Fatalf("a second bench of the device: %v", err)
	}
	// Only node a's executor may take it.
	if reservation, err := store.ReserveNext(ctx, "exec-b"); err != nil || reservation != nil {
		t.Fatalf("exec-b reserved %+v %v", reservation, err)
	}
	launch := launchOn(t, store, "exec-a")
	if launch.Lease.ExecutionID != plan.Run.ExecutionID {
		t.Fatalf("launched %s, want the bench's %s", launch.Lease.ExecutionID, plan.Run.ExecutionID)
	}
	run, rank, err := store.BenchOfAttempt(ctx, launch.Attempt.ID)
	if err != nil || run.ID != plan.Run.ID || rank != 0 {
		t.Fatalf("bench of attempt: %+v %d %v", run, rank, err)
	}
	status, err := store.BenchStatusOf(ctx, launch.Attempt.ID, launch.Lease.ID)
	if err != nil || status.Programs.GPU != "/opt/kairo/kairo-bench-gpu" || status.Capabilities == nil || len(status.OtherWork) != 0 || status.Effective != nil {
		t.Fatalf("status: %+v %v", status, err)
	}
	// The bench sets its device under its lease; the controller applies it.
	result, err := store.SetBenchSetting(ctx, launch.Attempt.ID, launch.Lease.ID, DeviceSettings{GraphicsClockMaxMHz: intp(1800)})
	if err != nil || result.Effective == nil || result.Effective.Owner != (DeviceOwner{Kind: OwnerBench, Ref: launch.Lease.ID}) {
		t.Fatalf("bench setting: %+v %v", result, err)
	}
	if err = store.StartBenchRun(ctx, launch.Attempt.ID, BenchStart{DriverBuild: "test", DriverVersion: "560.35", Device: json.RawMessage(`{"kind":"gpu"}`), IdleTemperatureC: ptr(34.0),
		Versions: map[string]BenchVersion{"gemm": {Program: "kairo-bench-gpu", Version: 1}}}); err != nil {
		t.Fatal(err)
	}
	point := BenchPoint{Seq: 1, Kind: "gemm", Phase: PhaseSweep, Settings: DeviceSettings{GraphicsClockMaxMHz: intp(1800)}, Valid: true,
		Metrics: BenchMetrics{Unit: "flop", WorkPerSecond: 6e13, RelPerf: ptr(0.97), JoulesPerWork: ptr(4e-12)}}
	if err = store.AddBenchPoints(ctx, launch.Attempt.ID, []BenchPoint{point, {Seq: 2, Kind: "cpu-compute", Phase: PhaseSweep}}); err == nil {
		t.Fatal("a point of a kind the run does not measure was taken")
	}
	point.Valid = false
	if err = store.AddBenchPoints(ctx, launch.Attempt.ID, []BenchPoint{point}); err != nil {
		t.Fatal(err)
	}
	point.Valid = true // sent again: replaces
	if err = store.AddBenchPoints(ctx, launch.Attempt.ID, []BenchPoint{point}); err != nil {
		t.Fatal(err)
	}
	// A kind done is recorded as it is done; the finish adds the others.
	if err = store.RecordBenchKind(ctx, launch.Attempt.ID, "gemm", BenchReference{Unit: "flop", WorkPerSecond: []float64{6.2e13, 6.2e13, 6.21e13}, Mean: 6.2e13, Valid: true}); err != nil {
		t.Fatal(err)
	}
	if err = store.RecordBenchKind(ctx, launch.Attempt.ID, "cpu-compute", BenchReference{Valid: true}); err == nil {
		t.Fatal("a kind the run does not measure was recorded")
	}
	refs := map[string]BenchReference{"pcie-h2d": {Unit: "byte", WorkPerSecond: []float64{2.4e10}, Mean: 2.4e10, Reason: "references disagree"}}
	if err = store.FinishBenchRun(ctx, launch.Attempt.ID, launch.Lease.ID, BenchFinish{References: refs, Valid: false, Reason: "pcie-h2d: references disagree"}); err != nil {
		t.Fatal(err)
	}
	// Some kinds valid: partially valid, their curves used.
	run, err = store.GetBenchRun(ctx, plan.Run.ID)
	if err != nil || run.State != BenchPartial || run.Valid == nil || *run.Valid || len(run.Points) != 1 || !run.Points[0].Valid || *run.IdleTemperatureC != 34 ||
		*run.DriverVersion != "560.35" || run.Versions["gemm"].Program != "kairo-bench-gpu" || !run.References["gemm"].Valid || run.References["pcie-h2d"].Valid ||
		run.Reason == nil || *run.Reason != "pcie-h2d: references disagree" {
		t.Fatalf("finished run: %+v %v", run, err)
	}
	// Its setting went with the finish; the run takes nothing more.
	if hardware, _ := store.ListHardware(ctx, "h"); hardware[0].Devices[0].Effective != nil || hardware[0].Devices[0].Bench == nil || hardware[0].Devices[0].Bench.ID != run.ID {
		t.Fatalf("after finish: %+v", hardware[0].Devices[0])
	}
	if err = store.AddBenchPoints(ctx, launch.Attempt.ID, []BenchPoint{point}); !errors.Is(err, ErrBenchConflict) {
		t.Fatalf("a point after the finish: %v", err)
	}
	for _, event := range []string{"hardware_bench_requested", "hardware_bench_started", "hardware_bench_finished"} {
		if eventCount(t, store, event) != 1 {
			t.Fatalf("%s events: %d", event, eventCount(t, store, event))
		}
	}
	if eventCount(t, store, "hardware_bench_invalid") != 0 {
		t.Fatal("a partially valid run was announced invalid")
	}
	// Another attempt carries out no bench.
	if _, _, err = store.BenchOfAttempt(ctx, "att_none"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("bench of another attempt: %v", err)
	}
}

func TestBenchRefusesDevicesItMayNotChange(t *testing.T) {
	store := openCoordinationStore(t)
	setupHost(t, store)
	ctx := context.Background()
	benchNode(t, store, "a", true)
	if _, err := store.SetDeviceSetting(ctx, "h", "GPU-S", DeviceOwner{Kind: OwnerManual}, DeviceSettings{PowerLimitW: intp(250)}, "operator"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateBenchRun(ctx, BenchRequest{HostID: "h", DeviceID: "GPU-S"}); err == nil || !strings.Contains(err.Error(), "manual setting") {
		t.Fatalf("manual setting: %v", err)
	}
	host := testHost
	host.Devices = []machine.Device{{ID: "GPU-S", Control: new(false)}}
	if _, err := store.ApplyMachines(ctx, hostSpec(host), false); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateBenchRun(ctx, BenchRequest{HostID: "h", DeviceID: "GPU-S"}); err == nil || !strings.Contains(err.Error(), "control = false") {
		t.Fatalf("control false: %v", err)
	}
	host = testHost
	host.Controller = ""
	if _, err := store.ApplyMachines(ctx, hostSpec(host), false); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateBenchRun(ctx, BenchRequest{HostID: "h", DeviceID: "cpu"}); err == nil || !strings.Contains(err.Error(), "no controller") {
		t.Fatalf("no controller: %v", err)
	}
	// A storage bench needs no controller: it measures the defaults.
	if _, err := store.CreateBenchRun(ctx, BenchRequest{HostID: "h", DeviceID: "disk:/srv", DryRun: true}); err == nil || !strings.Contains(err.Error(), "filesystem /srv") {
		t.Fatalf("unregistered filesystem: %v", err)
	}
}

func TestCPUBenchHoldsTheWholeNodeCPU(t *testing.T) {
	store := openCoordinationStore(t)
	setupHost(t, store)
	ctx := context.Background()
	benchNode(t, store, "a", false)
	if _, err := store.CreateBenchRun(ctx, BenchRequest{HostID: "h", DeviceID: "cpu"}); err == nil || !strings.Contains(err.Error(), "its CPU") {
		t.Fatalf("no CPU observed: %v", err)
	}
	cpuPoll(t, store, "a", 16000)
	plan, err := store.CreateBenchRun(ctx, BenchRequest{HostID: "h", DeviceID: "cpu"})
	if err != nil {
		t.Fatal(err)
	}
	c := plan.Execution.Capacity
	if c.CPUMillis != 16000 || c.CPUMillisMin != 16000 || len(plan.Execution.Exclusive) != 0 || !slices.Equal(plan.Run.Kinds, []string{"cpu-compute", "cpu-single", "cpu-stream"}) {
		t.Fatalf("plan: %+v", plan)
	}
	launch := launchOn(t, store, "exec-a")
	// What else runs on the host's nodes makes the bench's points invalid.
	status, err := store.BenchStatusOf(ctx, launch.Attempt.ID, launch.Lease.ID)
	if err != nil || len(status.OtherWork) != 0 {
		t.Fatalf("status: %+v %v", status, err)
	}
	// While it holds a lease, the policy leaves the CPU alone.
	devices, err := store.PolicyDevices(ctx, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range devices {
		if d.DeviceID == "cpu" && !d.Bench {
			t.Fatalf("cpu bench not seen: %+v", d)
		}
	}
}

func TestReconcileBenchRunsFailsARunWhoseExecutionEnded(t *testing.T) {
	store := openCoordinationStore(t)
	setupHost(t, store)
	ctx := context.Background()
	benchNode(t, store, "a", true)
	cpuPoll(t, store, "a", 8000)
	plan, err := store.CreateBenchRun(ctx, BenchRequest{HostID: "h", DeviceID: "GPU-S", Kinds: []string{"copy"}})
	if err != nil {
		t.Fatal(err)
	}
	if err = store.ReconcileBenchRuns(ctx); err != nil {
		t.Fatal(err)
	}
	if run, _ := store.GetBenchRun(ctx, plan.Run.ID); run.State != "waiting" {
		t.Fatalf("a waiting run was ended: %+v", run)
	}
	if err = store.WithdrawExecution(ctx, plan.Run.ExecutionID); err != nil {
		t.Fatal(err)
	}
	// Until its task is reconciled, the run's task goes on (as it does after
	// a suspend, until the continuation).
	if err = store.ReconcileBenchRuns(ctx); err != nil {
		t.Fatal(err)
	}
	if run, _ := store.GetBenchRun(ctx, plan.Run.ID); run.State != "waiting" {
		t.Fatalf("the run of a live task was ended: %+v", run)
	}
	if err = store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	if err = store.ReconcileBenchRuns(ctx); err != nil {
		t.Fatal(err)
	}
	run, err := store.GetBenchRun(ctx, plan.Run.ID)
	if err != nil || run.State != "failed" || run.Reason == nil || !strings.Contains(*run.Reason, "without its results") || eventCount(t, store, "hardware_bench_invalid") != 1 {
		t.Fatalf("ended run: %+v %v", run, err)
	}
	// A new bench of the device may be asked for.
	if _, err = store.CreateBenchRun(ctx, BenchRequest{HostID: "h", DeviceID: "GPU-S", Kinds: []string{"copy"}, DryRun: true}); err != nil {
		t.Fatal(err)
	}
}

func TestHostCPUPolicySettingBelongsToTheHostAndGoesWithItsPolicy(t *testing.T) {
	store := openCoordinationStore(t)
	setupHost(t, store)
	ctx := context.Background()
	policy := DeviceOwner{Kind: OwnerPolicy}
	if _, err := store.SetDeviceSetting(ctx, "h", "GPU-S", policy, DeviceSettings{GraphicsClockMaxMHz: intp(1800)}, "policy"); err == nil {
		t.Fatal("a GPU policy setting without a lease was taken")
	}
	if _, err := store.SetDeviceSetting(ctx, "h", "cpu", DeviceOwner{Kind: OwnerPolicy, Ref: "lease-x"}, DeviceSettings{MaxFreqMHz: intp(5000)}, "policy"); err == nil {
		t.Fatal("a CPU policy setting of a lease was taken")
	}
	result, err := store.SetDeviceSetting(ctx, "h", "cpu", policy, DeviceSettings{MaxFreqMHz: intp(5000)}, "policy")
	if err != nil || result.Effective == nil || result.Effective.Owner != (DeviceOwner{Kind: OwnerPolicy, Ref: HostPolicyRef}) {
		t.Fatalf("host policy setting: %+v %v", result, err)
	}
	if polled(t, store, "cpu").Settings.MaxFreqMHz == nil {
		t.Fatal("the controller does not see the host's policy setting")
	}
	// max_slowdown 0: the policy leaves the hardware at its defaults.
	host := testHost
	host.MaxSlowdown = 0
	apply, err := store.ApplyMachines(ctx, hostSpec(host), true)
	if err != nil || len(apply.ClearedSettings) != 1 || apply.ClearedSettings[0].Owner.Kind != OwnerPolicy {
		t.Fatalf("dry run: %+v %v", apply, err)
	}
	if _, err = store.ApplyMachines(ctx, hostSpec(host), false); err != nil {
		t.Fatal(err)
	}
	if d := polled(t, store, "cpu"); !d.Settings.IsDefault() {
		t.Fatalf("setting left after max_slowdown 0: %+v", d)
	}
}

func TestControllerSamplesGiveTheHostsCPUEnergy(t *testing.T) {
	store := openCoordinationStore(t)
	setupHost(t, store)
	ctx := context.Background()
	t0 := time.Date(2026, 10, 27, 10, 0, 0, 0, time.UTC)
	sample := func(at time.Duration, watts float64) ControllerSample {
		return ControllerSample{DeviceID: "cpu", ObservedAt: t0.Add(at).Format(time.RFC3339Nano), ValidUntil: t0.Add(at + 20*time.Second).Format(time.RFC3339Nano), PowerW: &watts}
	}
	samples := []ControllerSample{sample(0, 40), sample(10*time.Second, 50), sample(20*time.Second, 60), sample(30*time.Second, 70), sample(70*time.Second, 80)}
	skipped, err := store.RecordControllerSamples(ctx, "ctl", samples)
	if err != nil || len(skipped) != 0 {
		t.Fatalf("record: %v %v", skipped, err)
	}
	if skipped, err = store.RecordControllerSamples(ctx, "a", samples[:1]); err != nil || len(skipped) != 1 {
		t.Fatalf("a node that controls no host: %v %v", skipped, err)
	}
	// [5 s, 25 s]: 5 s of the 50 W span, 10 s at 60 W, 5 s of the 70 W span.
	e, err := store.HostCPUEnergy(ctx, "h", t0.Add(5*time.Second), t0.Add(25*time.Second))
	if err != nil || e.Device != HostCPUDevice("h") || math.Abs(e.Joules-(5*50+10*60+5*70)) > 1e-6 || e.SecondsCovered != 20 {
		t.Fatalf("energy: %+v %v", e, err)
	}
	// 30-70 s is a gap the samples do not vouch for (the 30 s one is valid until 50 s).
	if e, err = store.HostCPUEnergy(ctx, "h", t0.Add(25*time.Second), t0.Add(70*time.Second)); err != nil || e.SecondsCovered != 5 {
		t.Fatalf("energy over a gap: %+v %v", e, err)
	}
	if _, err = store.HostCPUEnergy(ctx, "h", t0.Add(time.Hour), t0.Add(2*time.Hour)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("no samples: %v", err)
	}
}

func TestPolicyDevicesShowTheLeaseAndItsUsage(t *testing.T) {
	store := openCoordinationStore(t)
	setupHost(t, store)
	ctx := context.Background()
	launch := startShared(t, store, "train", "a", 0)
	// What the GPU was seen doing since the attempt began.
	stamp := time.Now().UTC().Add(time.Second)
	total, free, util, mem := int64(12<<30), int64(2<<30), 98.0, 41.0
	for i := range 3 {
		at := stamp.Add(time.Duration(i) * time.Second)
		observation := Observation{ResourceID: "gpu-GPU-S", ObservedAt: at.Format(time.RFC3339Nano), ValidUntil: at.Add(time.Minute).Format(time.RFC3339Nano),
			TotalBytes: &total, FreeBytes: &free, Utilization: &util, Device: &DeviceSample{MemUtilization: &mem}}
		if err := store.ApplyObservationBatch(ctx, "a-gpu", []ResourceInstance{{ID: "gpu-GPU-S", NodeID: "a", ProviderID: "a-gpu", Kind: "gpu", StableIdentity: "GPU-S", Binding: json.RawMessage(`{}`)}},
			[]Observation{observation}, nil); err != nil {
			t.Fatal(err)
		}
	}
	devices, err := store.PolicyDevices(ctx, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	var gpu PolicyDevice
	for _, d := range devices {
		if d.DeviceID == "GPU-S" {
			gpu = d
		}
	}
	if gpu.Lease == nil || gpu.Lease.LeaseID != launch.Lease.ID || gpu.Lease.AttemptID != launch.Attempt.ID || gpu.Lease.Bench ||
		len(gpu.Usage.UtilGPUPct) != 3 || len(gpu.Usage.UtilMemPct) != 3 || gpu.Usage.UtilMemPct[0] != 41 {
		t.Fatalf("gpu: %+v", gpu)
	}
	// A policy setting of the lease, its decision, and what it saved.
	if _, err = store.SetDeviceSetting(ctx, "h", "GPU-S", DeviceOwner{Kind: OwnerPolicy, Ref: launch.Lease.ID}, DeviceSettings{GraphicsClockMaxMHz: intp(1800)}, "policy"); err != nil {
		t.Fatal(err)
	}
	if err = store.RecordPolicyDecision(ctx, PolicyDecision{HostID: "h", DeviceID: "GPU-S", LeaseID: launch.Lease.ID, Basis: BasisFallback, Settings: DeviceSettings{GraphicsClockMaxMHz: intp(1800)}}); err != nil {
		t.Fatal(err)
	}
	if err = store.AccruePolicySavings(ctx, launch.Attempt.ID, 60, ptr(3000.0)); err != nil {
		t.Fatal(err)
	}
	if err = store.AccruePolicySavings(ctx, launch.Attempt.ID, 60, nil); err != nil {
		t.Fatal(err)
	}
	var seconds, saved, unestimated float64
	if err = store.db.QueryRow(`SELECT policy_seconds,policy_saved_joules,policy_unestimated_seconds FROM attempt_energy WHERE attempt_id=$1`, launch.Attempt.ID).Scan(&seconds, &saved, &unestimated); err != nil ||
		seconds != 120 || saved != 3000 || unestimated != 60 {
		t.Fatalf("savings: %v %v %v %v", seconds, saved, unestimated, err)
	}
	hardware, err := store.ListHardware(ctx, "h")
	if err != nil || hardware[0].Devices[0].Policy == nil || hardware[0].Devices[0].Policy.Basis != BasisFallback || eventCount(t, store, "device_policy_decision") != 1 {
		t.Fatalf("hardware: %+v %v", hardware[0].Devices[0], err)
	}
}

func TestStorageAndNetworkBenchesNeedNoControllerAndHoldTheirDevice(t *testing.T) {
	store := openCoordinationStore(t)
	setupHost(t, store)
	ctx := context.Background()
	benchNode(t, store, "a", false)
	benchNode(t, store, "b", false)
	// a's filesystem provider reports /srv.
	if err := store.UpsertProvider(ctx, "a-fs", "a", "filesystem", nil); err != nil {
		t.Fatal(err)
	}
	stamp := time.Now().UTC()
	total, free := int64(500<<30), int64(200<<30)
	if err := store.ApplyObservationBatch(ctx, "a-fs", []ResourceInstance{{ID: "a-disk-srv", NodeID: "a", ProviderID: "a-fs", Kind: "disk", StableIdentity: "/srv", Binding: json.RawMessage(`{}`)}},
		[]Observation{{ResourceID: "a-disk-srv", ObservedAt: stamp.Format(time.RFC3339Nano), ValidUntil: stamp.Add(time.Minute).Format(time.RFC3339Nano), TotalBytes: &total, FreeBytes: &free}}, nil); err != nil {
		t.Fatal(err)
	}
	// Neither needs the controller: without one they are still planned.
	host := testHost
	host.Controller = ""
	if _, err := store.ApplyMachines(ctx, hostSpec(host), false); err != nil {
		t.Fatal(err)
	}
	plan, err := store.CreateBenchRun(ctx, BenchRequest{HostID: "h", DeviceID: "disk:/srv", Kinds: []string{"disk-rand"}, DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Run.Class != BenchStorage || plan.Run.Target != "/srv" || plan.Run.NodeID != "a" || !slices.Equal(plan.Run.Kinds, []string{"disk-rand-read", "disk-rand-read-qd1", "disk-rand-write"}) ||
		len(plan.Execution.Capacity.Disks) != 1 || plan.Execution.Capacity.Disks[0].Filesystem != "/srv" {
		t.Fatalf("storage plan: %+v", plan)
	}
	plan, err = store.CreateBenchRun(ctx, BenchRequest{HostID: "h", DeviceID: "net:a", Node: "b"})
	if err != nil {
		t.Fatal(err)
	}
	g := plan.Execution.Gang
	if plan.Run.Class != BenchNetwork || plan.Run.NodeID != "b" || *plan.Run.PeerNodeID != "a" || g == nil || g.Size != 2 || len(g.Ranks) != 2 ||
		!strings.Contains(string(g.Ranks[0].ExecutorSelector), `"node":"b"`) || !strings.Contains(string(g.Ranks[1].ExecutorSelector), `"node":"a"`) {
		t.Fatalf("network plan: %+v", plan)
	}
	// Both ranks' executions belong to the run.
	var n int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM bench_run_executions WHERE run_id=$1`, plan.Run.ID).Scan(&n); err != nil || n != 2 {
		t.Fatalf("run executions: %d %v", n, err)
	}
	if _, err = store.CreateBenchRun(ctx, BenchRequest{HostID: "h", DeviceID: "net:a", Node: "a"}); err == nil {
		t.Fatal("a network bench from a node to itself was planned")
	}
}
