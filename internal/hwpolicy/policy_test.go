package hwpolicy

import (
	"context"
	"io"
	"log/slog"
	"slices"
	"testing"
	"time"

	"kairo/internal/machine"
	"kairo/internal/store"
)

// fakeStore keeps one view of the devices, applies the policy's settings to
// it (and its controller applies them at once), and records the rest.
type fakeStore struct {
	devices   []store.PolicyDevice
	runs      map[string][]store.BenchRun
	sets      []store.DeviceSettings
	clears    int
	decisions []store.PolicyDecision
	savings   []float64 // joules, -1 for an unestimated accrual
	seconds   float64
}

func (f *fakeStore) PolicyDevices(context.Context, time.Duration) ([]store.PolicyDevice, error) {
	return slices.Clone(f.devices), nil
}

func (f *fakeStore) ListBenchRuns(_ context.Context, _, deviceID string, _ bool) ([]store.BenchRun, error) {
	return f.runs[deviceID], nil
}

func (f *fakeStore) device(id string) *store.PolicyDevice {
	for i := range f.devices {
		if f.devices[i].DeviceID == id {
			return &f.devices[i]
		}
	}
	panic("no device " + id)
}

func (f *fakeStore) SetDeviceSetting(_ context.Context, _, deviceID string, owner store.DeviceOwner, settings store.DeviceSettings, _ string) (store.DeviceSettingResult, error) {
	f.sets = append(f.sets, settings)
	d := f.device(deviceID)
	setting := store.DeviceSetting{DeviceID: deviceID, Owner: owner, Settings: settings}
	d.Settings, d.Effective = []store.DeviceSetting{setting}, &setting
	d.Generation++
	d.AppliedGeneration, d.Applied = &d.Generation, &setting.Settings
	return store.DeviceSettingResult{}, nil
}

func (f *fakeStore) ClearDeviceSetting(_ context.Context, _, deviceID string, _ store.DeviceOwner, _ string) (store.DeviceSettingResult, error) {
	f.clears++
	d := f.device(deviceID)
	d.Settings, d.Effective = nil, nil
	d.Generation++
	d.AppliedGeneration, d.Applied = &d.Generation, &store.DeviceSettings{}
	return store.DeviceSettingResult{}, nil
}

func (f *fakeStore) RecordPolicyDecision(_ context.Context, d store.PolicyDecision) error {
	f.decisions = append(f.decisions, d)
	f.device(d.DeviceID).Policy = &d
	return nil
}

func (f *fakeStore) AccruePolicySavings(_ context.Context, _ string, seconds float64, saved *float64) error {
	f.seconds += seconds
	if saved == nil {
		f.savings = append(f.savings, -1)
	} else {
		f.savings = append(f.savings, *saved)
	}
	return nil
}

func (f *fakeStore) ReconcileBenchRuns(context.Context) error { return nil }

var ctl = "ctl"

func gpuDevice(slowdown float64) store.PolicyDevice {
	d := store.PolicyDevice{Lease: &store.PolicyLease{LeaseID: "lease-1", AttemptID: "att-1", ActivatedAt: "2026-10-27T10:00:00Z"}}
	d.DevicePolicy = machine.DevicePolicy{HostID: "h", DeviceID: "GPU-1", Kind: "gpu", MaxSlowdown: slowdown, Control: true, Controller: &ctl}
	d.Capabilities = &store.DeviceCapabilities{Settings: []string{store.SettingGraphicsClockMax}, GraphicsClocksMHz: []int{210, 1500, 1800, 1900, 2000}}
	return d
}

func cpuDevice(slowdown float64) store.PolicyDevice {
	low, high := 800, 5000
	d := store.PolicyDevice{}
	d.DevicePolicy = machine.DevicePolicy{HostID: "h", DeviceID: "cpu", Kind: "cpu", MaxSlowdown: slowdown, Control: true, Controller: &ctl}
	d.Capabilities = &store.DeviceCapabilities{Settings: []string{store.SettingMaxFreq}, MaxFreqMinMHz: &low, MaxFreqMaxMHz: &high}
	return d
}

// clock steps p's clock by minutes.
type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }

func newPolicy(f *fakeStore) (*Policy, *clock) {
	c := &clock{t: time.Date(2026, 10, 27, 10, 0, 0, 0, time.UTC)}
	return &Policy{Store: f, Now: c.now, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, c
}

func (p *Policy) steps(c *clock, n int) {
	for range n {
		p.Step(context.Background())
		c.t = c.t.Add(time.Minute)
	}
}

// profileRun is a finished bench of kinds, each with a reference and points
// at 1800 MHz (rel, W) and 1500 MHz.
func profileRun(id string, kinds map[string][2][2]float64, fingerprints map[string]store.Fingerprint) store.BenchRun {
	run := store.BenchRun{ID: id, State: "finished", References: map[string]store.BenchReference{}}
	seq := 0
	for kind, at := range kinds {
		fp := fingerprints[kind]
		run.References[kind] = store.BenchReference{Valid: true, Mean: 100, PowerW: new(300.0), JoulesPerWork: new(3.0), Fingerprint: &fp}
		add := func(settings store.DeviceSettings, rel, watts float64) {
			seq++
			jpw := watts / (100 * rel)
			run.Points = append(run.Points, store.BenchPoint{Seq: seq, Kind: kind, Settings: settings, Valid: true,
				Metrics: store.BenchMetrics{RelPerf: &rel, PowerW: &watts, JoulesPerWork: &jpw}})
		}
		add(store.DeviceSettings{}, 1, 300)
		for i, mhz := range []int{1800, 1500} {
			clock := mhz
			if kind == "cpu-compute" || kind == "cpu-single" || kind == "cpu-stream" {
				add(store.DeviceSettings{MaxFreqMHz: &clock}, at[i][0], at[i][1])
			} else {
				add(store.DeviceSettings{GraphicsClockMaxMHz: &clock}, at[i][0], at[i][1])
			}
		}
	}
	return run
}

func TestNewChoiceWaitsThreeEvaluationsAndFiveMinutes(t *testing.T) {
	f := &fakeStore{devices: []store.PolicyDevice{gpuDevice(0.1)}, runs: map[string][]store.BenchRun{}}
	p, c := newPolicy(f)
	// No profile: the fallback cap of 0.9 x 2000 MHz, after three evaluations.
	p.steps(c, 2)
	if len(f.sets) != 0 {
		t.Fatalf("changed before three evaluations: %+v", f.sets)
	}
	p.steps(c, 1)
	if len(f.sets) != 1 || *f.sets[0].GraphicsClockMaxMHz != 1800 || f.decisions[0].Basis != store.BasisFallback || f.decisions[0].LeaseID != "lease-1" {
		t.Fatalf("fallback: %+v %+v", f.sets, f.decisions)
	}
	// A profile appears: gemm-like usage, gemm saves the most at 1500 MHz.
	f.runs["GPU-1"] = []store.BenchRun{profileRun("bnc_1", map[string][2][2]float64{"gemm": {{0.98, 250}, {0.93, 200}}},
		map[string]store.Fingerprint{"gemm": {UtilGPUPct: 99, UtilMemPct: 40}})}
	f.device("GPU-1").Usage = store.PolicyUsage{UtilGPUPct: []float64{99, 98, 99, 99, 98}, UtilMemPct: []float64{40, 41, 39, 42, 40}}
	// Stable from the next evaluation (3 min after the change), applied once
	// five minutes have passed since it.
	for range 4 {
		p.steps(c, 1)
		if len(f.sets) != 1 {
			t.Fatalf("changed again within five minutes at %s: %+v", c.t, f.sets)
		}
	}
	p.steps(c, 1)
	if len(f.sets) != 2 || *f.sets[1].GraphicsClockMaxMHz != 1500 || f.decisions[1].Basis != store.BasisProfile || !slices.Equal(f.decisions[1].Candidates, []string{"gemm"}) {
		t.Fatalf("profile choice: %+v %+v", f.sets, f.decisions)
	}
	if e := f.decisions[1].Expected; len(e) != 1 || e[0].RelPerf != 0.93 || *e[0].SavingW != 0.93*300-200 {
		t.Fatalf("expected: %+v", e)
	}
	// It stays: nothing more changes.
	p.steps(c, 10)
	if len(f.sets) != 2 {
		t.Fatalf("a stable choice changed: %+v", f.sets)
	}
	// In place and applied: its savings accrue per evaluation (60 s at 79 W).
	if last := f.savings[len(f.savings)-1]; last < 60*(0.93*300-200)-1e-6 || last > 60*(0.93*300-200)+1e-6 {
		t.Fatalf("savings: %v", f.savings)
	}
}

func TestChoiceFlappingNeverApplies(t *testing.T) {
	f := &fakeStore{devices: []store.PolicyDevice{gpuDevice(0.1)}, runs: map[string][]store.BenchRun{}}
	f.runs["GPU-1"] = []store.BenchRun{profileRun("bnc_1", map[string][2][2]float64{"gemm": {{0.98, 250}, {0.93, 200}}},
		map[string]store.Fingerprint{"gemm": {UtilGPUPct: 99, UtilMemPct: 40}})}
	p, c := newPolicy(f)
	gemmLike := store.PolicyUsage{UtilGPUPct: []float64{99, 98, 99, 99, 98}, UtilMemPct: []float64{40, 41, 39, 42, 40}}
	for i := range 12 {
		// the usage is clear every other evaluation: profile, fallback, ...
		if i%2 == 0 {
			f.device("GPU-1").Usage = gemmLike
		} else {
			f.device("GPU-1").Usage = store.PolicyUsage{}
		}
		p.steps(c, 1)
	}
	if len(f.sets) != 0 {
		t.Fatalf("a flapping choice was applied: %+v", f.sets)
	}
}

func TestLeasesBenchesAndPolicyOff(t *testing.T) {
	f := &fakeStore{devices: []store.PolicyDevice{gpuDevice(0.1)}, runs: map[string][]store.BenchRun{}}
	p, c := newPolicy(f)
	p.steps(c, 2)
	// A new lease: what was pending for the old one starts over.
	f.device("GPU-1").Lease = &store.PolicyLease{LeaseID: "lease-2", AttemptID: "att-2", ActivatedAt: "2026-10-27T10:05:00Z"}
	p.steps(c, 2)
	if len(f.sets) != 0 {
		t.Fatalf("changed for a new lease within three evaluations: %+v", f.sets)
	}
	p.steps(c, 1)
	if len(f.sets) != 1 || f.decisions[0].LeaseID != "lease-2" {
		t.Fatalf("new lease: %+v %+v", f.sets, f.decisions)
	}
	// A bench's lease, and a lease whose attempt has not started, are left alone.
	for _, lease := range []*store.PolicyLease{{LeaseID: "bench", AttemptID: "att-b", ActivatedAt: "x", Bench: true}, {LeaseID: "lease-3"}, nil} {
		f.devices = []store.PolicyDevice{gpuDevice(0.1)}
		f.device("GPU-1").Lease = lease
		before := len(f.sets)
		p.steps(c, 8)
		if len(f.sets) != before {
			t.Fatalf("lease %+v got a policy setting", lease)
		}
	}
	// max_slowdown 0 with a policy setting left: cleared at once.
	d := gpuDevice(0)
	d.Settings = []store.DeviceSetting{{Owner: store.DeviceOwner{Kind: store.OwnerPolicy, Ref: "lease-1"}}}
	f.devices = []store.PolicyDevice{d}
	p.steps(c, 1)
	if f.clears != 1 || f.decisions[len(f.decisions)-1].Basis != store.BasisDefault {
		t.Fatalf("policy off: %d clears, %+v", f.clears, f.decisions)
	}
}

func TestCPUSuitsEveryKindAndYieldsToItsBench(t *testing.T) {
	f := &fakeStore{devices: []store.PolicyDevice{cpuDevice(0.05)}, runs: map[string][]store.BenchRun{}}
	p, c := newPolicy(f)
	// cpu-stream (memory-bound) loses little at 1500 MHz, cpu-compute too
	// much: the worst case takes 1800.
	f.runs["cpu"] = []store.BenchRun{profileRun("bnc_c", map[string][2][2]float64{
		"cpu-compute": {{0.97, 200}, {0.80, 150}},
		"cpu-single":  {{0.96, 210}, {0.79, 160}},
		"cpu-stream":  {{0.99, 220}, {0.97, 180}},
	}, nil)}
	p.steps(c, 3)
	if len(f.sets) != 1 || *f.sets[0].MaxFreqMHz != 1800 || len(f.decisions[0].Candidates) != 3 {
		t.Fatalf("cpu: %+v %+v", f.sets, f.decisions)
	}
	// The setting belongs to the host.
	if owner := f.device("cpu").Effective.Owner; owner.Ref != store.HostPolicyRef {
		t.Fatalf("owner %+v", owner)
	}
	// A bench of the host's CPU: the policy setting goes at once.
	f.device("cpu").Bench = true
	p.steps(c, 1)
	if f.clears != 1 || f.device("cpu").Effective != nil {
		t.Fatalf("bench: %d clears", f.clears)
	}
}

func TestDecideFallsBackWithoutAConfidentProfile(t *testing.T) {
	d := gpuDevice(0.1)
	run := profileRun("bnc_1", map[string][2][2]float64{"gemm": {{0.98, 250}, {0.93, 200}}, "copy": {{0.999, 280}, {0.995, 270}}},
		map[string]store.Fingerprint{"gemm": {UtilGPUPct: 99, UtilMemPct: 40}, "copy": {UtilGPUPct: 99, UtilMemPct: 95}})
	// No usage yet: the fallback.
	if decision := Decide(d, []store.BenchRun{run}); decision.Basis != store.BasisFallback || *decision.Settings.GraphicsClockMaxMHz != 1800 {
		t.Fatalf("no usage: %+v", decision)
	}
	// Usage halfway between gemm and copy is like neither: the fallback.
	d.Usage = store.PolicyUsage{UtilGPUPct: []float64{99, 99, 99, 99, 99}, UtilMemPct: []float64{67, 68, 67, 68, 67}}
	if decision := Decide(d, []store.BenchRun{run}); decision.Basis != store.BasisFallback || decision.Reason == "" {
		t.Fatalf("matching neither closely: %+v", decision)
	}
	d.Usage.UtilMemPct = []float64{55, 54, 56, 55, 55}
	decision := Decide(d, []store.BenchRun{run})
	if decision.Basis != store.BasisProfile || !slices.Equal(decision.Candidates, []string{"gemm"}) || *decision.Settings.GraphicsClockMaxMHz != 1500 {
		t.Fatalf("gemm: %+v", decision)
	}
	// A CPU profile missing a kind is not the worst case of every kind.
	cpu := cpuDevice(0.1)
	partial := profileRun("bnc_c", map[string][2][2]float64{"cpu-compute": {{0.97, 200}, {0.90, 150}}}, nil)
	if decision := Decide(cpu, []store.BenchRun{partial}); decision.Basis != store.BasisFallback || *decision.Settings.MaxFreqMHz != 4500 {
		t.Fatalf("partial cpu profile: %+v", decision)
	}
	// Nothing to cap from (Windows): the defaults.
	cpu.Capabilities = &store.DeviceCapabilities{Settings: []string{store.SettingMaxFreq}}
	if decision := Decide(cpu, nil); decision.Basis != store.BasisDefault || !decision.Settings.IsDefault() {
		t.Fatalf("no range: %+v", decision)
	}
}

func TestAPartiallyValidRunGivesItsValidKindsCurves(t *testing.T) {
	d := gpuDevice(0.1)
	// gemm's references spread too much; copy's agree.
	run := profileRun("bnc_1", map[string][2][2]float64{"gemm": {{0.98, 250}, {0.93, 200}}, "copy": {{0.999, 280}, {0.92, 230}}},
		map[string]store.Fingerprint{"gemm": {UtilGPUPct: 99, UtilMemPct: 40}, "copy": {UtilGPUPct: 99, UtilMemPct: 95}})
	run.State = store.BenchPartial
	gemm := run.References["gemm"]
	gemm.Valid, gemm.Reason = false, "references spread 2.3% (more than 1%)"
	run.References["gemm"] = gemm
	// Copy-like work: copy's curve is used.
	d.Usage = store.PolicyUsage{UtilGPUPct: []float64{99, 99, 99, 99, 99}, UtilMemPct: []float64{95, 94, 96, 95, 95}}
	decision := Decide(d, []store.BenchRun{run})
	if decision.Basis != store.BasisProfile || !slices.Equal(decision.Candidates, []string{"copy"}) || *decision.Settings.GraphicsClockMaxMHz != 1500 || !slices.Equal(decision.RunIDs, []string{"bnc_1"}) {
		t.Fatalf("copy-like work: %+v", decision)
	}
	// Gemm-like work matches no valid curve: the fallback.
	d.Usage.UtilMemPct = []float64{40, 41, 39, 40, 40}
	if decision = Decide(d, []store.BenchRun{run}); decision.Basis != store.BasisFallback || decision.Reason != "no benched kind matches the workload's usage" {
		t.Fatalf("gemm-like work: %+v", decision)
	}
	// A run that failed (its task ended without its results) gives nothing.
	run.State = store.BenchFailed
	d.Usage.UtilMemPct = []float64{95, 94, 96, 95, 95}
	if decision = Decide(d, []store.BenchRun{run}); decision.Basis != store.BasisFallback || decision.Reason != "no valid bench profile" {
		t.Fatalf("failed run: %+v", decision)
	}
}
