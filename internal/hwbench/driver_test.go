package hwbench

import (
	"context"
	"io"
	"log/slog"
	"math"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"kairo/internal/store"
)

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func gpuCaps() *store.DeviceCapabilities {
	var clocks []int
	for c := 1000; c <= 2000; c += 50 {
		clocks = append(clocks, c)
	}
	low, high, def := 100.0, 350.0, 300.0
	return &store.DeviceCapabilities{Settings: []string{store.SettingGraphicsClockMax, store.SettingMemoryClock, store.SettingPowerLimit},
		GraphicsClocksMHz: clocks, MemoryClocksMHz: []int{5001, 9501}, PowerLimitMinW: &low, PowerLimitMaxW: &high, PowerLimitDefaultW: &def}
}

func gpuRun(kinds ...string) store.BenchRun {
	return store.BenchRun{ID: "bnc-1", HostID: "h", DeviceID: "GPU-1", Class: store.BenchGPU, Kinds: kinds, NodeID: "a", State: "waiting"}
}

func TestGPUSweepMeasuresTheGridBetweenReferences(t *testing.T) {
	d := newFakeDaemon(gpuRun("gemm"), gpuCaps())
	d.applyAfter = 2
	r := &fakeRunner{d: d}
	if err := testDriver(d, r).Run(context.Background(), "bnc-1"); err != nil {
		t.Fatal(err)
	}
	argv := strings.Join(r.argvs[0], " ")
	if !strings.HasPrefix(argv, "/opt/kairo/kairo-bench-gpu gemm --device GPU-1 --seconds ") || !strings.Contains(argv, "--interval 1") {
		t.Fatalf("argv: %s", argv)
	}
	if d.finish == nil || !d.finish.Valid || d.finish.Reason != "" {
		t.Fatalf("finish: %+v", d.finish)
	}
	ref := d.finish.References["gemm"]
	if !ref.Valid || len(ref.WorkPerSecond) != 3 || ref.Spread != 0 || ref.Mean != 1e12 || ref.PowerW == nil || math.Abs(*ref.PowerW-300) > 1e-6 ||
		ref.JoulesPerWork == nil || ref.Fingerprint == nil || ref.Fingerprint.UtilGPUPct != 98 || ref.Unit != "flop" {
		t.Fatalf("reference: %+v", ref)
	}
	points := d.sortedPoints()
	// 11 graphics caps, 2 memory clocks, 4 power limits and 3 references
	if len(points) != 20 {
		t.Fatalf("%d points: %+v", len(points), points)
	}
	phases := ""
	for _, p := range points {
		phases += p.Phase[:1]
		if !p.Valid || p.Metrics.RelPerf == nil || p.Metrics.JoulesPerWork == nil || p.Metrics.EnergySource != "nvml" || math.Abs(p.Metrics.Seconds-30) > 1e-9 {
			t.Fatalf("point: %+v", p)
		}
	}
	// the references open the sweep, split it in half and close it
	if phases != "r"+strings.Repeat("s", 8)+"r"+strings.Repeat("s", 9)+"r" {
		t.Fatalf("phases %s", phases)
	}
	half := points[6] // graphics clock cap 1500 MHz: 75% of the work
	if *half.Settings.GraphicsClockMaxMHz != 1500 || math.Abs(*half.Metrics.RelPerf-0.75) > 1e-9 || math.Abs(*half.Metrics.PowerW-300*0.75*0.75) > 1e-6 {
		t.Fatalf("1500 MHz point: %+v", half)
	}
	// the setting is cleared at the end, and the device's facts recorded
	if last := d.sets[len(d.sets)-1]; !last.IsDefault() {
		t.Fatalf("last setting %s", last)
	}
	if len(d.starts) < 2 || d.starts[0].IdleTemperatureC == nil || *d.starts[0].IdleTemperatureC != 31 || d.starts[1].DriverVersion != "580.00" ||
		d.starts[1].Versions["gemm"].Program != "kairo-bench-gpu" || d.starts[1].DriverBuild == "" {
		t.Fatalf("starts: %+v", d.starts)
	}
	// points are uploaded as measured, then again with the final reference
	if d.uploads != 40 {
		t.Fatalf("%d uploads", d.uploads)
	}
}

func TestSettingNeverAppliedInvalidatesItsPoints(t *testing.T) {
	d := newFakeDaemon(gpuRun("copy"), gpuCaps())
	d.applyAfter = -1
	if err := testDriver(d, &fakeRunner{d: d}).Run(context.Background(), "bnc-1"); err != nil {
		t.Fatal(err)
	}
	points := d.sortedPoints()
	if len(points) != 16 || !points[0].Valid {
		t.Fatalf("points: %+v", points)
	}
	for _, p := range points[1:] {
		if p.Valid || !strings.HasPrefix(p.Flags[0], FlagNotApplied) {
			t.Fatalf("point %d: %+v", p.Seq, p)
		}
	}
	if d.finish == nil || d.finish.Valid || d.finish.References["copy"].Valid || !strings.Contains(d.finish.Reason, "copy: reference") {
		t.Fatalf("finish: %+v", d.finish)
	}
}

func TestThermalSlowdownAndOtherWorkInvalidatePoints(t *testing.T) {
	d := newFakeDaemon(gpuRun("copy"), gpuCaps())
	r := &fakeRunner{d: d, thermal: true}
	if err := testDriver(d, r).Run(context.Background(), "bnc-1"); err != nil {
		t.Fatal(err)
	}
	// A capped point held below its cap by heat says nothing about the cap; a
	// reference (uncapped) measures what the defaults do with the heat.
	for _, p := range d.sortedPoints() {
		capped := p.Settings.GraphicsClockMaxMHz != nil
		if capped && (p.Valid || !strings.HasPrefix(p.Flags[0], FlagThermal)) || !capped && slices.ContainsFunc(p.Flags, func(f string) bool { return strings.HasPrefix(f, FlagThermal) }) {
			t.Fatalf("point %d: %+v", p.Seq, p)
		}
	}
	d = newFakeDaemon(gpuRun("copy"), gpuCaps())
	// a process appears on the GPU after the first reference
	d.onPoll = func(polls int, status *store.BenchStatus) {
		if polls == 4 {
			status.OtherWork = []string{"external_process on a (pid 77)"}
		}
	}
	if err := testDriver(d, &fakeRunner{d: d}).Run(context.Background(), "bnc-1"); err != nil {
		t.Fatal(err)
	}
	points := d.sortedPoints()
	if !points[0].Valid || points[1].Valid || !slices.Contains(points[1].Flags, FlagOtherWork+": external_process on a (pid 77)") {
		t.Fatalf("points: %+v %+v", points[0], points[1])
	}
}

func TestCPUBenchTakesItsEnergyFromTheHostsPackage(t *testing.T) {
	minFreq, maxFreq := 800, 5000
	caps := &store.DeviceCapabilities{Settings: []string{store.SettingMaxFreq}, MaxFreqMinMHz: &minFreq, MaxFreqMaxMHz: &maxFreq}
	run := store.BenchRun{ID: "bnc-2", HostID: "h", DeviceID: "cpu", Class: store.BenchCPU, Kinds: []string{"cpu-compute"}, NodeID: "a", State: "waiting"}
	d := newFakeDaemon(run, caps)
	d.status.TemperatureC = nil
	r := &fakeRunner{d: d}
	if err := testDriver(d, r).Run(context.Background(), "bnc-2"); err != nil {
		t.Fatal(err)
	}
	if argv := strings.Join(r.argvs[0], " "); !strings.HasPrefix(argv, "/opt/kairo/kairo bench run cpu-compute --seconds ") {
		t.Fatalf("argv: %s", argv)
	}
	points := d.sortedPoints()
	if len(points) != 14 || d.energies != 14 || !d.finish.Valid {
		t.Fatalf("%d points, %d energies, finish %+v", len(points), d.energies, d.finish)
	}
	for _, p := range points {
		if p.Metrics.EnergySource != "cpu_package" || p.Metrics.PowerW == nil || *p.Metrics.PowerW != 100 || p.Metrics.JoulesPerWork == nil {
			t.Fatalf("point: %+v", p.Metrics)
		}
	}
	// the 50% cap halves the work at the same power: twice the energy per op
	low := points[len(points)-2]
	if *low.Settings.MaxFreqMHz != 2500 || math.Abs(*low.Metrics.RelPerf-0.5) > 1e-9 || math.Abs(*low.Metrics.JoulesPerWork / *points[0].Metrics.JoulesPerWork - 2) > 1e-9 {
		t.Fatalf("2500 MHz point: %+v", low)
	}
}

func TestStorageKindIsMeasuredOnceAtTheDefaults(t *testing.T) {
	run := store.BenchRun{ID: "bnc-3", HostID: "h", DeviceID: "disk:/srv", Class: store.BenchStorage, Kinds: []string{"disk-seq-read", "disk-sustained"}, NodeID: "a", Target: "/srv", State: "waiting"}
	d := newFakeDaemon(run, nil)
	r := &fakeRunner{d: d, limit: 45}
	if err := testDriver(d, r).Run(context.Background(), "bnc-3"); err != nil {
		t.Fatal(err)
	}
	if argv := strings.Join(r.argvs[0], " "); !strings.Contains(argv, "bench run disk-seq ") || !strings.HasSuffix(argv, "--mode read --qd 32 --path /srv") {
		t.Fatalf("argv: %s", argv)
	}
	if len(d.sets) != 0 {
		t.Fatalf("a storage bench changed settings: %+v", d.sets)
	}
	points := d.sortedPoints()
	if len(points) != 2 || points[0].Phase != store.PhaseMeasure || !points[0].Valid || points[0].Metrics.WorkPerSecond != 2e9 || *points[0].Metrics.LatencyP99NS != 5000 {
		t.Fatalf("points: %+v", points)
	}
	// disk-sustained measures up to its end (its write limit), after the settle
	if !points[1].Valid || points[1].Metrics.Seconds != 40 || !d.finish.Valid || !d.finish.References["disk-sustained"].Valid {
		t.Fatalf("sustained: %+v finish %+v", points[1], d.finish)
	}
}

func TestPrimitiveEndingEarlyInvalidatesTheKind(t *testing.T) {
	run := store.BenchRun{ID: "bnc-3", HostID: "h", DeviceID: "disk:/srv", Class: store.BenchStorage, Kinds: []string{"disk-seq-write"}, NodeID: "a", Target: "/srv", State: "waiting"}
	d := newFakeDaemon(run, nil)
	if err := testDriver(d, &fakeRunner{d: d, limit: 20}).Run(context.Background(), "bnc-3"); err != nil {
		t.Fatal(err)
	}
	if d.finish.Valid || !strings.Contains(d.finish.Reason, "disk-seq-write") {
		t.Fatalf("finish: %+v", d.finish)
	}
}

func TestOtherWorkThatStaysEndsTheRunInvalid(t *testing.T) {
	run := store.BenchRun{ID: "bnc-4", HostID: "h", DeviceID: "net:b", Class: store.BenchNetwork, Kinds: []string{"net-latency"}, NodeID: "a", Target: "b", State: "waiting"}
	d := newFakeDaemon(run, nil)
	d.status.Rank = 1
	d.status.OtherWork = []string{"gang gng-1 spans a and b"}
	driver := testDriver(d, &fakeRunner{d: d})
	driver.Timing.OtherWorkWait = 0
	if err := driver.Run(context.Background(), "bnc-4"); err != nil {
		t.Fatal(err)
	}
	if d.finish == nil || d.finish.Valid || !strings.Contains(d.finish.Reason, "gang gng-1") {
		t.Fatalf("finish: %+v", d.finish)
	}
}

func TestNetworkClientReachesRankZero(t *testing.T) {
	run := store.BenchRun{ID: "bnc-4", HostID: "h", DeviceID: "net:b", Class: store.BenchNetwork, Kinds: []string{"net-bandwidth-send"}, NodeID: "a", Target: "b", State: "waiting"}
	d := newFakeDaemon(run, nil)
	d.status.Rank = 1
	r := &fakeRunner{d: d}
	driver := testDriver(d, r)
	driver.Env = func(key string) string {
		return map[string]string{"KAIRO_GANG_MASTER_ADDR": "10.0.0.1", "KAIRO_GANG_MASTER_PORT": "29511"}[key]
	}
	dials := 0
	driver.Dial = func(_ context.Context, addr string) error {
		if dials++; addr != "10.0.0.1:29511" || dials < 3 {
			return io.ErrUnexpectedEOF
		}
		return nil
	}
	if err := driver.Run(context.Background(), "bnc-4"); err != nil {
		t.Fatal(err)
	}
	if argv := strings.Join(r.argvs[0], " "); !strings.HasSuffix(argv, "--dir send --streams 4 --peer 10.0.0.1:29511") || !d.finish.Valid {
		t.Fatalf("argv %s, finish %+v", argv, d.finish)
	}
}

// serveProcess is a bench server that runs until stopped.
type serveProcess struct {
	once    sync.Once
	stopped chan struct{}
}

func (p *serveProcess) Next() (Record, error) {
	<-p.stopped
	return Record{}, io.EOF
}
func (p *serveProcess) Stop() error { p.once.Do(func() { close(p.stopped) }); return nil }
func (p *serveProcess) Wait() error { return nil }

type serveRunner struct{ argv []string }

func (r *serveRunner) Start(_ context.Context, argv []string) (Process, error) {
	r.argv = argv
	return &serveProcess{stopped: make(chan struct{})}, nil
}

func TestNetworkServerRunsUntilTheRunEnds(t *testing.T) {
	run := store.BenchRun{ID: "bnc-4", HostID: "h", DeviceID: "net:b", Class: store.BenchNetwork, Kinds: []string{"net-latency"}, NodeID: "a", Target: "b", State: "running"}
	d := newFakeDaemon(run, nil)
	d.onPoll = func(polls int, status *store.BenchStatus) {
		if polls == 3 {
			status.Run.State = "finished"
		}
	}
	r := &serveRunner{}
	driver := testDriver(d, r)
	driver.Timing.Poll = time.Millisecond
	driver.Env = func(key string) string {
		return map[string]string{"KAIRO_GANG_MASTER_ADDR": "10.0.0.1", "KAIRO_GANG_MASTER_PORT": "29511"}[key]
	}
	if err := driver.Run(context.Background(), "bnc-4"); err != nil {
		t.Fatal(err)
	}
	if strings.Join(r.argv, " ") != "/opt/kairo/kairo bench serve --listen 10.0.0.1:29511 --idle-timeout 3m" || d.polls != 3 || len(d.starts) != 0 {
		t.Fatalf("argv %v, %d polls, starts %+v", r.argv, d.polls, d.starts)
	}
}

func TestRunOfAnotherAttemptIsRefused(t *testing.T) {
	d := newFakeDaemon(gpuRun("gemm"), gpuCaps())
	if err := testDriver(d, &fakeRunner{d: d}).Run(context.Background(), "bnc-other"); err == nil || !strings.Contains(err.Error(), "not bnc-other") {
		t.Fatalf("err %v", err)
	}
}

func TestReferenceSpreadDecidesValidity(t *testing.T) {
	k := &kindRun{s: &stream{unit: "op"}}
	refs := func(rates ...float64) []store.BenchPoint {
		var out []store.BenchPoint
		for i, r := range rates {
			out = append(out, store.BenchPoint{Seq: i, Valid: true, Metrics: store.BenchMetrics{WorkPerSecond: r}})
		}
		return out
	}
	if ref := k.reference(refs(100, 100.4, 100.8)); !ref.Valid || math.Abs(ref.Mean-100.4) > 1e-9 {
		t.Fatalf("within 1%%: %+v", ref)
	}
	if ref := k.reference(refs(100, 100.5, 102)); ref.Valid || !strings.Contains(ref.Reason, "spread 2.0%") {
		t.Fatalf("2%% apart: %+v", ref)
	}
	bad := refs(100, 100, 100)
	bad[1].Valid, bad[1].Flags = false, []string{FlagNotApplied + ": x"}
	if ref := k.reference(bad); ref.Valid || !strings.Contains(ref.Reason, "reference 1 invalid") {
		t.Fatalf("invalid reference: %+v", ref)
	}
}
