package hwbench

import (
	"math/rand/v2"
	"slices"
	"testing"

	"kairo/internal/store"
)

func clockPoint(seq, clock int, rel, joulesPerWork, watts float64, valid bool) store.BenchPoint {
	p := store.BenchPoint{Seq: seq, Kind: "gemm", Phase: store.PhaseSweep, Valid: valid,
		Metrics: store.BenchMetrics{RelPerf: &rel, JoulesPerWork: &joulesPerWork, PowerW: &watts}}
	if clock != 0 {
		p.Settings.GraphicsClockMaxMHz = &clock
	} else {
		p.Phase = store.PhaseReference
	}
	return p
}

func TestSelectTakesTheLeastEnergyWithinTheTolerance(t *testing.T) {
	points := []store.BenchPoint{
		clockPoint(1, 0, 1.0, 10, 300, true),
		clockPoint(2, 1900, 0.97, 8, 250, true),
		clockPoint(3, 1700, 0.945, 7, 220, true), // within 0.05 thanks to the slack
		clockPoint(4, 1500, 0.90, 6, 190, true),  // too slow
		clockPoint(5, 1600, 0.99, 5, 200, false), // invalid
	}
	if p := Select(points, 0.05); p == nil || p.Seq != 3 {
		t.Fatalf("select 0.05: %+v", p)
	}
	if p := Select(points, 0.02); p == nil || p.Seq != 2 {
		t.Fatalf("select 0.02: %+v", p)
	}
	if p := Select(points, 0); p == nil || p.Seq != 1 {
		t.Fatalf("select 0: %+v", p)
	}
	if p := Select(points[3:], 0.01); p != nil {
		t.Fatalf("nothing qualifies, got %+v", p)
	}
}

func curve(kind string, ref store.BenchReference, points ...store.BenchPoint) Curve {
	for i := range points {
		points[i].Kind = kind
	}
	return Curve{Kind: kind, RunID: "bnc_" + kind, Reference: ref, Points: points}
}

func TestSelectCommonSuitsTheWorstKind(t *testing.T) {
	ref := store.BenchReference{Valid: true, JoulesPerWork: new(10.0), PowerW: new(300.0)}
	gemm := curve("gemm", ref,
		clockPoint(1, 0, 1, 10, 300, true), clockPoint(2, 1800, 0.96, 8, 240, true), clockPoint(3, 1500, 0.95, 7, 210, true))
	copyRef := store.BenchReference{Valid: true, JoulesPerWork: new(4.0), PowerW: new(250.0)}
	cp := curve("copy", copyRef,
		clockPoint(1, 0, 1, 4, 250, true), clockPoint(2, 1800, 0.99, 3.5, 220, true), clockPoint(3, 1500, 0.90, 3.0, 200, true))
	// gemm alone takes 1500 MHz; with copy, 1500 loses copy 10%: 1800 suits both.
	choice, ok := SelectCommon([]Curve{gemm}, 0.05)
	if !ok || *choice.Settings.GraphicsClockMaxMHz != 1500 {
		t.Fatalf("gemm alone: %+v %v", choice, ok)
	}
	choice, ok = SelectCommon([]Curve{gemm, cp}, 0.05)
	if !ok || choice.Settings.GraphicsClockMaxMHz == nil || *choice.Settings.GraphicsClockMaxMHz != 1800 || len(choice.Expected) != 2 {
		t.Fatalf("gemm and copy: %+v %v", choice, ok)
	}
	// saving for the same work: 0.96 x 300 W - 240 W
	if e := choice.Expected[0]; e.Kind != "gemm" || e.SavingW == nil || *e.SavingW < 47.99 || *e.SavingW > 48.01 {
		t.Fatalf("expected: %+v", choice.Expected)
	}
	// Nothing within 1%: the defaults.
	choice, ok = SelectCommon([]Curve{gemm, cp}, 0)
	if !ok || !choice.Settings.IsDefault() {
		t.Fatalf("tolerance 0: %+v %v", choice, ok)
	}
	if _, ok = SelectCommon(nil, 0.05); ok {
		t.Fatal("no curves chose something")
	}
}

func TestCurvesComeFromTheLatestRunWithValidReferences(t *testing.T) {
	rel := 1.0
	runs := []store.BenchRun{
		{ID: "old", State: "finished", References: map[string]store.BenchReference{"gemm": {Valid: true}, "copy": {Valid: true}},
			Points: []store.BenchPoint{{Kind: "gemm", Valid: true, Metrics: store.BenchMetrics{RelPerf: &rel}}, {Kind: "copy", Valid: true, Metrics: store.BenchMetrics{RelPerf: &rel}}}},
		{ID: "new", State: "invalid", References: map[string]store.BenchReference{"gemm": {Valid: true}, "copy": {Valid: false}},
			Points: []store.BenchPoint{{Kind: "gemm", Valid: true, Metrics: store.BenchMetrics{RelPerf: &rel}}, {Kind: "gemm", Valid: false, Metrics: store.BenchMetrics{RelPerf: &rel}}}},
		{ID: "running", State: "running", References: map[string]store.BenchReference{"gemm": {Valid: true}}},
	}
	curves := Curves(runs)
	if curves["gemm"].RunID != "new" || len(curves["gemm"].Points) != 1 || curves["copy"].RunID != "old" {
		t.Fatalf("curves: %+v", curves)
	}
}

func TestFallbackLosesAtMostTheToleranceOfClock(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	for range 500 {
		var clocks []int
		for c := 210 + rng.IntN(30); c <= 1500+rng.IntN(800); c += 7 + rng.IntN(20) {
			clocks = append(clocks, c)
		}
		s := rng.Float64() * 0.49
		caps := &store.DeviceCapabilities{Settings: []string{store.SettingGraphicsClockMax}, GraphicsClocksMHz: clocks}
		settings, ok := FallbackGPU(caps, s)
		highest := slices.Max(clocks)
		if !ok {
			continue // nothing to cap: the defaults lose nothing
		}
		cap := *settings.GraphicsClockMaxMHz
		if !slices.Contains(clocks, cap) || float64(highest-cap)/float64(highest) > s+1e-9 || cap >= highest {
			t.Fatalf("clocks %d..%d s=%.3f: cap %d", clocks[0], highest, s, cap)
		}
	}
	if _, ok := FallbackGPU(&store.DeviceCapabilities{Settings: []string{store.SettingPowerLimit}}, 0.1); ok {
		t.Fatal("a fallback without graphics clocks")
	}
	if _, ok := FallbackGPU(nil, 0.1); ok {
		t.Fatal("a fallback without capabilities")
	}
}

func TestFallbackCPU(t *testing.T) {
	low, high, minPct := 800, 5000, 20
	settings, ok := FallbackCPU(&store.DeviceCapabilities{Settings: []string{store.SettingMaxFreq}, MaxFreqMinMHz: &low, MaxFreqMaxMHz: &high}, 0.1)
	if !ok || *settings.MaxFreqMHz != 4500 {
		t.Fatalf("cpufreq: %+v %v", settings, ok)
	}
	settings, ok = FallbackCPU(&store.DeviceCapabilities{Settings: []string{store.SettingMaxPerf}, MaxPerfPctMin: &minPct}, 0.07)
	if !ok || *settings.MaxPerfPct != 93 {
		t.Fatalf("intel_pstate: %+v %v", settings, ok)
	}
	// Windows reports no frequency range: nothing to cap from.
	if _, ok = FallbackCPU(&store.DeviceCapabilities{Settings: []string{store.SettingMaxFreq}}, 0.1); ok {
		t.Fatal("a fallback without a range")
	}
}

func TestClassifyNeedsAConfidentUsage(t *testing.T) {
	curves := map[string]Curve{
		"gemm": {Kind: "gemm", Reference: store.BenchReference{Fingerprint: &store.Fingerprint{UtilGPUPct: 99, UtilMemPct: 40}}},
		"copy": {Kind: "copy", Reference: store.BenchReference{Fingerprint: &store.Fingerprint{UtilGPUPct: 99, UtilMemPct: 95}}},
		"pcie": {Kind: "pcie-h2d", Reference: store.BenchReference{Fingerprint: &store.Fingerprint{UtilGPUPct: 99, UtilMemPct: 40}}},
	}
	swept := map[string]bool{"gemm": true, "copy": true}
	usage := store.PolicyUsage{UtilGPUPct: []float64{97, 98, 99, 98, 97}, UtilMemPct: []float64{35, 42, 45, 38, 41}}
	fp, candidates, ok := Classify(usage, curves, swept)
	if !ok || !slices.Equal(candidates, []string{"gemm"}) || fp.UtilMemPct != 40.2 {
		t.Fatalf("training-like: %+v %v %v", fp, candidates, ok)
	}
	if _, _, ok = Classify(store.PolicyUsage{UtilGPUPct: []float64{99, 99}, UtilMemPct: []float64{40, 40}}, curves, swept); ok {
		t.Fatal("two samples were confident")
	}
	bursty := store.PolicyUsage{UtilGPUPct: []float64{0, 100, 0, 100, 0}, UtilMemPct: []float64{40, 40, 40, 40, 40}}
	if _, _, ok = Classify(bursty, curves, swept); ok {
		t.Fatal("bursty usage was confident")
	}
	idle := store.PolicyUsage{UtilGPUPct: []float64{5, 6, 5, 6, 5}, UtilMemPct: []float64{1, 1, 1, 1, 1}}
	if _, candidates, ok = Classify(idle, curves, swept); !ok || len(candidates) != 0 {
		t.Fatalf("usage like no kind: %v %v", candidates, ok)
	}
}

func TestSnapping(t *testing.T) {
	clocks := []int{210, 900, 1500, 1800, 2130}
	for _, c := range []struct {
		target   float64
		up, near int
	}{{1600, 1800, 1500}, {1800, 1800, 1800}, {2500, 2130, 2130}, {100, 210, 210}, {1650, 1800, 1500}} {
		up, _ := SnapUp(clocks, c.target)
		near, _ := SnapNearest(clocks, c.target)
		if up != c.up || near != c.near {
			t.Fatalf("target %.0f: up %d near %d, want %d %d", c.target, up, near, c.up, c.near)
		}
	}
	if _, ok := SnapUp(nil, 1); ok {
		t.Fatal("snapped to nothing")
	}
}
