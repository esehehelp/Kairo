package main

import (
	"bytes"
	"strings"
	"testing"

	"kairo/internal/machine"
	"kairo/internal/store"
)

func benchPoint(seq int, phase string, settings store.DeviceSettings, rel, watts, jpw float64, valid bool, flags ...string) store.BenchPoint {
	clock := 1800.0
	return store.BenchPoint{Seq: seq, Kind: "gemm", Phase: phase, Settings: settings, Valid: valid, Flags: flags,
		Metrics: store.BenchMetrics{Unit: "flop", RelPerf: &rel, PowerW: &watts, JoulesPerWork: &jpw, SMClockMHz: &clock}}
}

func TestRenderProfileMarksThePointForMaxSlowdownAndDrift(t *testing.T) {
	controller := "ctl"
	host := store.HardwareHost{ID: "pve0", Devices: []store.HardwareDevice{
		{DevicePolicy: machine.DevicePolicy{HostID: "pve0", DeviceID: "GPU-a", Kind: "gpu", MaxSlowdown: 0.05, Control: true, Controller: &controller}},
		{DevicePolicy: machine.DevicePolicy{HostID: "pve0", DeviceID: "cpu", Kind: "cpu", MaxSlowdown: 0.05, Control: true, Controller: &controller}},
	}}
	ended, started, idle, valid := "2026-10-20T12:00:00Z", "2026-10-27T10:00:00Z", 34.0, true
	jpwOld, jpwNew := 5e-12, 4.9e-12
	runs := []store.BenchRun{
		{ID: "bnc_old", DeviceID: "GPU-a", State: "finished", Kinds: []string{"gemm"}, CreatedAt: ended, EndedAt: &ended, Valid: &valid,
			References: map[string]store.BenchReference{"gemm": {Unit: "flop", Mean: 6.0e13, JoulesPerWork: &jpwOld, Valid: true}}},
		{ID: "bnc_new", DeviceID: "GPU-a", State: "finished", Kinds: []string{"gemm", "copy"}, CreatedAt: started, StartedAt: &started, EndedAt: &started, Valid: &valid, IdleTemperatureC: &idle,
			References: map[string]store.BenchReference{
				"gemm": {Unit: "flop", Mean: 6.06e13, WorkPerSecond: []float64{6.05e13, 6.06e13, 6.07e13}, Spread: 0.0033, JoulesPerWork: &jpwNew, Valid: true},
				"copy": {Unit: "byte", Valid: false, Reason: "references spread 2.0% (more than 1%)"},
			},
			Points: []store.BenchPoint{
				benchPoint(0, store.PhaseReference, store.DeviceSettings{}, 1, 300, 4.9e-12, true),
				benchPoint(1, store.PhaseSweep, store.DeviceSettings{GraphicsClockMaxMHz: intp(1800)}, 0.97, 250, 4.2e-12, true),
				benchPoint(2, store.PhaseSweep, store.DeviceSettings{GraphicsClockMaxMHz: intp(1500)}, 0.90, 200, 3.7e-12, true),
				benchPoint(3, store.PhaseSweep, store.DeviceSettings{GraphicsClockMaxMHz: intp(1700)}, 0.96, 230, 3.9e-12, false, "thermal"),
			}},
	}
	var out bytes.Buffer
	if err := renderProfile(&out, host, runs, ""); err != nil {
		t.Fatal(err)
	}
	text := out.String()
	for _, want := range []string{
		"pve0/GPU-a  max_slowdown 0.05",
		"bnc_new finished 10-27",
		"idle 34 °C",
		"copy: references spread 2.0% (more than 1%)",
		"gemm (bnc_new): reference 60.6 Tflop/s (spread 0.3%), 4.9 pJ/flop",
		"*  graphics_clock_max_mhz=1800MHz",
		"graphics_clock_max_mhz=1700MHz",
		"no: thermal",
		"drift from bnc_old",
		"work/s +1.0%, energy/work -2.0%",
		"pve0/cpu  max_slowdown 0.05",
		"not benched (kairo hardware bench pve0 cpu)",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("profile lacks %q:\n%s", want, text)
		}
	}
	// 1500 MHz loses 10%: not within 0.05; 1700 is invalid.
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "*") && !strings.Contains(line, "1800MHz") {
			t.Fatalf("marked the wrong point: %q", line)
		}
	}
}

func TestRenderBenchPlan(t *testing.T) {
	plan := store.BenchPlan{
		Run: store.BenchRun{ID: "bnc_1", HostID: "pve0", DeviceID: "GPU-a", Class: "gpu", Kinds: []string{"gemm", "copy"}, NodeID: "pve0-train", ExecutionID: "exe_1"},
		Execution: store.ExecutionSpec{Scope: store.ScopePath{Project: store.BenchProject, Queue: store.BenchQueue}, Priority: store.BenchPriority,
			Argv: []string{"/opt/kairo", "hardware", "bench-run", "--run", "bnc_1"}, CWD: "/opt",
			Exclusive: []store.ExclusiveRequest{{Kind: "gpu", Count: 1, Device: "GPU-a"}}, Capacity: store.CapacityRequest{CPUMillis: 1000, RAMBytes: 2 << 30}},
	}
	var out bytes.Buffer
	if err := renderBenchPlan(&out, plan); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"bench bnc_1 of pve0/GPU-a (gpu: gemm, copy) on pve0-train: execution exe_1",
		"holds GPU GPU-a, 1 CPU, 2 GiB RAM, at priority -1000000 in kairo/hardware-bench",
		"argv /opt/kairo hardware bench-run --run bnc_1 (cwd /opt)",
		"kairo hardware profile pve0 GPU-a",
	} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("plan lacks %q:\n%s", want, out.String())
		}
	}
}

func TestPolicyAndSavingsText(t *testing.T) {
	w, def := 250.0, 300.0
	d := store.PolicyDecision{Settings: store.DeviceSettings{GraphicsClockMaxMHz: intp(1800)}, Basis: store.BasisProfile, LeaseID: "lease-1", DecidedAt: "2026-10-27T10:01:00Z",
		Expected: []store.PolicyExpectation{{Kind: "gemm", RelPerf: 0.97, PowerW: &w, DefaultPowerW: &def}}}
	if got := policyText(d); !strings.HasPrefix(got, "graphics_clock_max_mhz=1800MHz (profile, for gemm perf 0.970, 250 W vs 300 W) for lease lease-1 at 10-27") {
		t.Fatalf("policy: %q", got)
	}
	fallback := store.PolicyDecision{Settings: store.DeviceSettings{GraphicsClockMaxMHz: intp(1920)}, Basis: store.BasisFallback, Reason: "no valid bench profile", DecidedAt: "2026-10-27T10:01:00Z"}
	if got := policyText(fallback); !strings.Contains(got, "(fallback: no valid bench profile)") {
		t.Fatalf("fallback: %q", got)
	}
	e := store.AttemptEnergy{GPUJoules: 4428000, GPUSecondsCovered: 3600, PolicySeconds: 10800, PolicySavedJoules: 432000, PolicyUnestimatedSeconds: 3600}
	if got := energyText(e); got != "GPU 1.23 kWh; policy 3.0 h, saved ~120.0 Wh (estimate), 1.0 h without a profile to estimate from" {
		t.Fatalf("savings: %q", got)
	}
}
