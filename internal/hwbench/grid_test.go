package hwbench

import (
	"strings"
	"testing"

	"kairo/internal/store"
)

func gridText(grid []store.DeviceSettings) string {
	parts := make([]string, len(grid))
	for i, s := range grid {
		parts[i] = s.String()
	}
	return strings.Join(parts, "; ")
}

func kind(name string) store.BenchKind {
	k, _ := store.FindBenchKind(name)
	return k
}

func TestGPUGridSnapsCapsToSupportedClocks(t *testing.T) {
	low, high, def := 150.0, 350.0, 320.0
	caps := &store.DeviceCapabilities{Settings: []string{store.SettingGraphicsClockMax, store.SettingMemoryClock, store.SettingPowerLimit},
		// irregular steps: 95% of 2100 (1995) snaps to 2010, 50% (1050) to 1005
		GraphicsClocksMHz: []int{210, 1005, 1200, 1410, 1500, 1605, 1695, 1800, 1905, 2010, 2100},
		MemoryClocksMHz:   []int{9501, 405, 5001}, PowerLimitMinW: &low, PowerLimitMaxW: &high, PowerLimitDefaultW: &def}
	got := gridText(Grid(kind("gemm"), caps))
	want := "graphics_clock_max_mhz=2100MHz; graphics_clock_max_mhz=2010MHz; graphics_clock_max_mhz=1905MHz; graphics_clock_max_mhz=1800MHz; " +
		"graphics_clock_max_mhz=1695MHz; graphics_clock_max_mhz=1605MHz; graphics_clock_max_mhz=1500MHz; graphics_clock_max_mhz=1410MHz; " +
		"graphics_clock_max_mhz=1200MHz; graphics_clock_max_mhz=1005MHz; " +
		// memory clocks below the greatest at the 100% and 85% caps (1785 -> 1800)
		"graphics_clock_max_mhz=2100MHz memory_clock_mhz=5001MHz; graphics_clock_max_mhz=2100MHz memory_clock_mhz=405MHz; " +
		"graphics_clock_max_mhz=1800MHz memory_clock_mhz=5001MHz; graphics_clock_max_mhz=1800MHz memory_clock_mhz=405MHz; " +
		// 60% of 320 W (192) is above the least, 150 W
		"power_limit_w=288W; power_limit_w=256W; power_limit_w=224W; power_limit_w=192W"
	if got != want {
		t.Fatalf("gemm grid:\n%s\nwant\n%s", got, want)
	}
	// copy sweeps no power limit; a power limit under the least is skipped
	low = 250
	got = gridText(Grid(kind("copy"), caps))
	if strings.Contains(got, "power_limit") || !strings.HasSuffix(got, "memory_clock_mhz=405MHz") {
		t.Fatalf("copy grid: %s", got)
	}
	if got = gridText(Grid(kind("gemm"), caps)); !strings.HasSuffix(got, "power_limit_w=288W; power_limit_w=256W") {
		t.Fatalf("gemm grid with a 250 W floor: %s", got)
	}
	// a kind measured at the defaults has no grid; neither has a device that
	// reports nothing it can set
	if grid := Grid(kind("pcie-h2d"), caps); grid != nil {
		t.Fatalf("pcie grid: %v", grid)
	}
	if grid := Grid(kind("gemm"), &store.DeviceCapabilities{Settings: []string{}}); len(grid) != 0 {
		t.Fatalf("empty capabilities: %v", grid)
	}
}

func TestCPUGridFollowsTheReportedRange(t *testing.T) {
	minPct := 60
	got := gridText(Grid(kind("cpu-compute"), &store.DeviceCapabilities{Settings: []string{store.SettingMaxPerf}, MaxPerfPctMin: &minPct}))
	if got != "max_perf_pct=100%; max_perf_pct=95%; max_perf_pct=90%; max_perf_pct=85%; max_perf_pct=80%; max_perf_pct=75%; max_perf_pct=70%; max_perf_pct=65%; max_perf_pct=60%" {
		t.Fatalf("intel_pstate grid: %s", got)
	}
	minFreq, maxFreq := 2000, 4000
	got = gridText(Grid(kind("cpu-stream"), &store.DeviceCapabilities{Settings: []string{store.SettingMaxFreq}, MaxFreqMinMHz: &minFreq, MaxFreqMaxMHz: &maxFreq}))
	if got != "max_freq_mhz=4000MHz; max_freq_mhz=3800MHz; max_freq_mhz=3600MHz; max_freq_mhz=3400MHz; max_freq_mhz=3200MHz; max_freq_mhz=3000MHz; "+
		"max_freq_mhz=2800MHz; max_freq_mhz=2600MHz; max_freq_mhz=2400MHz; max_freq_mhz=2200MHz; max_freq_mhz=2000MHz" {
		t.Fatalf("cpufreq grid: %s", got)
	}
	// Windows reports max_freq_mhz without a range: nothing to sweep
	if grid := Grid(kind("cpu-single"), &store.DeviceCapabilities{Settings: []string{store.SettingMaxFreq}}); len(grid) != 0 {
		t.Fatalf("Windows grid: %v", grid)
	}
}
