package hwbench

import (
	"math"
	"slices"

	"kairo/internal/store"
)

// The grid of settings a bench sweeps, from what the device's controller
// reports it can set (its capabilities) and nothing else: no clock or power
// figure is taken from a specification or from observation.

// gridStep and gridFloor: caps from 100% of the greatest value down to
// gridFloor, in steps of gridStep of the greatest.
const (
	gridStep  = 0.05
	gridFloor = 0.50
)

// memoryCoreShares are the graphics clock caps (shares of the greatest) at
// which the memory clocks are swept; powerShares the power limits (shares of
// the default) a gemm sweep adds.
var (
	memoryCoreShares = []float64{1.00, 0.85}
	powerShares      = []float64{0.90, 0.80, 0.70, 0.60}
)

// shares are 1.00, 0.95, ..., gridFloor.
func shares() []float64 {
	var out []float64
	for i := 0; ; i++ {
		share := 1 - gridStep*float64(i)
		if share < gridFloor-1e-9 {
			return out
		}
		out = append(out, share)
	}
}

// Grid is what a bench of kind sweeps on a device with caps, in the order
// it is measured; nil for a kind measured at the defaults only, or a device
// that reports nothing it can set.
func Grid(kind store.BenchKind, caps *store.DeviceCapabilities) []store.DeviceSettings {
	if caps == nil {
		return nil
	}
	switch kind.Sweep {
	case store.SweepClocks, store.SweepClocksPower:
		return gpuGrid(caps, kind.Sweep == store.SweepClocksPower)
	case store.SweepFrequency:
		return cpuGrid(caps)
	}
	return nil
}

// gpuGrid: the graphics clock cap from the greatest supported clock down to
// half of it in steps of 5% of it (each snapped to the nearest supported
// clock, repeats dropped) at the default memory clock, which under load is
// the greatest; every supported memory clock below the greatest at the caps
// of 100% and 85%; and, with power, 90/80/70/60% of the default power limit
// (none below the least the device takes).
func gpuGrid(caps *store.DeviceCapabilities, power bool) []store.DeviceSettings {
	var out []store.DeviceSettings
	seen := map[string]bool{}
	add := func(s store.DeviceSettings) {
		if key := s.String(); !seen[key] {
			seen[key] = true
			out = append(out, s)
		}
	}
	clocks := caps.GraphicsClocksMHz
	graphics := slices.Contains(caps.Settings, store.SettingGraphicsClockMax) && len(clocks) != 0
	capAt := func(share float64) (int, bool) {
		if !graphics {
			return 0, false
		}
		return SnapNearest(clocks, share*float64(slices.Max(clocks)))
	}
	for _, share := range shares() {
		if clock, ok := capAt(share); ok {
			add(store.DeviceSettings{GraphicsClockMaxMHz: &clock})
		}
	}
	if slices.Contains(caps.Settings, store.SettingMemoryClock) && len(caps.MemoryClocksMHz) > 1 {
		memory := slices.Clone(caps.MemoryClocksMHz)
		slices.Sort(memory)
		slices.Reverse(memory)
		for _, share := range memoryCoreShares {
			clock, ok := capAt(share)
			for _, mhz := range memory[1:] {
				s := store.DeviceSettings{MemoryClockMHz: &mhz}
				if ok {
					s.GraphicsClockMaxMHz = &clock
				} else if share != 1 {
					continue // without a graphics cap there is only the 100% one
				}
				add(s)
			}
		}
	}
	if power && slices.Contains(caps.Settings, store.SettingPowerLimit) && caps.PowerLimitDefaultW != nil {
		for _, share := range powerShares {
			watts := int(math.Round(share * *caps.PowerLimitDefaultW))
			if caps.PowerLimitMinW != nil && float64(watts) < *caps.PowerLimitMinW || caps.PowerLimitMaxW != nil && float64(watts) > *caps.PowerLimitMaxW {
				continue
			}
			add(store.DeviceSettings{PowerLimitW: &watts})
		}
	}
	return out
}

// cpuGrid: intel_pstate's max_perf_pct from 100 down to 50 in steps of 5, or
// cpufreq's (and Windows') max_freq_mhz from the greatest frequency down to
// half of it in steps of 5% of it, none below the least the device takes.
// Nothing when the controller reports no range (Windows reports none).
func cpuGrid(caps *store.DeviceCapabilities) []store.DeviceSettings {
	var out []store.DeviceSettings
	switch {
	case slices.Contains(caps.Settings, store.SettingMaxPerf):
		for _, share := range shares() {
			pct := int(math.Round(share * 100))
			if caps.MaxPerfPctMin != nil && pct < *caps.MaxPerfPctMin {
				break
			}
			out = append(out, store.DeviceSettings{MaxPerfPct: &pct})
		}
	case slices.Contains(caps.Settings, store.SettingMaxFreq) && caps.MaxFreqMaxMHz != nil:
		for _, share := range shares() {
			mhz := int(math.Round(share * float64(*caps.MaxFreqMaxMHz)))
			if caps.MaxFreqMinMHz != nil && mhz < *caps.MaxFreqMinMHz {
				break
			}
			out = append(out, store.DeviceSettings{MaxFreqMHz: &mhz})
		}
	}
	return out
}
