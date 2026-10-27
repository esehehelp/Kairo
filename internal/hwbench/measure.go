package hwbench

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"kairo/internal/store"
)

// The records of the measurement primitives (README "Hardware benchmarks",
// kairo.bench/v1), as far as the driver reads them, and what a window of
// interval records measured.

// Schema is the record format the driver reads.
const Schema = "kairo.bench/v1"

// Record is one JSON line of a primitive: a start, interval or end record.
type Record struct {
	Schema   string `json:"schema"`
	Type     string `json:"type"`
	RunID    string `json:"run_id"`
	Program  string `json:"program"`
	Workload string `json:"workload"`
	Wall     string `json:"wall"`
	MonoNS   int64  `json:"mono_ns"`
	// start
	Version int             `json:"version"`
	Build   string          `json:"build"`
	Units   []string        `json:"units"`
	Device  json.RawMessage `json:"device"`
	// interval
	Index        int                `json:"index"`
	BeginNS      int64              `json:"begin_ns"`
	EndNS        int64              `json:"end_ns"`
	Work         map[string]float64 `json:"work"`
	DeviceBusyNS *int64             `json:"device_busy_ns"`
	Latency      *Latency           `json:"latency"`
	NVML         *NVML              `json:"nvml"`
	// end
	Reason string  `json:"reason"`
	Limit  string  `json:"limit"`
	Error  *string `json:"error"`
}

// Seconds is an interval record's length.
func (r Record) Seconds() float64 { return float64(r.EndNS-r.BeginNS) / 1e9 }

// Latency is an interval's latency summary.
type Latency struct {
	Count uint64 `json:"count"`
	P50NS *int64 `json:"p50_ns"`
	P99NS *int64 `json:"p99_ns"`
}

// Stat is the {min, mean, max} of an interval's NVML samples.
type Stat struct {
	Min  *float64 `json:"min"`
	Mean *float64 `json:"mean"`
	Max  *float64 `json:"max"`
}

// NVML is what kairo-bench-gpu read of the GPU over an interval.
type NVML struct {
	Samples            int      `json:"samples"`
	EnergyMJ           *float64 `json:"energy_mj"`
	PowerMW            *Stat    `json:"power_mw"`
	SMClockMHz         *Stat    `json:"sm_clock_mhz"`
	MemClockMHz        *Stat    `json:"mem_clock_mhz"`
	TemperatureC       *Stat    `json:"temperature_c"`
	UtilGPUPct         *Stat    `json:"util_gpu_pct"`
	UtilMemPct         *Stat    `json:"util_mem_pct"`
	ClocksEventReasons *int64   `json:"clocks_event_reasons"`
}

// temperature is an interval's mean GPU temperature (nil: not reported).
func (r Record) temperature() *float64 {
	if r.NVML == nil || r.NVML.TemperatureC == nil {
		return nil
	}
	return r.NVML.TemperatureC.Mean
}

// parseRecord reads one line; a line of another schema is an error.
func parseRecord(line []byte) (Record, error) {
	var r Record
	if err := json.Unmarshal(line, &r); err != nil {
		return r, fmt.Errorf("bench record: %w", err)
	}
	if r.Schema != Schema {
		return r, fmt.Errorf("bench record of schema %q, not %s", r.Schema, Schema)
	}
	return r, nil
}

// Thermal clock event reasons (NVML): software and hardware thermal slowdown.
const thermalReasons = 0x20 | 0x40

// Point flags. A point with an invalidating flag is not valid; the others
// are notes.
const (
	FlagNotApplied   = "not applied"
	FlagThermal      = "thermal"
	FlagOtherWork    = "other work"
	FlagManual       = "manual setting"
	FlagPolicy       = "policy setting in effect"
	FlagNoWork       = "no work"
	FlagNotSteady    = "not steady"
	FlagNoCPUEnergy  = "cpu energy not covered"
	FlagPrimitiveEnd = "primitive ended"
)

var invalidating = []string{FlagNotApplied, FlagThermal, FlagOtherWork, FlagManual, FlagPolicy, FlagNoWork, FlagPrimitiveEnd}

// invalidates reports whether flag makes its point invalid (a flag may carry
// a detail after ": ").
func invalidates(flag string) bool {
	name, _, _ := strings.Cut(flag, ":")
	return slices.Contains(invalidating, name)
}

// validFlags reports whether no flag invalidates.
func validFlags(flags []string) bool {
	for _, f := range flags {
		if invalidates(f) {
			return false
		}
	}
	return true
}

// window collects the interval records of one measurement.
type window struct {
	unit      string
	start     time.Time // the start record's wall time: mono 0
	intervals []Record
}

func (w *window) add(r Record) { w.intervals = append(w.intervals, r) }

func (w *window) seconds() float64 {
	total := 0.0
	for _, r := range w.intervals {
		total += r.Seconds()
	}
	return total
}

// bounds are the window's wall times.
func (w *window) bounds() (time.Time, time.Time) {
	if len(w.intervals) == 0 {
		return w.start, w.start
	}
	return w.start.Add(time.Duration(w.intervals[0].BeginNS)), w.start.Add(time.Duration(w.intervals[len(w.intervals)-1].EndNS))
}

// metrics are the window's measurements; the GPU's energy is the sum of its
// intervals' energy counter deltas (none when an interval lacks one), the
// clocks, temperature and utilization the means of the interval means.
func (w *window) metrics() store.BenchMetrics {
	begin, end := w.bounds()
	m := store.BenchMetrics{Unit: w.unit, Begin: begin.UTC().Format(time.RFC3339Nano), End: end.UTC().Format(time.RFC3339Nano), Seconds: w.seconds()}
	for _, r := range w.intervals {
		m.Work += r.Work[w.unit]
	}
	if m.Seconds > 0 {
		m.WorkPerSecond = m.Work / m.Seconds
	}
	energy, energyKnown := 0.0, len(w.intervals) != 0
	var power, sm, mem, temperature, utilGPU, utilMem, p50, p99 []float64
	var reasons *int64
	var hottest *float64
	statMean := func(s *Stat, into *[]float64, scale float64) {
		if s != nil && s.Mean != nil {
			*into = append(*into, *s.Mean*scale)
		}
	}
	for _, r := range w.intervals {
		if r.Latency != nil {
			if r.Latency.P50NS != nil {
				p50 = append(p50, float64(*r.Latency.P50NS))
			}
			if r.Latency.P99NS != nil {
				p99 = append(p99, float64(*r.Latency.P99NS))
			}
		}
		n := r.NVML
		if n == nil || n.EnergyMJ == nil {
			energyKnown = false
		}
		if n == nil {
			continue
		}
		if n.EnergyMJ != nil {
			energy += *n.EnergyMJ / 1000
		}
		statMean(n.PowerMW, &power, 1e-3)
		statMean(n.SMClockMHz, &sm, 1)
		statMean(n.MemClockMHz, &mem, 1)
		statMean(n.TemperatureC, &temperature, 1)
		statMean(n.UtilGPUPct, &utilGPU, 1)
		statMean(n.UtilMemPct, &utilMem, 1)
		if t := n.TemperatureC; t != nil && t.Max != nil && (hottest == nil || *t.Max > *hottest) {
			hottest = ptr(*t.Max)
		}
		if n.ClocksEventReasons != nil {
			if reasons == nil {
				reasons = ptr(int64(0))
			}
			*reasons |= *n.ClocksEventReasons
		}
	}
	if energyKnown {
		m.Joules, m.EnergySource = ptr(energy), "nvml"
		if m.Seconds > 0 {
			m.PowerW = ptr(energy / m.Seconds)
		}
	} else if len(power) != 0 {
		m.PowerW = ptr(mean(power))
	}
	meanOf := func(values []float64) *float64 {
		if len(values) == 0 {
			return nil
		}
		return ptr(mean(values))
	}
	m.SMClockMHz, m.MemClockMHz, m.TemperatureC, m.UtilGPUPct, m.UtilMemPct = meanOf(sm), meanOf(mem), meanOf(temperature), meanOf(utilGPU), meanOf(utilMem)
	m.TemperatureMaxC, m.EventReasons = hottest, reasons
	if v := meanOf(p50); v != nil {
		m.LatencyP50NS = ptr(int64(*v))
	}
	if v := meanOf(p99); v != nil {
		m.LatencyP99NS = ptr(int64(*v))
	}
	m.JoulesPerWork = joulesPerWork(m)
	return m
}

func joulesPerWork(m store.BenchMetrics) *float64 {
	if m.Joules == nil || m.Work <= 0 {
		return nil
	}
	return ptr(*m.Joules / m.Work)
}

// withCPUEnergy adds the host's CPU package energy over the window: its mean
// power over the part the samples cover, for the whole window. Less than 90%
// covered is no energy (flagged, not invalid).
func withCPUEnergy(m *store.BenchMetrics, e store.CPUEnergy) []string {
	if e.Seconds <= 0 || e.SecondsCovered < 0.9*e.Seconds {
		return []string{fmt.Sprintf("%s: %.0f of %.0f s", FlagNoCPUEnergy, e.SecondsCovered, e.Seconds)}
	}
	power := e.MeanPowerW()
	m.PowerW, m.Joules, m.EnergySource = ptr(power), ptr(power*m.Seconds), "cpu_package"
	m.JoulesPerWork = joulesPerWork(*m)
	return nil
}

// thermalFlag is the "thermal" flag of a window held below its clock cap by
// thermal slowdown: the cap was not reached because of heat, so the point
// says nothing about the setting.
func thermalFlag(m store.BenchMetrics, targetMHz float64) []string {
	if m.EventReasons == nil || *m.EventReasons&thermalReasons == 0 || m.SMClockMHz == nil || targetMHz <= 0 {
		return nil
	}
	if *m.SMClockMHz < 0.98*targetMHz {
		return []string{fmt.Sprintf("%s: %.0f of %.0f MHz, clock event reasons 0x%x", FlagThermal, *m.SMClockMHz, targetMHz, *m.EventReasons)}
	}
	return nil
}
