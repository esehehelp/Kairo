package hwbench

// Operating point selection: pure functions over a device's bench profile,
// used by the policy (internal/hwpolicy) and by `kairo hardware profile`.

import (
	"math"
	"slices"
	"sort"

	"kairo/internal/store"
)

// Slack is added to a tolerance when points are compared with it: a
// measurement is good to about 1%, so a point within s + Slack of the
// reference is within s.
const Slack = 0.01

// Curve is one kind's profile: the valid points of the latest run whose
// references of that kind agree, with those references.
type Curve struct {
	Kind      string               `json:"kind"`
	RunID     string               `json:"run_id"`
	Reference store.BenchReference `json:"reference"`
	Points    []store.BenchPoint   `json:"points"`
}

// Curves are the profile of a device by kind, from its runs (oldest first):
// for each kind the latest ended run (finished, partially valid or invalid)
// in which it has valid references; another kind's invalid references do not
// matter. A failed run (its task ended without finishing it) and a live one
// count for nothing, whatever kinds they recorded.
func Curves(runs []store.BenchRun) map[string]Curve {
	out := map[string]Curve{}
	for _, run := range runs {
		if !run.Ended() {
			continue
		}
		for kind, ref := range run.References {
			if !ref.Valid {
				continue
			}
			curve := Curve{Kind: kind, RunID: run.ID, Reference: ref}
			for _, p := range run.Points {
				if p.Kind == kind && p.Valid && p.Metrics.RelPerf != nil {
					curve.Points = append(curve.Points, p)
				}
			}
			out[kind] = curve
		}
	}
	return out
}

// Select is the point of a kind for tolerance s: among the valid points whose
// performance is at least 1 - s - Slack of the reference, the one with the
// least energy per unit of work. Nil: none (the defaults).
func Select(points []store.BenchPoint, s float64) *store.BenchPoint {
	var best *store.BenchPoint
	for i := range points {
		p := &points[i]
		if !p.Valid || p.Metrics.RelPerf == nil || p.Metrics.JoulesPerWork == nil || *p.Metrics.RelPerf < 1-s-Slack {
			continue
		}
		if best == nil || *p.Metrics.JoulesPerWork < *best.Metrics.JoulesPerWork {
			best = p
		}
	}
	return best
}

// settingKey identifies a setting among a curve's points.
func settingKey(s store.DeviceSettings) string { return s.String() }

// atSetting is a curve's mean at one setting (several references share the
// defaults).
type atSetting struct {
	settings      store.DeviceSettings
	relPerf       float64
	joulesPerWork *float64
	powerW        *float64
}

func (c Curve) bySetting() map[string]atSetting {
	type acc struct {
		settings        store.DeviceSettings
		rel, jpw, power []float64
	}
	groups := map[string]*acc{}
	for _, p := range c.Points {
		key := settingKey(p.Settings)
		a := groups[key]
		if a == nil {
			a = &acc{settings: p.Settings}
			groups[key] = a
		}
		a.rel = append(a.rel, *p.Metrics.RelPerf)
		if p.Metrics.JoulesPerWork != nil {
			a.jpw = append(a.jpw, *p.Metrics.JoulesPerWork)
		}
		if p.Metrics.PowerW != nil {
			a.power = append(a.power, *p.Metrics.PowerW)
		}
	}
	out := map[string]atSetting{}
	for key, a := range groups {
		at := atSetting{settings: a.settings, relPerf: mean(a.rel)}
		// a mean only when every point has the value
		if len(a.jpw) == len(a.rel) {
			at.joulesPerWork = ptr(mean(a.jpw))
		}
		if len(a.power) == len(a.rel) {
			at.powerW = ptr(mean(a.power))
		}
		out[key] = at
	}
	return out
}

// Choice is a setting chosen for a set of kinds, and what the profile says it
// does to each.
type Choice struct {
	Settings store.DeviceSettings
	Expected []store.PolicyExpectation
	RunIDs   []string
}

// SelectCommon chooses one setting for every curve (the worst case of
// kinds that may be running): among the settings at which every curve has a
// valid point within tolerance s, the one using the least energy per unit of
// work relative to each kind's reference, on average (ties: the better
// worst performance). The defaults always qualify, so ok is false only
// without curves or when a curve has no energy at its reference.
func SelectCommon(curves []Curve, s float64) (Choice, bool) {
	if len(curves) == 0 {
		return Choice{}, false
	}
	tables := make([]map[string]atSetting, len(curves))
	for i, c := range curves {
		tables[i] = c.bySetting()
	}
	type option struct {
		key        string
		energy     float64
		worstPerf  float64
		perKind    []atSetting
		isDefaults bool
	}
	var options []option
	for key, first := range tables[0] {
		o := option{key: key, worstPerf: math.Inf(1), isDefaults: first.settings.IsDefault()}
		feasible := true
		for i, table := range tables {
			at, ok := table[key]
			ref := curves[i].Reference.JoulesPerWork
			if !ok || at.joulesPerWork == nil || ref == nil || *ref <= 0 || at.relPerf < 1-s-Slack {
				feasible = false
				break
			}
			o.energy += *at.joulesPerWork / *ref
			o.worstPerf = min(o.worstPerf, at.relPerf)
			o.perKind = append(o.perKind, at)
		}
		if feasible {
			o.energy /= float64(len(tables))
			options = append(options, o)
		}
	}
	if len(options) == 0 {
		return Choice{}, false
	}
	sort.Slice(options, func(i, j int) bool {
		a, b := options[i], options[j]
		if a.energy != b.energy {
			return a.energy < b.energy
		}
		if a.worstPerf != b.worstPerf {
			return a.worstPerf > b.worstPerf
		}
		return a.key < b.key
	})
	best := options[0]
	choice := Choice{}
	if !best.isDefaults {
		choice.Settings = best.perKind[0].settings
	}
	for i, at := range best.perKind {
		e := store.PolicyExpectation{Kind: curves[i].Kind, RelPerf: at.relPerf, PowerW: at.powerW, DefaultPowerW: curves[i].Reference.PowerW}
		if at.powerW != nil && e.DefaultPowerW != nil {
			e.SavingW = ptr(at.relPerf**e.DefaultPowerW - *at.powerW)
		}
		choice.Expected = append(choice.Expected, e)
		if !slices.Contains(choice.RunIDs, curves[i].RunID) {
			choice.RunIDs = append(choice.RunIDs, curves[i].RunID)
		}
	}
	return choice, true
}

// FingerprintTolerance is how far (in utilization percentage points) what a
// GPU is observed doing may be from a kind's bench fingerprint for the kind
// to be a candidate; MaxUsageSpread is the most the observed utilization may
// vary (standard deviation) for a classification to be confident.
const (
	FingerprintTolerance = 15.0
	MaxUsageSpread       = 15.0
	// MinUsageSamples is the fewest samples of each utilization a
	// classification needs.
	MinUsageSamples = 5
)

// Classify finds the swept kinds whose fingerprint is compatible with what a
// GPU was observed doing. ok is false when the observation is not enough to
// tell (too few samples, or utilization that varies too much); candidates is
// empty when no kind matches.
func Classify(usage store.PolicyUsage, curves map[string]Curve, sweeps map[string]bool) (store.Fingerprint, []string, bool) {
	if len(usage.UtilGPUPct) < MinUsageSamples || len(usage.UtilMemPct) < MinUsageSamples {
		return store.Fingerprint{}, nil, false
	}
	fp := store.Fingerprint{UtilGPUPct: mean(usage.UtilGPUPct), UtilMemPct: mean(usage.UtilMemPct)}
	if stddev(usage.UtilGPUPct) > MaxUsageSpread || stddev(usage.UtilMemPct) > MaxUsageSpread {
		return fp, nil, false
	}
	var candidates []string
	for kind, c := range curves {
		ref := c.Reference.Fingerprint
		if !sweeps[kind] || ref == nil {
			continue
		}
		if math.Abs(ref.UtilGPUPct-fp.UtilGPUPct) <= FingerprintTolerance && math.Abs(ref.UtilMemPct-fp.UtilMemPct) <= FingerprintTolerance {
			candidates = append(candidates, kind)
		}
	}
	sort.Strings(candidates)
	return fp, candidates, true
}

// FallbackGPU caps the highest graphics clock at (1 - s) of the GPU's
// greatest supported graphics clock, rounded up to a supported clock. The
// GPU's default clocks never exceed its greatest, so this loses at most s of
// clock, and performance loses no more than clock does, whatever runs. ok is
// false when the controller reports no graphics clocks, or the cap would be
// the greatest clock (nothing to cap).
func FallbackGPU(caps *store.DeviceCapabilities, s float64) (store.DeviceSettings, bool) {
	if caps == nil || !slices.Contains(caps.Settings, store.SettingGraphicsClockMax) || len(caps.GraphicsClocksMHz) == 0 || s <= 0 {
		return store.DeviceSettings{}, false
	}
	highest := slices.Max(caps.GraphicsClocksMHz)
	clock, ok := SnapUp(caps.GraphicsClocksMHz, (1-s)*float64(highest))
	if !ok || clock >= highest {
		return store.DeviceSettings{}, false
	}
	return store.DeviceSettings{GraphicsClockMaxMHz: &clock}, true
}

// FallbackCPU caps the CPU at (1 - s) of its greatest frequency (or
// performance percentage), rounded up. ok is false when the controller
// reports no range to cap within (Windows reports no frequency range).
func FallbackCPU(caps *store.DeviceCapabilities, s float64) (store.DeviceSettings, bool) {
	if caps == nil || s <= 0 {
		return store.DeviceSettings{}, false
	}
	switch {
	case slices.Contains(caps.Settings, store.SettingMaxPerf):
		pct := int(math.Ceil((1 - s) * 100))
		if caps.MaxPerfPctMin != nil {
			pct = max(pct, *caps.MaxPerfPctMin)
		}
		if pct >= 100 {
			return store.DeviceSettings{}, false
		}
		return store.DeviceSettings{MaxPerfPct: &pct}, true
	case slices.Contains(caps.Settings, store.SettingMaxFreq) && caps.MaxFreqMaxMHz != nil:
		mhz := int(math.Ceil((1 - s) * float64(*caps.MaxFreqMaxMHz)))
		if caps.MaxFreqMinMHz != nil {
			mhz = max(mhz, *caps.MaxFreqMinMHz)
		}
		if mhz >= *caps.MaxFreqMaxMHz {
			return store.DeviceSettings{}, false
		}
		return store.DeviceSettings{MaxFreqMHz: &mhz}, true
	}
	return store.DeviceSettings{}, false
}

// SnapUp is the least supported value at or above target (the greatest
// when none is).
func SnapUp(supported []int, target float64) (int, bool) {
	if len(supported) == 0 {
		return 0, false
	}
	best, found := 0, false
	for _, v := range supported {
		if float64(v) >= target && (!found || v < best) {
			best, found = v, true
		}
	}
	if !found {
		return slices.Max(supported), true
	}
	return best, true
}

// SnapNearest is the supported value nearest target (the lower on a tie).
func SnapNearest(supported []int, target float64) (int, bool) {
	if len(supported) == 0 {
		return 0, false
	}
	best := supported[0]
	for _, v := range supported[1:] {
		if d, b := math.Abs(float64(v)-target), math.Abs(float64(best)-target); d < b || d == b && v < best {
			best = v
		}
	}
	return best, true
}

func mean(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

func stddev(values []float64) float64 {
	if len(values) < 2 {
		return 0
	}
	m, sum := mean(values), 0.0
	for _, v := range values {
		sum += (v - m) * (v - m)
	}
	return math.Sqrt(sum / float64(len(values)-1))
}

func ptr[T any](v T) *T { return &v }
