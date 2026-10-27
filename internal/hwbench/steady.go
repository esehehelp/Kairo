package hwbench

import "math"

// Steady state: a device under a constant load warms up, and its clocks and
// throughput follow its temperature. A bench measures its first reference
// only once the temperature has stopped climbing; a device that reports no
// temperature (a CPU through kairo bench run) is taken to be steady once its
// throughput is.

// Steady-state thresholds.
const (
	// SteadyTemperatureSlope is the most a steady device's temperature
	// changes, in °C per minute.
	SteadyTemperatureSlope = 0.5
	// SteadyRateSlope is the most a steady device's throughput changes, as a
	// share of itself per minute (without a temperature).
	SteadyRateSlope = 0.005
)

// steadySample is one interval for the detector: its end in seconds of
// stream time, its mean temperature (nil: not reported) and its throughput.
type steadySample struct {
	at, rate    float64
	temperature *float64
}

// Steady tells, from the intervals of a warming device, whether it has
// reached steady state: over the last Window seconds the slope (least
// squares) of its temperature, or without one of its throughput, is under
// the thresholds.
type Steady struct {
	Window  float64
	samples []steadySample
}

// Add records an interval ending at seconds of stream time.
func (s *Steady) Add(at float64, temperature *float64, rate float64) {
	s.samples = append(s.samples, steadySample{at: at, rate: rate, temperature: temperature})
}

// Reached reports whether the last Window seconds are steady; false until
// the samples span Window.
func (s *Steady) Reached() bool {
	if len(s.samples) < 3 {
		return false
	}
	last := s.samples[len(s.samples)-1].at
	var window []steadySample
	for _, x := range s.samples {
		if x.at >= last-s.Window {
			window = append(window, x)
		}
	}
	// the window must reach back Window seconds, give or take an interval
	first := s.samples[0].at
	if last-first < s.Window || len(window) < 3 {
		return false
	}
	withTemperature := true
	for _, x := range window {
		withTemperature = withTemperature && x.temperature != nil
	}
	ts := make([]float64, len(window))
	ys := make([]float64, len(window))
	for i, x := range window {
		ts[i] = x.at
		if withTemperature {
			ys[i] = *x.temperature
		} else {
			ys[i] = x.rate
		}
	}
	perMinute := slope(ts, ys) * 60
	if withTemperature {
		return math.Abs(perMinute) < SteadyTemperatureSlope
	}
	m := mean(ys)
	return m > 0 && math.Abs(perMinute)/m < SteadyRateSlope
}

// slope is the least-squares slope of ys over ts.
func slope(ts, ys []float64) float64 {
	mt, my := mean(ts), mean(ys)
	var num, den float64
	for i := range ts {
		num += (ts[i] - mt) * (ys[i] - my)
		den += (ts[i] - mt) * (ts[i] - mt)
	}
	if den == 0 {
		return 0
	}
	return num / den
}
