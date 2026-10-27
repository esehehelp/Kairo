// Package cpupower reads the power of a machine's CPU packages: on Linux from
// the RAPL energy counters of powercap (/sys/class/powercap), on Windows from
// the Energy Meter performance counters (pdh.dll). Both report the average
// power since the previous read. It only reads: it never changes a power
// setting.
//
// A machine without a readable meter (no RAPL domains, an unprivileged
// container where the counters are not readable, Windows without the Energy
// Meter counters) reports nothing, every time, without an error to log.
package cpupower

import "time"

// Reading is the power of a machine's CPU packages at a time: the average
// since the previous reading, summed over the packages.
type Reading struct {
	At       time.Time
	Watts    float64
	Packages int
}

// Meter reads CPU package power. Read reports false when it has nothing to
// report: the first read (an average needs two), a machine without a
// readable meter, or a read that failed. A Meter is safe for concurrent use.
type Meter interface {
	Read() (Reading, bool)
}

// None is the meter of a machine that has none.
type None struct{}

func (None) Read() (Reading, bool) { return Reading{}, false }
