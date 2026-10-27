//go:build linux

package cpupower

// New is this machine's meter: the RAPL counters of powercap.
func New() Meter { return &RAPL{} }
