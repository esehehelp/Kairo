//go:build windows && (amd64 || arm64)

package cpupower

import "kairo/internal/pdh"

// New is this machine's meter: the Energy Meter performance counters.
func New() Meter {
	return &pdhMeter{source: &pdh.Query{Path: EnergyMeterPowerCounter}}
}
