//go:build linux || windows

package provider

import (
	"context"
	"testing"
	"time"

	"kairo/internal/cpupower"
)

type fakeMeter struct {
	readings []cpupower.Reading // an empty one: nothing to report
}

func (m *fakeMeter) Read() (cpupower.Reading, bool) {
	r := m.readings[0]
	m.readings = m.readings[1:]
	return r, r.Packages > 0
}

// The node's CPU package power rides on its cpu observation when the meter
// has a reading, and is left out when it has none.
func TestHostReportsCPUPackagePowerWithTheCPU(t *testing.T) {
	p := &Host{ProviderID: "host", NodeID: "n", Meter: &fakeMeter{readings: []cpupower.Reading{{}, {At: time.Now(), Watts: 42.311, Packages: 1}}}}
	for _, want := range []*float64{nil, f(42.311)} {
		snap, err := p.Observe(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		cpu, ram := snap.Observations[0], snap.Observations[1]
		if snap.Resources[0].Kind != "cpu" || ram.Device != nil {
			t.Fatalf("snapshot %+v", snap)
		}
		switch {
		case want == nil && cpu.Device != nil:
			t.Fatalf("no reading, yet a sample %+v", *cpu.Device)
		case want != nil && (cpu.Device == nil || cpu.Device.PowerW == nil || *cpu.Device.PowerW != *want):
			t.Fatalf("cpu sample %+v, want %v W", cpu.Device, *want)
		}
	}
}
