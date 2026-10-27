package cpupower

import (
	"strings"
	"sync"
	"time"
)

// EnergyMeterPowerCounter is the Windows performance counter of the RAPL
// domains' power, in milliwatts, averaged between two collections; its
// English name, since counter names are localized.
const EnergyMeterPowerCounter = `\Energy Meter(*)\Power`

// counterSource is what the Windows meter needs from PDH: open the counter,
// then collect it and return each instance's value (an average counter has
// none at the first collection). Tests fake it.
type counterSource interface {
	Open() error
	Collect() (map[string]float64, error)
}

// pdhMeter reads the CPU package instances of the Energy Meter power counter
// (rapl_package0_pkg, rapl_package1_pkg, ...): the cores and DRAM instances
// are parts of a package, and _Total adds them all up again.
type pdhMeter struct {
	source counterSource
	now    func() time.Time

	mu       sync.Mutex
	opened   bool
	disabled bool
}

// packageInstance reports whether an Energy Meter instance is a CPU package.
func packageInstance(name string) bool {
	name = strings.ToLower(name)
	return strings.HasPrefix(name, "rapl_package") && strings.HasSuffix(name, "_pkg")
}

func (m *pdhMeter) Read() (Reading, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.opened {
		m.opened = true
		if err := m.source.Open(); err != nil {
			// No Energy Meter counters on this machine (or no PDH).
			m.disabled = true
		}
	}
	if m.disabled {
		return Reading{}, false
	}
	values, err := m.source.Collect()
	if err != nil {
		return Reading{}, false
	}
	milliwatts, packages := 0.0, 0
	for name, value := range values {
		if packageInstance(name) {
			milliwatts += value
			packages++
		}
	}
	if packages == 0 {
		return Reading{}, false
	}
	now := time.Now
	if m.now != nil {
		now = m.now
	}
	return Reading{At: now(), Watts: milliwatts / 1000, Packages: packages}, true
}
