package cpupower

import (
	"errors"
	"io/fs"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// DefaultPowercapRoot is where Linux exposes the RAPL energy counters.
const DefaultPowercapRoot = "/sys/class/powercap"

// packageZone is a top-level RAPL zone (intel-rapl:0; AMD processors use the
// same control type). Subzones (intel-rapl:0:0, the cores) are part of their
// package, and the MMIO interface (intel-rapl-mmio:0) duplicates a package's
// counter, so neither is read.
var packageZone = regexp.MustCompile(`^[a-z]+-rapl:[0-9]+$`)

// RAPL reads the package energy counters of powercap (energy_uj, in
// microjoules, wrapping at max_energy_range_uj). FS is the powercap tree
// (default: DefaultPowercapRoot); Now the clock (default time.Now).
//
// The zones are found at the first read. When there are none, or a counter
// is not readable (permission denied: an unprivileged container, or a kernel
// that keeps energy_uj to root), the meter reports nothing from then on.
type RAPL struct {
	FS  fs.FS
	Now func() time.Time

	mu       sync.Mutex
	found    bool
	disabled bool
	zones    []raplZone
	at       time.Time
}

type raplZone struct {
	dir   string
	max   uint64 // max_energy_range_uj; 0 when unknown
	last  uint64
	valid bool
}

func (r *RAPL) readUint(name string) (uint64, error) {
	body, err := fs.ReadFile(r.FS, name)
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(strings.TrimSpace(string(body)), 10, 64)
}

// discover lists the package zones: the top-level zones whose name starts
// with "package".
func (r *RAPL) discover() {
	if r.FS == nil {
		r.FS = os.DirFS(DefaultPowercapRoot)
	}
	entries, _ := fs.ReadDir(r.FS, ".")
	for _, e := range entries {
		if !packageZone.MatchString(e.Name()) {
			continue
		}
		name, err := fs.ReadFile(r.FS, path.Join(e.Name(), "name"))
		if err != nil || !strings.HasPrefix(strings.TrimSpace(string(name)), "package") {
			continue
		}
		zone := raplZone{dir: e.Name()}
		zone.max, _ = r.readUint(path.Join(zone.dir, "max_energy_range_uj"))
		r.zones = append(r.zones, zone)
	}
	r.disabled = len(r.zones) == 0
}

func (r *RAPL) Read() (Reading, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.found {
		r.discover()
		r.found = true
	}
	if r.disabled {
		return Reading{}, false
	}
	now := time.Now
	if r.Now != nil {
		now = r.Now
	}
	at := now()
	elapsed := at.Sub(r.at).Seconds()
	joules, complete := 0.0, true
	for i := range r.zones {
		z := &r.zones[i]
		energy, err := r.readUint(path.Join(z.dir, "energy_uj"))
		if errors.Is(err, fs.ErrPermission) {
			r.disabled = true
			return Reading{}, false
		}
		if err != nil {
			z.valid = false
			complete = false
			continue
		}
		if !z.valid {
			complete = false
		} else if delta, ok := counterDelta(z.last, energy, z.max); ok {
			joules += float64(delta) / 1e6
		} else {
			complete = false
		}
		z.last, z.valid = energy, true
	}
	r.at = at
	if !complete || elapsed <= 0 {
		return Reading{}, false
	}
	return Reading{At: at, Watts: joules / elapsed, Packages: len(r.zones)}, true
}

// counterDelta is how far a counter that wraps after max went from last to
// current: at most one wrap is assumed. A counter that went back without a
// known range has no delta.
func counterDelta(last, current, max uint64) (uint64, bool) {
	if current >= last {
		return current - last, true
	}
	if max == 0 || last > max {
		return 0, false
	}
	return max - last + current, true
}
