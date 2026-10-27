package cpupower

import (
	"io/fs"
	"math"
	"strings"
	"testing"
	"testing/fstest"
	"time"
)

// powercap is a fake powercap tree: "zone/file" -> contents.
func powercap(files map[string]string) fstest.MapFS {
	tree := fstest.MapFS{}
	for name, body := range files {
		tree[name] = &fstest.MapFile{Data: []byte(body + "\n")}
	}
	return tree
}

func setFile(tree fstest.MapFS, name, body string) {
	tree[name] = &fstest.MapFile{Data: []byte(body + "\n")}
}

// clock is a fake time source advanced by the test.
type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }

func near(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

// Only package zones count: not a package's subzones (cores), not a non-
// package zone (psys), not the MMIO duplicate of a package's counter.
func TestRAPLAveragesPackagePowerSinceTheLastRead(t *testing.T) {
	tree := powercap(map[string]string{
		"intel-rapl:0/name": "package-0", "intel-rapl:0/energy_uj": "1000000", "intel-rapl:0/max_energy_range_uj": "262143328850",
		"intel-rapl:1/name": "package-1", "intel-rapl:1/energy_uj": "5000000", "intel-rapl:1/max_energy_range_uj": "262143328850",
		"intel-rapl:0:0/name": "core", "intel-rapl:0:0/energy_uj": "1",
		"intel-rapl:2/name": "psys", "intel-rapl:2/energy_uj": "1",
		"intel-rapl-mmio:0/name": "package-0", "intel-rapl-mmio:0/energy_uj": "1",
	})
	c := &clock{time.Unix(1000, 0)}
	m := &RAPL{FS: tree, Now: c.now}
	if _, ok := m.Read(); ok {
		t.Fatal("the first read has no interval to average over")
	}
	c.t = c.t.Add(2 * time.Second)
	setFile(tree, "intel-rapl:0/energy_uj", "61000000") // +60 J
	setFile(tree, "intel-rapl:1/energy_uj", "45000000") // +40 J
	setFile(tree, "intel-rapl:2/energy_uj", "9999999")  // not a package
	setFile(tree, "intel-rapl-mmio:0/energy_uj", "9999999")
	r, ok := m.Read()
	if !ok || !near(r.Watts, 50) || r.Packages != 2 || !r.At.Equal(c.t) {
		t.Fatalf("reading %+v ok %v, want 50 W over 2 packages", r, ok)
	}
}

func TestRAPLCountsAWrappedCounter(t *testing.T) {
	tree := powercap(map[string]string{"intel-rapl:0/name": "package-0", "intel-rapl:0/energy_uj": "999000000", "intel-rapl:0/max_energy_range_uj": "1000000000"})
	c := &clock{time.Unix(1000, 0)}
	m := &RAPL{FS: tree, Now: c.now}
	m.Read()
	c.t = c.t.Add(time.Second)
	setFile(tree, "intel-rapl:0/energy_uj", "29000000") // 1 J to the wrap, 29 J after it
	if r, ok := m.Read(); !ok || !near(r.Watts, 30) {
		t.Fatalf("reading %+v ok %v, want 30 W across the wrap", r, ok)
	}
	// A counter that goes back without a known range gives no reading, and
	// the next read goes on from it.
	tree = powercap(map[string]string{"amd-rapl:0/name": "package-0", "amd-rapl:0/energy_uj": "500"})
	m = &RAPL{FS: tree, Now: c.now}
	m.Read()
	c.t = c.t.Add(time.Second)
	setFile(tree, "amd-rapl:0/energy_uj", "100")
	if r, ok := m.Read(); ok {
		t.Fatalf("a counter gone back without max_energy_range_uj: %+v", r)
	}
	c.t = c.t.Add(time.Second)
	setFile(tree, "amd-rapl:0/energy_uj", "1000100")
	if r, ok := m.Read(); !ok || !near(r.Watts, 1) {
		t.Fatalf("the read after: %+v ok %v, want 1 W", r, ok)
	}
}

// deniedFS refuses to open the energy counters, as an unprivileged container
// sees them. (Only Open: MapFS's own ReadFile would go around it.)
type deniedFS struct {
	tree  fstest.MapFS
	opens *int
}

func (d deniedFS) Open(name string) (fs.File, error) {
	if strings.HasSuffix(name, "energy_uj") {
		*d.opens++
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrPermission}
	}
	return d.tree.Open(name)
}

// An unprivileged container sees the zones but may not read their counters:
// the meter reports nothing and stops reading, without an error.
func TestRAPLStopsSilentlyWhenTheCounterIsNotReadable(t *testing.T) {
	opens := 0
	m := &RAPL{FS: deniedFS{powercap(map[string]string{"intel-rapl:0/name": "package-0", "intel-rapl:0/energy_uj": "1"}), &opens}}
	for range 3 {
		if r, ok := m.Read(); ok {
			t.Fatalf("an unreadable counter gave %+v", r)
		}
	}
	if opens != 1 {
		t.Fatalf("energy_uj opened %d times; once denied, the meter must stop", opens)
	}
	if r, ok := (&RAPL{FS: fstest.MapFS{}}).Read(); ok {
		t.Fatalf("a machine without RAPL zones reported %+v", r)
	}
}
