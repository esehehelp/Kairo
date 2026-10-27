package cpupower

import (
	"errors"
	"testing"
	"time"
)

type fakeCounters struct {
	openErr  error
	opens    int
	collects []map[string]float64 // nil entry: a failed collection
}

func (f *fakeCounters) Open() error {
	f.opens++
	return f.openErr
}

func (f *fakeCounters) Collect() (map[string]float64, error) {
	if len(f.collects) == 0 {
		return nil, errors.New("no more data")
	}
	values := f.collects[0]
	f.collects = f.collects[1:]
	if values == nil {
		return nil, errors.New("PDH_INVALID_DATA")
	}
	return values, nil
}

// Only the package instances count, in watts: the cores are part of their
// package and _Total adds everything up again.
func TestPDHMeterSumsThePackageInstances(t *testing.T) {
	at := time.Unix(2000, 0)
	source := &fakeCounters{collects: []map[string]float64{
		nil, // the first collection of an average counter has no value
		{"rapl_package0_pkg": 42311, "RAPL_Package1_PKG": 1000, "rapl_package0_core0_core": 183, "_total": 0},
	}}
	m := &pdhMeter{source: source, now: func() time.Time { return at }}
	if _, ok := m.Read(); ok {
		t.Fatal("a failed collection must give no reading")
	}
	r, ok := m.Read()
	if !ok || !near(r.Watts, 43.311) || r.Packages != 2 || !r.At.Equal(at) {
		t.Fatalf("reading %+v ok %v, want 43.311 W over 2 packages", r, ok)
	}
	if source.opens != 1 {
		t.Fatalf("the query was opened %d times, want once", source.opens)
	}
}

// Without the Energy Meter counters the meter reports nothing, and does not
// try again.
func TestPDHMeterWithoutTheCountersReportsNothing(t *testing.T) {
	source := &fakeCounters{openErr: errors.New("PDH_CSTATUS_NO_OBJECT")}
	m := &pdhMeter{source: source}
	for range 3 {
		if r, ok := m.Read(); ok {
			t.Fatalf("no counters gave %+v", r)
		}
	}
	if source.opens != 1 {
		t.Fatalf("opened %d times", source.opens)
	}
	// A machine whose meter has no package instance reports nothing either.
	m = &pdhMeter{source: &fakeCounters{collects: []map[string]float64{{"_total": 5}}}}
	if r, ok := m.Read(); ok {
		t.Fatalf("no package instance gave %+v", r)
	}
}
