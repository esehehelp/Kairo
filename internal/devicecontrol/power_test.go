package devicecontrol

import (
	"context"
	"testing"
	"time"

	"kairo/internal/cpupower"
	"kairo/internal/store"
)

// sampleDaemon also takes what the controller reads of its host.
type sampleDaemon struct {
	*fakeDaemon
	samples []store.ControllerSample
	skip    bool
}

func (s *sampleDaemon) ReportSamples(_ context.Context, _ string, samples []store.ControllerSample) ([]string, error) {
	s.samples = append(s.samples, samples...)
	if s.skip {
		return []string{"cpu"}, nil
	}
	return []string{}, nil
}

type fakeMeter struct {
	readings []cpupower.Reading
}

func (m *fakeMeter) Read() (cpupower.Reading, bool) {
	if len(m.readings) == 0 {
		return cpupower.Reading{}, false
	}
	r := m.readings[0]
	m.readings = m.readings[1:]
	return r, true
}

func TestEveryPollReportsTheHostsCPUPackagePower(t *testing.T) {
	at := time.Date(2026, 10, 27, 10, 0, 0, 0, time.UTC)
	daemon := &sampleDaemon{fakeDaemon: &fakeDaemon{devices: []store.ControlledDevice{cpu()}}}
	c := newTestController(daemon.fakeDaemon, &fakeActuator{}, &fakeActuator{}, nil, &clock{at})
	c.Daemon, c.PollInterval = daemon, 10*time.Second
	c.CPUMeter = &fakeMeter{readings: []cpupower.Reading{{At: at, Watts: 61.5, Packages: 1}}}
	ctx := context.Background()
	c.Step(ctx)
	if len(daemon.samples) != 1 || daemon.samples[0].DeviceID != "cpu" || *daemon.samples[0].PowerW != 61.5 ||
		daemon.samples[0].ValidUntil != at.Add(20*time.Second).Format(time.RFC3339Nano) {
		t.Fatalf("samples: %+v", daemon.samples)
	}
	// Nothing to read (the first read of a meter, or none): nothing sent.
	c.Step(ctx)
	if len(daemon.samples) != 1 {
		t.Fatalf("sent without a reading: %+v", daemon.samples)
	}
	// A Daemon that takes no samples gets none.
	plain := newTestController(&fakeDaemon{}, &fakeActuator{}, &fakeActuator{}, nil, &clock{at})
	plain.CPUMeter = &fakeMeter{readings: []cpupower.Reading{{At: at, Watts: 1}}}
	plain.Step(ctx)
}
