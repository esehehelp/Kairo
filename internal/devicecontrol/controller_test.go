package devicecontrol

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"kairo/internal/store"
)

// fakeDaemon answers a controller's polls with devices and keeps its reports.
type fakeDaemon struct {
	mu       sync.Mutex
	devices  []store.ControlledDevice
	down     bool
	reports  []store.DeviceReport
	register int
}

var errDown = errors.New("connection refused")

func (f *fakeDaemon) RegisterController(context.Context, store.Node, string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.down {
		return errDown
	}
	f.register++
	return nil
}

func (f *fakeDaemon) DeviceSettings(context.Context, string) ([]store.ControlledDevice, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.down {
		return nil, errDown
	}
	return slices.Clone(f.devices), nil
}

func (f *fakeDaemon) ReportDevices(_ context.Context, _ string, reports []store.DeviceReport) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.down {
		return nil, errDown
	}
	f.reports = append(f.reports, reports...)
	return nil, nil
}

// last is the latest report of a device.
func (f *fakeDaemon) last(t *testing.T, device string) store.DeviceReport {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := len(f.reports) - 1; i >= 0; i-- {
		if f.reports[i].DeviceID == device {
			return f.reports[i]
		}
	}
	t.Fatalf("no report of %s", device)
	return store.DeviceReport{}
}

// fakeActuator records what it was asked to do; fail makes Apply fail.
type fakeActuator struct {
	calls []string // "device settings"
	fail  error
	// current is what each device holds.
	current map[string]store.DeviceSettings
	// capabilities are reported (nil: power_limit_w).
	capabilities *store.DeviceCapabilities
}

func (a *fakeActuator) Capabilities(context.Context, string) (store.DeviceCapabilities, error) {
	if a.capabilities != nil {
		return *a.capabilities, nil
	}
	return store.DeviceCapabilities{Settings: []string{store.SettingPowerLimit}}, nil
}

func (a *fakeActuator) Apply(_ context.Context, device string, s store.DeviceSettings) error {
	a.calls = append(a.calls, device+" "+s.String())
	if a.fail != nil {
		return a.fail
	}
	if a.current == nil {
		a.current = map[string]store.DeviceSettings{}
	}
	a.current[device] = s
	return nil
}

func (a *fakeActuator) take() []string {
	calls := a.calls
	a.calls = nil
	return calls
}

type clock struct{ t time.Time }

func (c *clock) now() time.Time          { return c.t }
func (c *clock) advance(d time.Duration) { c.t = c.t.Add(d) }
func power(w int) store.DeviceSettings   { return store.DeviceSettings{PowerLimitW: &w} }
func gpu(gen int64, s store.DeviceSettings) store.ControlledDevice {
	return store.ControlledDevice{HostID: "h", DeviceID: "GPU-1", Kind: "gpu", Generation: gen, Settings: s}
}
func cpu() store.ControlledDevice {
	return store.ControlledDevice{HostID: "h", DeviceID: "cpu", Kind: "cpu"}
}

func newTestController(daemon *fakeDaemon, gpus, cpus *fakeActuator, state *State, c *clock) *Controller {
	return &Controller{
		Daemon:     daemon,
		Node:       store.Node{ID: "ctl", Name: "ctl", OS: "linux", Architecture: "amd64"},
		Actuators:  map[string]Actuator{"gpu": gpus, "cpu": cpus},
		State:      state,
		ResetAfter: 2 * time.Minute,
		Now:        c.now,
		Logger:     quiet,
	}
}

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func TestStartReconcilesEveryDeviceAndThenOnlyChanges(t *testing.T) {
	daemon := &fakeDaemon{devices: []store.ControlledDevice{gpu(3, power(250)), cpu()}}
	gpus, cpus := &fakeActuator{}, &fakeActuator{}
	c := newTestController(daemon, gpus, cpus, nil, &clock{time.Now()})
	ctx := context.Background()
	c.Step(ctx)
	// a desired setting is applied, a device without one is reset (a crash may have left settings)
	if got := gpus.take(); !slices.Equal(got, []string{"GPU-1 power_limit_w=250W"}) {
		t.Fatalf("GPU: %v", got)
	}
	if got := cpus.take(); !slices.Equal(got, []string{"cpu defaults"}) {
		t.Fatalf("CPU: %v", got)
	}
	report := daemon.last(t, "GPU-1")
	if report.AppliedGeneration == nil || *report.AppliedGeneration != 3 || !report.Applied.Equal(power(250)) || report.Capabilities == nil || daemon.register != 1 {
		t.Fatalf("report: %+v", report)
	}
	c.Step(ctx)
	if len(gpus.calls)+len(cpus.calls) != 0 {
		t.Fatalf("nothing changed, yet: %v %v", gpus.calls, cpus.calls)
	}
	// a new generation with the same settings is reported, not carried out again
	daemon.devices[0] = gpu(4, power(250))
	c.Step(ctx)
	if len(gpus.take()) != 0 || *daemon.last(t, "GPU-1").AppliedGeneration != 4 {
		t.Fatal("same settings were applied again")
	}
	daemon.devices[0] = gpu(5, store.DeviceSettings{})
	c.Step(ctx)
	if got := gpus.take(); !slices.Equal(got, []string{"GPU-1 defaults"}) {
		t.Fatalf("cleared setting: %v", got)
	}
}

func TestAFailedChangeIsReportedAndRetriedWithBackoff(t *testing.T) {
	daemon := &fakeDaemon{devices: []store.ControlledDevice{gpu(1, power(250))}}
	gpus := &fakeActuator{fail: errors.New("nvidia-smi -pl 250: exit status 4")}
	clk := &clock{time.Now()}
	c := newTestController(daemon, gpus, &fakeActuator{}, nil, clk)
	ctx := context.Background()
	c.Step(ctx)
	if report := daemon.last(t, "GPU-1"); !strings.Contains(report.Error, "exit status 4") || report.Applied != nil {
		t.Fatalf("failure report: %+v", report)
	}
	gpus.take()
	clk.advance(time.Second)
	c.Step(ctx)
	if len(gpus.take()) != 0 {
		t.Fatal("retried before its delay")
	}
	clk.advance(5 * time.Second)
	c.Step(ctx)
	if len(gpus.take()) != 1 {
		t.Fatal("not retried after its delay")
	}
	// the second failure waits twice as long
	clk.advance(5 * time.Second)
	c.Step(ctx)
	if len(gpus.take()) != 0 {
		t.Fatal("the delay did not grow")
	}
	// a new request is tried at once
	daemon.devices[0] = gpu(2, power(200))
	gpus.fail = nil
	c.Step(ctx)
	if got := gpus.take(); !slices.Equal(got, []string{"GPU-1 power_limit_w=200W"}) {
		t.Fatalf("new generation: %v", got)
	}
	if report := daemon.last(t, "GPU-1"); report.Error != "" || *report.AppliedGeneration != 2 {
		t.Fatalf("recovered: %+v", report)
	}
}

func TestStopReturnsChangedDevicesToTheirDefaults(t *testing.T) {
	daemon := &fakeDaemon{devices: []store.ControlledDevice{gpu(1, power(250)), cpu()}}
	gpus, cpus := &fakeActuator{}, &fakeActuator{}
	c := newTestController(daemon, gpus, cpus, nil, &clock{time.Now()})
	c.Step(context.Background())
	gpus.take()
	cpus.take()
	c.Stop(context.Background())
	if got := gpus.take(); !slices.Equal(got, []string{"GPU-1 defaults"}) {
		t.Fatalf("GPU on stop: %v", got)
	}
	if got := cpus.take(); len(got) != 0 {
		t.Fatalf("the CPU was at its defaults already: %v", got)
	}
	if report := daemon.last(t, "GPU-1"); report.Note != "controller stopped" || !report.Applied.IsDefault() || report.AppliedGeneration != nil {
		t.Fatalf("stop report: %+v", report)
	}
}

func TestRunStopsGracefullyWhenItsContextEnds(t *testing.T) {
	daemon := &fakeDaemon{devices: []store.ControlledDevice{gpu(1, power(250))}}
	gpus := &fakeActuator{}
	c := newTestController(daemon, gpus, &fakeActuator{}, nil, &clock{time.Now()})
	c.Now = nil
	c.PollInterval = time.Hour
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- c.Run(ctx) }()
	for {
		daemon.mu.Lock()
		reported := len(daemon.reports)
		daemon.mu.Unlock()
		if reported != 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !gpus.current["GPU-1"].IsDefault() {
		t.Fatalf("left in place after stop: %+v", gpus.current)
	}
}

func TestAnUnreachableDaemonGetsDevicesResetAfterTheBound(t *testing.T) {
	daemon := &fakeDaemon{devices: []store.ControlledDevice{gpu(1, power(250)), cpu()}}
	gpus, cpus := &fakeActuator{}, &fakeActuator{}
	clk := &clock{time.Now()}
	c := newTestController(daemon, gpus, cpus, nil, clk)
	ctx := context.Background()
	c.Step(ctx)
	gpus.take()
	cpus.take()
	daemon.down = true
	clk.advance(time.Minute)
	c.Step(ctx)
	if len(gpus.calls) != 0 {
		t.Fatal("reset before the bound")
	}
	clk.advance(time.Minute)
	c.Step(ctx)
	if got := gpus.take(); !slices.Equal(got, []string{"GPU-1 defaults"}) {
		t.Fatalf("after the bound: %v", got)
	}
	clk.advance(time.Minute)
	c.Step(ctx)
	if len(gpus.calls)+len(cpus.calls) != 0 {
		t.Fatal("reset again")
	}
	// the daemon is back: its settings are applied again, and the reset is reported
	daemon.down = false
	c.Step(ctx)
	if got := gpus.take(); !slices.Equal(got, []string{"GPU-1 power_limit_w=250W"}) {
		t.Fatalf("daemon back: %v", got)
	}
	var notes []string
	for _, r := range daemon.reports {
		notes = append(notes, r.Note)
	}
	if !slices.Contains(notes, "") {
		t.Fatalf("reports: %+v", daemon.reports)
	}
}

func TestADeviceNoLongerControlledIsResetOnceAndForgotten(t *testing.T) {
	daemon := &fakeDaemon{devices: []store.ControlledDevice{gpu(1, power(250)), cpu()}}
	gpus, cpus := &fakeActuator{}, &fakeActuator{}
	state, err := OpenState(filepath.Join(t.TempDir(), "controller.state.json"))
	if err != nil {
		t.Fatal(err)
	}
	c := newTestController(daemon, gpus, cpus, state, &clock{time.Now()})
	ctx := context.Background()
	c.Step(ctx)
	gpus.take()
	cpus.take()
	if len(state.Devices()) != 2 {
		t.Fatalf("state: %+v", state.Devices())
	}
	daemon.devices = []store.ControlledDevice{cpu()}
	c.Step(ctx)
	if got := gpus.take(); !slices.Equal(got, []string{"GPU-1 defaults"}) {
		t.Fatalf("GPU left control: %v", got)
	}
	c.Step(ctx)
	if len(gpus.calls) != 0 || len(state.Devices()) != 1 {
		t.Fatalf("touched again: %v, state %+v", gpus.calls, state.Devices())
	}
}

func TestAfterARestartWithoutTheDaemonTheKnownDevicesAreReset(t *testing.T) {
	path := filepath.Join(t.TempDir(), "controller.state.json")
	state, err := OpenState(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = state.SetDevices([]StateDevice{{HostID: "h", DeviceID: "GPU-1", Kind: "gpu"}}); err != nil {
		t.Fatal(err)
	}
	// a crashed controller's successor finds the daemon down
	if state, err = OpenState(path); err != nil {
		t.Fatal(err)
	}
	daemon := &fakeDaemon{down: true}
	gpus := &fakeActuator{}
	clk := &clock{time.Now()}
	c := newTestController(daemon, gpus, &fakeActuator{}, state, clk)
	c.Step(context.Background())
	clk.advance(2 * time.Minute)
	c.Step(context.Background())
	if got := gpus.take(); !slices.Equal(got, []string{"GPU-1 defaults"}) {
		t.Fatalf("devices of the earlier run: %v", got)
	}
}

func TestOnlyListedDevicesAreTouched(t *testing.T) {
	daemon := &fakeDaemon{}
	gpus, cpus := &fakeActuator{}, &fakeActuator{}
	c := newTestController(daemon, gpus, cpus, nil, &clock{time.Now()})
	c.Step(context.Background())
	c.Stop(context.Background())
	if len(gpus.calls)+len(cpus.calls) != 0 {
		t.Fatalf("touched devices nobody listed: %v %v", gpus.calls, cpus.calls)
	}
}

func TestADeviceKindWithoutActuatorFailsWithoutCrashing(t *testing.T) {
	daemon := &fakeDaemon{devices: []store.ControlledDevice{{HostID: "h", DeviceID: "npu", Kind: "npu", Generation: 1}}}
	c := newTestController(daemon, &fakeActuator{}, &fakeActuator{}, nil, &clock{time.Now()})
	c.Step(context.Background())
	if report := daemon.last(t, "npu"); !strings.Contains(report.Error, "cannot change") {
		t.Fatalf("report: %+v", report)
	}
}

func TestAnUnreadableStateFileIsNeverReplaced(t *testing.T) {
	path := filepath.Join(t.TempDir(), "controller.state.json")
	if err := writeFile(path, "{not json"); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenState(path); err == nil || !strings.Contains(err.Error(), "fix or remove it by hand") {
		t.Fatalf("unreadable state: %v", err)
	}
}
