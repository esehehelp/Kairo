package hwbench

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"time"

	"kairo/internal/store"
)

// fakeDaemon is a daemon and controller in one: a setting it is asked for is
// reported applied after applyAfter status polls (never when negative).
type fakeDaemon struct {
	mu         sync.Mutex
	status     store.BenchStatus
	applyAfter int
	pending    int
	desired    store.DeviceSettings
	polls      int
	// onPoll may change the status at each poll.
	onPoll    func(polls int, status *store.BenchStatus)
	sets      []store.DeviceSettings
	points    map[int]store.BenchPoint
	uploads   int
	starts    []store.BenchStart
	finish    *store.BenchFinish
	cpuPowerW float64
	energies  int
	// commands are the attempt's pending ones; onCommands may add some at
	// each poll of them (commandPolls counts the polls).
	commands     []store.Command
	onCommands   func(polls int, d *fakeDaemon)
	commandPolls int
	acks         []fakeAck
	recorded     []string
}

type fakeAck struct {
	command, phase string
	payload        map[string]any
}

func (d *fakeDaemon) RecordKind(_ context.Context, kind string, ref store.BenchReference) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.status.Run.State != "running" {
		return fmt.Errorf("run is %s", d.status.Run.State)
	}
	if d.status.Run.References == nil {
		d.status.Run.References = map[string]store.BenchReference{}
	}
	d.status.Run.References[kind] = ref
	d.recorded = append(d.recorded, kind)
	return nil
}

func (d *fakeDaemon) Commands(context.Context) ([]store.Command, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.commandPolls++
	if d.onCommands != nil {
		d.onCommands(d.commandPolls, d)
	}
	return slices.Clone(d.commands), nil
}

// Ack takes an acknowledgement as the store does: a command it does not have
// is 404, and one acknowledged checkpointed or rejected is no longer pending.
func (d *fakeDaemon) Ack(_ context.Context, commandID, phase string, payload map[string]any) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	i := slices.IndexFunc(d.commands, func(c store.Command) bool { return c.ID == commandID })
	if i < 0 {
		return &APIError{Status: 404, Code: "not_found", Message: "command " + commandID}
	}
	d.acks = append(d.acks, fakeAck{commandID, phase, payload})
	if phase == "checkpointed" || phase == "rejected" {
		d.commands = slices.Delete(d.commands, i, i+1)
	}
	return nil
}

// suspendAt makes the attempt's command poll number n (and later) deliver a
// suspend.
func (d *fakeDaemon) suspendAt(n int) {
	d.onCommands = func(polls int, d *fakeDaemon) {
		if polls == n {
			d.commands = append(d.commands, store.Command{ID: "cmd-1", Kind: "suspend", Origin: "priority_preemption", Reason: "test", State: "pending"})
		}
	}
}

func newFakeDaemon(run store.BenchRun, caps *store.DeviceCapabilities) *fakeDaemon {
	zero := int64(0)
	d := &fakeDaemon{points: map[int]store.BenchPoint{}, cpuPowerW: 100}
	d.status = store.BenchStatus{Run: run, LeaseID: "lease-1", Capabilities: caps, OtherWork: []string{},
		Programs:   store.NodeBench{Kairo: "/opt/kairo/kairo", GPU: "/opt/kairo/kairo-bench-gpu"},
		Generation: 0, AppliedGeneration: &zero, Applied: &store.DeviceSettings{}, TemperatureC: ptr(31.0)}
	return d
}

func (d *fakeDaemon) apply() {
	generation, applied := d.status.Generation, d.desired
	d.status.AppliedGeneration, d.status.Applied = &generation, &applied
}

// applied is what the device holds now (for the fake primitives).
func (d *fakeDaemon) applied() store.DeviceSettings {
	d.mu.Lock()
	defer d.mu.Unlock()
	return *d.status.Applied
}

func (d *fakeDaemon) Status(context.Context) (store.BenchStatus, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.polls++
	if d.pending > 0 {
		if d.pending--; d.pending == 0 {
			d.apply()
		}
	}
	if d.onPoll != nil {
		d.onPoll(d.polls, &d.status)
	}
	status := d.status
	status.OtherWork = slices.Clone(d.status.OtherWork)
	return status, nil
}

func (d *fakeDaemon) SetSetting(_ context.Context, s store.DeviceSettings) (store.DeviceSettingResult, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.sets = append(d.sets, s)
	if !s.Equal(d.desired) {
		d.desired = s
		d.status.Generation++
		d.status.Effective = nil
		if !s.IsDefault() {
			d.status.Effective = &store.DeviceSetting{Owner: store.DeviceOwner{Kind: store.OwnerBench, Ref: "lease-1"}, Settings: s}
		}
		switch {
		case d.applyAfter == 0:
			d.apply()
		case d.applyAfter > 0:
			d.pending = d.applyAfter
		}
	}
	return store.DeviceSettingResult{Generation: d.status.Generation}, nil
}

func (d *fakeDaemon) CPUEnergy(_ context.Context, from, to time.Time) (store.CPUEnergy, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.energies++
	seconds := to.Sub(from).Seconds()
	return store.CPUEnergy{Device: "a-cpu", Joules: d.cpuPowerW * seconds, SecondsCovered: seconds, Seconds: seconds}, nil
}

func (d *fakeDaemon) Start(_ context.Context, start store.BenchStart) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.starts = append(d.starts, start)
	d.status.Run.State = "running"
	return nil
}

func (d *fakeDaemon) AddPoints(_ context.Context, points []store.BenchPoint) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.status.Run.State != "running" {
		return fmt.Errorf("run is %s", d.status.Run.State)
	}
	for _, p := range points {
		d.points[p.Seq] = p
		d.uploads++
	}
	return nil
}

func (d *fakeDaemon) Finish(_ context.Context, finish store.BenchFinish) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.finish = &finish
	d.status.Run.State = "finished"
	return nil
}

// sortedPoints are the points as last uploaded, by seq.
func (d *fakeDaemon) sortedPoints() []store.BenchPoint {
	var out []store.BenchPoint
	for seq := 0; seq < len(d.points); seq++ {
		out = append(out, d.points[seq])
	}
	return out
}

// fakeRunner starts fake primitives whose throughput and power follow what
// the fake daemon reports applied.
type fakeRunner struct {
	d     *fakeDaemon
	mu    sync.Mutex
	argvs [][]string
	// limit ends a primitive after that many intervals (0: until stopped),
	// with end reason "limit".
	limit int
	// thermal makes GPU intervals report thermal slowdown below their cap.
	thermal bool
}

func (r *fakeRunner) Start(_ context.Context, argv []string) (Process, error) {
	r.mu.Lock()
	r.argvs = append(r.argvs, argv)
	r.mu.Unlock()
	return &fakeProcess{r: r, argv: argv, stopped: make(chan struct{})}, nil
}

type fakeProcess struct {
	r        *fakeRunner
	argv     []string
	index    int
	started  bool
	ended    bool
	stopOnce sync.Once
	stopped  chan struct{}
	stopFlag bool
}

var fakeWall = time.Date(2026, 10, 27, 10, 0, 0, 0, time.UTC)

func (p *fakeProcess) gpu() bool { return strings.HasSuffix(p.argv[0], "kairo-bench-gpu") }

func (p *fakeProcess) workload() string {
	if p.gpu() {
		return p.argv[1]
	}
	return p.argv[3]
}

func (p *fakeProcess) line(v map[string]any) (Record, error) {
	v["schema"], v["run_id"], v["workload"] = Schema, "00000000deadbeef", p.workload()
	v["program"] = "kairo"
	if p.gpu() {
		v["program"] = "kairo-bench-gpu"
	}
	body, err := json.Marshal(v)
	if err != nil {
		return Record{}, err
	}
	return parseRecord(body)
}

func (p *fakeProcess) Next() (Record, error) {
	if p.ended {
		return Record{}, io.EOF
	}
	if !p.started {
		p.started = true
		unit, device := "byte", map[string]any{"kind": "storage"}
		switch {
		case p.gpu():
			unit, device = "flop", map[string]any{"kind": "gpu", "uuid": "GPU-1", "driver_version": "580.00"}
		case strings.HasPrefix(p.workload(), "cpu"):
			unit, device = "op", map[string]any{"kind": "cpu", "model": "fake"}
		}
		return p.line(map[string]any{"type": "start", "wall": fakeWall.Format(time.RFC3339Nano), "mono_ns": 0, "version": 1, "build": "test", "units": []string{unit}, "device": device})
	}
	select {
	case <-p.stopped:
		p.stopFlag = true
	default:
	}
	if p.stopFlag || p.r.limit > 0 && p.index >= p.r.limit {
		p.ended = true
		reason := "signal"
		if !p.stopFlag {
			reason = "limit"
		}
		return p.line(map[string]any{"type": "end", "reason": reason, "elapsed_ns": int64(p.index) * 1e9})
	}
	i := p.index
	p.index++
	s := p.r.d.applied()
	rec := map[string]any{"type": "interval", "index": i, "begin_ns": int64(i) * 1e9, "end_ns": int64(i+1) * 1e9, "mono_ns": int64(i+1) * 1e9}
	switch {
	case p.gpu():
		clock := 2000.0
		if s.GraphicsClockMaxMHz != nil {
			clock = float64(*s.GraphicsClockMaxMHz)
		}
		rate, power := 1e12*clock/2000, 300*(clock/2000)*(clock/2000)
		if s.MemoryClockMHz != nil && *s.MemoryClockMHz < 9501 {
			rate *= 0.9
			power -= 20
		}
		if s.PowerLimitW != nil && float64(*s.PowerLimitW) < power {
			rate *= (float64(*s.PowerLimitW) / power) * 1.1
			power = float64(*s.PowerLimitW)
		}
		// warms up over the first 30 s, then holds
		temperature := 60 + min(float64(i), 30)/3
		sm := clock
		reasons := 0
		if p.r.thermal {
			sm, reasons = clock*0.9, 0x40
		}
		stat := func(v float64) map[string]float64 { return map[string]float64{"min": v, "mean": v, "max": v} }
		rec["work"] = map[string]float64{"flop": rate}
		rec["nvml"] = map[string]any{"samples": 10, "energy_mj": power * 1000, "power_mw": stat(power * 1000), "sm_clock_mhz": stat(sm), "mem_clock_mhz": stat(9501),
			"temperature_c": stat(temperature), "util_gpu_pct": stat(98), "util_mem_pct": stat(40), "clocks_event_reasons": reasons}
	case strings.HasPrefix(p.workload(), "cpu"):
		freq := 5000.0
		if s.MaxFreqMHz != nil {
			freq = float64(*s.MaxFreqMHz)
		}
		rec["work"] = map[string]float64{"op": 1e9 * freq / 5000}
	default:
		rec["work"] = map[string]float64{"byte": 2e9}
		rec["latency"] = map[string]any{"count": 10, "p50_ns": 1000, "p99_ns": 5000}
	}
	return p.line(rec)
}

func (p *fakeProcess) Stop() error {
	p.stopOnce.Do(func() { close(p.stopped) })
	return nil
}

func (p *fakeProcess) Wait() error { return nil }

// testDriver drives d with r, with no wall-clock waits.
func testDriver(d *fakeDaemon, r Runner) *Driver {
	return &Driver{Daemon: d, Runner: r, Logger: discardLogger(), Timing: DefaultTiming(),
		Sleep: func(ctx context.Context, _ time.Duration) error { return ctx.Err() },
		Env:   func(string) string { return "" }}
}
