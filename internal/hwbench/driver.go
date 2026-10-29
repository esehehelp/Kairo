package hwbench

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os/exec"
	"runtime"
	"slices"
	"strings"
	"syscall"
	"time"

	"kairo/internal/buildinfo"
	"kairo/internal/store"
)

// Daemon is the daemon as a bench driver reaches it with its attempt token
// (Client): its own run only.
type Daemon interface {
	Status(ctx context.Context) (store.BenchStatus, error)
	// SetSetting makes s the run's bench setting; the zero setting clears it.
	SetSetting(ctx context.Context, s store.DeviceSettings) (store.DeviceSettingResult, error)
	// CPUEnergy is the host's CPU package energy over [from, to]; nothing
	// covered (no samples) is a zero CPUEnergy, not an error.
	CPUEnergy(ctx context.Context, from, to time.Time) (store.CPUEnergy, error)
	Start(ctx context.Context, start store.BenchStart) error
	AddPoints(ctx context.Context, points []store.BenchPoint) error
	// RecordKind records a kind's reference as soon as the kind is done: a
	// resumed run does not measure it again.
	RecordKind(ctx context.Context, kind string, ref store.BenchReference) error
	Finish(ctx context.Context, finish store.BenchFinish) error
	// Commands are the attempt's pending commands (the worker API's, as an
	// SDK's safe point polls them); Ack acknowledges one.
	Commands(ctx context.Context) ([]store.Command, error)
	Ack(ctx context.Context, commandID, phase string, payload map[string]any) error
}

// Process is a running primitive.
type Process interface {
	// Next is its next record; io.EOF after the last.
	Next() (Record, error)
	// Stop ends it (it writes its end record and exits); Wait reaps it.
	Stop() error
	Wait() error
}

// Runner starts primitives.
type Runner interface {
	Start(ctx context.Context, argv []string) (Process, error)
}

// Timing is how long each step of a bench takes. The settle, measure, apply
// and warm-up times are seconds of the primitive's own stream (its interval
// records), so the driver's clock is the work it watches; the waits for other
// work and the network server are wall time.
type Timing struct {
	// Settle follows a change of setting before measuring; ShortSettle the
	// start of a kind measured once.
	Settle, ShortSettle float64
	Measure             float64
	// ApplyTimeout is how long the controller has to report a setting
	// applied.
	ApplyTimeout float64
	// SteadyWindow and SteadyMax: the warm-up's steady-state window, and
	// the longest it waits for it.
	SteadyWindow, SteadyMax float64
	// OtherWorkWait is how long the device may be in use by something else
	// before the run gives up; Poll how often it looks again (and how often
	// a network server looks whether its run has ended).
	OtherWorkWait, Poll time.Duration
	// ServerWait is how long a network client waits for its server.
	ServerWait time.Duration
	// CommandPoll is how often (stream seconds) the warm-up looks for a
	// suspend; every point looks once before it starts.
	CommandPoll float64
}

// DefaultTiming is the agreed procedure: 20 s settle, 30 s measurement, 60 s
// for a setting to apply, a warm-up of up to 5 minutes until the temperature
// changes by less than 0.5 °C/min over 60 s.
func DefaultTiming() Timing {
	return Timing{Settle: 20, ShortSettle: 5, Measure: 30, ApplyTimeout: 60, SteadyWindow: 60, SteadyMax: 300,
		OtherWorkWait: 10 * time.Minute, Poll: 5 * time.Second, ServerWait: 2 * time.Minute, CommandPoll: 15}
}

// Driver carries out one bench run from inside its attempt.
type Driver struct {
	Daemon Daemon
	Runner Runner
	Logger *slog.Logger
	// Env reads the attempt's environment (the gang variables).
	Env    func(string) string
	Timing Timing
	// Sleep and Dial are the wall-clock waits and the network server probe
	// (tests replace them).
	Sleep func(ctx context.Context, d time.Duration) error
	Dial  func(ctx context.Context, addr string) error
	// Continuation is KAIRO_CONTINUATION_REF: what the attempt this one
	// resumes published when it was suspended (a Continuation).
	Continuation string
	// Stopping reports whether a stop signal came (SIGTERM, CTRL_BREAK: the
	// graceful phase of a force stop); nil: none comes.
	Stopping func() bool

	status   store.BenchStatus
	seq      int
	versions map[string]store.BenchVersion
	started  bool
	// recorded are the kinds this attempt recorded as done.
	recorded map[string]bool
	// checkpoints are the continuations published, by command: a command
	// delivered again gets the same one.
	checkpoints map[string]string
}

// Continuation is the continuation_ref a suspended run publishes: the run,
// and the kinds it is done with (their references are in the store). The
// attempt that resumes it measures the others, the interrupted one from the
// start.
type Continuation struct {
	Run  string   `json:"run"`
	Done []string `json:"done"`
}

func (c Continuation) String() string {
	body, _ := json.Marshal(c)
	return string(body)
}

// ParseContinuation reads a Continuation.
func ParseContinuation(ref string) (Continuation, error) {
	var c Continuation
	if err := json.Unmarshal([]byte(ref), &c); err != nil || c.Run == "" {
		return c, fmt.Errorf("%q is not a bench continuation", ref)
	}
	return c, nil
}

// interrupted leaves the run before it is done: a suspend command, which is
// acknowledged with a checkpoint once the workload is stopped, or (no
// command) a stop signal.
type interrupted struct{ command *store.Command }

func (e interrupted) Error() string {
	if e.command == nil {
		return "stopped by a signal"
	}
	return "suspended (" + e.command.Origin + ")"
}

// daemonError is a failure to talk to the daemon, which ends the run: what
// cannot be reported cannot be measured.
type daemonError struct{ err error }

func (e daemonError) Error() string { return "daemon: " + e.err.Error() }
func (e daemonError) Unwrap() error { return e.err }

// errEnded: the primitive wrote its end record (or its output ended).
var errEnded = errors.New("the primitive ended")

func (d *Driver) logger() *slog.Logger {
	if d.Logger != nil {
		return d.Logger
	}
	return slog.Default()
}

func (d *Driver) sleep(ctx context.Context, wait time.Duration) error {
	if d.Sleep != nil {
		return d.Sleep(ctx, wait)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(wait):
		return nil
	}
}

// call runs a daemon call, retrying what may pass (the daemon unreachable or
// busy) a few times; what is left is a daemonError.
func (d *Driver) call(ctx context.Context, what string, f func() error) error {
	var err error
	for attempt, wait := 0, 2*time.Second; attempt < 4; attempt, wait = attempt+1, 2*wait {
		if err = f(); err == nil || !retryable(err) || ctx.Err() != nil {
			break
		}
		d.logger().Warn("daemon call failed; retrying", "call", what, "error", err)
		if d.sleep(ctx, wait) != nil {
			break
		}
	}
	if err != nil {
		return daemonError{fmt.Errorf("%s: %w", what, err)}
	}
	return nil
}

func (d *Driver) refresh(ctx context.Context) (store.BenchStatus, error) {
	err := d.call(ctx, "status", func() error {
		status, err := d.Daemon.Status(ctx)
		if err == nil {
			d.status = status
		}
		return err
	})
	return d.status, err
}

// controlled reports whether the run sweeps settings (GPU and CPU benches).
func (d *Driver) controlled() bool {
	return d.status.Run.Class == store.BenchGPU || d.status.Run.Class == store.BenchCPU
}

// Run carries out the run runID: the token's run, which runID must name. A
// resumed run (its earlier attempt suspended) measures only the kinds it is
// not done with. Suspended or stopped by a signal, Run leaves at the next
// point with nil (the attempt exits 0); see safePoint.
func (d *Driver) Run(ctx context.Context, runID string) error {
	if d.Timing == (Timing{}) {
		d.Timing = DefaultTiming()
	}
	d.versions, d.recorded, d.checkpoints = map[string]store.BenchVersion{}, map[string]bool{}, map[string]string{}
	status, err := d.refresh(ctx)
	if err != nil {
		return err
	}
	if status.Run.ID != runID {
		return fmt.Errorf("this attempt carries out bench run %s, not %s", status.Run.ID, runID)
	}
	if status.Run.Class == store.BenchNetwork && status.Rank == 0 {
		return d.serve(ctx)
	}
	if !status.Run.Live() {
		return fmt.Errorf("bench run %s is %s", runID, status.Run.State)
	}
	d.seq = status.NextSeq
	log := d.logger().With("run", runID, "host", status.Run.HostID, "device", status.Run.DeviceID)
	// The kinds an earlier attempt finished, as the store has them.
	references := map[string]store.BenchReference{}
	for kind, ref := range status.Run.References {
		references[kind] = ref
	}
	if d.Continuation != "" {
		c, err := ParseContinuation(d.Continuation)
		switch {
		case err != nil || c.Run != runID:
			log.Warn("the continuation is not this run's; resuming from what the run recorded", "continuation", d.Continuation)
		default:
			for _, kind := range c.Done {
				if _, ok := references[kind]; !ok {
					log.Warn("a kind the continuation says is done has no reference recorded: measuring it again", "kind", kind)
				}
			}
		}
		log.Info("resuming the bench run", "done", d.doneKinds())
	}
	// Whatever else uses the device (a process Kairo did not start, a gang
	// across the nodes) is waited out, for a while.
	for deadline := time.Now().Add(d.Timing.OtherWorkWait); len(d.status.OtherWork) != 0; {
		if time.Now().After(deadline) {
			reason := FlagOtherWork + ": " + strings.Join(d.status.OtherWork, "; ")
			log.Warn("giving up: the device is in use", "other_work", d.status.OtherWork)
			if err = d.start(ctx, store.BenchStart{}); err != nil {
				return err
			}
			return d.finish(ctx, references, reason)
		}
		log.Info("waiting for other work on the device to end", "other_work", d.status.OtherWork)
		if err = d.safePoint(ctx); err != nil {
			return d.leave(ctx, err)
		}
		if err = d.sleep(ctx, d.Timing.Poll); err != nil {
			return err
		}
		if _, err = d.refresh(ctx); err != nil {
			return err
		}
	}
	if err = d.start(ctx, store.BenchStart{IdleTemperatureC: d.status.TemperatureC}); err != nil {
		return err
	}
	for _, kind := range status.Run.Kinds {
		if _, done := references[kind]; done {
			log.Info("bench kind done before the run was suspended", "kind", kind)
			continue
		}
		if err = d.safePoint(ctx); err != nil {
			return d.leave(ctx, err)
		}
		log.Info("bench kind starting", "kind", kind)
		ref, err := d.runKind(ctx, kind)
		var intr interrupted
		if errors.As(err, &intr) || err != nil && d.stopping() {
			// (a stop signal reaches the primitive too, which then ends)
			log.Info("leaving the bench run", "kind", kind, "error", err)
			return d.leave(ctx, intr)
		}
		var de daemonError
		if errors.As(err, &de) || ctx.Err() != nil {
			return errors.Join(err, ctx.Err())
		}
		if err != nil {
			log.Warn("bench kind failed", "kind", kind, "error", err)
			ref.Valid = false
			if ref.Reason == "" {
				ref.Reason = err.Error()
			}
		}
		references[kind] = ref
		d.recordKind(ctx, kind, ref)
		log.Info("bench kind done", "kind", kind, "valid", ref.Valid, "work_per_second", ref.Mean, "spread", ref.Spread, "reason", ref.Reason)
	}
	var reasons []string
	for _, kind := range status.Run.Kinds {
		if ref := references[kind]; !ref.Valid {
			reasons = append(reasons, kind+": "+ref.Reason)
		}
	}
	return d.finish(ctx, references, strings.Join(reasons, "; "))
}

func (d *Driver) stopping() bool { return d.Stopping != nil && d.Stopping() }

// recordKind records a kind as done. A daemon that cannot (one from before
// runs were resumed) only makes a resumed run measure the kind again.
func (d *Driver) recordKind(ctx context.Context, kind string, ref store.BenchReference) {
	if err := d.call(ctx, "record kind", func() error { return d.Daemon.RecordKind(ctx, kind, ref) }); err != nil {
		d.logger().Warn("cannot record the kind as done; a resumed run measures it again", "kind", kind, "error", err)
		return
	}
	d.recorded[kind] = true
}

// doneKinds are the run's kinds the store has a reference of.
func (d *Driver) doneKinds() []string {
	var done []string
	for _, kind := range d.status.Run.Kinds {
		if _, ok := d.status.Run.References[kind]; ok || d.recorded[kind] {
			done = append(done, kind)
		}
	}
	return done
}

// gone: the daemon no longer has what the request names (a command
// withdrawn meanwhile).
func gone(err error) bool {
	var apiErr *APIError
	return errors.As(err, &apiErr) && apiErr.Status == 404
}

// safePoint is where the run may be left, as an SDK's safe point: before
// every point (and kind), every CommandPoll seconds of a warm-up, and while
// waiting for other work. A stop signal leaves at once. A suspend command is
// acknowledged accepted and checkpointing and leaves (leave publishes the
// checkpoint once the workload is stopped); one withdrawn meanwhile is
// skipped, as are other kinds of command. Commands that cannot be polled are
// looked for at the next safe point.
func (d *Driver) safePoint(ctx context.Context) error {
	if d.stopping() {
		return interrupted{}
	}
	var commands []store.Command
	if err := d.call(ctx, "commands", func() error {
		var err error
		commands, err = d.Daemon.Commands(ctx)
		return err
	}); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		d.logger().Warn("cannot poll the attempt's commands; looking again at the next point", "error", err)
		return nil
	}
	for _, command := range commands {
		if command.Kind != "suspend" {
			d.logger().Debug("skipping a command of an unknown kind", "command", command.ID, "kind", command.Kind)
			continue
		}
		err := d.ack(ctx, command.ID, "accepted", nil)
		if err == nil {
			err = d.ack(ctx, command.ID, "checkpointing", nil)
		}
		if gone(err) {
			d.logger().Info("the suspend was withdrawn", "command", command.ID)
			continue
		}
		if err != nil {
			return err
		}
		d.logger().Info("suspend received: leaving the run at this point", "command", command.ID, "origin", command.Origin, "reason", command.Reason)
		return interrupted{command: &command}
	}
	return nil
}

func (d *Driver) ack(ctx context.Context, commandID, phase string, payload map[string]any) error {
	return d.call(ctx, "ack "+phase, func() error { return d.Daemon.Ack(ctx, commandID, phase, payload) })
}

// leave ends the attempt at an interruption (err, from safePoint; any other
// error is returned as it is), its workload stopped: the bench setting is
// cleared (the lease's release would anyway, returning the device to its
// defaults), and a suspend is acknowledged checkpointed with the run's
// Continuation, the same for a command delivered again. A stop signal
// publishes nothing: a force stop plans no continuation, and its run fails.
func (d *Driver) leave(ctx context.Context, err error) error {
	var intr interrupted
	if !errors.As(err, &intr) {
		return err
	}
	if d.controlled() {
		if err := d.call(ctx, "clear setting", func() error { _, err := d.Daemon.SetSetting(ctx, store.DeviceSettings{}); return err }); err != nil {
			d.logger().Warn("cannot clear the bench setting; the lease's release will", "error", err)
		}
	}
	if intr.command == nil {
		d.logger().Info("stop signal: left the bench run", "done", d.doneKinds())
		return nil
	}
	ref, ok := d.checkpoints[intr.command.ID]
	if !ok {
		ref = Continuation{Run: d.status.Run.ID, Done: d.doneKinds()}.String()
		d.checkpoints[intr.command.ID] = ref
	}
	if err := d.ack(ctx, intr.command.ID, "checkpointed", map[string]any{"continuation_ref": ref}); err != nil {
		return err
	}
	d.logger().Info("suspended: checkpoint published", "command", intr.command.ID, "continuation", ref)
	return nil
}

func (d *Driver) start(ctx context.Context, start store.BenchStart) error {
	start.DriverBuild = buildinfo.Version()
	start.Versions = d.versions
	err := d.call(ctx, "start", func() error { return d.Daemon.Start(ctx, start) })
	d.started = d.started || err == nil
	return err
}

// finish clears the bench setting (the lease's release would) and reports
// the outcome.
func (d *Driver) finish(ctx context.Context, references map[string]store.BenchReference, reason string) error {
	if d.controlled() {
		if err := d.call(ctx, "clear setting", func() error { _, err := d.Daemon.SetSetting(ctx, store.DeviceSettings{}); return err }); err != nil {
			d.logger().Warn("cannot clear the bench setting; the lease's release will", "error", err)
		}
	}
	return d.call(ctx, "finish", func() error {
		return d.Daemon.Finish(ctx, store.BenchFinish{References: references, Valid: reason == "", Reason: reason})
	})
}

// stream reads a primitive's records.
type stream struct {
	proc  Process
	start Record
	wall  time.Time // the start record's wall time
	unit  string
	// at is the stream time (seconds) of the latest interval's end.
	at float64
}

// open reads up to the start record.
func (s *stream) open() error {
	for {
		r, err := s.proc.Next()
		if errors.Is(err, io.EOF) {
			return errors.New("the primitive ended before its start record")
		}
		if err != nil {
			return err
		}
		switch r.Type {
		case "start":
			s.start = r
			if len(r.Units) == 0 {
				return errors.New("the start record names no unit")
			}
			s.unit = r.Units[0]
			if s.wall, err = time.Parse(time.RFC3339Nano, r.Wall); err != nil {
				return fmt.Errorf("start record wall time: %w", err)
			}
			return nil
		case "end":
			return endError(r)
		}
	}
}

func endError(r Record) error {
	text := r.Reason
	if r.Error != nil {
		text += ": " + *r.Error
	}
	return fmt.Errorf("%w (%s)", errEnded, text)
}

// next is the next interval record; errEnded at the end record.
func (s *stream) next() (Record, error) {
	for {
		r, err := s.proc.Next()
		if errors.Is(err, io.EOF) {
			return r, fmt.Errorf("%w (output closed)", errEnded)
		}
		if err != nil {
			return r, err
		}
		switch r.Type {
		case "interval":
			s.at = float64(r.EndNS) / 1e9
			return r, nil
		case "end":
			return r, endError(r)
		}
	}
}

// kindRun is one kind's process and points.
type kindRun struct {
	d      *Driver
	kind   store.BenchKind
	work   workload
	s      *stream
	points []store.BenchPoint
	// firstRef is the first reference's throughput, which points are
	// compared with until every reference is in.
	firstRef float64
}

// next is the stream's next interval, unless a stop signal came: the run is
// left within a second of it (a force stop's grace period is short), the
// point it was measuring dropped.
func (k *kindRun) next() (Record, error) {
	if k.d.stopping() {
		return Record{}, interrupted{}
	}
	return k.s.next()
}

// seconds bounds a kind's primitive run: the whole procedure with every wait
// at its longest, and a margin.
func (d *Driver) seconds(kind store.BenchKind, w workload, points int) int {
	t := d.Timing
	if w.untilEnd {
		return 3600
	}
	if kind.Sweep == "" {
		return int(t.ShortSettle+t.ApplyTimeout+t.Measure) + 60
	}
	return int(t.SteadyMax+float64(points+3)*(t.ApplyTimeout+t.Settle+t.Measure+5)) + 120
}

// peer is the network server: rank 0's address and port.
func (d *Driver) peer() string {
	env := d.Env
	if env == nil {
		return ""
	}
	return net.JoinHostPort(env("KAIRO_GANG_MASTER_ADDR"), env("KAIRO_GANG_MASTER_PORT"))
}

func (d *Driver) runKind(ctx context.Context, name string) (store.BenchReference, error) {
	kind, ok := store.FindBenchKind(name)
	if !ok {
		return store.BenchReference{}, fmt.Errorf("unknown bench kind %q", name)
	}
	w := workloads[name]
	grid := Grid(kind, d.status.Capabilities)
	peer := ""
	if d.status.Run.Class == store.BenchNetwork {
		peer = d.peer()
		if err := d.waitServer(ctx, peer); err != nil {
			return store.BenchReference{}, err
		}
	}
	args, err := argv(name, d.status, peer, d.seconds(kind, w, len(grid)))
	if err != nil {
		return store.BenchReference{}, err
	}
	proc, err := d.Runner.Start(ctx, args)
	if err != nil {
		return store.BenchReference{}, fmt.Errorf("start %s: %w", args[0], err)
	}
	defer func() {
		if err := proc.Stop(); err != nil {
			d.logger().Debug("stop primitive", "kind", name, "error", err)
		}
		if err := proc.Wait(); err != nil {
			d.logger().Debug("primitive exit", "kind", name, "error", err)
		}
	}()
	k := &kindRun{d: d, kind: kind, work: w, s: &stream{proc: proc}}
	if err = k.s.open(); err != nil {
		return store.BenchReference{}, err
	}
	d.versions[name] = store.BenchVersion{Program: k.s.start.Program, Version: k.s.start.Version, Build: k.s.start.Build}
	var device struct {
		DriverVersion string `json:"driver_version"`
	}
	_ = json.Unmarshal(k.s.start.Device, &device)
	if err = d.start(ctx, store.BenchStart{DriverVersion: device.DriverVersion, Device: k.s.start.Device}); err != nil {
		return store.BenchReference{}, err
	}
	if kind.Sweep == "" {
		p, err := k.point(ctx, store.PhaseMeasure, store.DeviceSettings{}, d.Timing.ShortSettle, nil)
		if p == nil {
			return store.BenchReference{}, err
		}
		return k.reference([]store.BenchPoint{*p}), stopAtEnd(err, w)
	}
	return k.sweep(ctx, grid)
}

// stopAtEnd: a kind measured until its end record ended as it should.
func stopAtEnd(err error, w workload) error {
	if w.untilEnd && errors.Is(err, errEnded) {
		return nil
	}
	return err
}

// sweep: the defaults, a warm-up, then the references around the two halves
// of the grid.
func (k *kindRun) sweep(ctx context.Context, grid []store.DeviceSettings) (store.BenchReference, error) {
	d := k.d
	if err := k.request(ctx, store.DeviceSettings{}); err != nil {
		return store.BenchReference{}, err
	}
	var notes []string
	warm := Steady{Window: d.Timing.SteadyWindow}
	for begin, polled := k.s.at, k.s.at; ; {
		r, err := k.next()
		if err != nil {
			return store.BenchReference{Reason: "warm-up: " + err.Error()}, err
		}
		warm.Add(k.s.at, r.temperature(), r.Work[k.s.unit]/max(r.Seconds(), 1e-9))
		if warm.Reached() {
			break
		}
		if k.s.at-begin >= d.Timing.SteadyMax {
			notes = append(notes, fmt.Sprintf("%s after %.0f s", FlagNotSteady, k.s.at-begin))
			break
		}
		// A warm-up may take minutes: a suspend need not wait for it.
		if k.s.at-polled >= d.Timing.CommandPoll {
			polled = k.s.at
			if err = d.safePoint(ctx); err != nil {
				return store.BenchReference{}, err
			}
		}
	}
	d.logger().Info("warmed up", "kind", k.kind.Name, "seconds", k.s.at, "notes", notes)
	var refs []store.BenchPoint
	reference := func(notes []string) error {
		p, err := k.point(ctx, store.PhaseReference, store.DeviceSettings{}, d.Timing.Settle, notes)
		if p != nil {
			refs = append(refs, *p)
		}
		return err
	}
	half := len(grid) / 2
	steps := []func() error{func() error { return reference(notes) }}
	for i, s := range grid {
		if i == half {
			steps = append(steps, func() error { return reference(nil) })
		}
		steps = append(steps, func() error { _, err := k.point(ctx, store.PhaseSweep, s, d.Timing.Settle, nil); return err })
	}
	steps = append(steps, func() error { return reference(nil) })
	var stepErr error
	for _, step := range steps {
		if stepErr = step(); stepErr != nil {
			break
		}
	}
	// Left part-way: the kind is measured again from the start, and its
	// points so far go.
	if intr := (interrupted{}); errors.As(stepErr, &intr) || stepErr != nil && d.stopping() {
		return store.BenchReference{}, stepErr
	}
	ref := k.reference(refs)
	if stepErr != nil {
		ref.Valid = false
		ref.Reason = stepErr.Error()
	} else if len(refs) != 3 {
		ref.Valid, ref.Reason = false, fmt.Sprintf("%d of 3 references", len(refs))
	}
	// The points against the mean of every reference, replacing the copies
	// sent as they were measured.
	for i := range k.points {
		if ref.Mean > 0 {
			k.points[i].Metrics.RelPerf = ptr(k.points[i].Metrics.WorkPerSecond / ref.Mean)
		}
	}
	if len(k.points) != 0 {
		if err := d.call(ctx, "points", func() error { return d.Daemon.AddPoints(ctx, k.points) }); err != nil {
			return ref, err
		}
	}
	var de daemonError
	if errors.As(stepErr, &de) {
		return ref, stepErr
	}
	return ref, nil
}

// reference is a kind's reference from its reference points.
func (k *kindRun) reference(refs []store.BenchPoint) store.BenchReference {
	ref := store.BenchReference{Unit: k.s.unit, WorkPerSecond: []float64{}, Valid: len(refs) != 0}
	var power, jpw, utilGPU, utilMem []float64
	for _, p := range refs {
		ref.WorkPerSecond = append(ref.WorkPerSecond, p.Metrics.WorkPerSecond)
		if !p.Valid {
			ref.Valid = false
			ref.Reason = fmt.Sprintf("reference %d invalid: %s", p.Seq, strings.Join(p.Flags, "; "))
		}
		if p.Metrics.PowerW != nil {
			power = append(power, *p.Metrics.PowerW)
		}
		if p.Metrics.JoulesPerWork != nil {
			jpw = append(jpw, *p.Metrics.JoulesPerWork)
		}
		if p.Metrics.UtilGPUPct != nil && p.Metrics.UtilMemPct != nil {
			utilGPU = append(utilGPU, *p.Metrics.UtilGPUPct)
			utilMem = append(utilMem, *p.Metrics.UtilMemPct)
		}
	}
	ref.Mean = mean(ref.WorkPerSecond)
	ref.Spread = store.RelSpread(ref.WorkPerSecond)
	if len(power) == len(refs) && len(refs) != 0 {
		ref.PowerW = ptr(mean(power))
	}
	if len(jpw) == len(refs) && len(refs) != 0 {
		ref.JoulesPerWork = ptr(mean(jpw))
	}
	if len(utilGPU) == len(refs) && len(refs) != 0 {
		ref.Fingerprint = &store.Fingerprint{UtilGPUPct: mean(utilGPU), UtilMemPct: mean(utilMem)}
	}
	if ref.Valid && ref.Spread > MaxReferenceSpread {
		ref.Valid = false
		ref.Reason = fmt.Sprintf("references spread %.1f%% (more than %.0f%%)", 100*ref.Spread, 100*MaxReferenceSpread)
	}
	return ref
}

// MaxReferenceSpread is the most a kind's references may disagree ((max -
// min) / mean) for its curve to be trusted.
const MaxReferenceSpread = 0.01

// request asks for s as the bench setting (the zero setting clears it). A
// setting the daemon refuses is no error: the wait for it reports it.
func (k *kindRun) request(ctx context.Context, s store.DeviceSettings) error {
	d := k.d
	if !d.controlled() {
		return nil
	}
	var refused error
	err := d.call(ctx, "set setting", func() error {
		_, err := d.Daemon.SetSetting(ctx, s)
		var apiErr *APIError
		if errors.As(err, &apiErr) && apiErr.Status < 500 {
			refused, err = err, nil
		}
		return err
	})
	if refused != nil {
		d.logger().Warn("bench setting refused", "settings", s.String(), "error", refused)
	}
	return err
}

// holds reports whether the device is known to hold want: its controller
// applied the generation in effect, and what is in effect is want (the
// defaults: no setting at all, not even the policy's).
func holds(status store.BenchStatus, want store.DeviceSettings) bool {
	if !status.Applies() || !status.Applied.Equal(want) {
		return false
	}
	if want.IsDefault() {
		return status.Effective == nil
	}
	return status.Effective != nil && status.Effective.Owner.Kind == store.OwnerBench && status.Effective.Settings.Equal(want)
}

// point measures one setting: request it, wait until its controller reports
// it applied, settle, measure, and look again at what else uses the device.
// The point is uploaded as soon as it is measured. A nil point comes with an
// error that ended the kind before anything was measured.
func (k *kindRun) point(ctx context.Context, phase string, want store.DeviceSettings, settle float64, notes []string) (*store.BenchPoint, error) {
	d := k.d
	// Between points the run may be left (suspended, or stopped).
	if err := d.safePoint(ctx); err != nil {
		return nil, err
	}
	flags := append([]string{}, notes...)
	otherWork := func(status store.BenchStatus) {
		for _, w := range status.OtherWork {
			if flag := FlagOtherWork + ": " + w; !slices.Contains(flags, flag) {
				flags = append(flags, flag)
			}
		}
	}
	if d.controlled() {
		lastError := d.status.LastError
		if err := k.request(ctx, want); err != nil {
			return nil, err
		}
		for waited := 0.0; ; {
			status, err := d.refresh(ctx)
			if err != nil {
				return nil, err
			}
			if holds(status, want) {
				break
			}
			if status.Effective != nil && status.Effective.Owner.Kind == store.OwnerManual {
				flags = append(flags, FlagManual+": "+status.Effective.Settings.String())
				break
			}
			if status.LastError != nil && (lastError == nil || *status.LastError != *lastError) {
				flags = append(flags, FlagNotApplied+": "+*status.LastError)
				break
			}
			if waited >= d.Timing.ApplyTimeout {
				if want.IsDefault() && status.Effective != nil && status.Effective.Owner.Kind == store.OwnerPolicy {
					flags = append(flags, FlagPolicy+": "+status.Effective.Settings.String())
				} else {
					flags = append(flags, fmt.Sprintf("%s: %s not reported applied within %.0f s", FlagNotApplied, want, d.Timing.ApplyTimeout))
				}
				break
			}
			r, err := k.next()
			if err != nil {
				return nil, err
			}
			waited += r.Seconds()
		}
	} else if _, err := d.refresh(ctx); err != nil {
		return nil, err
	}
	otherWork(d.status)
	var ended error
	for settled := 0.0; settled < settle; {
		r, err := k.next()
		if err != nil {
			return nil, err
		}
		settled += r.Seconds()
	}
	win := &window{unit: k.s.unit, start: k.s.wall}
	for k.work.untilEnd || win.seconds() < d.Timing.Measure {
		r, err := k.next()
		if err != nil {
			ended = err
			break
		}
		win.add(r)
	}
	if ended != nil && !(k.work.untilEnd && errors.Is(ended, errEnded)) {
		if !errors.Is(ended, errEnded) {
			return nil, ended
		}
		flags = append(flags, FlagPrimitiveEnd+": "+ended.Error())
	}
	status, err := d.refresh(ctx)
	if err != nil {
		return nil, err
	}
	otherWork(status)
	if d.controlled() && !holds(status, want) && validFlags(flags) {
		flags = append(flags, FlagNotApplied+": the setting changed during the measurement")
	}
	m := win.metrics()
	if d.status.Run.Class == store.BenchCPU && len(win.intervals) != 0 {
		from, to := win.bounds()
		var energy store.CPUEnergy
		if err := d.call(ctx, "cpu energy", func() error {
			var err error
			energy, err = d.Daemon.CPUEnergy(ctx, from, to)
			return err
		}); err != nil {
			return nil, err
		}
		flags = append(flags, withCPUEnergy(&m, energy)...)
	}
	if d.status.Run.Class == store.BenchGPU {
		flags = append(flags, thermalFlag(m, k.clockTarget(want))...)
	}
	if m.Work <= 0 {
		flags = append(flags, FlagNoWork)
	}
	p := store.BenchPoint{Seq: d.seq, Kind: k.kind.Name, Phase: phase, Settings: want, Metrics: m, Valid: validFlags(flags), Flags: flags, MeasuredAt: time.Now().UTC().Format(time.RFC3339Nano)}
	d.seq++
	switch {
	case phase != store.PhaseSweep && k.firstRef == 0:
		k.firstRef = m.WorkPerSecond
		p.Metrics.RelPerf = ptr(1.0)
	case k.firstRef > 0:
		p.Metrics.RelPerf = ptr(m.WorkPerSecond / k.firstRef)
	}
	k.points = append(k.points, p)
	d.logger().Info("bench point", "kind", p.Kind, "phase", phase, "settings", want.String(), "work_per_second", m.WorkPerSecond, "valid", p.Valid, "flags", flags)
	if err := d.call(ctx, "points", func() error { return d.Daemon.AddPoints(ctx, []store.BenchPoint{p}) }); err != nil {
		return &p, err
	}
	return &p, ended
}

// clockTarget is the graphics clock a GPU point should reach: its cap, else
// the greatest supported clock.
func (k *kindRun) clockTarget(want store.DeviceSettings) float64 {
	if want.GraphicsClockMaxMHz != nil {
		return float64(*want.GraphicsClockMaxMHz)
	}
	// Uncapped (the defaults, a power limit): what the GPU does with its heat
	// is what the point measures, so there is no clock it falls short of.
	return 0
}

// waitServer waits until the network bench's server accepts connections.
func (d *Driver) waitServer(ctx context.Context, addr string) error {
	dial := d.Dial
	if dial == nil {
		dial = func(ctx context.Context, addr string) error {
			conn, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, "tcp", addr)
			if err == nil {
				conn.Close()
			}
			return err
		}
	}
	deadline := time.Now().Add(d.Timing.ServerWait)
	for {
		err := dial(ctx, addr)
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("the network bench server %s does not accept connections: %w", addr, err)
		}
		if err := d.sleep(ctx, 2*time.Second); err != nil {
			return err
		}
	}
}

// serve is rank 0 of a network bench: it serves the client rank until the
// run ends, or the server exits by itself (idle). Suspended (the gang's
// every rank is), it publishes the run's checkpoint: the leader's is the
// one the task's continuation follows.
func (d *Driver) serve(ctx context.Context) error {
	args, err := serveArgv(d.status, d.peer())
	if err != nil {
		return err
	}
	proc, err := d.Runner.Start(ctx, args)
	if err != nil {
		return fmt.Errorf("start %s: %w", args[0], err)
	}
	d.logger().Info("serving the network bench", "listen", d.peer())
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			if _, err := proc.Next(); err != nil && !strings.Contains(err.Error(), "bench record") {
				return // the output ended
			}
		}
	}()
	defer func() {
		_ = proc.Stop()
		<-done
		_ = proc.Wait()
	}()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
			return nil
		case <-time.After(d.Timing.Poll):
		}
		if status, err := d.refresh(ctx); err != nil || !status.Run.Live() {
			return err
		}
		if err := d.safePoint(ctx); err != nil {
			return d.leave(ctx, err)
		}
	}
}

// ExecRunner runs primitives as child processes, reading their records from
// stdout; their stderr goes to Stderr.
type ExecRunner struct {
	Stderr io.Writer
}

func (r ExecRunner) Start(ctx context.Context, argv []string) (Process, error) {
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.Stderr = r.Stderr
	out, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err = cmd.Start(); err != nil {
		return nil, err
	}
	scanner := bufio.NewScanner(out)
	scanner.Buffer(make([]byte, 0, 64<<10), 4<<20)
	return &execProcess{cmd: cmd, scanner: scanner}, nil
}

type execProcess struct {
	cmd     *exec.Cmd
	scanner *bufio.Scanner
}

func (p *execProcess) Next() (Record, error) {
	for p.scanner.Scan() {
		line := bytes.TrimSpace(p.scanner.Bytes())
		if len(line) != 0 {
			return parseRecord(line)
		}
	}
	if err := p.scanner.Err(); err != nil {
		return Record{}, err
	}
	return Record{}, io.EOF
}

// Stop asks the primitive to end: SIGTERM, which it handles by writing its
// final interval and end record. Windows cannot signal a child that way, so
// it is killed: the primitives leave nothing behind (their test files are
// deleted on close, GPU memory goes with the process).
func (p *execProcess) Stop() error {
	if p.cmd.Process == nil || p.cmd.ProcessState != nil {
		return nil
	}
	if runtime.GOOS == "windows" {
		return p.cmd.Process.Kill()
	}
	return p.cmd.Process.Signal(syscall.SIGTERM)
}

// Wait reads what is left of the output (so the child never blocks on a full
// pipe) and reaps it.
func (p *execProcess) Wait() error {
	for p.scanner.Scan() {
	}
	return p.cmd.Wait()
}
