// Package devicecontrol is `kairo controller`: the process on a physical host
// that keeps the settings of the devices its node controls (GPU clocks and
// power limits, CPU maximum frequency) as the daemon's desired state says,
// and reports what is in place. It is the only part of Kairo that changes
// hardware settings, and it errs toward the hardware's defaults: a device
// without a desired setting is reset, and so is every device it changed when
// it stops or when the daemon stays unreachable.
package devicecontrol

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"runtime"
	"slices"
	"time"

	"kairo/internal/cpupower"
	"kairo/internal/store"
)

// Daemon is the daemon's side of a controller (agentclient.Client).
type Daemon interface {
	RegisterController(ctx context.Context, node store.Node, version string) error
	DeviceSettings(ctx context.Context, nodeID string) ([]store.ControlledDevice, error)
	ReportDevices(ctx context.Context, nodeID string, reports []store.DeviceReport) ([]string, error)
}

// SampleReporter is a Daemon that takes what the controller reads of its host
// (agentclient.Client does): the CPU package power, which on a Proxmox host
// only the host itself can read.
type SampleReporter interface {
	ReportSamples(ctx context.Context, nodeID string, samples []store.ControllerSample) ([]string, error)
}

// Actuator changes the settings of one kind of device.
type Actuator interface {
	// Capabilities reads what can be changed on the device, and within which
	// ranges.
	Capabilities(ctx context.Context, device string) (store.DeviceCapabilities, error)
	// Apply makes the device's settings s: a setting s leaves out is returned
	// to the hardware's default, so the zero value resets the device.
	Apply(ctx context.Context, device string, s store.DeviceSettings) error
}

// HostActuators are this host's actuators: nvidia-smi for GPUs, and powercfg
// (Windows) or sysfs (Linux) for the CPU.
func HostActuators(state *State) map[string]Actuator {
	var cpu Actuator = &Sysfs{}
	if runtime.GOOS == "windows" {
		cpu = &Powercfg{State: state}
	}
	return map[string]Actuator{"gpu": &NVIDIA{}, "cpu": cpu}
}

// Defaults of a Controller's timing.
const (
	DefaultPollInterval = 10 * time.Second
	DefaultResetAfter   = 2 * time.Minute
	// capabilitiesRefresh is how often a device's capabilities are read again,
	// reapplyEvery how often a setting in place is carried out again.
	capabilitiesRefresh = 10 * time.Minute
	reapplyEvery        = 10 * time.Minute
	// Failed changes are retried after retryBase, doubling up to retryMax.
	retryBase = 5 * time.Second
	retryMax  = 5 * time.Minute
	// A device that left the controller's control is forgotten after this many
	// failed resets.
	abandonAfter = 5
	// stopTimeout bounds the resets and the report of a stopping controller.
	stopTimeout = 2 * time.Minute
)

// Controller keeps the devices its node controls as the daemon says. Run
// drives it; Step and Stop are one poll and the shutdown, for tests.
type Controller struct {
	Daemon  Daemon
	Node    store.Node
	Version string
	// Actuators change devices by kind ("gpu", "cpu"); a kind without one
	// fails every change of such a device.
	Actuators map[string]Actuator
	// State keeps the controlled devices across a restart (nil: nothing kept).
	State        *State
	PollInterval time.Duration
	// ResetAfter is how long the daemon may be unreachable before every
	// device is returned to its defaults.
	ResetAfter time.Duration
	// CPUMeter reads the host's CPU package power, reported at every poll to
	// a Daemon that is a SampleReporter (nil: not read).
	CPUMeter cpupower.Meter
	Now      func() time.Time
	Logger   *slog.Logger

	devices     map[deviceKey]*device
	registered  bool
	lastContact time.Time
	// unreachableSince is when the daemon stopped answering (zero: it answers).
	unreachableSince time.Time
	lossReset        bool
	// powerSkipped: the daemon skipped the latest power sample (logged once).
	powerSkipped bool
}

type deviceKey struct{ host, id string }

func (k deviceKey) String() string { return k.host + "/" + k.id }

// device is what the controller knows of one device.
type device struct {
	kind string
	// listed: the daemon's latest poll lists it; desired is that poll's entry.
	listed  bool
	desired store.ControlledDevice
	// inPlace is what this process put in place (nil: not known, so neither
	// the defaults), inPlaceGeneration the generation it carried out.
	inPlace           *store.DeviceSettings
	inPlaceGeneration *int64
	// appliedAt is when inPlace was last carried out.
	appliedAt time.Time
	// failures counts the failed changes in a row; the next is tried at retryAt.
	failures int
	retryAt  time.Time
	failedOf int64 // the generation that failed
	// leftTries counts the resets tried since the device left control.
	leftTries int
	capsAt    time.Time
	report    *store.DeviceReport
}

func (c *Controller) now() time.Time {
	if c.Now != nil {
		return c.Now()
	}
	return time.Now()
}

func (c *Controller) logger() *slog.Logger {
	if c.Logger != nil {
		return c.Logger
	}
	return slog.Default()
}

func (c *Controller) init() {
	if c.devices != nil {
		return
	}
	if c.PollInterval == 0 {
		c.PollInterval = DefaultPollInterval
	}
	if c.ResetAfter == 0 {
		c.ResetAfter = DefaultResetAfter
	}
	c.devices = map[deviceKey]*device{}
	c.lastContact = c.now()
	if c.State != nil {
		// Devices of an earlier run: their settings are not known, so they
		// are reset unless the daemon says otherwise.
		for _, d := range c.State.Devices() {
			c.devices[deviceKey{d.HostID, d.DeviceID}] = &device{kind: d.Kind}
		}
	}
}

// Run polls until ctx ends, then returns every device it changed to its
// defaults and reports that.
func (c *Controller) Run(ctx context.Context) error {
	c.init()
	ticker := time.NewTicker(c.PollInterval)
	defer ticker.Stop()
	for {
		c.Step(ctx)
		select {
		case <-ctx.Done():
			stopCtx, cancel := context.WithTimeout(context.Background(), stopTimeout)
			defer cancel()
			c.Stop(stopCtx)
			return nil
		case <-ticker.C:
		}
	}
}

// Step is one poll: register if not yet, fetch the desired settings, bring
// every device to them, and report. A daemon that does not answer for
// ResetAfter gets every device returned to its defaults.
func (c *Controller) Step(ctx context.Context) {
	c.init()
	if !c.registered {
		if err := c.Daemon.RegisterController(ctx, c.Node, c.Version); err != nil {
			c.unreachable(ctx, err)
			return
		}
		c.registered = true
		c.logger().Info("controller registered", "node_id", c.Node.ID)
	}
	listed, err := c.Daemon.DeviceSettings(ctx, c.Node.ID)
	if err != nil {
		c.registered = false // a daemon that lost its node learns it again
		c.unreachable(ctx, err)
		return
	}
	if !c.unreachableSince.IsZero() {
		c.logger().Info("daemon reachable again", "after_seconds", c.now().Sub(c.unreachableSince).Seconds())
	}
	c.lastContact, c.unreachableSince, c.lossReset = c.now(), time.Time{}, false
	c.sync(ctx, listed)
	for _, key := range c.keys() {
		if d := c.devices[key]; d.listed {
			c.reconcile(ctx, key, d)
		}
	}
	c.flush(ctx)
	c.reportPower(ctx)
}

// reportPower sends the host's CPU package power since the previous poll. A
// reading the daemon does not take is dropped: the next covers its own span
// only, and the gap shows as a gap.
func (c *Controller) reportPower(ctx context.Context) {
	reporter, ok := c.Daemon.(SampleReporter)
	if c.CPUMeter == nil || !ok {
		return
	}
	reading, ok := c.CPUMeter.Read()
	if !ok {
		return
	}
	watts := reading.Watts
	sample := store.ControllerSample{DeviceID: "cpu", ObservedAt: reading.At.UTC().Format(time.RFC3339Nano),
		// valid until the next poll's reading, with a poll to spare
		ValidUntil: reading.At.Add(2 * c.PollInterval).UTC().Format(time.RFC3339Nano), PowerW: &watts}
	skipped, err := reporter.ReportSamples(ctx, c.Node.ID, []store.ControllerSample{sample})
	switch {
	case err != nil:
		c.logger().Debug("cannot report the CPU package power", "error", err)
	case len(skipped) != 0 && !c.powerSkipped:
		c.powerSkipped = true
		c.logger().Info("daemon skipped the CPU package power: no host names this controller")
	case len(skipped) == 0:
		c.powerSkipped = false
	}
}

// Stop returns every device not known to be at its defaults to them, and
// reports what is in place.
func (c *Controller) Stop(ctx context.Context) {
	c.init()
	c.logger().Info("controller stopping: returning devices to their defaults")
	// A reset that fails is tried once more before giving up.
	for range 2 {
		for _, key := range c.keys() {
			c.reset(ctx, key, c.devices[key], "controller stopped", true)
		}
	}
	c.flush(ctx)
}

func (c *Controller) keys() []deviceKey {
	return slices.SortedFunc(maps.Keys(c.devices), func(a, b deviceKey) int {
		return cmp.Or(cmp.Compare(a.host, b.host), cmp.Compare(a.id, b.id))
	})
}

// unreachable handles a failed poll: past ResetAfter without an answer,
// every device goes back to its defaults (and is retried until it does).
func (c *Controller) unreachable(ctx context.Context, err error) {
	if c.unreachableSince.IsZero() {
		c.unreachableSince = c.now()
		c.logger().Warn("daemon unreachable; retrying", "error", err)
	}
	if c.now().Sub(c.lastContact) < c.ResetAfter {
		return
	}
	first := !c.lossReset
	if first {
		c.lossReset = true
		c.logger().Warn("daemon unreachable too long: returning devices to their defaults", "since", c.lastContact.Format(time.RFC3339), "reset_after", c.ResetAfter.String())
	}
	// The first reset does not wait for the retry delay of a failed change.
	for _, key := range c.keys() {
		c.reset(ctx, key, c.devices[key], "daemon unreachable", first)
	}
}

// sync takes the daemon's list: new devices are added (not known to be at
// their defaults, so reconcile sets them), devices no longer listed are reset
// if this controller may have changed them, then forgotten.
func (c *Controller) sync(ctx context.Context, listed []store.ControlledDevice) {
	for _, d := range c.devices {
		d.listed = false
	}
	for _, entry := range listed {
		key := deviceKey{entry.HostID, entry.DeviceID}
		d := c.devices[key]
		if d == nil {
			d = &device{}
			c.devices[key] = d
		}
		if d.desired.Generation != entry.Generation && d.failedOf != entry.Generation {
			d.retryAt = time.Time{} // a new request is tried at once
		}
		d.kind, d.listed, d.desired, d.leftTries = entry.Kind, true, entry, 0
	}
	changed := false
	for _, key := range c.keys() {
		d := c.devices[key]
		if d.listed {
			continue
		}
		// The first reset after it left is tried at once; then with the retry
		// delay, abandonAfter times at most.
		tried := d.leftTries == 0 || !c.now().Before(d.retryAt)
		if c.reset(ctx, key, d, "no longer controlled", d.leftTries == 0) {
			delete(c.devices, key)
			changed = true
			continue
		}
		if tried {
			d.leftTries++
		}
		if d.leftTries >= abandonAfter {
			c.logger().Error("giving up resetting a device no longer controlled", "device", key.String(), "tries", d.leftTries)
			delete(c.devices, key)
			changed = true
		}
	}
	if c.State == nil {
		return
	}
	var keep []StateDevice
	for _, key := range c.keys() {
		keep = append(keep, StateDevice{HostID: key.host, DeviceID: key.id, Kind: c.devices[key].kind})
	}
	if changed || !slices.Equal(keep, c.State.Devices()) {
		if err := c.State.SetDevices(keep); err != nil {
			c.logger().Error("cannot record the controlled devices", "error", err)
		}
	}
}

// reconcile brings one listed device to its desired settings.
func (c *Controller) reconcile(ctx context.Context, key deviceKey, d *device) {
	actuator := c.Actuators[d.kind]
	if actuator != nil && (d.capsAt.IsZero() || c.now().Sub(d.capsAt) >= capabilitiesRefresh) {
		caps, err := actuator.Capabilities(ctx, key.id)
		if err != nil {
			c.logger().Warn("cannot read device capabilities", "device", key.String(), "error", err)
			d.capsAt = c.now().Add(retryMax).Add(-capabilitiesRefresh) // read again in retryMax
		} else {
			d.capsAt = c.now()
			c.pending(key, d).Capabilities = &caps
		}
	}
	want, generation := d.desired.Settings, d.desired.Generation
	// A setting in place is carried out again every reapplyEvery: a driver
	// that reloaded, or another controller's reset, may have undone it.
	due := !want.IsDefault() && c.now().Sub(d.appliedAt) >= reapplyEvery
	if d.inPlace != nil && d.inPlace.Equal(want) && !due {
		if d.inPlaceGeneration == nil || *d.inPlaceGeneration != generation {
			d.inPlaceGeneration = &generation
			c.applied(key, d, "")
		}
		return
	}
	if c.now().Before(d.retryAt) {
		return
	}
	err := c.apply(ctx, d, key, want)
	if err != nil {
		c.failed(key, d, generation, err)
		return
	}
	again := d.inPlace != nil && d.inPlace.Equal(want) && d.inPlaceGeneration != nil && *d.inPlaceGeneration == generation
	d.inPlace, d.inPlaceGeneration, d.appliedAt = &want, &generation, c.now()
	d.failures, d.retryAt = 0, time.Time{}
	if again {
		return
	}
	c.logger().Info("device settings applied", "device", key.String(), "generation", generation, "settings", want.String())
	c.applied(key, d, "")
}

// reset returns a device to its defaults unless it is known to be there;
// force ignores the retry delay. It reports whether the device is at its
// defaults.
func (c *Controller) reset(ctx context.Context, key deviceKey, d *device, note string, force bool) bool {
	if d.inPlace != nil && d.inPlace.IsDefault() {
		return true
	}
	if !force && c.now().Before(d.retryAt) {
		return false
	}
	if err := c.apply(ctx, d, key, store.DeviceSettings{}); err != nil {
		c.failed(key, d, d.desired.Generation, err)
		return false
	}
	d.inPlace, d.inPlaceGeneration = &store.DeviceSettings{}, nil
	d.failures, d.retryAt = 0, time.Time{}
	c.logger().Info("device returned to its defaults", "device", key.String(), "reason", note)
	c.applied(key, d, note)
	return true
}

func (c *Controller) apply(ctx context.Context, d *device, key deviceKey, s store.DeviceSettings) error {
	actuator := c.Actuators[d.kind]
	if actuator == nil {
		return fmt.Errorf("this controller cannot change a %q device", d.kind)
	}
	err := actuator.Apply(ctx, key.id, s)
	if err != nil {
		// a change that failed may have been carried out in part
		d.inPlace, d.inPlaceGeneration = nil, nil
	}
	return err
}

// failed records a failed change: reported, and retried after a delay that
// doubles with each failure in a row.
func (c *Controller) failed(key deviceKey, d *device, generation int64, err error) {
	d.failures++
	d.failedOf = generation
	delay := min(retryBase<<min(d.failures-1, 10), retryMax)
	d.retryAt = c.now().Add(delay)
	c.logger().Warn("device settings change failed; retrying", "device", key.String(), "error", err, "failures", d.failures, "retry_in", delay.String())
	if errors.Is(err, context.Canceled) {
		return
	}
	c.pending(key, d).Error = err.Error()
}

// applied queues the report of what is in place now.
func (c *Controller) applied(key deviceKey, d *device, note string) {
	r := c.pending(key, d)
	applied := *d.inPlace
	r.Applied, r.AppliedGeneration, r.Note, r.Error = &applied, d.inPlaceGeneration, note, ""
}

func (c *Controller) pending(key deviceKey, d *device) *store.DeviceReport {
	if d.report == nil {
		d.report = &store.DeviceReport{HostID: key.host, DeviceID: key.id}
	}
	return d.report
}

// flush sends the queued reports; they are kept for the next poll when the
// daemon does not take them.
func (c *Controller) flush(ctx context.Context) {
	var reports []store.DeviceReport
	var from []*device
	for _, key := range c.keys() {
		if d := c.devices[key]; d.report != nil {
			reports = append(reports, *d.report)
			from = append(from, d)
		}
	}
	if len(reports) == 0 {
		return
	}
	skipped, err := c.Daemon.ReportDevices(ctx, c.Node.ID, reports)
	if err != nil {
		c.logger().Warn("cannot report device settings; retrying at the next poll", "error", err)
		return
	}
	for _, d := range from {
		d.report = nil
	}
	if len(skipped) != 0 {
		c.logger().Info("daemon skipped reports of devices this controller no longer controls", "devices", skipped)
	}
}
