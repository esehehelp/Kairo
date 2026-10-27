// Package hwpolicy is the operating-point policy: a daemon loop that, for
// every device a controller may change whose max_slowdown is above 0,
// chooses the setting that keeps performance within max_slowdown for the
// least energy and asks for it as the device's policy setting. A GPU's choice
// suits every kind of work its bench profile says the running workload may
// be (from how busy it keeps the GPU); a CPU, shared by whatever runs on its
// host, gets the setting that suits every CPU kind. Without a profile, or
// without a confident idea of the workload, the highest clock is capped at
// (1 - max_slowdown) of the device's maximum, which can lose no more than
// that. A choice changes only once it has stayed the same for StableEvaluations
// evaluations, and a device's setting at most once per MinChangeInterval.
//
// The policy never holds back admission and nothing waits for it: a setting
// the controller does not apply is only a fact, and a bench of the device
// overrides it.
package hwpolicy

import (
	"context"
	"log/slog"
	"time"

	"kairo/internal/hwbench"
	"kairo/internal/machine"
	"kairo/internal/store"
)

// Store is what the policy reads and records (*store.Store).
type Store interface {
	PolicyDevices(ctx context.Context, window time.Duration) ([]store.PolicyDevice, error)
	ListBenchRuns(ctx context.Context, hostID, deviceID string, points bool) ([]store.BenchRun, error)
	SetDeviceSetting(ctx context.Context, hostID, deviceID string, owner store.DeviceOwner, settings store.DeviceSettings, actor string) (store.DeviceSettingResult, error)
	ClearDeviceSetting(ctx context.Context, hostID, deviceID string, owner store.DeviceOwner, actor string) (store.DeviceSettingResult, error)
	RecordPolicyDecision(ctx context.Context, d store.PolicyDecision) error
	AccruePolicySavings(ctx context.Context, attemptID string, seconds float64, savedJoules *float64) error
	ReconcileBenchRuns(ctx context.Context) error
}

const (
	// DefaultInterval is how often the policy evaluates.
	DefaultInterval = time.Minute
	// UsageWindow is how much of a GPU's recent observation classifies its
	// workload.
	UsageWindow = 5 * time.Minute
	// StableEvaluations: a new choice is applied once it was the choice this
	// many evaluations in a row.
	StableEvaluations = 3
	// MinChangeInterval: a device's policy setting changes at most this often.
	MinChangeInterval = 5 * time.Minute
	// actor is who policy settings are recorded as requested by.
	actor = "policy"
)

// Policy is the loop; Run drives it, Step is one evaluation (for tests).
type Policy struct {
	Store    Store
	Interval time.Duration
	Now      func() time.Time
	Logger   *slog.Logger

	devices  map[[2]string]*deviceState
	lastStep time.Time
}

// deviceState is the hysteresis of one device.
type deviceState struct {
	// owner is the lease (GPU) or host the current setting belongs to; a new
	// lease starts at the defaults.
	owner string
	// pending is the choice that differs from what is in place, seen
	// pendingCount evaluations in a row.
	pending      string
	pendingCount int
	lastChange   time.Time
	lastError    string
}

func (p *Policy) now() time.Time {
	if p.Now != nil {
		return p.Now()
	}
	return time.Now()
}

func (p *Policy) logger() *slog.Logger {
	if p.Logger != nil {
		return p.Logger
	}
	return slog.Default()
}

// Run evaluates every Interval until ctx ends.
func (p *Policy) Run(ctx context.Context) {
	interval := p.Interval
	if interval == 0 {
		interval = DefaultInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		p.Step(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// Step is one evaluation of every device. A failure is logged and changes
// nothing; the next evaluation tries again.
func (p *Policy) Step(ctx context.Context) {
	if p.devices == nil {
		p.devices = map[[2]string]*deviceState{}
	}
	if err := p.Store.ReconcileBenchRuns(ctx); err != nil && ctx.Err() == nil {
		p.logger().Error("bench run reconciliation failed", "error", err)
	}
	now := p.now()
	interval := p.Interval
	if interval == 0 {
		interval = DefaultInterval
	}
	// The seconds since the last evaluation, at most two intervals: a daemon
	// that was stopped saves nothing meanwhile.
	elapsed := 0.0
	if !p.lastStep.IsZero() {
		elapsed = min(now.Sub(p.lastStep), 2*interval).Seconds()
	}
	p.lastStep = now
	devices, err := p.Store.PolicyDevices(ctx, UsageWindow)
	if err != nil {
		if ctx.Err() == nil {
			p.logger().Error("policy cannot read the devices", "error", err)
		}
		return
	}
	for _, d := range devices {
		p.evaluate(ctx, d, now, elapsed)
	}
}

func (p *Policy) evaluate(ctx context.Context, d store.PolicyDevice, now time.Time, elapsed float64) {
	key := [2]string{d.HostID, d.DeviceID}
	st := p.devices[key]
	if st == nil {
		st = &deviceState{}
		p.devices[key] = st
	}
	current := d.PolicySetting()
	owner := store.DeviceOwner{Kind: store.OwnerPolicy, Ref: store.HostPolicyRef}
	switch {
	case !d.Controllable() || d.MaxSlowdown == 0:
		// The declaration's apply removes these; one left over goes now.
		if current != nil {
			p.clear(ctx, d, current.Owner, "the device's policy is off")
		}
		return
	case d.Kind == machine.CPU && d.Bench:
		// A bench of the host's CPU measures from its defaults.
		if current != nil {
			p.clear(ctx, d, current.Owner, "a bench of the host's cpu is running")
		}
		*st = deviceState{lastChange: st.lastChange}
		return
	case d.Kind != machine.CPU:
		if d.Lease == nil || d.Lease.Bench || d.Lease.AttemptID == "" || d.Lease.ActivatedAt == "" {
			return // nothing runs on it (a bench's own lease sets its own settings)
		}
		owner.Ref = d.Lease.LeaseID
		if current != nil && current.Owner.Ref == owner.Ref {
			p.accrue(ctx, d, elapsed)
		}
	}
	if st.owner != owner.Ref {
		// another lease: its setting went with the old lease
		st.owner, st.pending, st.pendingCount = owner.Ref, "", 0
	}
	runs, err := p.Store.ListBenchRuns(ctx, d.HostID, d.DeviceID, true)
	if err != nil {
		p.logger().Warn("policy cannot read the bench profile", "device", d.HostID+"/"+d.DeviceID, "error", err)
		return
	}
	decision := Decide(d, runs)
	have := store.DeviceSettings{}
	if current != nil {
		have = current.Settings
	}
	want := decision.Settings.String()
	if want == have.String() {
		st.pending, st.pendingCount = "", 0
		return
	}
	if want == st.pending {
		st.pendingCount++
	} else {
		st.pending, st.pendingCount = want, 1
	}
	if st.pendingCount < StableEvaluations || !st.lastChange.IsZero() && now.Sub(st.lastChange) < MinChangeInterval {
		return
	}
	if decision.Settings.IsDefault() {
		_, err = p.Store.ClearDeviceSetting(ctx, d.HostID, d.DeviceID, owner, actor)
	} else {
		_, err = p.Store.SetDeviceSetting(ctx, d.HostID, d.DeviceID, owner, decision.Settings, actor)
	}
	if err != nil {
		st.pendingCount = 0 // tried again once the choice has been stable again
		if err.Error() != st.lastError {
			st.lastError = err.Error()
			p.logger().Warn("policy setting refused", "device", d.HostID+"/"+d.DeviceID, "settings", decision.Settings.String(), "error", err)
		}
		return
	}
	st.pending, st.pendingCount, st.lastChange, st.lastError = "", 0, now, ""
	decision.DecidedAt = now.UTC().Format(time.RFC3339Nano)
	if err = p.Store.RecordPolicyDecision(ctx, decision); err != nil {
		p.logger().Warn("cannot record a policy decision", "device", d.HostID+"/"+d.DeviceID, "error", err)
	}
	p.logger().Info("policy setting changed", "device", d.HostID+"/"+d.DeviceID, "settings", decision.Settings.String(), "basis", decision.Basis, "candidates", decision.Candidates)
}

func (p *Policy) clear(ctx context.Context, d store.PolicyDevice, owner store.DeviceOwner, reason string) {
	if _, err := p.Store.ClearDeviceSetting(ctx, d.HostID, d.DeviceID, owner, actor); err != nil {
		p.logger().Warn("cannot clear a policy setting", "device", d.HostID+"/"+d.DeviceID, "error", err)
		return
	}
	if err := p.Store.RecordPolicyDecision(ctx, store.PolicyDecision{HostID: d.HostID, DeviceID: d.DeviceID, MaxSlowdown: d.MaxSlowdown, Basis: store.BasisDefault, Reason: reason,
		DecidedAt: p.now().UTC().Format(time.RFC3339Nano)}); err != nil {
		p.logger().Warn("cannot record a policy decision", "device", d.HostID+"/"+d.DeviceID, "error", err)
	}
}

// accrue adds the seconds since the last evaluation to the attempt holding a
// GPU while the policy's setting is in place on it (applied by its
// controller), with the energy its decision's profile says that saved.
func (p *Policy) accrue(ctx context.Context, d store.PolicyDevice, elapsed float64) {
	effective := d.Effective
	inPlace := effective != nil && effective.Owner.Kind == store.OwnerPolicy && d.AppliedGeneration != nil && *d.AppliedGeneration == d.Generation &&
		d.Applied != nil && d.Applied.Equal(effective.Settings)
	if !inPlace || elapsed <= 0 {
		return
	}
	var saved *float64
	if decision := d.Policy; decision != nil && decision.LeaseID == d.Lease.LeaseID && decision.Settings.Equal(effective.Settings) {
		if w := decision.SavingW(); w != nil {
			joules := *w * elapsed
			saved = &joules
		}
	}
	if err := p.Store.AccruePolicySavings(ctx, d.Lease.AttemptID, elapsed, saved); err != nil {
		p.logger().Warn("cannot record the policy's savings", "attempt_id", d.Lease.AttemptID, "error", err)
	}
}

// Decide is the policy's choice for a device now, from its bench runs: for a
// GPU, the profile's setting that suits every kind its observed usage may be,
// for a CPU the one that suits every CPU kind; else the fallback cap.
func Decide(d store.PolicyDevice, runs []store.BenchRun) store.PolicyDecision {
	s := d.MaxSlowdown
	decision := store.PolicyDecision{HostID: d.HostID, DeviceID: d.DeviceID, MaxSlowdown: s}
	if d.Lease != nil {
		decision.LeaseID, decision.AttemptID = d.Lease.LeaseID, d.Lease.AttemptID
	}
	curves := hwbench.Curves(runs)
	swept := map[string]bool{}
	var sweptCurves []hwbench.Curve
	for _, kind := range store.BenchKinds(d.Kind) {
		if kind.Sweep == "" {
			continue
		}
		swept[kind.Name] = true
		if c, ok := curves[kind.Name]; ok {
			sweptCurves = append(sweptCurves, c)
		}
	}
	var chosen []hwbench.Curve
	switch {
	case len(sweptCurves) == 0:
		decision.Reason = "no valid bench profile"
	case d.Kind == machine.CPU:
		// Whatever shares the CPU: the worst case of every CPU kind.
		if len(sweptCurves) < len(swept) {
			decision.Reason = "the bench profile lacks a valid curve of every cpu kind"
			break
		}
		chosen = sweptCurves
	default:
		fp, candidates, confident := hwbench.Classify(d.Usage, curves, swept)
		if confident {
			decision.Fingerprint = &fp
		}
		switch {
		case !confident:
			decision.Reason = "the workload's usage is not clear yet"
		case len(candidates) == 0:
			decision.Reason = "no benched kind matches the workload's usage"
		}
		for _, kind := range candidates {
			chosen = append(chosen, curves[kind])
		}
	}
	if len(chosen) != 0 {
		for _, c := range chosen {
			decision.Candidates = append(decision.Candidates, c.Kind)
		}
		if choice, ok := hwbench.SelectCommon(chosen, s); ok {
			decision.Settings, decision.Expected, decision.RunIDs = choice.Settings, choice.Expected, choice.RunIDs
			decision.Basis = store.BasisProfile
			if choice.Settings.IsDefault() {
				decision.Basis = store.BasisDefault
				decision.Reason = "the defaults use the least energy within max_slowdown"
			}
			return decision
		}
		decision.Reason = "the bench profile has no energy for these kinds"
	}
	var fallback store.DeviceSettings
	var ok bool
	if d.Kind == machine.CPU {
		fallback, ok = hwbench.FallbackCPU(d.Capabilities, s)
	} else {
		fallback, ok = hwbench.FallbackGPU(d.Capabilities, s)
	}
	if !ok {
		decision.Basis = store.BasisDefault
		decision.Reason += "; no maximum clock to cap from"
		return decision
	}
	decision.Settings, decision.Basis = fallback, store.BasisFallback
	return decision
}
