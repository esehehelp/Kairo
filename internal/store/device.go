package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"kairo/internal/machine"
)

// Device settings are desired state: an owner asks for settings of one
// device of a host (a GPU by UUID, or "cpu"), and the host's controller
// (`kairo controller`) carries them out and reports what is in place. Nothing
// here is read by admission: a setting that fails never holds back a job, it
// is recorded as a fact.

// Owners of a device setting, by precedence: a manual setting overrides a
// bench run's, which overrides the policy's.
const (
	OwnerManual = "manual"
	OwnerBench  = "bench"
	OwnerPolicy = "policy"
)

// HostPolicyRef is the Ref of a CPU's policy setting, which belongs to its
// host rather than to a lease.
const HostPolicyRef = "host"

// ErrDeviceSettingConflict: the device's bench or policy setting belongs to
// another lease.
var ErrDeviceSettingConflict = errors.New("device setting belongs to another lease")

// DeviceOwner is who asked for a setting: Ref is the actor of a manual
// setting, the lease a bench or policy setting belongs to; a CPU's policy
// setting belongs to its host (HostPolicyRef), not to a lease.
type DeviceOwner struct {
	Kind string `json:"kind"`
	Ref  string `json:"ref,omitempty"`
}

func (o DeviceOwner) String() string {
	if o.Ref == "" {
		return o.Kind
	}
	return o.Kind + " " + o.Ref
}

// Setting names, as JSON fields of DeviceSettings and in
// DeviceCapabilities.Settings.
const (
	SettingGraphicsClockMax = "graphics_clock_max_mhz"
	SettingMemoryClock      = "memory_clock_mhz"
	SettingPowerLimit       = "power_limit_w"
	SettingMaxFreq          = "max_freq_mhz"
	SettingMaxPerf          = "max_perf_pct"
)

// kindSettings are the settings of each kind of device.
var kindSettings = map[string][]string{
	"gpu": {SettingGraphicsClockMax, SettingMemoryClock, SettingPowerLimit},
	"cpu": {SettingMaxFreq, SettingMaxPerf},
}

// DeviceSettings are what a controller sets on a device; a field left out is
// the hardware's default, so the zero value is the defaults. A GPU takes a
// graphics clock maximum, a locked memory clock and a power limit; a CPU a
// maximum frequency (Windows, Linux cpufreq) or a maximum performance
// percentage (Linux intel_pstate).
type DeviceSettings struct {
	GraphicsClockMaxMHz *int `json:"graphics_clock_max_mhz,omitempty"`
	MemoryClockMHz      *int `json:"memory_clock_mhz,omitempty"`
	PowerLimitW         *int `json:"power_limit_w,omitempty"`
	MaxFreqMHz          *int `json:"max_freq_mhz,omitempty"`
	MaxPerfPct          *int `json:"max_perf_pct,omitempty"`
}

type namedSetting struct {
	name  string
	value *int
	unit  string
}

func (s DeviceSettings) named() []namedSetting {
	return []namedSetting{
		{SettingGraphicsClockMax, s.GraphicsClockMaxMHz, "MHz"},
		{SettingMemoryClock, s.MemoryClockMHz, "MHz"},
		{SettingPowerLimit, s.PowerLimitW, "W"},
		{SettingMaxFreq, s.MaxFreqMHz, "MHz"},
		{SettingMaxPerf, s.MaxPerfPct, "%"},
	}
}

// Names are the settings s sets.
func (s DeviceSettings) Names() []string {
	var out []string
	for _, f := range s.named() {
		if f.value != nil {
			out = append(out, f.name)
		}
	}
	return out
}

// IsDefault reports whether s sets nothing.
func (s DeviceSettings) IsDefault() bool { return len(s.Names()) == 0 }

func (s DeviceSettings) Equal(o DeviceSettings) bool {
	a, b := s.named(), o.named()
	for i := range a {
		if (a[i].value == nil) != (b[i].value == nil) || a[i].value != nil && *a[i].value != *b[i].value {
			return false
		}
	}
	return true
}

// String is s as "graphics_clock_max_mhz=1800MHz power_limit_w=250W", or
// "defaults".
func (s DeviceSettings) String() string {
	var parts []string
	for _, f := range s.named() {
		if f.value != nil {
			parts = append(parts, fmt.Sprintf("%s=%d%s", f.name, *f.value, f.unit))
		}
	}
	if len(parts) == 0 {
		return "defaults"
	}
	return strings.Join(parts, " ")
}

// DeviceCapabilities are what a controller can change on a device, as it
// reads them from the hardware: the settings it supports and their ranges.
type DeviceCapabilities struct {
	Settings []string `json:"settings"`
	// GPU: the supported clocks (nvidia-smi -q -d SUPPORTED_CLOCKS), sorted,
	// and the power limit range and default.
	GraphicsClocksMHz  []int    `json:"graphics_clocks_mhz,omitempty"`
	MemoryClocksMHz    []int    `json:"memory_clocks_mhz,omitempty"`
	PowerLimitMinW     *float64 `json:"power_limit_min_w,omitempty"`
	PowerLimitMaxW     *float64 `json:"power_limit_max_w,omitempty"`
	PowerLimitDefaultW *float64 `json:"power_limit_default_w,omitempty"`
	// CPU: the range of max_freq_mhz (Linux cpufreq; Windows has none), or the
	// least max_perf_pct (intel_pstate; the most is 100).
	MaxFreqMinMHz *int `json:"max_freq_min_mhz,omitempty"`
	MaxFreqMaxMHz *int `json:"max_freq_max_mhz,omitempty"`
	MaxPerfPctMin *int `json:"max_perf_pct_min,omitempty"`
}

// Check refuses settings the device does not support or values outside the
// ranges it reported.
func (c DeviceCapabilities) Check(s DeviceSettings) error {
	for _, name := range s.Names() {
		if !slices.Contains(c.Settings, name) {
			return fmt.Errorf("the device does not support %s (it supports: %s)", name, orNone(c.Settings))
		}
	}
	within := func(name string, v int, low, high float64, unit string) error {
		if float64(v) < low || float64(v) > high {
			return fmt.Errorf("%s %d%s is outside the device's range %g-%g%s", name, v, unit, low, high, unit)
		}
		return nil
	}
	for _, f := range s.named() {
		if f.value != nil && *f.value <= 0 {
			return fmt.Errorf("%s must be positive (clear the setting to return to the default)", f.name)
		}
	}
	checks := []struct {
		value *int
		name  string
		list  []int
		low   *float64
		high  *float64
		unit  string
	}{
		{s.GraphicsClockMaxMHz, SettingGraphicsClockMax, c.GraphicsClocksMHz, nil, nil, "MHz"},
		{s.MemoryClockMHz, SettingMemoryClock, c.MemoryClocksMHz, nil, nil, "MHz"},
		{s.PowerLimitW, SettingPowerLimit, nil, c.PowerLimitMinW, c.PowerLimitMaxW, "W"},
		{s.MaxFreqMHz, SettingMaxFreq, nil, intFloat(c.MaxFreqMinMHz), intFloat(c.MaxFreqMaxMHz), "MHz"},
		{s.MaxPerfPct, SettingMaxPerf, nil, intFloat(c.MaxPerfPctMin), ptr(100.0), "%"},
	}
	for _, check := range checks {
		if check.value == nil {
			continue
		}
		low, high := 0.0, float64(1<<31)
		if len(check.list) != 0 {
			low, high = float64(slices.Min(check.list)), float64(slices.Max(check.list))
		}
		if check.low != nil {
			low = *check.low
		}
		if check.high != nil {
			high = *check.high
		}
		if err := within(check.name, *check.value, low, high, check.unit); err != nil {
			return err
		}
	}
	return nil
}

func intFloat(v *int) *float64 {
	if v == nil {
		return nil
	}
	return ptr(float64(*v))
}

func ptr[T any](v T) *T { return &v }

func orNone(names []string) string {
	if len(names) == 0 {
		return "nothing"
	}
	return strings.Join(names, ", ")
}

// DeviceSetting is one owner's setting of a device.
type DeviceSetting struct {
	HostID      string         `json:"host_id"`
	DeviceID    string         `json:"device_id"`
	Owner       DeviceOwner    `json:"owner"`
	Settings    DeviceSettings `json:"settings"`
	RequestedBy string         `json:"requested_by"`
	RequestedAt string         `json:"requested_at"`
}

// DeviceSettingResult is a device after a setting was set or cleared: its
// generation and the setting now in effect (nil: the defaults).
type DeviceSettingResult struct {
	HostID     string         `json:"host_id"`
	DeviceID   string         `json:"device_id"`
	Generation int64          `json:"generation"`
	Effective  *DeviceSetting `json:"effective"`
	// Cleared reports, for a clear, whether there was a setting to remove.
	Cleared bool `json:"cleared,omitempty"`
}

// ControlledDevice is a device as its controller is told to keep it: the
// effective setting (zero: the defaults) and its generation.
type ControlledDevice struct {
	HostID     string         `json:"host_id"`
	DeviceID   string         `json:"device_id"`
	Kind       string         `json:"kind"`
	Generation int64          `json:"generation"`
	Owner      *DeviceOwner   `json:"owner,omitempty"`
	Settings   DeviceSettings `json:"settings"`
}

// DeviceReport is what a controller reports of one device. Applied is what is
// in place after its latest successful change ({} for the defaults) and
// AppliedGeneration the generation that change carried out; nil when the
// controller reset the device on its own (Note says why: stopping, the
// daemon unreachable). Error is the latest change's failure, empty when it
// succeeded. Capabilities, when present, replace the ones reported before.
type DeviceReport struct {
	HostID            string              `json:"host_id"`
	DeviceID          string              `json:"device_id"`
	Applied           *DeviceSettings     `json:"applied,omitempty"`
	AppliedGeneration *int64              `json:"applied_generation,omitempty"`
	Note              string              `json:"note,omitempty"`
	Error             string              `json:"error,omitempty"`
	Capabilities      *DeviceCapabilities `json:"capabilities,omitempty"`
}

// HardwareHost is a declared host with its controller and devices, for
// `kairo hardware show`.
type HardwareHost struct {
	ID          string   `json:"id"`
	Nodes       []string `json:"nodes"`
	Controller  *string  `json:"controller,omitempty"`
	MaxSlowdown float64  `json:"max_slowdown"`
	// ControllerStartedAt and ControllerLastSeenAt: when the controller last
	// started and last polled.
	ControllerStartedAt  *string          `json:"controller_started_at,omitempty"`
	ControllerLastSeenAt *string          `json:"controller_last_seen_at,omitempty"`
	ControllerVersion    *string          `json:"controller_version,omitempty"`
	Devices              []HardwareDevice `json:"devices"`
}

// HardwareDevice is a device's policy, desired and applied settings.
type HardwareDevice struct {
	machine.DevicePolicy
	Generation int64 `json:"generation"`
	// Effective is the setting in effect (nil: the defaults); Settings every
	// owner's, by precedence.
	Effective         *DeviceSetting      `json:"effective,omitempty"`
	Settings          []DeviceSetting     `json:"settings"`
	AppliedGeneration *int64              `json:"applied_generation,omitempty"`
	Applied           *DeviceSettings     `json:"applied,omitempty"`
	AppliedNote       *string             `json:"applied_note,omitempty"`
	AppliedAt         *string             `json:"applied_at,omitempty"`
	LastError         *string             `json:"last_error,omitempty"`
	LastErrorAt       *string             `json:"last_error_at,omitempty"`
	Capabilities      *DeviceCapabilities `json:"capabilities,omitempty"`
	CapabilitiesAt    *string             `json:"capabilities_at,omitempty"`
	ReportedAt        *string             `json:"reported_at,omitempty"`
	// Policy is the operating-point policy's latest decision; Bench the
	// device's latest bench run (without its points).
	Policy *PolicyDecision `json:"policy,omitempty"`
	Bench  *BenchRun       `json:"bench,omitempty"`
}

// SetDeviceSetting stores owner's setting of a device, replacing the one the
// owner kind had. The device must be controllable, s must set something the
// device's controller reported it supports, within its ranges, and a bench
// or policy setting needs a live lease (owner.Ref) on the host that, for a
// GPU, holds that GPU; a CPU's policy setting belongs to its host instead
// (HostPolicyRef). Another lease's bench or policy setting is
// ErrDeviceSettingConflict: the owner clears its own first.
func (s *Store) SetDeviceSetting(ctx context.Context, hostID, deviceID string, owner DeviceOwner, settings DeviceSettings, actor string) (DeviceSettingResult, error) {
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return DeviceSettingResult{}, err
	}
	defer tx.Rollback()
	policy, host, err := devicePolicyTx(ctx, tx, hostID, deviceID)
	if err != nil {
		return DeviceSettingResult{}, err
	}
	if !policy.Controllable() {
		return DeviceSettingResult{}, errNotControllable(policy)
	}
	if owner.Kind == OwnerPolicy && policy.MaxSlowdown == 0 {
		return DeviceSettingResult{}, fmt.Errorf("device %s of host %s has max_slowdown 0: the policy leaves it at its defaults", deviceID, hostID)
	}
	if settings.IsDefault() {
		return DeviceSettingResult{}, errors.New("the setting sets nothing (clear the setting to return to the defaults)")
	}
	for _, name := range settings.Names() {
		if !slices.Contains(kindSettings[policy.Kind], name) {
			return DeviceSettingResult{}, fmt.Errorf("%s is not a setting of a %s (its settings: %s)", name, policy.Kind, strings.Join(kindSettings[policy.Kind], ", "))
		}
	}
	var capsJSON sql.NullString
	if err = tx.QueryRowContext(ctx, `SELECT capabilities_json FROM device_state WHERE host_id=$1 AND device_id=$2`, hostID, deviceID).Scan(&capsJSON); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return DeviceSettingResult{}, err
	}
	if !capsJSON.Valid {
		return DeviceSettingResult{}, fmt.Errorf("the controller of host %s (%s) has not reported what device %s supports yet: start it (kairo controller) first", hostID, *policy.Controller, deviceID)
	}
	var caps DeviceCapabilities
	if err = json.Unmarshal([]byte(capsJSON.String), &caps); err != nil {
		return DeviceSettingResult{}, err
	}
	if err = caps.Check(settings); err != nil {
		return DeviceSettingResult{}, fmt.Errorf("device %s of host %s: %w", deviceID, hostID, err)
	}
	var leaseID any
	switch owner.Kind {
	case OwnerManual:
		owner.Ref = actor
	case OwnerBench, OwnerPolicy:
		if owner.Kind == OwnerPolicy && policy.Kind == machine.CPU {
			// A host's CPU is shared by whatever runs on it: its policy
			// setting belongs to the host, not to a lease.
			if owner.Ref != "" && owner.Ref != HostPolicyRef {
				return DeviceSettingResult{}, fmt.Errorf("the policy setting of a host's cpu belongs to the host, not to lease %s", owner.Ref)
			}
			owner.Ref = HostPolicyRef
		} else {
			if err = checkSettingLeaseTx(ctx, tx, policy, host, owner.Ref); err != nil {
				return DeviceSettingResult{}, err
			}
			leaseID = owner.Ref
		}
		var current string
		err = tx.QueryRowContext(ctx, `SELECT owner_ref FROM device_settings WHERE host_id=$1 AND device_id=$2 AND owner_kind=$3`, hostID, deviceID, owner.Kind).Scan(&current)
		if err == nil && current != owner.Ref {
			return DeviceSettingResult{}, fmt.Errorf("%w: device %s of host %s has a %s setting of lease %s", ErrDeviceSettingConflict, deviceID, hostID, owner.Kind, current)
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return DeviceSettingResult{}, err
		}
	default:
		return DeviceSettingResult{}, fmt.Errorf("owner kind %q is not %s, %s or %s", owner.Kind, OwnerManual, OwnerBench, OwnerPolicy)
	}
	before, err := deviceSettingsTx(ctx, tx, hostID, deviceID)
	if err != nil {
		return DeviceSettingResult{}, err
	}
	body, err := json.Marshal(settings)
	if err != nil {
		return DeviceSettingResult{}, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO device_settings(host_id,device_id,owner_kind,owner_ref,lease_id,settings_json,requested_by,requested_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8)
		ON CONFLICT(host_id,device_id,owner_kind) DO UPDATE SET owner_ref=excluded.owner_ref,lease_id=excluded.lease_id,settings_json=excluded.settings_json,requested_by=excluded.requested_by,requested_at=excluded.requested_at`,
		hostID, deviceID, owner.Kind, owner.Ref, leaseID, string(body), actor, now()); err != nil {
		return DeviceSettingResult{}, err
	}
	result, err := settingChangedTx(ctx, tx, hostID, deviceID, before)
	if err != nil {
		return DeviceSettingResult{}, err
	}
	if err = appendCoordinationEventTx(ctx, tx, "device_setting_requested", nil, "device", hostID+"/"+deviceID, nil, map[string]any{
		"host_id": hostID, "device_id": deviceID, "owner": owner, "settings": settings, "actor": actor, "generation": result.Generation}); err != nil {
		return DeviceSettingResult{}, err
	}
	return result, tx.Commit()
}

// ClearDeviceSetting removes the owner kind's setting of a device (for a
// bench or policy owner with a Ref, only that lease's), so the device follows
// the next owner's setting, or returns to its defaults.
func (s *Store) ClearDeviceSetting(ctx context.Context, hostID, deviceID string, owner DeviceOwner, actor string) (DeviceSettingResult, error) {
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return DeviceSettingResult{}, err
	}
	defer tx.Rollback()
	if _, _, err = devicePolicyTx(ctx, tx, hostID, deviceID); err != nil {
		return DeviceSettingResult{}, err
	}
	settings, err := deviceSettingsTx(ctx, tx, hostID, deviceID)
	if err != nil {
		return DeviceSettingResult{}, err
	}
	for _, setting := range settings {
		if setting.Owner.Kind != owner.Kind {
			continue
		}
		if owner.Kind != OwnerManual && owner.Ref != "" && owner.Ref != setting.Owner.Ref {
			return DeviceSettingResult{}, fmt.Errorf("%w: device %s of host %s has a %s setting of lease %s", ErrDeviceSettingConflict, deviceID, hostID, owner.Kind, setting.Owner.Ref)
		}
		if err = deleteDeviceSettingTx(ctx, tx, setting, "cleared by "+actor); err != nil {
			return DeviceSettingResult{}, err
		}
		result, err := deviceResultTx(ctx, tx, hostID, deviceID)
		if err != nil {
			return DeviceSettingResult{}, err
		}
		result.Cleared = true
		return result, tx.Commit()
	}
	result, err := deviceResultTx(ctx, tx, hostID, deviceID)
	return result, err
}

// checkSettingLeaseTx: a bench or policy setting needs a lease that is
// reserved, prepared or active, on a node of the host and, for a GPU,
// holding a row of that GPU.
func checkSettingLeaseTx(ctx context.Context, tx *sql.Tx, policy machine.DevicePolicy, host machine.Host, leaseID string) error {
	if leaseID == "" {
		return errors.New("a bench or policy setting names the lease it belongs to")
	}
	var state, nodeID string
	err := tx.QueryRowContext(ctx, `SELECT l.state,x.node_id FROM leases l JOIN executors x ON x.id=l.executor_id WHERE l.id=$1`, leaseID).Scan(&state, &nodeID)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: lease %s", ErrNotFound, leaseID)
	}
	if err != nil {
		return err
	}
	if !slices.Contains([]string{"reserved", "prepared", "active"}, state) {
		return fmt.Errorf("lease %s is %s: a setting needs a reserved, prepared or active lease", leaseID, state)
	}
	if !slices.Contains(host.Nodes, nodeID) {
		return fmt.Errorf("lease %s is on node %s, which is not on host %s", leaseID, nodeID, host.ID)
	}
	if policy.Kind != "gpu" {
		return nil
	}
	var holds int
	if err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM lease_items li JOIN resource_instances r ON r.id=li.resource_id WHERE li.lease_id=$1 AND r.kind='gpu' AND r.stable_identity=$2`, leaseID, policy.DeviceID).Scan(&holds); err != nil {
		return err
	}
	if holds == 0 {
		return fmt.Errorf("lease %s does not hold %s", leaseID, policy.DeviceID)
	}
	return nil
}

// deviceSettingsTx are a device's settings by precedence (the first is in
// effect).
func deviceSettingsTx(ctx context.Context, q queryer, hostID, deviceID string) ([]DeviceSetting, error) {
	rows, err := q.QueryContext(ctx, `SELECT owner_kind,owner_ref,settings_json,requested_by,requested_at FROM device_settings WHERE host_id=$1 AND device_id=$2
		ORDER BY CASE owner_kind WHEN 'manual' THEN 0 WHEN 'bench' THEN 1 ELSE 2 END`, hostID, deviceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []DeviceSetting{}
	for rows.Next() {
		setting := DeviceSetting{HostID: hostID, DeviceID: deviceID}
		var body string
		if err = rows.Scan(&setting.Owner.Kind, &setting.Owner.Ref, &body, &setting.RequestedBy, &setting.RequestedAt); err != nil {
			return nil, err
		}
		if err = json.Unmarshal([]byte(body), &setting.Settings); err != nil {
			return nil, err
		}
		out = append(out, setting)
	}
	return out, rows.Err()
}

func effectiveOf(settings []DeviceSetting) *DeviceSetting {
	if len(settings) == 0 {
		return nil
	}
	return &settings[0]
}

func sameEffective(a, b *DeviceSetting) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Owner == b.Owner && a.Settings.Equal(b.Settings)
}

// settingChangedTx bumps the device's generation when its effective setting
// differs from before's, and returns the device as it is now.
func settingChangedTx(ctx context.Context, tx *sql.Tx, hostID, deviceID string, before []DeviceSetting) (DeviceSettingResult, error) {
	after, err := deviceSettingsTx(ctx, tx, hostID, deviceID)
	if err != nil {
		return DeviceSettingResult{}, err
	}
	if !sameEffective(effectiveOf(before), effectiveOf(after)) {
		if _, err = tx.ExecContext(ctx, `INSERT INTO device_state(host_id,device_id,generation) VALUES($1,$2,1) ON CONFLICT(host_id,device_id) DO UPDATE SET generation=device_state.generation+1`, hostID, deviceID); err != nil {
			return DeviceSettingResult{}, err
		}
	}
	return deviceResultTx(ctx, tx, hostID, deviceID)
}

func deviceResultTx(ctx context.Context, tx *sql.Tx, hostID, deviceID string) (DeviceSettingResult, error) {
	result := DeviceSettingResult{HostID: hostID, DeviceID: deviceID}
	settings, err := deviceSettingsTx(ctx, tx, hostID, deviceID)
	if err != nil {
		return result, err
	}
	result.Effective = effectiveOf(settings)
	if err = tx.QueryRowContext(ctx, `SELECT generation FROM device_state WHERE host_id=$1 AND device_id=$2`, hostID, deviceID).Scan(&result.Generation); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return result, err
	}
	return result, nil
}

// deleteDeviceSettingTx removes one setting (event device_setting_cleared,
// with why).
func deleteDeviceSettingTx(ctx context.Context, tx *sql.Tx, setting DeviceSetting, reason string) error {
	before, err := deviceSettingsTx(ctx, tx, setting.HostID, setting.DeviceID)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM device_settings WHERE host_id=$1 AND device_id=$2 AND owner_kind=$3`, setting.HostID, setting.DeviceID, setting.Owner.Kind); err != nil {
		return err
	}
	result, err := settingChangedTx(ctx, tx, setting.HostID, setting.DeviceID, before)
	if err != nil {
		return err
	}
	return appendCoordinationEventTx(ctx, tx, "device_setting_cleared", nil, "device", setting.HostID+"/"+setting.DeviceID, nil, map[string]any{
		"host_id": setting.HostID, "device_id": setting.DeviceID, "owner": setting.Owner, "settings": setting.Settings, "reason": reason, "generation": result.Generation})
}

// clearReleasedLeaseSettingsTx removes the settings of leases that have been
// released: no setting outlives its lease. Every path that releases a lease
// calls it in the releasing transaction.
func clearReleasedLeaseSettingsTx(ctx context.Context, tx *sql.Tx) error {
	rows, err := tx.QueryContext(ctx, `SELECT s.host_id,s.device_id,s.owner_kind,s.owner_ref,s.settings_json FROM device_settings s JOIN leases l ON l.id=s.lease_id WHERE l.state='released'`)
	if err != nil {
		return err
	}
	var released []DeviceSetting
	for rows.Next() {
		var setting DeviceSetting
		var body string
		if err = rows.Scan(&setting.HostID, &setting.DeviceID, &setting.Owner.Kind, &setting.Owner.Ref, &body); err == nil {
			err = json.Unmarshal([]byte(body), &setting.Settings)
		}
		if err != nil {
			rows.Close()
			return err
		}
		released = append(released, setting)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return err
	}
	for _, setting := range released {
		if err = deleteDeviceSettingTx(ctx, tx, setting, "lease "+setting.Owner.Ref+" released"); err != nil {
			return err
		}
	}
	return nil
}

// uncontrollableSettingsTx are the stored settings of devices that policies
// do not make controllable, and the policy settings of devices whose
// max_slowdown is 0.
func uncontrollableSettingsTx(ctx context.Context, tx *sql.Tx, policies []machine.DevicePolicy) ([]DeviceSetting, error) {
	controllable := map[[2]string]bool{}
	noSlowdown := map[[2]string]bool{}
	for _, policy := range policies {
		controllable[[2]string{policy.HostID, policy.DeviceID}] = policy.Controllable()
		noSlowdown[[2]string{policy.HostID, policy.DeviceID}] = policy.MaxSlowdown == 0
	}
	out := []DeviceSetting{}
	var slowdownZero [][2]string
	if err := eachRow(ctx, tx, `SELECT host_id,device_id FROM device_settings WHERE owner_kind='policy' ORDER BY host_id,device_id`, nil, func(rows *sql.Rows) error {
		var device [2]string
		if err := rows.Scan(&device[0], &device[1]); err != nil {
			return err
		}
		if controllable[device] && noSlowdown[device] {
			slowdownZero = append(slowdownZero, device)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	for _, device := range slowdownZero {
		settings, err := deviceSettingsTx(ctx, tx, device[0], device[1])
		if err != nil {
			return nil, err
		}
		for _, setting := range settings {
			if setting.Owner.Kind == OwnerPolicy {
				out = append(out, setting)
			}
		}
	}
	rows, err := tx.QueryContext(ctx, `SELECT DISTINCT host_id,device_id FROM device_settings ORDER BY host_id,device_id`)
	if err != nil {
		return nil, err
	}
	var devices [][2]string
	for rows.Next() {
		var device [2]string
		if err = rows.Scan(&device[0], &device[1]); err != nil {
			rows.Close()
			return nil, err
		}
		if !controllable[device] {
			devices = append(devices, device)
		}
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return nil, err
	}
	for _, device := range devices {
		settings, err := deviceSettingsTx(ctx, tx, device[0], device[1])
		if err != nil {
			return nil, err
		}
		out = append(out, settings...)
	}
	return out, nil
}

// pruneDeviceStateTx forgets what controllers reported of devices that are
// no longer declared.
func pruneDeviceStateTx(ctx context.Context, tx *sql.Tx, policies []machine.DevicePolicy) error {
	declared := map[[2]string]bool{}
	for _, policy := range policies {
		declared[[2]string{policy.HostID, policy.DeviceID}] = true
	}
	rows, err := tx.QueryContext(ctx, `SELECT host_id,device_id FROM device_state`)
	if err != nil {
		return err
	}
	var gone [][2]string
	for rows.Next() {
		var device [2]string
		if err = rows.Scan(&device[0], &device[1]); err != nil {
			rows.Close()
			return err
		}
		if !declared[device] {
			gone = append(gone, device)
		}
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return err
	}
	for _, device := range gone {
		if _, err = tx.ExecContext(ctx, `DELETE FROM device_state WHERE host_id=$1 AND device_id=$2`, device[0], device[1]); err != nil {
			return err
		}
	}
	return nil
}

// RegisterDeviceController records that node's controller started. A node
// not known yet is created (a controller's node has no executors and is
// never scheduled); a known one, such as a node that also runs an agent, is
// left as it is.
func (s *Store) RegisterDeviceController(ctx context.Context, node Node, version string) error {
	if node.ID == "" || node.Name == "" || node.OS == "" || node.Architecture == "" {
		return errors.New("node id, name, os, and architecture are required")
	}
	t := now()
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `INSERT INTO nodes(id,name,os,architecture,attributes_json,enabled,created_at,updated_at) VALUES($1,$2,$3,$4,$5,TRUE,$6,$7) ON CONFLICT(id) DO NOTHING`,
		node.ID, node.Name, node.OS, node.Architecture, validJSON(node.Attributes), t, t); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO device_controllers(node_id,version,started_at,last_seen_at) VALUES($1,$2,$3,$4) ON CONFLICT(node_id) DO UPDATE SET version=excluded.version,started_at=excluded.started_at,last_seen_at=excluded.last_seen_at`,
		node.ID, version, t, t); err != nil {
		return err
	}
	return tx.Commit()
}

// PollDeviceSettings is a controller's poll (and heartbeat): the devices
// node controls (control = true on the host naming it) with the setting each
// is to have. Settings of released leases are removed first.
func (s *Store) PollDeviceSettings(ctx context.Context, nodeID string) ([]ControlledDevice, error) {
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if err = clearReleasedLeaseSettingsTx(ctx, tx); err != nil {
		return nil, err
	}
	t := now()
	if _, err = tx.ExecContext(ctx, `INSERT INTO device_controllers(node_id,started_at,last_seen_at) VALUES($1,$2,$3) ON CONFLICT(node_id) DO UPDATE SET last_seen_at=excluded.last_seen_at`, nodeID, t, t); err != nil {
		return nil, err
	}
	spec, err := declaredSpec(ctx, tx)
	if err != nil {
		return nil, err
	}
	out := []ControlledDevice{}
	for _, policy := range spec.DevicePolicies() {
		if !policy.Controllable() || *policy.Controller != nodeID {
			continue
		}
		result, err := deviceResultTx(ctx, tx, policy.HostID, policy.DeviceID)
		if err != nil {
			return nil, err
		}
		device := ControlledDevice{HostID: policy.HostID, DeviceID: policy.DeviceID, Kind: policy.Kind, Generation: result.Generation}
		if result.Effective != nil {
			device.Owner = &result.Effective.Owner
			device.Settings = result.Effective.Settings
		}
		out = append(out, device)
	}
	return out, tx.Commit()
}

// ReportDevices records what node's controller reports of its devices
// (events device_setting_applied and device_setting_failed). A report of a
// device node does not control is skipped and returned.
func (s *Store) ReportDevices(ctx context.Context, nodeID string, reports []DeviceReport) ([]string, error) {
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	spec, err := declaredSpec(ctx, tx)
	if err != nil {
		return nil, err
	}
	controlled := map[[2]string]bool{}
	for _, policy := range spec.DevicePolicies() {
		controlled[[2]string{policy.HostID, policy.DeviceID}] = policy.Controllable() && *policy.Controller == nodeID
	}
	skipped := []string{}
	t := now()
	for _, report := range reports {
		if !controlled[[2]string{report.HostID, report.DeviceID}] {
			skipped = append(skipped, report.HostID+"/"+report.DeviceID)
			continue
		}
		if err = recordDeviceReportTx(ctx, tx, report, t); err != nil {
			return nil, err
		}
	}
	return skipped, tx.Commit()
}

func recordDeviceReportTx(ctx context.Context, tx *sql.Tx, report DeviceReport, t string) error {
	if _, err := tx.ExecContext(ctx, `INSERT INTO device_state(host_id,device_id) VALUES($1,$2) ON CONFLICT(host_id,device_id) DO NOTHING`, report.HostID, report.DeviceID); err != nil {
		return err
	}
	var appliedGeneration sql.NullInt64
	var appliedJSON, lastError sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT applied_generation,applied_json,last_error FROM device_state WHERE host_id=$1 AND device_id=$2`, report.HostID, report.DeviceID).
		Scan(&appliedGeneration, &appliedJSON, &lastError); err != nil {
		return err
	}
	aggregate := report.HostID + "/" + report.DeviceID
	if report.Capabilities != nil {
		body, err := json.Marshal(report.Capabilities)
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE device_state SET capabilities_json=$1,capabilities_at=$2 WHERE host_id=$3 AND device_id=$4`, string(body), t, report.HostID, report.DeviceID); err != nil {
			return err
		}
	}
	if report.Applied != nil {
		body, err := json.Marshal(report.Applied)
		if err != nil {
			return err
		}
		var generation any
		if report.AppliedGeneration != nil {
			generation = *report.AppliedGeneration
		}
		changed := !appliedJSON.Valid || appliedJSON.String != string(body) || appliedGeneration.Valid != (report.AppliedGeneration != nil) ||
			report.AppliedGeneration != nil && appliedGeneration.Int64 != *report.AppliedGeneration
		if _, err = tx.ExecContext(ctx, `UPDATE device_state SET applied_generation=$1,applied_json=$2,applied_note=$3 WHERE host_id=$4 AND device_id=$5`,
			generation, string(body), nullable(report.Note), report.HostID, report.DeviceID); err != nil {
			return err
		}
		if changed {
			if _, err = tx.ExecContext(ctx, `UPDATE device_state SET applied_at=$1 WHERE host_id=$2 AND device_id=$3`, t, report.HostID, report.DeviceID); err != nil {
				return err
			}
			if err = appendCoordinationEventTx(ctx, tx, "device_setting_applied", nil, "device", aggregate, nil, map[string]any{
				"host_id": report.HostID, "device_id": report.DeviceID, "generation": report.AppliedGeneration, "settings": report.Applied, "note": report.Note}); err != nil {
				return err
			}
		}
	}
	switch {
	case report.Error != "":
		if !lastError.Valid || lastError.String != report.Error {
			if err := appendCoordinationEventTx(ctx, tx, "device_setting_failed", nil, "device", aggregate, nil, map[string]any{
				"host_id": report.HostID, "device_id": report.DeviceID, "error": report.Error}); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `UPDATE device_state SET last_error=$1,last_error_at=$2 WHERE host_id=$3 AND device_id=$4`, report.Error, t, report.HostID, report.DeviceID); err != nil {
				return err
			}
		}
	case report.Applied != nil:
		if _, err := tx.ExecContext(ctx, `UPDATE device_state SET last_error=NULL,last_error_at=NULL WHERE host_id=$1 AND device_id=$2`, report.HostID, report.DeviceID); err != nil {
			return err
		}
	}
	_, err := tx.ExecContext(ctx, `UPDATE device_state SET reported_at=$1 WHERE host_id=$2 AND device_id=$3`, t, report.HostID, report.DeviceID)
	return err
}

// ListHardware is every declared host (or only hostID) with its controller
// and its devices' policies, settings and what the controller reports.
func (s *Store) ListHardware(ctx context.Context, hostID string) ([]HardwareHost, error) {
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	spec, err := declaredSpec(ctx, tx)
	if err != nil {
		return nil, err
	}
	policies := spec.DevicePolicies()
	out := []HardwareHost{}
	for _, host := range spec.Hosts {
		if hostID != "" && host.ID != hostID {
			continue
		}
		entry := HardwareHost{ID: host.ID, Nodes: host.Nodes, MaxSlowdown: host.MaxSlowdown, Devices: []HardwareDevice{}}
		if host.Controller != "" {
			entry.Controller = &host.Controller
			err = tx.QueryRowContext(ctx, `SELECT started_at,last_seen_at,version FROM device_controllers WHERE node_id=$1`, host.Controller).
				Scan(&entry.ControllerStartedAt, &entry.ControllerLastSeenAt, &entry.ControllerVersion)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return nil, err
			}
		}
		for _, policy := range policies {
			if policy.HostID != host.ID {
				continue
			}
			device, err := hardwareDeviceTx(ctx, tx, policy)
			if err != nil {
				return nil, err
			}
			entry.Devices = append(entry.Devices, device)
		}
		out = append(out, entry)
	}
	if hostID != "" && len(out) == 0 {
		return nil, fmt.Errorf("%w: host %s is not declared", ErrNotFound, hostID)
	}
	return out, nil
}

func hardwareDeviceTx(ctx context.Context, tx *sql.Tx, policy machine.DevicePolicy) (HardwareDevice, error) {
	device := HardwareDevice{DevicePolicy: policy}
	var err error
	if device.Settings, err = deviceSettingsTx(ctx, tx, policy.HostID, policy.DeviceID); err != nil {
		return device, err
	}
	if device.Policy, err = devicePolicyDecisionTx(ctx, tx, policy.HostID, policy.DeviceID); err != nil {
		return device, err
	}
	if device.Bench, err = latestBenchRunTx(ctx, tx, policy.HostID, policy.DeviceID); err != nil {
		return device, err
	}
	if len(device.Settings) != 0 {
		device.Effective = &device.Settings[0]
	}
	var appliedJSON, capsJSON sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT generation,applied_generation,applied_json,applied_note,applied_at,last_error,last_error_at,capabilities_json,capabilities_at,reported_at FROM device_state WHERE host_id=$1 AND device_id=$2`, policy.HostID, policy.DeviceID).
		Scan(&device.Generation, &device.AppliedGeneration, &appliedJSON, &device.AppliedNote, &device.AppliedAt, &device.LastError, &device.LastErrorAt, &capsJSON, &device.CapabilitiesAt, &device.ReportedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return device, nil
	}
	if err != nil {
		return device, err
	}
	if appliedJSON.Valid {
		device.Applied = &DeviceSettings{}
		if err = json.Unmarshal([]byte(appliedJSON.String), device.Applied); err != nil {
			return device, err
		}
	}
	if capsJSON.Valid {
		device.Capabilities = &DeviceCapabilities{}
		if err = json.Unmarshal([]byte(capsJSON.String), device.Capabilities); err != nil {
			return device, err
		}
	}
	return device, nil
}
