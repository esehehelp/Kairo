package store

// Device samples: what providers read of a device's power and operating point
// (a GPU's power draw, power limit, clocks, P-state, clock event reasons and
// PCIe link; a node's CPU package power), kept as facts with their time.
// Nothing here compares a reading with what the device should do.
//
// A device is its physical identity, not its resource row: the samples every
// provider of a shared GPU reports are one series, and the energy the GPU
// uses is counted once. The energy an attempt's GPUs use while it runs is
// added to attempt_energy as their samples arrive (accrueAttemptEnergyTx), so
// reading it never scans the samples. A CPU package is shared by whatever
// runs on its node: its power is kept for the node only.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// DeviceSample is what a provider read of a device at one time, besides the
// observation it comes with. A field is nil when the device or its driver
// does not report it.
type DeviceSample struct {
	// PowerW is the device's power draw (a GPU's board power averaged over
	// about a second; a CPU package's average since the provider's previous
	// reading). PowerLimitW is the power limit in force.
	PowerW      *float64 `json:"power_w,omitempty"`
	PowerLimitW *float64 `json:"power_limit_w,omitempty"`
	SMClockMHz  *int64   `json:"sm_clock_mhz,omitempty"`
	MemClockMHz *int64   `json:"mem_clock_mhz,omitempty"`
	PState      *string  `json:"pstate,omitempty"`
	// EventReasons is the bitmask of the reasons the clocks are held down
	// (NVML's clocks event reasons: 0x1 idle, 0x4 software power cap, ...).
	EventReasons   *int64   `json:"event_reasons,omitempty"`
	MemUtilization *float64 `json:"mem_util,omitempty"`
	FanPercent     *float64 `json:"fan_pct,omitempty"`
	PCIeGen        *int64   `json:"pcie_gen,omitempty"`
	PCIeWidth      *int64   `json:"pcie_width,omitempty"`
	DeviceProperties
}

// DeviceProperties are what hardly changes of a device, kept once per device
// as last reported (device_properties).
type DeviceProperties struct {
	DefaultPowerLimitW *float64 `json:"default_power_limit_w,omitempty"`
	MinPowerLimitW     *float64 `json:"min_power_limit_w,omitempty"`
	MaxPowerLimitW     *float64 `json:"max_power_limit_w,omitempty"`
	MaxSMClockMHz      *int64   `json:"max_sm_clock_mhz,omitempty"`
	MaxMemClockMHz     *int64   `json:"max_mem_clock_mhz,omitempty"`
	PCIeGenMax         *int64   `json:"pcie_gen_max,omitempty"`
	PCIeWidthMax       *int64   `json:"pcie_width_max,omitempty"`
}

func (p DeviceProperties) empty() bool {
	return p == DeviceProperties{}
}

// DeviceReading is a device's latest sample, by whichever provider of it
// reported last, with its properties.
type DeviceReading struct {
	ObservedAt string `json:"observed_at"`
	DeviceSample
}

// AttemptEnergy is the energy the GPUs of an attempt's lease used while it
// ran. GPUSecondsCovered is the device-seconds it integrates (an attempt on
// two GPUs for an hour covers 7200); Partial means part of the run is not in
// it.
type AttemptEnergy struct {
	GPUJoules         float64 `json:"gpu_joules"`
	GPUSecondsCovered float64 `json:"gpu_seconds_covered"`
	Partial           bool    `json:"partial"`
	// PolicySeconds are the device-seconds the operating-point policy's
	// setting was in place on the attempt's GPUs, PolicySavedJoules the energy
	// the device's bench profile says that saved for the same work (an
	// estimate), PolicyUnestimatedSeconds the part of PolicySeconds without a
	// profile to estimate from.
	PolicySeconds            float64 `json:"policy_seconds,omitempty"`
	PolicySavedJoules        float64 `json:"policy_saved_joules,omitempty"`
	PolicyUnestimatedSeconds float64 `json:"policy_unestimated_seconds,omitempty"`
	UpdatedAt                string  `json:"updated_at"`
}

// sampleStampLayout is RFC 3339 with a fixed number of fractional digits:
// device sample times order as text, so their index serves time ranges.
const sampleStampLayout = "2006-01-02T15:04:05.000000000Z07:00"

func sampleStamp(t time.Time) string { return t.UTC().Format(sampleStampLayout) }

// deviceKey is the SQL device of resource row r: a GPU's stable identity
// (its UUID), any other resource's own id.
const deviceKey = `CASE WHEN r.kind='gpu' THEN r.stable_identity ELSE r.id END`

// recordDeviceSampleTx records sample, which the provider of resourceID read
// at observedAt and vouches for until validUntil, and adds the energy a GPU
// used since its previous sample to the attempts holding it.
func recordDeviceSampleTx(ctx context.Context, tx *sql.Tx, resourceID, observedAt, validUntil string, sample DeviceSample) error {
	var device, kind string
	if err := tx.QueryRowContext(ctx, `SELECT `+deviceKey+`,r.kind FROM resource_instances r WHERE r.id=$1`, resourceID).Scan(&device, &kind); err != nil {
		return err
	}
	at, err := time.Parse(time.RFC3339Nano, observedAt)
	if err != nil {
		return fmt.Errorf("device sample of %s: observed_at: %w", resourceID, err)
	}
	until, err := time.Parse(time.RFC3339Nano, validUntil)
	if err != nil {
		return fmt.Errorf("device sample of %s: valid_until: %w", resourceID, err)
	}
	if kind == "gpu" {
		if err = accrueAttemptEnergyTx(ctx, tx, device, at, sample.PowerW); err != nil {
			return err
		}
	}
	stamp := sampleStamp(at)
	if _, err = tx.ExecContext(ctx, `INSERT INTO device_samples(device,resource_id,observed_at,valid_until,power_w,power_limit_w,sm_clock_mhz,mem_clock_mhz,pstate,event_reasons,mem_util,fan_pct,pcie_gen,pcie_width) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`,
		device, resourceID, stamp, sampleStamp(until), sample.PowerW, sample.PowerLimitW, sample.SMClockMHz, sample.MemClockMHz, sample.PState, sample.EventReasons, sample.MemUtilization, sample.FanPercent, sample.PCIeGen, sample.PCIeWidth); err != nil {
		return err
	}
	if p := sample.DeviceProperties; !p.empty() {
		_, err = tx.ExecContext(ctx, `INSERT INTO device_properties(device,default_power_limit_w,min_power_limit_w,max_power_limit_w,max_sm_clock_mhz,max_mem_clock_mhz,pcie_gen_max,pcie_width_max,observed_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
			ON CONFLICT(device) DO UPDATE SET default_power_limit_w=excluded.default_power_limit_w,min_power_limit_w=excluded.min_power_limit_w,max_power_limit_w=excluded.max_power_limit_w,
			max_sm_clock_mhz=excluded.max_sm_clock_mhz,max_mem_clock_mhz=excluded.max_mem_clock_mhz,pcie_gen_max=excluded.pcie_gen_max,pcie_width_max=excluded.pcie_width_max,observed_at=excluded.observed_at
			WHERE excluded.observed_at>=device_properties.observed_at`,
			device, p.DefaultPowerLimitW, p.MinPowerLimitW, p.MaxPowerLimitW, p.MaxSMClockMHz, p.MaxMemClockMHz, p.PCIeGenMax, p.PCIeWidthMax, stamp)
	}
	return err
}

// accrueAttemptEnergyTx adds the energy GPU device used between its latest
// sample and a new one at t1 (power p1) to the attempts whose lease held it
// meanwhile, each for the part of the interval it ran: the trapezoid of the
// two samples' power, cut at the attempt's start and end. An interval the
// samples do not vouch for (the earlier one expired before the next came, or
// either lacks power), or before the device's first sample, is not
// integrated and makes the attempts in it partial. A sample no later than
// one already recorded (a shared GPU's other provider reporting late) adds
// nothing: its interval is counted already.
//
// An attempt ends at its exit, its lease's release, or its lease going stale
// (Kairo no longer knows what runs; the rest of its run is partial). An exit
// recorded only after the next sample arrives counts up to one interval too
// much.
func accrueAttemptEnergyTx(ctx context.Context, tx *sql.Tx, device string, t1 time.Time, p1 *float64) error {
	stamp1 := sampleStamp(t1)
	var t0 time.Time // zero: the device has no earlier sample
	covered := false
	var p0 float64
	var at0, until0 string
	var power0 sql.NullFloat64
	// The device's latest sample, read down its index. (`EXISTS(... WHERE
	// observed_at>=$2)` was planned as a sequential scan that stops at the
	// first match, and there is usually none: the whole table, every sample.)
	err := tx.QueryRowContext(ctx, `SELECT observed_at,valid_until,power_w FROM device_samples WHERE device=$1 ORDER BY observed_at DESC LIMIT 1`, device).Scan(&at0, &until0, &power0)
	switch {
	case errors.Is(err, sql.ErrNoRows):
	case err != nil:
		return err
	case at0 >= stamp1:
		return nil // this sample's interval is counted already
	default:
		if t0, err = time.Parse(time.RFC3339Nano, at0); err != nil {
			return err
		}
		until, err := time.Parse(time.RFC3339Nano, until0)
		if err != nil {
			return err
		}
		covered = !t1.After(until) && power0.Valid && p1 != nil
		p0 = power0.Float64
	}
	type holder struct {
		id                        string
		start                     time.Time
		exited, released, staleAt *time.Time
	}
	var holders []holder
	err = eachRow(ctx, tx, `SELECT DISTINCT a.id,a.started_at,a.exited_at,l.released_at,l.stale_at FROM resource_instances r
		JOIN lease_items li ON li.resource_id=r.id JOIN leases l ON l.id=li.lease_id JOIN attempts a ON a.id=l.attempt_id
		WHERE r.kind='gpu' AND r.stable_identity=$1 AND a.started_at IS NOT NULL AND (a.started_at)::timestamptz<($2)::timestamptz
		AND (a.exited_at IS NULL OR (a.exited_at)::timestamptz>($3)::timestamptz) AND (l.released_at IS NULL OR (l.released_at)::timestamptz>($4)::timestamptz)`,
		[]any{device, stamp1, sampleStamp(t0), sampleStamp(t0)}, func(rows *sql.Rows) error {
			var h holder
			var started string
			var exited, released, staleAt sql.NullString
			if err := rows.Scan(&h.id, &started, &exited, &released, &staleAt); err != nil {
				return err
			}
			var err error
			if h.start, err = time.Parse(time.RFC3339Nano, started); err != nil {
				return err
			}
			for _, x := range []struct {
				text sql.NullString
				into **time.Time
			}{{exited, &h.exited}, {released, &h.released}, {staleAt, &h.staleAt}} {
				if x.text.Valid {
					t, err := time.Parse(time.RFC3339Nano, x.text.String)
					if err != nil {
						return err
					}
					*x.into = &t
				}
			}
			holders = append(holders, h)
			return nil
		})
	if err != nil {
		return err
	}
	powerAt := func(t time.Time) float64 {
		return p0 + (*p1-p0)*t.Sub(t0).Seconds()/t1.Sub(t0).Seconds()
	}
	updated := now()
	for _, h := range holders {
		from, to := h.start, t1
		if from.Before(t0) {
			from = t0
		}
		for _, end := range []*time.Time{h.exited, h.released, h.staleAt} {
			if end != nil && end.Before(to) {
				to = *end
			}
		}
		if !to.After(from) {
			continue
		}
		// What ran after the lease went stale is not known.
		partial := !covered || (h.staleAt != nil && h.staleAt.Before(t1) && (h.exited == nil || h.exited.After(*h.staleAt)))
		var joules, seconds float64
		if covered {
			seconds = to.Sub(from).Seconds()
			joules = seconds * (powerAt(from) + powerAt(to)) / 2
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO attempt_energy(attempt_id,gpu_joules,gpu_seconds_covered,partial,updated_at) VALUES($1,$2,$3,$4,$5)
			ON CONFLICT(attempt_id) DO UPDATE SET gpu_joules=attempt_energy.gpu_joules+excluded.gpu_joules,gpu_seconds_covered=attempt_energy.gpu_seconds_covered+excluded.gpu_seconds_covered,
			partial=attempt_energy.partial OR excluded.partial,updated_at=excluded.updated_at`, h.id, joules, seconds, partial, updated); err != nil {
			return err
		}
	}
	return nil
}

// deviceReadings is the latest device sample of every resource that has one,
// by resource ID: every row of a shared GPU shows the device's latest.
func (s *Store) deviceReadings(ctx context.Context) (map[string]DeviceReading, error) {
	out := map[string]DeviceReading{}
	err := eachRow(ctx, s.db, `SELECT r.id,d.observed_at,d.power_w,d.power_limit_w,d.sm_clock_mhz,d.mem_clock_mhz,d.pstate,d.event_reasons,d.mem_util,d.fan_pct,d.pcie_gen,d.pcie_width,
		p.default_power_limit_w,p.min_power_limit_w,p.max_power_limit_w,p.max_sm_clock_mhz,p.max_mem_clock_mhz,p.pcie_gen_max,p.pcie_width_max
		FROM resource_instances r JOIN device_samples d ON d.id=(SELECT x.id FROM device_samples x WHERE x.device=`+deviceKey+` ORDER BY x.observed_at DESC LIMIT 1)
		LEFT JOIN device_properties p ON p.device=d.device`, nil, func(rows *sql.Rows) error {
		var id string
		var d DeviceReading
		if err := rows.Scan(&id, &d.ObservedAt, &d.PowerW, &d.PowerLimitW, &d.SMClockMHz, &d.MemClockMHz, &d.PState, &d.EventReasons, &d.MemUtilization, &d.FanPercent, &d.PCIeGen, &d.PCIeWidth,
			&d.DefaultPowerLimitW, &d.MinPowerLimitW, &d.MaxPowerLimitW, &d.MaxSMClockMHz, &d.MaxMemClockMHz, &d.PCIeGenMax, &d.PCIeWidthMax); err != nil {
			return err
		}
		out[id] = d
		return nil
	})
	return out, err
}

// taskAttemptEnergy is the GPU energy of a task's attempts, by attempt.
func (s *Store) taskAttemptEnergy(ctx context.Context, taskScopeID string) (map[string]AttemptEnergy, error) {
	out := map[string]AttemptEnergy{}
	err := eachRow(ctx, s.db, `SELECT ae.attempt_id,ae.gpu_joules,ae.gpu_seconds_covered,ae.partial,ae.policy_seconds,ae.policy_saved_joules,ae.policy_unestimated_seconds,ae.updated_at
		FROM orchestration_task_executions te JOIN attempts a ON a.execution_id=te.execution_id JOIN attempt_energy ae ON ae.attempt_id=a.id
		WHERE te.task_scope_id=$1`, []any{taskScopeID}, func(rows *sql.Rows) error {
		var attemptID string
		var e AttemptEnergy
		if err := rows.Scan(&attemptID, &e.GPUJoules, &e.GPUSecondsCovered, &e.Partial, &e.PolicySeconds, &e.PolicySavedJoules, &e.PolicyUnestimatedSeconds, &e.UpdatedAt); err != nil {
			return err
		}
		out[attemptID] = e
		return nil
	})
	return out, err
}

// HostCPUDevice is the device_samples series of a host's CPU package as its
// controller reads it.
func HostCPUDevice(hostID string) string { return "host/" + hostID + "/cpu" }

// ControllerSample is what a controller read of a device of its host: the CPU
// package's power averaged since its previous reading.
type ControllerSample struct {
	DeviceID   string   `json:"device_id"`
	ObservedAt string   `json:"observed_at"`
	ValidUntil string   `json:"valid_until"`
	PowerW     *float64 `json:"power_w,omitempty"`
}

// RecordControllerSamples records what node's controller read of its host's
// devices (only "cpu" is read) into the host's series. A controller that no
// host names records nothing; the samples it sent are returned as skipped.
func (s *Store) RecordControllerSamples(ctx context.Context, nodeID string, samples []ControllerSample) ([]string, error) {
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	spec, err := declaredSpec(ctx, tx)
	if err != nil {
		return nil, err
	}
	hostID := ""
	for _, host := range spec.Hosts {
		if host.Controller == nodeID {
			hostID = host.ID
		}
	}
	skipped := []string{}
	for _, sample := range samples {
		if hostID == "" || sample.DeviceID != "cpu" {
			skipped = append(skipped, sample.DeviceID)
			continue
		}
		at, err := time.Parse(time.RFC3339Nano, sample.ObservedAt)
		if err != nil {
			return nil, fmt.Errorf("controller sample: observed_at: %w", err)
		}
		until, err := time.Parse(time.RFC3339Nano, sample.ValidUntil)
		if err != nil {
			return nil, fmt.Errorf("controller sample: valid_until: %w", err)
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO device_samples(device,resource_id,observed_at,valid_until,power_w) VALUES($1,NULL,$2,$3,$4)`,
			HostCPUDevice(hostID), sampleStamp(at), sampleStamp(until), sample.PowerW); err != nil {
			return nil, err
		}
	}
	return skipped, tx.Commit()
}

// CPUEnergy is the energy of a host's CPU packages over a window, from its
// samples: each sample is the average power since the one before it, so it
// counts for the part of that span inside the window.
type CPUEnergy struct {
	// Device is the series it was read from (a node's cpu resource, or the
	// controller's HostCPUDevice).
	Device         string  `json:"device"`
	Joules         float64 `json:"joules"`
	SecondsCovered float64 `json:"seconds_covered"`
	Seconds        float64 `json:"seconds"`
}

// MeanPowerW is the window's average power over the part its samples cover.
func (e CPUEnergy) MeanPowerW() float64 {
	if e.SecondsCovered == 0 {
		return 0
	}
	return e.Joules / e.SecondsCovered
}

// HostCPUEnergy is the CPU package energy of host over [from, to]: from the
// series that covers most of it, among the controller's and those of the
// host's nodes (the same packages, read by different readers, are never
// added together). ErrNotFound when no series covers any of it.
func (s *Store) HostCPUEnergy(ctx context.Context, hostID string, from, to time.Time) (CPUEnergy, error) {
	spec, err := declaredSpec(ctx, s.db)
	if err != nil {
		return CPUEnergy{}, err
	}
	devices := []string{HostCPUDevice(hostID)}
	for _, host := range spec.Hosts {
		if host.ID != hostID {
			continue
		}
		for _, node := range host.Nodes {
			err := eachRow(ctx, s.db, `SELECT id FROM resource_instances WHERE node_id=$1 AND kind='cpu' ORDER BY id`, []any{node}, func(rows *sql.Rows) error {
				var id string
				if err := rows.Scan(&id); err != nil {
					return err
				}
				devices = append(devices, id)
				return nil
			})
			if err != nil {
				return CPUEnergy{}, err
			}
		}
	}
	best := CPUEnergy{Seconds: to.Sub(from).Seconds()}
	for _, device := range devices {
		e, err := s.seriesEnergy(ctx, device, from, to)
		if err != nil {
			return CPUEnergy{}, err
		}
		if e.SecondsCovered > best.SecondsCovered {
			best = e
		}
	}
	if best.SecondsCovered == 0 {
		return best, fmt.Errorf("%w: no CPU package power of host %s between %s and %s", ErrNotFound, hostID, from.UTC().Format(time.RFC3339), to.UTC().Format(time.RFC3339))
	}
	return best, nil
}

// seriesEnergy integrates one series over [from, to]. A sample counts from
// the sample before it (when that one was still valid then) to its own time.
func (s *Store) seriesEnergy(ctx context.Context, device string, from, to time.Time) (CPUEnergy, error) {
	e := CPUEnergy{Device: device, Seconds: to.Sub(from).Seconds()}
	type sample struct {
		at, until time.Time
		power     sql.NullFloat64
	}
	var samples []sample
	// The sample before the window starts the first span.
	err := eachRow(ctx, s.db, `SELECT observed_at,valid_until,power_w FROM (SELECT observed_at,valid_until,power_w FROM device_samples WHERE device=$1 AND observed_at<$2 ORDER BY observed_at DESC LIMIT 1)
		UNION ALL SELECT observed_at,valid_until,power_w FROM (SELECT observed_at,valid_until,power_w FROM device_samples WHERE device=$3 AND observed_at>=$4 ORDER BY observed_at)
		ORDER BY observed_at`,
		[]any{device, sampleStamp(from), device, sampleStamp(from)}, func(rows *sql.Rows) error {
			var at, until string
			var x sample
			if err := rows.Scan(&at, &until, &x.power); err != nil {
				return err
			}
			var err error
			if x.at, err = time.Parse(time.RFC3339Nano, at); err != nil {
				return err
			}
			if x.until, err = time.Parse(time.RFC3339Nano, until); err != nil {
				return err
			}
			samples = append(samples, x)
			return nil
		})
	if err != nil {
		return e, err
	}
	for i := 1; i < len(samples); i++ {
		prev, cur := samples[i-1], samples[i]
		if !cur.power.Valid || cur.at.After(prev.until) {
			continue // the span is not vouched for
		}
		begin, end := laterOf(prev.at, from), earlierOf(cur.at, to)
		if !end.After(begin) {
			if cur.at.After(to) {
				break
			}
			continue
		}
		seconds := end.Sub(begin).Seconds()
		e.Joules += cur.power.Float64 * seconds
		e.SecondsCovered += seconds
	}
	return e, nil
}

func laterOf(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

func earlierOf(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
