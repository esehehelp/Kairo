package store

// The operating-point policy (internal/hwpolicy) runs in the daemon: for
// every controllable device whose max_slowdown is above 0 it chooses, from
// the device's bench profile and what it observes the device doing, the
// setting that keeps performance within max_slowdown for the least energy,
// and asks for it as the device's policy setting (a GPU's for the lease
// holding it, a CPU's for its host). This file is what it reads and records;
// it never holds back admission, and a setting the controller does not apply
// changes nothing else.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"kairo/internal/machine"
)

// Policy bases: where a decision's setting comes from.
const (
	// BasisProfile: a point of the device's bench profile that keeps every
	// candidate kind within max_slowdown.
	BasisProfile = "profile"
	// BasisFallback: no profile, or no confident idea of the workload: the
	// highest graphics clock (or CPU frequency) is capped at (1 -
	// max_slowdown) of the device's maximum, which loses at most that share
	// of performance whatever runs.
	BasisFallback = "fallback"
	// BasisDefault: the defaults (nothing in the profile saves energy within
	// max_slowdown, or there is nothing to cap).
	BasisDefault = "default"
)

// PolicyExpectation is what the profile says a decision does to one kind:
// its performance relative to the defaults, its power and the defaults'
// power, and SavingW, the power saved for the same work (RelPerf × default
// power − power).
type PolicyExpectation struct {
	Kind          string   `json:"kind"`
	RelPerf       float64  `json:"rel_perf"`
	PowerW        *float64 `json:"power_w,omitempty"`
	DefaultPowerW *float64 `json:"default_power_w,omitempty"`
	SavingW       *float64 `json:"saving_w,omitempty"`
}

// PolicyDecision is one choice of the policy for a device, with its inputs.
type PolicyDecision struct {
	HostID      string         `json:"host_id"`
	DeviceID    string         `json:"device_id"`
	LeaseID     string         `json:"lease_id,omitempty"`
	AttemptID   string         `json:"attempt_id,omitempty"`
	MaxSlowdown float64        `json:"max_slowdown"`
	Settings    DeviceSettings `json:"settings"`
	Basis       string         `json:"basis"`
	Reason      string         `json:"reason,omitempty"`
	// Candidates are the kinds the workload may be (GPU: those whose bench
	// fingerprint matches what the device is observed doing; CPU: every
	// kind); the setting suits all of them.
	Candidates  []string            `json:"candidates,omitempty"`
	Fingerprint *Fingerprint        `json:"fingerprint,omitempty"`
	RunIDs      []string            `json:"run_ids,omitempty"`
	Expected    []PolicyExpectation `json:"expected,omitempty"`
	DecidedAt   string              `json:"decided_at"`
}

// SavingW is the least power the decision saves over its candidate kinds, nil
// when the profile does not say.
func (d PolicyDecision) SavingW() *float64 {
	var least *float64
	for _, e := range d.Expected {
		if e.SavingW == nil {
			return nil
		}
		if least == nil || *e.SavingW < *least {
			least = ptr(*e.SavingW)
		}
	}
	return least
}

// PolicyLease is a live lease holding a device.
type PolicyLease struct {
	LeaseID   string `json:"lease_id"`
	AttemptID string `json:"attempt_id,omitempty"`
	// ActivatedAt is when its attempt began to run (empty: not yet).
	ActivatedAt string `json:"activated_at,omitempty"`
	// Bench: the lease is a bench run's.
	Bench bool `json:"bench"`
}

// PolicyUsage is what a GPU was observed doing: utilization.gpu of its
// observations and utilization.memory of its samples, oldest first.
type PolicyUsage struct {
	UtilGPUPct []float64 `json:"util_gpu_pct"`
	UtilMemPct []float64 `json:"util_mem_pct"`
}

// PolicyDevice is a device as the policy sees it.
type PolicyDevice struct {
	HardwareDevice
	// Lease is the live lease holding a GPU (nil: none); for a CPU, Bench
	// tells whether a bench of it holds a lease on the host.
	Lease *PolicyLease `json:"lease,omitempty"`
	Bench bool         `json:"bench"`
	// Usage: a GPU's since its lease's attempt began to run, within the window.
	Usage PolicyUsage `json:"usage"`
}

// PolicySetting is the device's policy setting (nil: none).
func (d PolicyDevice) PolicySetting() *DeviceSetting {
	for i := range d.Settings {
		if d.Settings[i].Owner.Kind == OwnerPolicy {
			return &d.Settings[i]
		}
	}
	return nil
}

// PolicyDevices are every declared device with its settings, the lease
// holding it and what it was observed doing over the last window.
func (s *Store) PolicyDevices(ctx context.Context, window time.Duration) ([]PolicyDevice, error) {
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	spec, err := declaredSpec(ctx, tx)
	if err != nil {
		return nil, err
	}
	since := time.Now().Add(-window)
	var out []PolicyDevice
	for _, policy := range spec.DevicePolicies() {
		hardware, err := hardwareDeviceTx(ctx, tx, policy)
		if err != nil {
			return nil, err
		}
		device := PolicyDevice{HardwareDevice: hardware}
		if policy.Kind == machine.CPU {
			if device.Bench, err = hostCPUBenchTx(ctx, tx, policy.HostID); err != nil {
				return nil, err
			}
			out = append(out, device)
			continue
		}
		var lease PolicyLease
		var attempt, activated sql.NullString
		err = tx.QueryRowContext(ctx, `SELECT l.id,l.attempt_id,a.started_at,EXISTS(SELECT 1 FROM bench_run_executions b WHERE b.execution_id=l.execution_id)
			FROM resource_instances r JOIN lease_items li ON li.resource_id=r.id JOIN leases l ON l.id=li.lease_id LEFT JOIN attempts a ON a.id=l.attempt_id
			WHERE r.kind='gpu' AND r.stable_identity=$1 AND l.state IN('reserved','prepared','active') ORDER BY l.created_at DESC LIMIT 1`, policy.DeviceID).
			Scan(&lease.LeaseID, &attempt, &activated, &lease.Bench)
		switch {
		case errors.Is(err, sql.ErrNoRows):
		case err != nil:
			return nil, err
		default:
			lease.AttemptID, lease.ActivatedAt = attempt.String, activated.String
			device.Lease = &lease
			if activated.Valid {
				from := since
				if at, err := time.Parse(time.RFC3339Nano, activated.String); err == nil && at.After(from) {
					from = at
				}
				if device.Usage, err = gpuUsageTx(ctx, tx, policy.DeviceID, from); err != nil {
					return nil, err
				}
			}
		}
		out = append(out, device)
	}
	return out, nil
}

// hostCPUBenchTx reports whether a CPU bench of host holds a lease.
func hostCPUBenchTx(ctx context.Context, tx *sql.Tx, hostID string) (bool, error) {
	var n int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM bench_runs b JOIN bench_run_executions e ON e.run_id=b.id JOIN leases l ON l.execution_id=e.execution_id
		WHERE b.host_id=$1 AND b.class='cpu' AND l.state IN`+liveLeaseStates, hostID).Scan(&n)
	return n != 0, err
}

func gpuUsageTx(ctx context.Context, tx *sql.Tx, uuid string, from time.Time) (PolicyUsage, error) {
	usage := PolicyUsage{UtilGPUPct: []float64{}, UtilMemPct: []float64{}}
	err := eachRow(ctx, tx, `SELECT o.utilization FROM resource_instances r JOIN resource_observations o ON o.resource_id=r.id
		WHERE r.kind='gpu' AND r.stable_identity=$1 AND o.utilization IS NOT NULL AND (o.observed_at)::timestamptz>=($2)::timestamptz ORDER BY o.observed_at`,
		[]any{uuid, from.UTC().Format(time.RFC3339Nano)}, func(rows *sql.Rows) error {
			var v float64
			if err := rows.Scan(&v); err != nil {
				return err
			}
			usage.UtilGPUPct = append(usage.UtilGPUPct, v)
			return nil
		})
	if err != nil {
		return usage, err
	}
	err = eachRow(ctx, tx, `SELECT mem_util FROM device_samples WHERE device=$1 AND observed_at>=$2 AND mem_util IS NOT NULL ORDER BY observed_at`,
		[]any{uuid, sampleStamp(from)}, func(rows *sql.Rows) error {
			var v float64
			if err := rows.Scan(&v); err != nil {
				return err
			}
			usage.UtilMemPct = append(usage.UtilMemPct, v)
			return nil
		})
	return usage, err
}

// RecordPolicyDecision keeps d as the device's latest decision (event
// device_policy_decision).
func (s *Store) RecordPolicyDecision(ctx context.Context, d PolicyDecision) error {
	if d.DecidedAt == "" {
		d.DecidedAt = now()
	}
	body, err := json.Marshal(d)
	if err != nil {
		return err
	}
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `INSERT INTO device_policy(host_id,device_id,decision_json,decided_at) VALUES($1,$2,$3,$4)
		ON CONFLICT(host_id,device_id) DO UPDATE SET decision_json=excluded.decision_json,decided_at=excluded.decided_at`, d.HostID, d.DeviceID, string(body), d.DecidedAt); err != nil {
		return err
	}
	if err = appendCoordinationEventTx(ctx, tx, "device_policy_decision", nil, "device", d.HostID+"/"+d.DeviceID, nil, d); err != nil {
		return err
	}
	return tx.Commit()
}

// AccruePolicySavings adds seconds in which the policy's setting was in place
// on an attempt's GPU, with the energy the profile says it saved (nil: no
// estimate, the seconds count as unestimated).
func (s *Store) AccruePolicySavings(ctx context.Context, attemptID string, seconds float64, savedJoules *float64) error {
	saved, unestimated := 0.0, seconds
	if savedJoules != nil {
		saved, unestimated = *savedJoules, 0
	}
	_, err := s.exec(ctx, `INSERT INTO attempt_energy(attempt_id,policy_seconds,policy_saved_joules,policy_unestimated_seconds,updated_at) VALUES($1,$2,$3,$4,$5)
		ON CONFLICT(attempt_id) DO UPDATE SET policy_seconds=attempt_energy.policy_seconds+excluded.policy_seconds,policy_saved_joules=attempt_energy.policy_saved_joules+excluded.policy_saved_joules,
		policy_unestimated_seconds=attempt_energy.policy_unestimated_seconds+excluded.policy_unestimated_seconds,updated_at=excluded.updated_at`,
		attemptID, seconds, saved, unestimated, now())
	return err
}

// devicePolicyDecisionTx is a device's latest policy decision (nil: none).
func devicePolicyDecisionTx(ctx context.Context, q queryer, hostID, deviceID string) (*PolicyDecision, error) {
	var body string
	err := q.QueryRowContext(ctx, `SELECT decision_json FROM device_policy WHERE host_id=$1 AND device_id=$2`, hostID, deviceID).Scan(&body)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var d PolicyDecision
	if err = json.Unmarshal([]byte(body), &d); err != nil {
		return nil, err
	}
	return &d, nil
}

// latestBenchRunTx is a device's latest bench (nil: none), with its kinds'
// references (what a partial run's valid kinds are) but not its points.
func latestBenchRunTx(ctx context.Context, q queryer, hostID, deviceID string) (*BenchRun, error) {
	run, err := scanBenchRun(q.QueryRowContext(ctx, `SELECT `+benchRunColumns+` FROM bench_runs WHERE host_id=$1 AND device_id=$2 ORDER BY created_at DESC,id DESC LIMIT 1`, hostID, deviceID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &run, nil
}
