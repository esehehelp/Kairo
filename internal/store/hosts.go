package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"

	"kairo/internal/machine"
)

// declaredSpec is the machine declaration as stored: the GPUs of every node
// (machine_gpus) and the hosts.
func declaredSpec(ctx context.Context, q queryer) (machine.Spec, error) {
	var spec machine.Spec
	rows, err := q.QueryContext(ctx, `SELECT node_id,uuid FROM machine_gpus ORDER BY node_id,uuid`)
	if err != nil {
		return spec, err
	}
	for rows.Next() {
		var nodeID, uuid string
		if err = rows.Scan(&nodeID, &uuid); err != nil {
			rows.Close()
			return spec, err
		}
		if n := len(spec.Nodes); n == 0 || spec.Nodes[n-1].ID != nodeID {
			spec.Nodes = append(spec.Nodes, machine.Node{ID: nodeID})
		}
		last := &spec.Nodes[len(spec.Nodes)-1]
		last.GPUs = append(last.GPUs, uuid)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return spec, err
	}
	rows, err = q.QueryContext(ctx, `SELECT declaration_json FROM machine_hosts ORDER BY id`)
	if err != nil {
		return spec, err
	}
	defer rows.Close()
	for rows.Next() {
		var body string
		if err = rows.Scan(&body); err != nil {
			return spec, err
		}
		var host machine.Host
		if err = json.Unmarshal([]byte(body), &host); err != nil {
			return spec, fmt.Errorf("stored host declaration: %w", err)
		}
		spec.Hosts = append(spec.Hosts, host)
	}
	return spec, rows.Err()
}

// hostChangesTx compares spec's hosts with the stored ones.
func hostChangesTx(ctx context.Context, tx *sql.Tx, spec machine.Spec) ([]HostChange, int, error) {
	current, err := declaredSpec(ctx, tx)
	if err != nil {
		return nil, 0, err
	}
	before := map[string]machine.Host{}
	for _, host := range current.Hosts {
		before[host.ID] = host
	}
	changes := []HostChange{}
	unchanged := 0
	for _, host := range spec.Hosts {
		to := host.Normalized()
		from, existed := before[host.ID]
		delete(before, host.ID)
		switch {
		case !existed:
			changes = append(changes, HostChange{Host: host.ID, To: &to})
		case sameHost(from, to):
			unchanged++
		default:
			changes = append(changes, HostChange{Host: host.ID, From: &from, To: &to})
		}
	}
	for _, from := range before {
		changes = append(changes, HostChange{Host: from.ID, From: &from})
	}
	sort.Slice(changes, func(i, j int) bool { return changes[i].Host < changes[j].Host })
	return changes, unchanged, nil
}

func sameHost(a, b machine.Host) bool {
	x, _ := json.Marshal(a.Normalized())
	y, _ := json.Marshal(b.Normalized())
	return bytes.Equal(x, y)
}

// applyHostsTx writes the host changes.
func applyHostsTx(ctx context.Context, tx *sql.Tx, changes []HostChange, t string) error {
	for _, change := range changes {
		if change.To == nil {
			if _, err := tx.ExecContext(ctx, `DELETE FROM machine_hosts WHERE id=$1`, change.Host); err != nil {
				return err
			}
		} else {
			body, err := json.Marshal(change.To)
			if err != nil {
				return err
			}
			if _, err = tx.ExecContext(ctx, `INSERT INTO machine_hosts(id,declaration_json,declared_at) VALUES($1,$2,$3) ON CONFLICT(id) DO UPDATE SET declaration_json=excluded.declaration_json,declared_at=excluded.declared_at`, change.Host, string(body), t); err != nil {
				return err
			}
		}
		if err := appendCoordinationEventTx(ctx, tx, "host_declaration_changed", nil, "host", change.Host, nil, change); err != nil {
			return err
		}
	}
	return nil
}

// DevicePolicies are the effective policies of every declared host's devices
// (each GPU declared for one of its nodes, and "cpu"): the device's
// max_slowdown and control, else the host's, and the host's controller.
func (s *Store) DevicePolicies(ctx context.Context) ([]machine.DevicePolicy, error) {
	spec, err := declaredSpec(ctx, s.db)
	if err != nil {
		return nil, err
	}
	return spec.DevicePolicies(), nil
}

// devicePolicyTx is the effective policy of one device; ErrNotFound when the
// host does not declare it.
func devicePolicyTx(ctx context.Context, q queryer, hostID, deviceID string) (machine.DevicePolicy, machine.Host, error) {
	spec, err := declaredSpec(ctx, q)
	if err != nil {
		return machine.DevicePolicy{}, machine.Host{}, err
	}
	for _, policy := range spec.DevicePolicies() {
		if policy.HostID == hostID && policy.DeviceID == deviceID {
			for _, host := range spec.Hosts {
				if host.ID == hostID {
					return policy, host, nil
				}
			}
		}
	}
	for _, host := range spec.Hosts {
		if host.ID == hostID {
			return machine.DevicePolicy{}, host, fmt.Errorf("%w: host %s has no device %s (its devices are %q and the GPUs declared for its nodes)", ErrNotFound, hostID, deviceID, machine.CPU)
		}
	}
	return machine.DevicePolicy{}, machine.Host{}, fmt.Errorf("%w: host %s is not declared", ErrNotFound, hostID)
}

// errNotControllable explains why a device's settings cannot be changed.
func errNotControllable(policy machine.DevicePolicy) error {
	if !policy.Control {
		return fmt.Errorf("device %s of host %s is declared control = false: its settings are never changed", policy.DeviceID, policy.HostID)
	}
	return fmt.Errorf("host %s declares no controller: its devices are only observed", policy.HostID)
}
