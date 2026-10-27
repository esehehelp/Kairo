package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"kairo/internal/machine"
)

// ErrMachineConflict: a machine declaration would move or drop a GPU that a
// live lease holds.
var ErrMachineConflict = errors.New("machine declaration conflicts with a leased GPU")

// MachineChange is one GPU whose declared nodes change (a shared GPU's are
// comma-separated, sorted): From is empty for a newly declared GPU, To for one
// no longer declared.
type MachineChange struct {
	UUID string `json:"uuid"`
	From string `json:"from,omitempty"`
	To   string `json:"to,omitempty"`
}

// HostChange is one host whose declaration changes (normalized): From is
// nil for a new host, To for one no longer declared.
type HostChange struct {
	Host string        `json:"host"`
	From *machine.Host `json:"from,omitempty"`
	To   *machine.Host `json:"to,omitempty"`
}

type MachineApplyResult struct {
	Changes   []MachineChange `json:"changes"`
	Unchanged int             `json:"unchanged"`
	// HostChanges and UnchangedHosts are the same for [[hosts]].
	HostChanges    []HostChange `json:"host_changes"`
	UnchangedHosts int          `json:"unchanged_hosts"`
	// ClearedSettings are the device settings removed because their device is
	// no longer controllable (no longer on a host, control = false, or its
	// host has no controller).
	ClearedSettings []DeviceSetting `json:"cleared_settings"`
	DryRun          bool            `json:"dry_run"`
}

// DeclaredGPU is one node's declaration of a GPU (a shared GPU has one a node)
// and what that node reports of it. State is "observed" (registered on its
// node, fresh observation), "stale" (registered on its node, no fresh
// observation), "pending" (still registered on a node it is no longer declared
// for, until its node's provider reports it) or "unseen" (never reported).
type DeclaredGPU struct {
	UUID       string  `json:"uuid"`
	NodeID     string  `json:"node_id"`
	ResourceID *string `json:"resource_id,omitempty"`
	State      string  `json:"state"`
	DeclaredAt string  `json:"declared_at"`
}

// UndeclaredGPU is a GPU a provider reports but may not register: it is
// declared for other nodes (DeclaredNodeID, comma-separated) or for none.
type UndeclaredGPU struct {
	UUID            string  `json:"uuid"`
	NodeID          string  `json:"node_id"`
	ProviderID      string  `json:"provider_id"`
	DeclaredNodeID  *string `json:"declared_node_id,omitempty"`
	FirstObservedAt string  `json:"first_observed_at"`
	LastObservedAt  string  `json:"last_observed_at"`
}

type Machines struct {
	GPUs       []DeclaredGPU   `json:"gpus"`
	Undeclared []UndeclaredGPU `json:"undeclared"`
	// Hosts are the declared hosts (normalized), Devices their devices'
	// effective policies.
	Hosts   []machine.Host         `json:"hosts"`
	Devices []machine.DevicePolicy `json:"devices"`
}

// ApplyMachines replaces the machine declaration with spec. It refuses to move
// or drop a GPU held by a live lease on a node it would no longer be declared
// for. A GPU that moves stays registered on its old node, unschedulable, until
// the new node's provider reports it; the old node's open claims on it are
// cleared, as they describe the old host. A shared GPU (declared for several
// nodes) gets a row on each of them as their providers report it. Hosts are
// replaced as a whole; the settings of devices no longer controllable are
// removed, so their controller returns them to defaults.
func (s *Store) ApplyMachines(ctx context.Context, spec machine.Spec, dryRun bool) (MachineApplyResult, error) {
	if err := spec.Validate(); err != nil {
		return MachineApplyResult{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return MachineApplyResult{}, err
	}
	defer tx.Rollback()
	var unknown []string
	for _, nodeID := range spec.NodeIDs() {
		var found int
		if err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM nodes WHERE id=?`, nodeID).Scan(&found); err != nil {
			return MachineApplyResult{}, err
		}
		if found == 0 {
			unknown = append(unknown, nodeID)
		}
	}
	if len(unknown) != 0 {
		return MachineApplyResult{}, fmt.Errorf("unknown node %s (a node is known once its daemon or agent has started)", strings.Join(unknown, ", "))
	}
	current := map[string][]string{}
	rows, err := tx.QueryContext(ctx, `SELECT uuid,node_id FROM machine_gpus ORDER BY uuid,node_id`)
	if err != nil {
		return MachineApplyResult{}, err
	}
	for rows.Next() {
		var uuid, nodeID string
		if err = rows.Scan(&uuid, &nodeID); err != nil {
			rows.Close()
			return MachineApplyResult{}, err
		}
		current[uuid] = append(current[uuid], nodeID)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return MachineApplyResult{}, err
	}
	declared := spec.GPUs()
	result := MachineApplyResult{Changes: []MachineChange{}, DryRun: dryRun}
	for uuid, to := range declared {
		if from := strings.Join(current[uuid], ","); from == strings.Join(to, ",") {
			result.Unchanged++
		} else {
			result.Changes = append(result.Changes, MachineChange{UUID: uuid, From: from, To: strings.Join(to, ",")})
		}
	}
	for uuid, from := range current {
		if _, kept := declared[uuid]; !kept {
			result.Changes = append(result.Changes, MachineChange{UUID: uuid, From: strings.Join(from, ",")})
		}
	}
	sort.Slice(result.Changes, func(i, j int) bool { return result.Changes[i].UUID < result.Changes[j].UUID })
	var leased []string
	for _, change := range result.Changes {
		// a live lease on the GPU's row of a node it would no longer be declared for
		var holders int
		if err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM resource_instances r JOIN lease_items li ON li.resource_id=r.id JOIN leases l ON l.id=li.lease_id
			WHERE r.kind='gpu' AND r.stable_identity=? AND instr(','||?||',',','||r.node_id||',')=0 AND l.state IN('reserved','prepared','active','releasing','stale','revocation_requested')`, change.UUID, change.To).Scan(&holders); err != nil {
			return MachineApplyResult{}, err
		}
		if holders != 0 {
			leased = append(leased, change.UUID)
		}
	}
	if len(leased) != 0 {
		return MachineApplyResult{}, fmt.Errorf("%w: %s (wait until the work on it has ended)", ErrMachineConflict, strings.Join(leased, ", "))
	}
	if result.HostChanges, result.UnchangedHosts, err = hostChangesTx(ctx, tx, spec); err != nil {
		return MachineApplyResult{}, err
	}
	policies := spec.DevicePolicies()
	if result.ClearedSettings, err = uncontrollableSettingsTx(ctx, tx, policies); err != nil {
		return MachineApplyResult{}, err
	}
	if dryRun || len(result.Changes) == 0 && len(result.HostChanges) == 0 {
		return result, nil
	}
	t := now()
	for _, change := range result.Changes {
		if _, err = tx.ExecContext(ctx, `DELETE FROM machine_gpus WHERE uuid=? AND instr(','||?||',',','||node_id||',')=0`, change.UUID, change.To); err != nil {
			return MachineApplyResult{}, err
		}
		for _, to := range declared[change.UUID] {
			if _, err = tx.ExecContext(ctx, `INSERT INTO machine_gpus(uuid,node_id,declared_at) VALUES(?,?,?) ON CONFLICT(uuid,node_id) DO NOTHING`, change.UUID, to, t); err != nil {
				return MachineApplyResult{}, err
			}
		}
		if _, err = tx.ExecContext(ctx, `UPDATE external_claims SET cleared_at=? WHERE cleared_at IS NULL AND resource_id IN(SELECT id FROM resource_instances WHERE kind='gpu' AND stable_identity=? AND instr(','||?||',',','||node_id||',')=0)`, t, change.UUID, change.To); err != nil {
			return MachineApplyResult{}, err
		}
		if err = appendCoordinationEventTx(ctx, tx, "gpu_declaration_changed", nil, "gpu", change.UUID, nil, change); err != nil {
			return MachineApplyResult{}, err
		}
	}
	if err = applyHostsTx(ctx, tx, result.HostChanges, t); err != nil {
		return MachineApplyResult{}, err
	}
	for _, setting := range result.ClearedSettings {
		if err = deleteDeviceSettingTx(ctx, tx, setting, "declaration"); err != nil {
			return MachineApplyResult{}, err
		}
	}
	if err = pruneDeviceStateTx(ctx, tx, policies); err != nil {
		return MachineApplyResult{}, err
	}
	return result, tx.Commit()
}

// ListMachines is the machine declaration with what the providers report.
func (s *Store) ListMachines(ctx context.Context) (Machines, error) {
	out := Machines{GPUs: []DeclaredGPU{}, Undeclared: []UndeclaredGPU{}}
	spec, err := declaredSpec(ctx, s.db)
	if err != nil {
		return out, err
	}
	out.Hosts = append([]machine.Host{}, spec.Hosts...)
	out.Devices = append([]machine.DevicePolicy{}, spec.DevicePolicies()...)
	// each declaration's row: the one on its node, else one still on a node the GPU is no longer declared for
	rows, err := s.db.QueryContext(ctx, `SELECT d.uuid,d.node_id,d.declared_at,r.id,r.node_id,
		(SELECT valid_until FROM resource_observations o WHERE o.resource_id=r.id ORDER BY id DESC LIMIT 1)
		FROM machine_gpus d LEFT JOIN resource_instances r ON r.id=COALESCE(
			(SELECT own.id FROM resource_instances own WHERE own.kind='gpu' AND own.stable_identity=d.uuid AND own.node_id=d.node_id ORDER BY own.id LIMIT 1),
			(SELECT old.id FROM resource_instances old WHERE old.kind='gpu' AND old.stable_identity=d.uuid AND NOT EXISTS(
				SELECT 1 FROM machine_gpus kept WHERE kept.uuid=d.uuid AND kept.node_id=old.node_id) ORDER BY old.id LIMIT 1))
		ORDER BY d.node_id,d.uuid`)
	if err != nil {
		return out, err
	}
	stamp := time.Now().UTC()
	for rows.Next() {
		var gpu DeclaredGPU
		var resourceNode, valid sql.NullString
		if err = rows.Scan(&gpu.UUID, &gpu.NodeID, &gpu.DeclaredAt, &gpu.ResourceID, &resourceNode, &valid); err != nil {
			rows.Close()
			return out, err
		}
		switch {
		case gpu.ResourceID == nil:
			gpu.State = "unseen"
		case resourceNode.String != gpu.NodeID:
			gpu.State = "pending"
		default:
			gpu.State = "stale"
			if deadline, e := time.Parse(time.RFC3339Nano, valid.String); valid.Valid && e == nil && deadline.After(stamp) {
				gpu.State = "observed"
			}
		}
		out.GPUs = append(out.GPUs, gpu)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return out, err
	}
	rows, err = s.db.QueryContext(ctx, `SELECT u.uuid,u.node_id,u.provider_id,
		(SELECT group_concat(node_id) FROM (SELECT d.node_id FROM machine_gpus d WHERE d.uuid=u.uuid ORDER BY d.node_id)),
		u.first_observed_at,u.last_observed_at
		FROM undeclared_gpus u ORDER BY u.node_id,u.uuid`)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var gpu UndeclaredGPU
		if err = rows.Scan(&gpu.UUID, &gpu.NodeID, &gpu.ProviderID, &gpu.DeclaredNodeID, &gpu.FirstObservedAt, &gpu.LastObservedAt); err != nil {
			return out, err
		}
		out.Undeclared = append(out.Undeclared, gpu)
	}
	return out, rows.Err()
}

// registerGPUTx decides whether a provider of node nodeID may register the
// GPU it reports, and under which resource id. A GPU declared for another
// node, or for none, is not registered (false). The GPU's row on this node is
// kept (the provider's own id, or the alias a shared GPU got here). Without
// one: the provider's id where no node has it, an alias (`<id>@<node>`) where
// another node it is still declared for has it (a shared GPU), else that row is
// taken over (the GPU moved), unless a live lease holds it.
func registerGPUTx(ctx context.Context, tx *sql.Tx, resource ResourceInstance, nodeID, t string) (string, bool, error) {
	var declared int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM machine_gpus WHERE uuid=? AND node_id=?`, resource.StableIdentity, nodeID).Scan(&declared); err != nil {
		return "", false, err
	}
	if declared == 0 {
		return "", false, nil
	}
	takeOver := func(id, fromNode string) error {
		var holders int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM lease_items li JOIN leases l ON l.id=li.lease_id WHERE li.resource_id=? AND l.state IN('reserved','prepared','active','releasing','stale','revocation_requested')`, id).Scan(&holders); err != nil {
			return err
		}
		if holders != 0 {
			return fmt.Errorf("%w: gpu %s is declared for node %s but still leased on node %s", ErrOwnershipConflict, id, nodeID, fromNode)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE resource_instances SET node_id=?,provider_id=?,updated_at=? WHERE id=?`, resource.NodeID, resource.ProviderID, t, id); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE external_claims SET cleared_at=? WHERE resource_id=? AND cleared_at IS NULL`, t, id); err != nil {
			return err
		}
		return appendCoordinationEventTx(ctx, tx, "gpu_ownership_transferred", nil, "resource", id, nil, map[string]string{"uuid": resource.StableIdentity, "from_node": fromNode, "to_node": resource.NodeID, "provider_id": resource.ProviderID})
	}
	// this node's row of the GPU (the provider's own id first)
	var ownID, ownProvider string
	err := tx.QueryRowContext(ctx, `SELECT id,provider_id FROM resource_instances WHERE kind='gpu' AND stable_identity=? AND node_id=? ORDER BY id<>? LIMIT 1`, resource.StableIdentity, nodeID, resource.ID).Scan(&ownID, &ownProvider)
	if err == nil {
		if ownProvider == resource.ProviderID {
			return ownID, true, nil
		}
		return ownID, true, takeOver(ownID, nodeID) // (another provider of this node registered it)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return "", false, err
	}
	var fromNode string
	err = tx.QueryRowContext(ctx, `SELECT node_id FROM resource_instances WHERE id=?`, resource.ID).Scan(&fromNode)
	if errors.Is(err, sql.ErrNoRows) {
		return resource.ID, true, nil
	}
	if err != nil {
		return "", false, err
	}
	var shared int
	if err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM machine_gpus WHERE uuid=? AND node_id=?`, resource.StableIdentity, fromNode).Scan(&shared); err != nil {
		return "", false, err
	}
	if shared != 0 {
		return resource.ID + "@" + nodeID, true, nil
	}
	return resource.ID, true, takeOver(resource.ID, fromNode)
}

// recordUndeclaredGPUsTx makes undeclared_gpus hold exactly the GPUs this
// provider's poll reported but may not register.
func recordUndeclaredGPUsTx(ctx context.Context, tx *sql.Tx, providerID, nodeID string, uuids []string, t string) error {
	keep := map[string]bool{}
	for _, uuid := range uuids {
		keep[uuid] = true
		if _, err := tx.ExecContext(ctx, `INSERT INTO undeclared_gpus(provider_id,uuid,node_id,first_observed_at,last_observed_at) VALUES(?,?,?,?,?) ON CONFLICT(provider_id,uuid) DO UPDATE SET node_id=excluded.node_id,last_observed_at=excluded.last_observed_at`, providerID, uuid, nodeID, t, t); err != nil {
			return err
		}
	}
	rows, err := tx.QueryContext(ctx, `SELECT uuid FROM undeclared_gpus WHERE provider_id=?`, providerID)
	if err != nil {
		return err
	}
	var gone []string
	for rows.Next() {
		var uuid string
		if err = rows.Scan(&uuid); err != nil {
			rows.Close()
			return err
		}
		if !keep[uuid] {
			gone = append(gone, uuid)
		}
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return err
	}
	for _, uuid := range gone {
		if _, err = tx.ExecContext(ctx, `DELETE FROM undeclared_gpus WHERE provider_id=? AND uuid=?`, providerID, uuid); err != nil {
			return err
		}
	}
	return nil
}

// declaredGPUFilter limits a query over resource_instances ri to GPUs
// declared for the node they are registered on.
const declaredGPUFilter = `EXISTS(SELECT 1 FROM machine_gpus d WHERE d.uuid=ri.stable_identity AND d.node_id=ri.node_id)`

// sameDevice is the SQL set of resource ids that are the device the resource id
// expression x names: every node's row of a shared GPU (its stable identity),
// else x alone. A lease or a claim on any of them is one on that device.
func sameDevice(x string) string {
	return `(SELECT sib.id FROM resource_instances sib JOIN resource_instances self ON self.id=` + x + ` AND self.kind='gpu' WHERE sib.kind='gpu' AND sib.stable_identity=self.stable_identity UNION SELECT ` + x + `)`
}
