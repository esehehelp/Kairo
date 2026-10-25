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

// MachineChange is one GPU whose declared node changes: From is empty for a
// newly declared GPU, To for one no longer declared.
type MachineChange struct {
	UUID string `json:"uuid"`
	From string `json:"from,omitempty"`
	To   string `json:"to,omitempty"`
}

type MachineApplyResult struct {
	Changes   []MachineChange `json:"changes"`
	Unchanged int             `json:"unchanged"`
	DryRun    bool            `json:"dry_run"`
}

// DeclaredGPU is a declared GPU and what its node reports of it. State is
// "observed" (registered on its node, fresh observation), "stale" (registered
// on its node, no fresh observation), "pending" (still registered on another
// node until its node's provider reports it) or "unseen" (never reported).
type DeclaredGPU struct {
	UUID       string  `json:"uuid"`
	NodeID     string  `json:"node_id"`
	ResourceID *string `json:"resource_id,omitempty"`
	State      string  `json:"state"`
	DeclaredAt string  `json:"declared_at"`
}

// UndeclaredGPU is a GPU a provider reports but may not register: it is
// declared for another node (DeclaredNodeID) or for none.
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
}

// ApplyMachines replaces the machine declaration with spec. It refuses to move
// or drop a GPU held by a live lease. A GPU that moves stays registered on its
// old node, unschedulable, until the new node's provider reports it; the old
// node's open claims on it are cleared, as they describe the old host.
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
	current := map[string]string{}
	rows, err := tx.QueryContext(ctx, `SELECT uuid,node_id FROM machine_gpus`)
	if err != nil {
		return MachineApplyResult{}, err
	}
	for rows.Next() {
		var uuid, nodeID string
		if err = rows.Scan(&uuid, &nodeID); err != nil {
			rows.Close()
			return MachineApplyResult{}, err
		}
		current[uuid] = nodeID
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return MachineApplyResult{}, err
	}
	declared := spec.GPUs()
	result := MachineApplyResult{Changes: []MachineChange{}, DryRun: dryRun}
	for uuid, to := range declared {
		if from := current[uuid]; from == to {
			result.Unchanged++
		} else {
			result.Changes = append(result.Changes, MachineChange{UUID: uuid, From: from, To: to})
		}
	}
	for uuid, from := range current {
		if _, kept := declared[uuid]; !kept {
			result.Changes = append(result.Changes, MachineChange{UUID: uuid, From: from})
		}
	}
	sort.Slice(result.Changes, func(i, j int) bool { return result.Changes[i].UUID < result.Changes[j].UUID })
	var leased []string
	for _, change := range result.Changes {
		var holders int
		if err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM resource_instances r JOIN lease_items li ON li.resource_id=r.id JOIN leases l ON l.id=li.lease_id
			WHERE r.kind='gpu' AND r.stable_identity=? AND r.node_id<>? AND l.state IN('reserved','prepared','active','releasing','stale','revocation_requested')`, change.UUID, change.To).Scan(&holders); err != nil {
			return MachineApplyResult{}, err
		}
		if holders != 0 {
			leased = append(leased, change.UUID)
		}
	}
	if len(leased) != 0 {
		return MachineApplyResult{}, fmt.Errorf("%w: %s (wait until the work on it has ended)", ErrMachineConflict, strings.Join(leased, ", "))
	}
	if dryRun || len(result.Changes) == 0 {
		return result, nil
	}
	t := now()
	for _, change := range result.Changes {
		if change.To == "" {
			_, err = tx.ExecContext(ctx, `DELETE FROM machine_gpus WHERE uuid=?`, change.UUID)
		} else {
			_, err = tx.ExecContext(ctx, `INSERT INTO machine_gpus(uuid,node_id,declared_at) VALUES(?,?,?) ON CONFLICT(uuid) DO UPDATE SET node_id=excluded.node_id,declared_at=excluded.declared_at`, change.UUID, change.To, t)
		}
		if err != nil {
			return MachineApplyResult{}, err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE external_claims SET cleared_at=? WHERE cleared_at IS NULL AND resource_id IN(SELECT id FROM resource_instances WHERE kind='gpu' AND stable_identity=? AND node_id<>?)`, t, change.UUID, change.To); err != nil {
			return MachineApplyResult{}, err
		}
		if err = appendCoordinationEventTx(ctx, tx, "gpu_declaration_changed", nil, "gpu", change.UUID, nil, change); err != nil {
			return MachineApplyResult{}, err
		}
	}
	return result, tx.Commit()
}

// ListMachines is the machine declaration with what the providers report.
func (s *Store) ListMachines(ctx context.Context) (Machines, error) {
	out := Machines{GPUs: []DeclaredGPU{}, Undeclared: []UndeclaredGPU{}}
	rows, err := s.db.QueryContext(ctx, `SELECT d.uuid,d.node_id,d.declared_at,r.id,r.node_id,
		(SELECT valid_until FROM resource_observations o WHERE o.resource_id=r.id ORDER BY id DESC LIMIT 1)
		FROM machine_gpus d LEFT JOIN resource_instances r ON r.kind='gpu' AND r.stable_identity=d.uuid
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
	rows, err = s.db.QueryContext(ctx, `SELECT u.uuid,u.node_id,u.provider_id,d.node_id,u.first_observed_at,u.last_observed_at
		FROM undeclared_gpus u LEFT JOIN machine_gpus d ON d.uuid=u.uuid ORDER BY u.node_id,u.uuid`)
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
// GPU it reports. A GPU declared for another node, or for none, is not
// registered (false). A GPU declared for this node that is still registered
// to another node is taken over, unless a live lease holds it.
func registerGPUTx(ctx context.Context, tx *sql.Tx, resource ResourceInstance, nodeID, t string) (bool, error) {
	var declared string
	err := tx.QueryRowContext(ctx, `SELECT node_id FROM machine_gpus WHERE uuid=?`, resource.StableIdentity).Scan(&declared)
	if errors.Is(err, sql.ErrNoRows) || err == nil && declared != nodeID {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var fromNode, fromProvider string
	err = tx.QueryRowContext(ctx, `SELECT node_id,provider_id FROM resource_instances WHERE id=?`, resource.ID).Scan(&fromNode, &fromProvider)
	if errors.Is(err, sql.ErrNoRows) || err == nil && fromNode == resource.NodeID && fromProvider == resource.ProviderID {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	var holders int
	if err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM lease_items li JOIN leases l ON l.id=li.lease_id WHERE li.resource_id=? AND l.state IN('reserved','prepared','active','releasing','stale','revocation_requested')`, resource.ID).Scan(&holders); err != nil {
		return false, err
	}
	if holders != 0 {
		return false, fmt.Errorf("%w: gpu %s is declared for node %s but still leased on node %s", ErrOwnershipConflict, resource.ID, nodeID, fromNode)
	}
	if _, err = tx.ExecContext(ctx, `UPDATE resource_instances SET node_id=?,provider_id=?,updated_at=? WHERE id=?`, resource.NodeID, resource.ProviderID, t, resource.ID); err != nil {
		return false, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE external_claims SET cleared_at=? WHERE resource_id=? AND cleared_at IS NULL`, t, resource.ID); err != nil {
		return false, err
	}
	return true, appendCoordinationEventTx(ctx, tx, "gpu_ownership_transferred", nil, "resource", resource.ID, nil, map[string]string{"uuid": resource.StableIdentity, "from_node": fromNode, "to_node": resource.NodeID, "provider_id": resource.ProviderID})
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
