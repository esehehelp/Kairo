package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

// Owner use (README: Owner use; issue #21). A resource its owner uses is
// Kairo's to give way on, as a workstation gives way to its owner at the
// keyboard: a GPU, or a node's CPU or RAM, taken by something that is not
// Kairo's work. It is a claim of kind owner_use on the resource:
//
//   - observed: the node's providers report it while they see the owner's
//     use (provider/owneruse.go); it is held ownerUseHold after it was last
//     seen, so that Kairo comes back only once the resource has been free
//     for a while (a game in its menu, a build between steps);
//   - manual: kairo node owner-use sets it on every resource of a node, or
//     those named (id owner-manual-<resource>), until kairo node
//     owner-done or the time it was given.
//
// While it holds, nothing is placed on the resource (wait reason
// owner_use; on a node's CPU or RAM, on the node), and while it is in use
// now (manual, or observed within ownerUseNow) the work holding it that can
// checkpoint (checkpointable and preemptible) is suspended (origin
// owner_use): on a GPU every attempt holding it; on a node's CPU or RAM the
// one holding the most of it, one at a time, while the owner's use goes on
// (all of them, for a manual one). The continuation goes elsewhere, or waits
// for the resource (outputhome.go). Work that cannot checkpoint runs on to
// its end (issue #44).

// ClaimOwnerUse is the kind of an owner's use claim.
const ClaimOwnerUse = "owner_use"

// ownerUseHold: an observed owner's use holds this long after it was last
// seen.
const ownerUseHold = 10 * time.Minute

// ownerUseNow: an observed owner's use seen this recently is going on now
// (the providers report every few seconds).
const ownerUseNow = 90 * time.Second

// manualOwnerPrefix begins the id of a claim set by hand.
const manualOwnerPrefix = "owner-manual-"

// ErrOwnerUseRefused: an owner's use naming what the node does not have, or
// a time that is no time to come.
var ErrOwnerUseRefused = errors.New("owner use refused")

// OwnerUseRequest is kairo node owner-use: the node's GPUs (by UUID), CPU
// and RAM its owner uses (none named: all of them), until a time ("" until
// owner-done).
type OwnerUseRequest struct {
	GPUs  []string `json:"gpus,omitempty"`
	CPU   bool     `json:"cpu,omitempty"`
	RAM   bool     `json:"ram,omitempty"`
	Until string   `json:"until,omitempty"`
	Actor string   `json:"actor,omitempty"`
}

// OwnerUse is a resource its owner uses, as set by hand.
type OwnerUse struct {
	ResourceID string `json:"resource_id"`
	Kind       string `json:"kind"`
	Identity   string `json:"identity"`
	Until      string `json:"until,omitempty"`
}

// UseNodeAsOwner records that the owner of node uses the resources the
// request names (owner-manual-* claims; see above).
func (s *Store) UseNodeAsOwner(ctx context.Context, node string, req OwnerUseRequest) ([]OwnerUse, error) {
	if req.Until != "" {
		until, err := time.Parse(time.RFC3339Nano, req.Until)
		if err != nil {
			return nil, fmt.Errorf("%w: until %q is no RFC 3339 time", ErrOwnerUseRefused, req.Until)
		}
		if !until.After(time.Now()) {
			return nil, fmt.Errorf("%w: until %s has passed", ErrOwnerUseRefused, req.Until)
		}
	}
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var exists bool
	if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM nodes WHERE id=$1)`, node).Scan(&exists); err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("%w: node %s", ErrNotFound, node)
	}
	all := len(req.GPUs) == 0 && !req.CPU && !req.RAM
	var uses []OwnerUse
	gpus := map[string]bool{}
	err = eachRow(ctx, tx, `SELECT id,kind,stable_identity FROM resource_instances WHERE node_id=$1 AND kind IN('gpu','cpu','ram') AND admin_state='enabled' ORDER BY kind,id`, []any{node}, func(rows *sql.Rows) error {
		var u OwnerUse
		if err := rows.Scan(&u.ResourceID, &u.Kind, &u.Identity); err != nil {
			return err
		}
		if u.Kind == "gpu" {
			gpus[u.Identity] = true
		}
		if all || u.Kind == "gpu" && slices.Contains(req.GPUs, u.Identity) || u.Kind == "cpu" && req.CPU || u.Kind == "ram" && req.RAM {
			u.Until = req.Until
			uses = append(uses, u)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	for _, g := range req.GPUs {
		if !gpus[g] {
			return nil, fmt.Errorf("%w: node %s reports no GPU %s", ErrOwnerUseRefused, node, g)
		}
	}
	if len(uses) == 0 {
		return nil, fmt.Errorf("%w: node %s reports none of the resources named", ErrOwnerUseRefused, node)
	}
	t := now()
	evidence, err := json.Marshal(map[string]string{"source": "manual", "actor": req.Actor, "until": req.Until})
	if err != nil {
		return nil, err
	}
	for _, u := range uses {
		if _, err = tx.ExecContext(ctx, `INSERT INTO external_claims(id,resource_id,claim_kind,evidence_json,first_observed_at,last_observed_at) VALUES($1,$2,$3,$4,$5,$6)
			ON CONFLICT(id) DO UPDATE SET evidence_json=excluded.evidence_json,last_observed_at=excluded.last_observed_at,
			first_observed_at=CASE WHEN external_claims.cleared_at IS NOT NULL THEN excluded.first_observed_at ELSE external_claims.first_observed_at END,cleared_at=NULL`,
			manualOwnerPrefix+u.ResourceID, u.ResourceID, ClaimOwnerUse, string(evidence), t, t); err != nil {
			return nil, err
		}
	}
	if err = appendCoordinationEventTx(ctx, tx, "owner_use_set", nil, "node", node, nil, map[string]any{"resources": uses, "until": req.Until, "actor": req.Actor}); err != nil {
		return nil, err
	}
	return uses, tx.Commit()
}

// OwnerDone ends what kairo node owner-use set on node; an owner's use its
// providers see goes on holding as observed. It returns how many ended.
func (s *Store) OwnerDone(ctx context.Context, node, actor string) (int, error) {
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE external_claims SET cleared_at=$1 WHERE cleared_at IS NULL AND claim_kind=$2 AND starts_with(id,$3)
		AND resource_id IN(SELECT id FROM resource_instances WHERE node_id=$4)`, now(), ClaimOwnerUse, manualOwnerPrefix, node)
	if err != nil {
		return 0, err
	}
	n, _ := result.RowsAffected()
	if err = appendCoordinationEventTx(ctx, tx, "owner_use_done", nil, "node", node, nil, map[string]any{"ended": n, "actor": actor}); err != nil {
		return 0, err
	}
	return int(n), tx.Commit()
}

// ownerUseClearable reports whether an owner_use claim a provider did not
// report now ends: never one set by hand, an observed one once the hold
// since it was last seen is over.
func ownerUseClearable(id, lastObserved string, now time.Time) bool {
	if strings.HasPrefix(id, manualOwnerPrefix) {
		return false
	}
	last, err := time.Parse(time.RFC3339Nano, lastObserved)
	return err != nil || now.Sub(last) >= ownerUseHold
}

// ReconcileOwnerUse ends the owner's uses that are over (an observed one
// not seen for ownerUseHold, its provider gone quiet; one set by hand past
// its until) and suspends the work that can checkpoint holding a resource
// its owner uses now (see above). It returns the suspends it asked for.
func (s *Store) ReconcileOwnerUse(ctx context.Context) (int, error) {
	t := time.Now().UTC()
	if _, err := s.exec(ctx, `UPDATE external_claims SET cleared_at=$1 WHERE claim_kind=$2 AND cleared_at IS NULL AND (
		starts_with(id,$3) AND COALESCE(evidence_json::jsonb->>'until','')<>'' AND (evidence_json::jsonb->>'until')::timestamptz<$4::timestamptz
		OR NOT starts_with(id,$3) AND last_observed_at::timestamptz<$5::timestamptz)`,
		t.Format(time.RFC3339Nano), ClaimOwnerUse, manualOwnerPrefix, t.Format(time.RFC3339Nano), t.Add(-ownerUseHold).Format(time.RFC3339Nano)); err != nil {
		return 0, err
	}
	type use struct {
		id, resource, kind, node string
		manual                   bool
	}
	var uses []use
	err := eachRow(ctx, s.db, `SELECT c.id,c.resource_id,r.kind,r.node_id FROM external_claims c JOIN resource_instances r ON r.id=c.resource_id
		WHERE c.claim_kind=$1 AND c.cleared_at IS NULL AND (starts_with(c.id,$2) OR c.last_observed_at::timestamptz>=$3::timestamptz) ORDER BY c.first_observed_at,c.id`,
		[]any{ClaimOwnerUse, manualOwnerPrefix, t.Add(-ownerUseNow).Format(time.RFC3339Nano)}, func(rows *sql.Rows) error {
			var u use
			if err := rows.Scan(&u.id, &u.resource, &u.kind, &u.node); err != nil {
				return err
			}
			u.manual = strings.HasPrefix(u.id, manualOwnerPrefix)
			uses = append(uses, u)
			return nil
		})
	if err != nil || len(uses) == 0 {
		return 0, err
	}
	// running work that can checkpoint, with no suspend asked of it yet
	const yieldable = `l.state='active' AND a.state='running' AND e.checkpointable AND e.preemptible
		AND NOT EXISTS(SELECT 1 FROM commands c WHERE c.attempt_id=a.id AND c.kind='suspend' AND c.state<>'rejected')`
	asked := 0
	seen := map[string]bool{}
	for _, u := range uses {
		var targets []string
		switch u.kind {
		case "gpu":
			err = eachRow(ctx, s.db, `SELECT DISTINCT a.id FROM lease_items li JOIN leases l ON l.id=li.lease_id JOIN attempts a ON a.id=l.attempt_id JOIN execution_requests e ON e.id=a.execution_id
				WHERE li.resource_id IN `+sameDevice("$1")+` AND `+yieldable+` ORDER BY a.id`, []any{u.resource}, func(rows *sql.Rows) error {
				var id string
				if err := rows.Scan(&id); err != nil {
					return err
				}
				targets = append(targets, id)
				return nil
			})
		case "cpu", "ram":
			if !u.manual {
				// one at a time while the owner's use goes on
				var inFlight bool
				if err = s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM commands c JOIN attempts a ON a.id=c.attempt_id JOIN executors x ON x.id=a.executor_id
					WHERE c.kind='suspend' AND c.origin=$1 AND c.state IN('pending','accepted','checkpointing') AND x.node_id=$2)`, ClaimOwnerUse, u.node).Scan(&inFlight); err != nil {
					return asked, err
				}
				if inFlight {
					continue
				}
			}
			err = eachRow(ctx, s.db, `SELECT a.id FROM lease_items li JOIN leases l ON l.id=li.lease_id JOIN attempts a ON a.id=l.attempt_id JOIN execution_requests e ON e.id=a.execution_id
				JOIN executors x ON x.id=l.executor_id LEFT JOIN attempt_usage u ON u.attempt_id=a.id
				WHERE x.node_id=$1 AND li.kind=$2 AND `+yieldable+`
				ORDER BY CASE WHEN $2='ram' THEN COALESCE(u.ram_bytes,li.quantity) ELSE li.quantity END DESC,a.id`, []any{u.node, u.kind}, func(rows *sql.Rows) error {
				var id string
				if err := rows.Scan(&id); err != nil {
					return err
				}
				targets = append(targets, id)
				return nil
			})
			if !u.manual && len(targets) > 1 {
				targets = targets[:1]
			}
		}
		if err != nil {
			return asked, err
		}
		for _, attemptID := range targets {
			if seen[attemptID] {
				continue
			}
			seen[attemptID] = true
			reason := fmt.Sprintf("its owner uses %s %s on node %s", u.kind, u.resource, u.node)
			if _, err := s.EnqueueSuspend(ctx, attemptID, ClaimOwnerUse, reason); err != nil {
				if errors.Is(err, ErrNotFound) {
					continue
				}
				return asked, fmt.Errorf("suspend %s: %w", attemptID, err)
			}
			asked++
		}
	}
	return asked, nil
}
