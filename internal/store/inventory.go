package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"kairo/internal/id"
)

func validJSON(value json.RawMessage) string {
	if len(value) == 0 || !json.Valid(value) {
		return "{}"
	}
	return string(value)
}

func (s *Store) UpsertNode(ctx context.Context, n Node) error {
	if n.ID == "" || n.Name == "" || n.OS == "" || n.Architecture == "" {
		return errors.New("node id, name, os, and architecture are required")
	}
	t := now()
	enabled := 0
	if n.Enabled {
		enabled = 1
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO nodes(id,name,os,architecture,attributes_json,enabled,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET name=excluded.name,os=excluded.os,architecture=excluded.architecture,attributes_json=excluded.attributes_json,enabled=excluded.enabled,updated_at=excluded.updated_at`, n.ID, n.Name, n.OS, n.Architecture, validJSON(n.Attributes), enabled, t, t)
	return err
}
func (s *Store) UpsertExecutor(ctx context.Context, e Executor) error {
	if e.ID == "" || e.NodeID == "" || e.Kind == "" {
		return errors.New("executor id, node id, and kind are required")
	}
	t := now()
	enabled := 0
	if e.Enabled {
		enabled = 1
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO executors(id,node_id,kind,attributes_json,enabled,last_seen_at,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET node_id=excluded.node_id,kind=excluded.kind,attributes_json=excluded.attributes_json,enabled=excluded.enabled,last_seen_at=excluded.last_seen_at,updated_at=excluded.updated_at`, e.ID, e.NodeID, e.Kind, validJSON(e.Attributes), enabled, e.LastSeenAt, t, t)
	return err
}
func (s *Store) UpsertProvider(ctx context.Context, providerID, nodeID, kind string, config json.RawMessage) error {
	t := now()
	_, err := s.db.ExecContext(ctx, `INSERT INTO resource_providers(id,node_id,kind,config_json,created_at,updated_at) VALUES(?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET node_id=excluded.node_id,kind=excluded.kind,config_json=excluded.config_json,updated_at=excluded.updated_at`, providerID, nodeID, kind, validJSON(config), t, t)
	return err
}
func (s *Store) UpsertResourceInstance(ctx context.Context, r ResourceInstance) error {
	if r.ID == "" || r.NodeID == "" || r.ProviderID == "" || r.Kind == "" || r.StableIdentity == "" {
		return errors.New("resource identity fields are required")
	}
	if r.AdminState == "" {
		r.AdminState = "enabled"
	}
	t := now()
	_, err := s.db.ExecContext(ctx, `INSERT INTO resource_instances(id,node_id,provider_id,kind,stable_identity,binding_json,attributes_json,admin_state,quarantine_reason,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET node_id=excluded.node_id,provider_id=excluded.provider_id,kind=excluded.kind,stable_identity=excluded.stable_identity,binding_json=excluded.binding_json,attributes_json=excluded.attributes_json,updated_at=excluded.updated_at`, r.ID, r.NodeID, r.ProviderID, r.Kind, r.StableIdentity, validJSON(r.Binding), validJSON(r.Attributes), r.AdminState, r.QuarantineReason, t, t)
	return err
}
func (s *Store) RecordObservation(ctx context.Context, o Observation) error {
	if o.ResourceID == "" || o.ObservedAt == "" || o.ValidUntil == "" {
		return errors.New("resource_id, observed_at, and valid_until are required")
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO resource_observations(resource_id,observed_at,valid_until,total_bytes,free_bytes,utilization,temperature_c,evidence_json) VALUES(?,?,?,?,?,?,?,?)`, o.ResourceID, o.ObservedAt, o.ValidUntil, o.TotalBytes, o.FreeBytes, o.Utilization, o.TemperatureC, validJSON(o.Evidence))
	return err
}

// ApplyObservationBatch persists one provider poll and only clears a known
// claim when the same fresh poll observed that resource without that claim.
func (s *Store) ApplyObservationBatch(ctx context.Context, providerID string, resources []ResourceInstance, observations []Observation, claims []ExternalClaim) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	t := now()
	for _, resource := range resources {
		if resource.ID == "" || resource.NodeID == "" || resource.ProviderID == "" || resource.Kind == "" || resource.StableIdentity == "" {
			return errors.New("resource identity fields are required")
		}
		if resource.ProviderID != providerID {
			return fmt.Errorf("resource %s belongs to provider %s, not observation provider %s", resource.ID, resource.ProviderID, providerID)
		}
		if resource.AdminState == "" {
			resource.AdminState = "enabled"
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO resource_instances(id,node_id,provider_id,kind,stable_identity,binding_json,attributes_json,admin_state,quarantine_reason,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET node_id=excluded.node_id,provider_id=excluded.provider_id,kind=excluded.kind,stable_identity=excluded.stable_identity,binding_json=excluded.binding_json,attributes_json=excluded.attributes_json,updated_at=excluded.updated_at`, resource.ID, resource.NodeID, resource.ProviderID, resource.Kind, resource.StableIdentity, validJSON(resource.Binding), validJSON(resource.Attributes), resource.AdminState, resource.QuarantineReason, t, t)
		if err != nil {
			return err
		}
	}
	for _, observation := range observations {
		if observation.ResourceID == "" || observation.ObservedAt == "" || observation.ValidUntil == "" {
			return errors.New("resource_id, observed_at, and valid_until are required")
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO resource_observations(resource_id,observed_at,valid_until,total_bytes,free_bytes,utilization,temperature_c,evidence_json) VALUES(?,?,?,?,?,?,?,?)`, observation.ResourceID, observation.ObservedAt, observation.ValidUntil, observation.TotalBytes, observation.FreeBytes, observation.Utilization, observation.TemperatureC, validJSON(observation.Evidence))
		if err != nil {
			return err
		}
	}
	seen := map[string]bool{}
	for _, claim := range claims {
		if claim.ID == "" {
			claim.ID = id.New("clm")
		}
		if claim.ResourceID == "" || claim.ClaimKind == "" {
			return errors.New("claim resource and kind are required")
		}
		if claim.ClaimKind == "external_process" && claim.ProcessIdentity != nil {
			var registered int
			if err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM attempt_processes ap JOIN attempts a ON a.id=ap.attempt_id JOIN leases l ON l.attempt_id=a.id JOIN lease_items li ON li.lease_id=l.id WHERE ap.process_identity=? AND ap.exited_at IS NULL AND a.state IN('running','suspend_requested') AND l.state IN('active','releasing') AND li.resource_id=?`, *claim.ProcessIdentity, claim.ResourceID).Scan(&registered); err != nil {
				return err
			}
			if registered > 0 {
				continue
			}
		}
		seen[claim.ID] = true
		if claim.FirstObservedAt == "" {
			claim.FirstObservedAt = t
		}
		if claim.LastObservedAt == "" {
			claim.LastObservedAt = t
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO external_claims(id,resource_id,claim_kind,process_identity,evidence_json,first_observed_at,last_observed_at) VALUES(?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET evidence_json=excluded.evidence_json,last_observed_at=excluded.last_observed_at,cleared_at=NULL`, claim.ID, claim.ResourceID, claim.ClaimKind, claim.ProcessIdentity, validJSON(claim.Evidence), claim.FirstObservedAt, claim.LastObservedAt)
		if err != nil {
			return err
		}
	}
	observed := map[string]bool{}
	for _, o := range observations {
		observed[o.ResourceID] = true
	}
	rows, err := tx.QueryContext(ctx, `SELECT c.id,c.resource_id FROM external_claims c JOIN resource_instances r ON r.id=c.resource_id WHERE r.provider_id=? AND c.cleared_at IS NULL`, providerID)
	if err != nil {
		return err
	}
	type active struct{ id, resource string }
	var activeClaims []active
	for rows.Next() {
		var c active
		if err := rows.Scan(&c.id, &c.resource); err != nil {
			rows.Close()
			return err
		}
		activeClaims = append(activeClaims, c)
	}
	rows.Close()
	for _, claim := range activeClaims {
		if observed[claim.resource] && !seen[claim.id] {
			if _, err := tx.ExecContext(ctx, `UPDATE external_claims SET cleared_at=? WHERE id=? AND cleared_at IS NULL`, t, claim.id); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}
func (s *Store) ObserveClaim(ctx context.Context, c ExternalClaim) error {
	if c.ID == "" {
		c.ID = id.New("clm")
	}
	if c.ResourceID == "" || c.ClaimKind == "" {
		return errors.New("claim resource and kind are required")
	}
	t := now()
	if c.FirstObservedAt == "" {
		c.FirstObservedAt = t
	}
	if c.LastObservedAt == "" {
		c.LastObservedAt = t
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO external_claims(id,resource_id,claim_kind,process_identity,evidence_json,first_observed_at,last_observed_at) VALUES(?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET evidence_json=excluded.evidence_json,last_observed_at=excluded.last_observed_at,cleared_at=NULL`, c.ID, c.ResourceID, c.ClaimKind, c.ProcessIdentity, validJSON(c.Evidence), c.FirstObservedAt, c.LastObservedAt)
	return err
}
func (s *Store) ClearClaim(ctx context.Context, claimID string) error {
	res, err := s.db.ExecContext(ctx, `UPDATE external_claims SET cleared_at=? WHERE id=? AND cleared_at IS NULL`, now(), claimID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}
func (s *Store) SetResourceAdminState(ctx context.Context, resourceID, state, reason string) error {
	if state != "enabled" && state != "disabled" && state != "quarantined" {
		return errors.New("invalid resource admin state")
	}
	res, err := s.db.ExecContext(ctx, `UPDATE resource_instances SET admin_state=?,quarantine_reason=?,updated_at=? WHERE id=?`, state, nullable(reason), now(), resourceID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) ListResourceStatus(ctx context.Context) ([]ResourceInstance, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT r.id,r.node_id,r.provider_id,r.kind,r.stable_identity,r.binding_json,r.attributes_json,r.admin_state,r.quarantine_reason,
	 (SELECT observed_at FROM resource_observations o WHERE o.resource_id=r.id ORDER BY observed_at DESC LIMIT 1),
	 (SELECT valid_until FROM resource_observations o WHERE o.resource_id=r.id ORDER BY observed_at DESC LIMIT 1),
	 EXISTS(SELECT 1 FROM lease_items li JOIN leases l ON l.id=li.lease_id WHERE li.resource_id=r.id AND l.state IN('reserved','prepared','active','releasing','stale','revocation_requested')),
	 EXISTS(SELECT 1 FROM external_claims c WHERE c.resource_id=r.id AND c.cleared_at IS NULL)
	 FROM resource_instances r ORDER BY r.node_id,r.kind,r.id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ResourceInstance
	for rows.Next() {
		var r ResourceInstance
		var binding, attrs string
		var observed, valid sql.NullString
		var leased, claimed bool
		if err := rows.Scan(&r.ID, &r.NodeID, &r.ProviderID, &r.Kind, &r.StableIdentity, &binding, &attrs, &r.AdminState, &r.QuarantineReason, &observed, &valid, &leased, &claimed); err != nil {
			return nil, err
		}
		r.Binding = json.RawMessage(binding)
		r.Attributes = json.RawMessage(attrs)
		r.DerivedState = "unknown"
		if r.AdminState != "enabled" {
			r.DerivedState = r.AdminState
		} else if leased {
			r.DerivedState = "leased"
		} else if claimed {
			r.DerivedState = "conflicted"
		} else if valid.Valid {
			deadline, e := time.Parse(time.RFC3339Nano, valid.String)
			if e == nil && deadline.After(time.Now().UTC()) {
				r.DerivedState = "available"
			} else {
				r.DerivedState = "stale"
			}
		}
		if observed.Valid {
			stamp, e := time.Parse(time.RFC3339Nano, observed.String)
			if e == nil {
				age := time.Since(stamp).Seconds()
				r.ObservationAgeSeconds = &age
			}
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
func (s *Store) ListClaims(ctx context.Context) ([]ExternalClaim, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id,resource_id,claim_kind,process_identity,evidence_json,first_observed_at,last_observed_at,cleared_at FROM external_claims WHERE cleared_at IS NULL ORDER BY first_observed_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ExternalClaim
	for rows.Next() {
		var c ExternalClaim
		var evidence string
		if err := rows.Scan(&c.ID, &c.ResourceID, &c.ClaimKind, &c.ProcessIdentity, &evidence, &c.FirstObservedAt, &c.LastObservedAt, &c.ClearedAt); err != nil {
			return nil, err
		}
		c.Evidence = json.RawMessage(evidence)
		out = append(out, c)
	}
	return out, rows.Err()
}

func validateEpochTx(ctx context.Context, tx *sql.Tx, attemptID, leaseID string, epoch int64) error {
	var attemptEpoch, leaseEpoch int64
	var actualLease string
	err := tx.QueryRowContext(ctx, `SELECT a.coordination_epoch,l.id,l.coordination_epoch FROM attempts a JOIN leases l ON l.attempt_id=a.id WHERE a.id=?`, attemptID).Scan(&attemptEpoch, &actualLease, &leaseEpoch)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if attemptEpoch != epoch || leaseEpoch != epoch || actualLease != leaseID {
		return fmt.Errorf("%w: attempt/lease epoch does not match", ErrStaleEpoch)
	}
	return nil
}

// ReconcileResource releases stale coordination only after fresh provider
// evidence shows every resource in the lease unclaimed and no registered
// process remains. Lease expiry alone is intentionally insufficient.
func (s *Store) ReconcileResource(ctx context.Context, resourceID string, confirmProcessAbsent ...bool) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT DISTINCT l.id,l.attempt_id,l.coordination_epoch FROM leases l JOIN lease_items li ON li.lease_id=l.id WHERE li.resource_id=? AND l.state='stale'`, resourceID)
	if err != nil {
		return err
	}
	type stale struct {
		lease   string
		attempt sql.NullString
		epoch   int64
	}
	var leases []stale
	for rows.Next() {
		var x stale
		if err := rows.Scan(&x.lease, &x.attempt, &x.epoch); err != nil {
			rows.Close()
			return err
		}
		leases = append(leases, x)
	}
	rows.Close()
	if len(leases) == 0 {
		return ErrNotFound
	}
	t := now()
	for _, x := range leases {
		var unsafe int
		if err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM lease_items li LEFT JOIN resource_observations o ON o.id=(SELECT id FROM resource_observations WHERE resource_id=li.resource_id ORDER BY observed_at DESC LIMIT 1) WHERE li.lease_id=? AND li.resource_id IS NOT NULL AND (o.id IS NULL OR o.valid_until<=? OR EXISTS(SELECT 1 FROM external_claims c WHERE c.resource_id=li.resource_id AND c.cleared_at IS NULL))`, x.lease, t).Scan(&unsafe); err != nil {
			return err
		}
		if unsafe > 0 {
			return fmt.Errorf("stale lease %s still lacks fresh unclaimed observations", x.lease)
		}
		if x.attempt.Valid {
			var processes int
			if err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM attempt_processes WHERE attempt_id=? AND exited_at IS NULL`, x.attempt.String).Scan(&processes); err != nil {
				return err
			}
			if processes > 0 {
				if len(confirmProcessAbsent) == 0 || !confirmProcessAbsent[0] {
					return fmt.Errorf("stale attempt %s still has registered processes; explicit absence confirmation is required", x.attempt.String)
				}
				if _, err = tx.ExecContext(ctx, `UPDATE attempt_processes SET exited_at=?,last_seen_at=? WHERE attempt_id=? AND exited_at IS NULL`, t, t, x.attempt.String); err != nil {
					return err
				}
			}
		}
		if _, err = tx.ExecContext(ctx, `UPDATE leases SET state='released',released_at=? WHERE id=? AND state='stale'`, t, x.lease); err != nil {
			return err
		}
		if x.attempt.Valid {
			if _, err = tx.ExecContext(ctx, `UPDATE attempts SET state='quiesced',quiesced_at=?,exited_at=COALESCE(exited_at,?) WHERE id=? AND state='lost'`, t, t, x.attempt.String); err != nil {
				return err
			}
			var taskID sql.NullString
			var revision sql.NullInt64
			if err = tx.QueryRowContext(ctx, `SELECT task_id,task_revision FROM attempts WHERE id=?`, x.attempt.String).Scan(&taskID, &revision); err != nil {
				return err
			}
			if taskID.Valid && revision.Valid {
				retry, delay, err := retryDecision(ctx, tx, taskID.String, int(revision.Int64), "lost_after_reconcile")
				if err != nil {
					return err
				}
				state := "failed"
				var next any
				if retry {
					state = "backoff"
					next = time.Now().UTC().Add(delay).Format(time.RFC3339Nano)
				}
				if _, err = tx.ExecContext(ctx, `UPDATE task_revisions SET scheduling_state=?,next_retry_at=? WHERE task_id=? AND revision=?`, state, next, taskID.String, revision.Int64); err != nil {
					return err
				}
				if _, err = tx.ExecContext(ctx, `UPDATE tasks SET scheduling_state=?,updated_at=? WHERE id=? AND current_revision=?`, state, t, taskID.String, revision.Int64); err != nil {
					return err
				}
			}
			if _, err = tx.ExecContext(ctx, `UPDATE attempts SET failure_counted=1 WHERE id=?`, x.attempt.String); err != nil {
				return err
			}
		}
		if err = appendEvent(ctx, tx, "stale_lease_reconciled", "lease", x.lease, &x.epoch, map[string]string{"resource_id": resourceID}); err != nil {
			return err
		}
	}
	return tx.Commit()
}
