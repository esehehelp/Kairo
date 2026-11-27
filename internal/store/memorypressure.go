package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Memory pressure (README: Owner use; issue #45). A node whose owner's use
// Kairo gives way to keeps part of its RAM free for its owner: the floor its
// host provider puts with the RAM's observation (evidence floor_bytes).
// Admission leaves the floor free (planner.go). Less than the floor can
// still be available, as work grows or something else takes RAM (the owner,
// a service, a kernel leak); the provider then tells since when (evidence
// under_floor_since). Who took it is not known, so it is no claim of the
// owner's: nothing is held off the node for it beyond what admission
// already leaves free. Once it has gone on for memoryPressureAfter, the work
// on the node that can checkpoint gives its RAM back, the attempt holding the
// most first, one at a time (a suspend of origin memory_pressure), for as
// long as the node stays under its floor.

// OriginMemoryPressure is the origin of a suspend for a node's memory
// pressure.
const OriginMemoryPressure = "memory_pressure"

// memoryPressureAfter: a node under its RAM floor for less long gives nothing
// back (a container starting, a short peak).
const memoryPressureAfter = 30 * time.Second

// ReconcileMemoryPressure suspends, on each node under its RAM floor for
// memoryPressureAfter, the attempt holding the most RAM there that can
// checkpoint, unless a suspend of this origin is already under way there. It
// returns the suspends it asked for.
func (s *Store) ReconcileMemoryPressure(ctx context.Context) (int, error) {
	t := time.Now().UTC()
	type pressed struct {
		node        string
		free, floor int64
		since       time.Time
	}
	var nodes []pressed
	err := eachRow(ctx, s.db, `SELECT ri.node_id,o.free_bytes,o.evidence_json FROM resource_instances ri
		JOIN resource_observations o ON o.id=(SELECT id FROM resource_observations WHERE resource_id=ri.id ORDER BY id DESC LIMIT 1)
		WHERE ri.kind='ram' AND ri.admin_state='enabled' AND o.valid_until::timestamptz>$1::timestamptz AND strpos(o.evidence_json,'under_floor_since')>0
		ORDER BY ri.node_id`, []any{t.Format(time.RFC3339Nano)}, func(rows *sql.Rows) error {
		var node, evidence string
		var free sql.NullInt64
		if err := rows.Scan(&node, &free, &evidence); err != nil {
			return err
		}
		var e struct {
			Floor int64  `json:"floor_bytes"`
			Since string `json:"under_floor_since"`
		}
		if json.Unmarshal([]byte(evidence), &e) != nil {
			return nil
		}
		since, err := time.Parse(time.RFC3339Nano, e.Since)
		if err != nil || t.Sub(since) < memoryPressureAfter {
			return nil
		}
		nodes = append(nodes, pressed{node: node, free: free.Int64, floor: e.Floor, since: since})
		return nil
	})
	if err != nil {
		return 0, err
	}
	asked := 0
	for _, n := range nodes {
		var inFlight bool
		if err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM commands c JOIN attempts a ON a.id=c.attempt_id JOIN executors x ON x.id=a.executor_id
			WHERE c.kind='suspend' AND c.origin=$1 AND c.state IN('pending','accepted','checkpointing') AND x.node_id=$2)`, OriginMemoryPressure, n.node).Scan(&inFlight); err != nil {
			return asked, err
		}
		if inFlight {
			continue
		}
		var attemptID string
		var held int64
		err := s.db.QueryRowContext(ctx, `SELECT a.id,COALESCE(u.ram_bytes,li.quantity) FROM lease_items li JOIN leases l ON l.id=li.lease_id JOIN attempts a ON a.id=l.attempt_id
			JOIN execution_requests e ON e.id=a.execution_id JOIN executors x ON x.id=l.executor_id LEFT JOIN attempt_usage u ON u.attempt_id=a.id
			WHERE x.node_id=$1 AND li.kind='ram' AND `+yieldableAttempt+`
			ORDER BY COALESCE(u.ram_bytes,li.quantity) DESC,a.id LIMIT 1`, n.node).Scan(&attemptID, &held)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return asked, err
		}
		reason := fmt.Sprintf("node %s has had less RAM available than the %s kept free since %s (%s now); this attempt holds %s",
			n.node, formatBytes(n.floor), n.since.Local().Format("15:04:05"), formatBytes(n.free), formatBytes(held))
		if _, err := s.EnqueueSuspend(ctx, attemptID, OriginMemoryPressure, reason); err != nil {
			if errors.Is(err, ErrNotFound) {
				continue
			}
			return asked, fmt.Errorf("suspend %s: %w", attemptID, err)
		}
		asked++
	}
	return asked, nil
}
