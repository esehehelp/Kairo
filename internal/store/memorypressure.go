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
// most first, one at a time (a suspend of origin memory_pressure): the next
// only once the last has ended and an observation made after its end still
// finds the node under its floor (memoryPressureSettled).

// OriginMemoryPressure is the origin of a suspend for a node's memory
// pressure.
const OriginMemoryPressure = "memory_pressure"

// memoryPressureAfter: a node under its RAM floor for less long gives nothing
// back (a container starting, a short peak).
const memoryPressureAfter = 30 * time.Second

// memoryPressureSettled bounds how far back a suspend for memory pressure is
// looked at: one older has long ended.
const memoryPressureSettled = time.Hour

// ReconcileMemoryPressure suspends, on each node under its RAM floor for
// memoryPressureAfter, the attempt holding the most RAM there that can
// checkpoint, unless one suspended there for it has not yet ended, or ended
// after the node's observation was made (what it gave back is not seen yet).
// It returns the suspends it asked for.
func (s *Store) ReconcileMemoryPressure(ctx context.Context) (int, error) {
	t := time.Now().UTC()
	type pressed struct {
		node, observed string
		free, floor    int64
		since          time.Time
	}
	var nodes []pressed
	err := eachRow(ctx, s.db, `SELECT ri.node_id,o.observed_at,o.free_bytes,o.evidence_json FROM resource_instances ri
		JOIN resource_observations o ON o.id=(SELECT id FROM resource_observations WHERE resource_id=ri.id ORDER BY id DESC LIMIT 1)
		WHERE ri.kind='ram' AND ri.admin_state='enabled' AND o.valid_until::timestamptz>$1::timestamptz AND strpos(o.evidence_json,'under_floor_since')>0
		ORDER BY ri.node_id`, []any{t.Format(time.RFC3339Nano)}, func(rows *sql.Rows) error {
		var node, observed, evidence string
		var free sql.NullInt64
		if err := rows.Scan(&node, &observed, &free, &evidence); err != nil {
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
		nodes = append(nodes, pressed{node: node, observed: observed, free: free.Int64, floor: e.Floor, since: since})
		return nil
	})
	if err != nil {
		return 0, err
	}
	asked := 0
	for _, n := range nodes {
		// the last one's RAM given back and seen: its attempt ended before
		// this observation (a rejected suspend gave nothing back)
		var unsettled bool
		if err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM commands c JOIN attempts a ON a.id=c.attempt_id JOIN executors x ON x.id=a.executor_id
			WHERE c.kind='suspend' AND c.origin=$1 AND c.state<>'rejected' AND x.node_id=$2 AND c.created_at::timestamptz>$3::timestamptz
			AND (a.state IN('authorized','running','quiescing') OR a.exited_at IS NOT NULL AND a.exited_at::timestamptz>=$4::timestamptz))`,
			OriginMemoryPressure, n.node, t.Add(-memoryPressureSettled).Format(time.RFC3339Nano), n.observed).Scan(&unsettled); err != nil {
			return asked, err
		}
		if unsettled {
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
