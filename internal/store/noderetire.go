package store

import (
	"context"
	"database/sql"
	"fmt"
)

// RetireNode: a node that is gone for good (its container deleted, its host
// decommissioned), as the operator confirms (kairo node retire NODE
// --confirm-node-gone). Its executors are fenced as FenceNode fences them (it
// is refused while one has polled within the live window: a node that
// answers is not gone), then every stale lease of its executors is released
// as ReconcileResource releases one, without the fresh observations and the
// process-absence confirmation ReconcileResource waits for: nothing is left
// there to observe or to run. Its lost attempts quiesce, its executors and
// the node are disabled (nothing is placed there again unless an agent
// registers it anew), and event node_retired says what was released. The
// node's stores stay declared until the machine declaration drops them.
func (s *Store) RetireNode(ctx context.Context, nodeID, actor, reason string) (int, error) {
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	if _, err := fenceNodeTx(ctx, tx, nodeID, actor, reason); err != nil {
		return 0, err
	}
	type stale struct {
		lease, executionID string
		attempt            sql.NullString
		epoch              int64
	}
	var leases []stale
	err = eachRow(ctx, tx, `SELECT l.id,l.attempt_id,l.coordination_epoch,l.execution_id FROM leases l JOIN executors x ON x.id=l.executor_id
		WHERE x.node_id=$1 AND l.state='stale' ORDER BY l.id`, []any{nodeID}, func(rows *sql.Rows) error {
		var x stale
		if err := rows.Scan(&x.lease, &x.attempt, &x.epoch, &x.executionID); err != nil {
			return err
		}
		leases = append(leases, x)
		return nil
	})
	if err != nil {
		return 0, err
	}
	t := now()
	for _, x := range leases {
		if x.attempt.Valid {
			if _, err = tx.ExecContext(ctx, `UPDATE attempt_processes SET exited_at=$1,last_seen_at=$2 WHERE attempt_id=$3 AND exited_at IS NULL`, t, t, x.attempt.String); err != nil {
				return 0, err
			}
		}
		if _, err = tx.ExecContext(ctx, `UPDATE leases SET state='released',released_at=$1 WHERE id=$2 AND state='stale'`, t, x.lease); err != nil {
			return 0, err
		}
		if x.attempt.Valid {
			if _, err = tx.ExecContext(ctx, `UPDATE attempts SET state='quiesced',quiesced_at=$1,exited_at=COALESCE(exited_at,$2) WHERE id=$3 AND state='lost'`, t, t, x.attempt.String); err != nil {
				return 0, err
			}
			if _, err = tx.ExecContext(ctx, `UPDATE pause_targets SET state='quiesced',blocker_reason=NULL,updated_at=$1 WHERE execution_id=$2 AND attempt_id=$3`, t, x.executionID, x.attempt.String); err != nil {
				return 0, err
			}
		}
		var projectID string
		if err = tx.QueryRowContext(ctx, `SELECT project_scope_id FROM execution_requests WHERE id=$1`, x.executionID).Scan(&projectID); err != nil {
			return 0, err
		}
		if err = appendCoordinationEventTx(ctx, tx, "stale_lease_reconciled", &projectID, "lease", x.lease, &x.epoch, map[string]string{"node_id": nodeID, "execution_id": x.executionID, "reason": "node retired"}); err != nil {
			return 0, err
		}
	}
	if err = clearReleasedLeaseSettingsTx(ctx, tx); err != nil {
		return 0, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE executors SET enabled=FALSE,updated_at=$1 WHERE node_id=$2`, t, nodeID); err != nil {
		return 0, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE nodes SET enabled=FALSE,updated_at=$1 WHERE id=$2`, t, nodeID); err != nil {
		return 0, fmt.Errorf("disable node %s: %w", nodeID, err)
	}
	if err = appendCoordinationEventTx(ctx, tx, "node_retired", nil, "node", nodeID, nil, map[string]any{"actor": actor, "reason": reason, "released_leases": len(leases)}); err != nil {
		return 0, err
	}
	return len(leases), tx.Commit()
}
