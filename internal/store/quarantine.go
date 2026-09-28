package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// AllNodes quarantines every node at once.
const AllNodes = "*"

// NodeQuarantine is one quarantine together with what it is still waiting on.
type NodeQuarantine struct {
	NodeID    string `json:"node_id"`
	Actor     string `json:"actor"`
	Reason    string `json:"reason"`
	CreatedAt string `json:"created_at"`
	// Attempts still alive on the covered nodes: the quarantine has taken
	// full effect once this is zero.
	LiveAttempts        int `json:"live_attempts"`
	PendingTerminations int `json:"pending_terminations"`
}

// QuarantineTermination is a termination an executor must carry out: the
// attempt's launcher process (and its descendants) on that executor's host.
type QuarantineTermination struct {
	AttemptID       string  `json:"attempt_id"`
	ExecutionID     string  `json:"execution_id"`
	PID             *int    `json:"pid"`
	ProcessIdentity *string `json:"process_identity"`
}

// QuarantineNode stops all work on nodeID (AllNodes for every node) until
// ReleaseNodeQuarantine. Idempotent: a second call keeps the first record.
func (s *Store) QuarantineNode(ctx context.Context, nodeID, actor, reason string) (NodeQuarantine, error) {
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" {
		return NodeQuarantine{}, errors.New("node id is required")
	}
	if strings.TrimSpace(actor) == "" {
		actor = "operator"
	}
	if strings.TrimSpace(reason) == "" {
		reason = "operator request"
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return NodeQuarantine{}, err
	}
	defer tx.Rollback()
	if nodeID != AllNodes {
		var exists int
		if err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM nodes WHERE id=?`, nodeID).Scan(&exists); err != nil {
			return NodeQuarantine{}, err
		}
		if exists == 0 {
			return NodeQuarantine{}, ErrNotFound
		}
	}
	t := now()
	res, err := tx.ExecContext(ctx, `INSERT INTO node_quarantines(node_id,actor,reason,created_at) VALUES(?,?,?,?) ON CONFLICT(node_id) DO NOTHING`, nodeID, actor, reason, t)
	if err != nil {
		return NodeQuarantine{}, err
	}
	if n, _ := res.RowsAffected(); n == 1 {
		if err = appendCoordinationEventTx(ctx, tx, "node_quarantined", nil, "node", nodeID, nil, map[string]any{"actor": actor, "reason": reason}); err != nil {
			return NodeQuarantine{}, err
		}
	}
	if err = tx.Commit(); err != nil {
		return NodeQuarantine{}, err
	}
	return s.getNodeQuarantine(ctx, nodeID)
}

// ReleaseNodeQuarantine lets nodeID (AllNodes: the all-node quarantine) run work again.
func (s *Store) ReleaseNodeQuarantine(ctx context.Context, nodeID, actor string) error {
	if strings.TrimSpace(actor) == "" {
		actor = "operator"
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, `DELETE FROM node_quarantines WHERE node_id=?`, nodeID)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	if err = appendCoordinationEventTx(ctx, tx, "node_quarantine_released", nil, "node", nodeID, nil, map[string]any{"actor": actor}); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) getNodeQuarantine(ctx context.Context, nodeID string) (NodeQuarantine, error) {
	all, err := s.ListNodeQuarantines(ctx)
	if err != nil {
		return NodeQuarantine{}, err
	}
	for _, q := range all {
		if q.NodeID == nodeID {
			return q, nil
		}
	}
	return NodeQuarantine{}, ErrNotFound
}

// ListNodeQuarantines returns the active quarantines and, for each, the
// attempts that are still alive on the nodes it covers.
func (s *Store) ListNodeQuarantines(ctx context.Context) ([]NodeQuarantine, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT node_id,actor,reason,created_at FROM node_quarantines ORDER BY created_at,node_id`)
	if err != nil {
		return nil, err
	}
	var out []NodeQuarantine
	for rows.Next() {
		var q NodeQuarantine
		if err = rows.Scan(&q.NodeID, &q.Actor, &q.Reason, &q.CreatedAt); err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, q)
	}
	if err = rows.Close(); err != nil {
		return nil, err
	}
	for i := range out {
		if err = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM attempts a JOIN executors x ON x.id=a.executor_id
			WHERE a.state IN('authorized','running','quiescing','exited') AND (?='*' OR x.node_id=?)`, out[i].NodeID, out[i].NodeID).Scan(&out[i].LiveAttempts); err != nil {
			return nil, err
		}
		if err = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM quarantine_terminations WHERE state='pending' AND (?='*' OR node_id=?)`, out[i].NodeID, out[i].NodeID).Scan(&out[i].PendingTerminations); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func nodeQuarantinedTx(ctx context.Context, tx *sql.Tx, nodeID string) (bool, error) {
	var n int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM node_quarantines WHERE node_id IN(?,'*')`, nodeID).Scan(&n)
	return n > 0, err
}

// ReconcileNodeQuarantines acts on the attempts running on quarantined nodes:
// a checkpointable attempt gets a suspend command (origin node_quarantine) and
// continues later from its checkpoint; any other attempt gets a termination
// that its executor carries out, after which the task runs again from the
// start (orchestration reason quarantine_restart). New work is kept off the
// node by ReserveNext / EnsurePriorityPreemption.
func (s *Store) ReconcileNodeQuarantines(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, `SELECT a.id,a.execution_id,a.executor_id,x.node_id,e.checkpointable
		FROM attempts a
		JOIN executors x ON x.id=a.executor_id
		JOIN execution_requests e ON e.id=a.execution_id
		JOIN leases l ON l.attempt_id=a.id
		WHERE a.state='running' AND l.state='active'
		AND EXISTS (SELECT 1 FROM node_quarantines q WHERE q.node_id IN(x.node_id,'*'))`)
	if err != nil {
		return err
	}
	type target struct {
		attemptID, executionID, executorID, nodeID string
		checkpointable                             bool
	}
	var targets []target
	for rows.Next() {
		var t target
		if err = rows.Scan(&t.attemptID, &t.executionID, &t.executorID, &t.nodeID, &t.checkpointable); err != nil {
			rows.Close()
			return err
		}
		targets = append(targets, t)
	}
	if err = rows.Close(); err != nil {
		return err
	}
	for _, t := range targets {
		if t.checkpointable {
			if _, err := s.EnqueueSuspend(ctx, t.attemptID, "node_quarantine", "node "+t.nodeID+" quarantined"); err != nil && !errors.Is(err, ErrNotFound) {
				return fmt.Errorf("suspend %s: %w", t.attemptID, err)
			}
			continue
		}
		if err := s.requestQuarantineTermination(ctx, t.attemptID, t.executionID, t.executorID, t.nodeID); err != nil {
			return fmt.Errorf("terminate %s: %w", t.attemptID, err)
		}
	}
	return nil
}

func (s *Store) requestQuarantineTermination(ctx context.Context, attemptID, executionID, executorID, nodeID string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, `INSERT INTO quarantine_terminations(attempt_id,execution_id,executor_id,node_id,state,requested_at) VALUES(?,?,?,?,'pending',?) ON CONFLICT(attempt_id) DO NOTHING`, attemptID, executionID, executorID, nodeID, now())
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 1 {
		projectID, err := executionProjectIDTx(ctx, tx, executionID)
		if err != nil {
			return err
		}
		if err = appendCoordinationEventTx(ctx, tx, "quarantine_termination_requested", &projectID, "attempt", attemptID, nil, map[string]any{"execution_id": executionID, "executor_id": executorID, "node_id": nodeID}); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// ListQuarantineTerminations returns the terminations executorID still has to
// carry out, with the attempt's launcher process.
func (s *Store) ListQuarantineTerminations(ctx context.Context, executorID string) ([]QuarantineTermination, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT t.attempt_id,t.execution_id,a.pid,a.process_identity FROM quarantine_terminations t JOIN attempts a ON a.id=t.attempt_id WHERE t.executor_id=? AND t.state='pending' ORDER BY t.requested_at`, executorID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []QuarantineTermination
	for rows.Next() {
		var t QuarantineTermination
		var pid sql.NullInt64
		var identity sql.NullString
		if err = rows.Scan(&t.AttemptID, &t.ExecutionID, &pid, &identity); err != nil {
			return nil, err
		}
		if pid.Valid {
			v := int(pid.Int64)
			t.PID = &v
		}
		if identity.Valid {
			t.ProcessIdentity = &identity.String
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// MarkQuarantineTerminated records that the executor terminated the attempt's
// processes (or found them already gone). Quiescence itself is still proven by
// the executor's process-absence check before the lease is released.
func (s *Store) MarkQuarantineTerminated(ctx context.Context, attemptID string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var executionID string
	if err = tx.QueryRowContext(ctx, `SELECT execution_id FROM quarantine_terminations WHERE attempt_id=?`, attemptID).Scan(&executionID); errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	} else if err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx, `UPDATE quarantine_terminations SET state='terminated',terminated_at=? WHERE attempt_id=? AND state='pending'`, now(), attemptID)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 1 {
		projectID, err := executionProjectIDTx(ctx, tx, executionID)
		if err != nil {
			return err
		}
		if err = appendCoordinationEventTx(ctx, tx, "quarantine_terminated", &projectID, "attempt", attemptID, nil, map[string]any{"execution_id": executionID}); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func quarantineTerminatedTx(ctx context.Context, tx *sql.Tx, attemptID string) (bool, error) {
	var n int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM quarantine_terminations WHERE attempt_id=? AND state='terminated'`, attemptID).Scan(&n)
	return n > 0, err
}
