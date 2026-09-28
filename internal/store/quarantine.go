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
	// SignalledAt is when the executor first signalled the process tree
	// (nil: not yet); a forced kill follows after a grace period.
	SignalledAt *string `json:"signalled_at"`
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

// ReleaseNodeQuarantine lets nodeID (AllNodes: the all-node quarantine) run work
// again. What the quarantine requested and has not yet reached the attempt is
// withdrawn for every node no longer covered by a quarantine: terminations not
// yet signalled, and node_quarantine suspends not yet delivered. A termination
// already signalled or a suspend already delivered runs its course.
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
	type withdrawn struct{ id, attemptID, executionID string }
	collect := func(query string, args ...any) ([]withdrawn, error) {
		rows, err := tx.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var out []withdrawn
		for rows.Next() {
			var w withdrawn
			if err := rows.Scan(&w.id, &w.attemptID, &w.executionID); err != nil {
				return nil, err
			}
			out = append(out, w)
		}
		return out, rows.Err()
	}
	terminations, err := collect(`DELETE FROM quarantine_terminations WHERE state='pending' AND signalled_at IS NULL
		AND NOT EXISTS(SELECT 1 FROM node_quarantines q WHERE q.node_id IN(quarantine_terminations.node_id,'*'))
		RETURNING attempt_id,attempt_id,execution_id`)
	if err != nil {
		return err
	}
	t := now()
	suspends, err := collect(`UPDATE commands SET state='rejected',payload_json=?,updated_at=?
		WHERE origin='node_quarantine' AND state='pending' AND delivery_count=0
		AND attempt_id IN(SELECT a.id FROM attempts a JOIN executors x ON x.id=a.executor_id
			WHERE NOT EXISTS(SELECT 1 FROM node_quarantines q WHERE q.node_id IN(x.node_id,'*')))
		RETURNING id,attempt_id,execution_id`, `{"withdrawn":"node quarantine released"}`, t)
	if err != nil {
		return err
	}
	for _, w := range terminations {
		projectID, err := executionProjectIDTx(ctx, tx, w.executionID)
		if err != nil {
			return err
		}
		if err = appendCoordinationEventTx(ctx, tx, "quarantine_termination_withdrawn", &projectID, "attempt", w.attemptID, nil, map[string]any{"execution_id": w.executionID, "actor": actor}); err != nil {
			return err
		}
	}
	for _, w := range suspends {
		projectID, err := executionProjectIDTx(ctx, tx, w.executionID)
		if err != nil {
			return err
		}
		if err = appendCoordinationEventTx(ctx, tx, "suspend_withdrawn", &projectID, "command", w.id, nil, map[string]any{"execution_id": w.executionID, "attempt_id": w.attemptID, "origin": "node_quarantine", "actor": actor}); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) getNodeQuarantine(ctx context.Context, nodeID string) (NodeQuarantine, error) {
	q := NodeQuarantine{NodeID: nodeID}
	if err := s.db.QueryRowContext(ctx, `SELECT actor,reason,created_at FROM node_quarantines WHERE node_id=?`, nodeID).Scan(&q.Actor, &q.Reason, &q.CreatedAt); errors.Is(err, sql.ErrNoRows) {
		return NodeQuarantine{}, ErrNotFound
	} else if err != nil {
		return NodeQuarantine{}, err
	}
	return q, s.countQuarantineWork(ctx, &q)
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
		if err = s.countQuarantineWork(ctx, &out[i]); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// countQuarantineWork fills in what q is still waiting on.
func (s *Store) countQuarantineWork(ctx context.Context, q *NodeQuarantine) error {
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM attempts a JOIN executors x ON x.id=a.executor_id
		WHERE a.state IN('authorized','running','quiescing','exited') AND (?='*' OR x.node_id=?)`, q.NodeID, q.NodeID).Scan(&q.LiveAttempts); err != nil {
		return err
	}
	return s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM quarantine_terminations WHERE state='pending' AND (?='*' OR node_id=?)`, q.NodeID, q.NodeID).Scan(&q.PendingTerminations)
}

func nodeQuarantinedTx(ctx context.Context, tx *sql.Tx, nodeID string) (bool, error) {
	var n int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM node_quarantines WHERE node_id IN(?,'*')`, nodeID).Scan(&n)
	return n > 0, err
}

// ReconcileNodeQuarantines acts on the attempts running on quarantined nodes:
// a checkpointable attempt gets a suspend command (origin node_quarantine) and
// continues later from its checkpoint; any other attempt, and a checkpointable
// one that rejected its node_quarantine suspend, gets a termination that its
// executor carries out, after which the task runs again from the continuation
// it started with, if any (orchestration reason quarantine_restart). New work
// is kept off the node by ReserveNext / EnsurePriorityPreemption. An attempt
// that fails is reported and the others are still acted on.
func (s *Store) ReconcileNodeQuarantines(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, `SELECT a.id,a.execution_id,a.executor_id,x.node_id,e.checkpointable,
		EXISTS(SELECT 1 FROM commands c JOIN command_acks k ON k.command_id=c.id
			WHERE c.attempt_id=a.id AND c.origin='node_quarantine' AND k.phase='rejected')
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
		checkpointable, rejected                   bool
	}
	var targets []target
	for rows.Next() {
		var t target
		if err = rows.Scan(&t.attemptID, &t.executionID, &t.executorID, &t.nodeID, &t.checkpointable, &t.rejected); err != nil {
			rows.Close()
			return err
		}
		targets = append(targets, t)
	}
	if err = rows.Close(); err != nil {
		return err
	}
	var errs []error
	for _, t := range targets {
		if t.checkpointable && !t.rejected {
			// The attempt may have moved on since the query: nothing to suspend.
			var notSuspendable notSuspendableError
			if _, err := s.EnqueueSuspend(ctx, t.attemptID, "node_quarantine", "node "+t.nodeID+" quarantined"); err != nil && !errors.Is(err, ErrNotFound) && !errors.As(err, &notSuspendable) {
				errs = append(errs, fmt.Errorf("suspend %s: %w", t.attemptID, err))
			}
			continue
		}
		if err := s.requestQuarantineTermination(ctx, t.attemptID, t.executionID, t.executorID, t.nodeID); err != nil {
			errs = append(errs, fmt.Errorf("terminate %s: %w", t.attemptID, err))
		}
	}
	return errors.Join(errs...)
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
	rows, err := s.db.QueryContext(ctx, `SELECT t.attempt_id,t.execution_id,a.pid,a.process_identity,t.signalled_at FROM quarantine_terminations t JOIN attempts a ON a.id=t.attempt_id WHERE t.executor_id=? AND t.state='pending' ORDER BY t.requested_at`, executorID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []QuarantineTermination
	for rows.Next() {
		var t QuarantineTermination
		var pid sql.NullInt64
		var identity, signalled sql.NullString
		if err = rows.Scan(&t.AttemptID, &t.ExecutionID, &pid, &identity, &signalled); err != nil {
			return nil, err
		}
		if pid.Valid {
			v := int(pid.Int64)
			t.PID = &v
		}
		if identity.Valid {
			t.ProcessIdentity = &identity.String
		}
		if signalled.Valid {
			t.SignalledAt = &signalled.String
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// MarkQuarantineSignalled records, before the executor sends its first signal,
// that the termination is under way: from then on a release no longer
// withdraws it. It returns when the tree was first signalled, and ErrNotFound
// when the termination is no longer pending (withdrawn or already settled), in
// which case the executor must not signal.
func (s *Store) MarkQuarantineSignalled(ctx context.Context, attemptID string) (string, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	var executionID string
	var signalled sql.NullString
	if err = tx.QueryRowContext(ctx, `SELECT execution_id,signalled_at FROM quarantine_terminations WHERE attempt_id=? AND state='pending'`, attemptID).Scan(&executionID, &signalled); errors.Is(err, sql.ErrNoRows) {
		return "", ErrNotFound
	} else if err != nil {
		return "", err
	}
	if signalled.Valid {
		return signalled.String, tx.Commit()
	}
	t := now()
	if _, err = tx.ExecContext(ctx, `UPDATE quarantine_terminations SET signalled_at=? WHERE attempt_id=?`, t, attemptID); err != nil {
		return "", err
	}
	projectID, err := executionProjectIDTx(ctx, tx, executionID)
	if err != nil {
		return "", err
	}
	if err = appendCoordinationEventTx(ctx, tx, "quarantine_termination_signalled", &projectID, "attempt", attemptID, nil, map[string]any{"execution_id": executionID}); err != nil {
		return "", err
	}
	return t, tx.Commit()
}

// MarkQuarantineTerminated records that the executor found the attempt's whole
// process tree gone, whether it signalled it or the tree had already exited.
// Until then the attempt's lease is not released (see FinalizeQuiescence).
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

// quarantineTerminationTx reports whether a node quarantine settled a
// termination of the attempt, and whether the executor had to signal it (as
// opposed to finding it already exited).
func quarantineTerminationTx(ctx context.Context, tx *sql.Tx, attemptID string) (terminated, signalled bool, err error) {
	err = tx.QueryRowContext(ctx, `SELECT state='terminated',signalled_at IS NOT NULL FROM quarantine_terminations WHERE attempt_id=?`, attemptID).Scan(&terminated, &signalled)
	if errors.Is(err, sql.ErrNoRows) {
		return false, false, nil
	}
	return terminated, signalled, err
}
