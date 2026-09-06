package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
)

// ReconcilePauseOperations advances durable scope-pause operations using only
// execution-coordination facts. It never decides whether an execution should
// be retried, resumed, or considered successful.
//
// When actuate is false (observe-only mode), admission remains closed but a
// running execution is reported as a blocker instead of receiving a command.
func (s *Store) ReconcilePauseOperations(ctx context.Context, actuate bool) error {
	operations, err := s.ListPauseOperations(ctx, PauseOperationFilter{Unfinished: true, Limit: 1000})
	if err != nil {
		return err
	}
	for _, operation := range operations {
		if err := s.reconcilePauseOperation(ctx, operation, actuate); err != nil {
			return fmt.Errorf("reconcile pause operation %s: %w", operation.ID, err)
		}
	}
	return nil
}

type pauseTargetFacts struct {
	checkpointable bool
	attemptID      *string
	attemptState   *string
	leaseID        *string
	leaseState     *string
	authorization  *string
	liveProcesses  int
}

func (s *Store) reconcilePauseOperation(ctx context.Context, operation PauseOperation, actuate bool) error {
	// Discover only while this operation still owns the closed-gate generation.
	// Resume increments the generation and opens admission; the old operation
	// continues converging its captured targets but must not sweep up executions
	// admitted after resume.
	scope, err := s.GetScope(ctx, operation.ScopeID)
	if err != nil {
		return err
	}
	var targets []PauseTarget
	if scope.Admission == "closed" && scope.Generation == operation.ScopeGeneration {
		targets, err = s.DiscoverPauseTargets(ctx, operation.ID)
	} else {
		targets, err = s.ListPauseTargets(ctx, operation.ID)
	}
	if err != nil {
		return err
	}
	for _, target := range targets {
		if err := s.reconcilePauseTarget(ctx, operation, target, actuate); err != nil {
			return err
		}
	}

	// Re-read targets because the per-target transactions above may have moved
	// them, or the executor may have finalized quiescence concurrently.
	targets, err = s.ListPauseTargets(ctx, operation.ID)
	if err != nil {
		return err
	}
	state := "quiesced"
	counts := map[string]int{}
	blockers := map[string]int{}
	for _, target := range targets {
		counts[target.State]++
		switch target.State {
		case "blocked":
			state = "blocked"
			if target.BlockerReason != nil {
				blockers[*target.BlockerReason]++
			}
		case "pending", "revoking", "quiescing":
			if state != "blocked" {
				state = "quiescing"
			}
		}
	}
	detail, err := json.Marshal(map[string]any{
		"target_count":  len(targets),
		"target_states": counts,
		"blockers":      blockers,
	})
	if err != nil {
		return err
	}
	return s.SetPauseOperationState(ctx, operation.ID, state, detail)
}

func (s *Store) reconcilePauseTarget(ctx context.Context, operation PauseOperation, target PauseTarget, actuate bool) error {
	facts, err := s.loadPauseTargetFacts(ctx, target)
	if err != nil {
		return err
	}
	update := PauseTargetUpdate{
		AttemptID: target.AttemptID,
		LeaseID:   target.LeaseID,
		CommandID: target.CommandID,
	}
	if facts.attemptID != nil {
		update.AttemptID = facts.attemptID
	}
	if facts.leaseID != nil {
		update.LeaseID = facts.leaseID
	}

	// Waiting requests are not targets: closing the admission gate is sufficient.
	if facts.leaseID == nil || facts.leaseState == nil {
		if facts.attemptState != nil && *facts.attemptState != "quiesced" {
			update.State = "blocked"
			update.BlockerReason = stringPointer("coordination_state_inconsistent")
			return s.setPauseTargetIfChanged(ctx, target, update)
		}
		update.State = "quiesced"
		return s.setPauseTargetIfChanged(ctx, target, update)
	}
	if *facts.leaseState == "released" {
		if facts.attemptState != nil && *facts.attemptState != "quiesced" {
			update.State = "blocked"
			update.BlockerReason = stringPointer("coordination_state_inconsistent")
			return s.setPauseTargetIfChanged(ctx, target, update)
		}
		update.State = "quiesced"
		return s.setPauseTargetIfChanged(ctx, target, update)
	}
	// A project-side rejection is durable coordination evidence. Retrying the
	// same suspend request automatically would reinterpret project policy and can
	// produce an unbounded rejected-command loop.
	if target.BlockerReason != nil && *target.BlockerReason == "checkpoint_rejected" {
		update.State = "blocked"
		update.BlockerReason = target.BlockerReason
		return s.setPauseTargetIfChanged(ctx, target, update)
	}
	// Observe-only mode may close admission, but it must not change any extant
	// lease -- including a reservation that looks safe to revoke from the DB.
	// Provider Prepare may already be in flight outside the transaction.
	if !actuate {
		update.State = "blocked"
		update.BlockerReason = stringPointer("observe_only")
		return s.setPauseTargetIfChanged(ctx, target, update)
	}
	if *facts.leaseState == "stale" || (facts.attemptState != nil && *facts.attemptState == "lost") {
		update.State = "blocked"
		update.BlockerReason = stringPointer("coordination_identity_unknown")
		return s.setPauseTargetIfChanged(ctx, target, update)
	}

	// A reservation or an issued-but-unconsumed launch authorization is purely a
	// coordination right. Revoke it atomically; no physical process is touched.
	if *facts.leaseState == "reserved" || *facts.leaseState == "prepared" || *facts.leaseState == "revocation_requested" {
		if *facts.leaseState == "revocation_requested" && target.State == "revoking" {
			return nil
		}
		revoked, err := s.revokeUnlaunchedPauseTarget(ctx, operation, target)
		if err != nil {
			return err
		}
		if revoked {
			return nil
		}
		// Activation won the race. Re-read on the next pass and use the running
		// execution path; never infer absence after authorization was consumed.
		update.State = "revoking"
		return s.setPauseTargetIfChanged(ctx, target, update)
	}

	if facts.attemptState == nil || facts.attemptID == nil {
		update.State = "blocked"
		update.BlockerReason = stringPointer("coordination_state_inconsistent")
		return s.setPauseTargetIfChanged(ctx, target, update)
	}
	switch *facts.attemptState {
	case "quiesced":
		if *facts.leaseState == "released" {
			update.State = "quiesced"
		} else {
			update.State = "blocked"
			update.BlockerReason = stringPointer("coordination_state_inconsistent")
		}
		return s.setPauseTargetIfChanged(ctx, target, update)
	case "exited", "quiescing":
		update.State = "quiescing"
		return s.setPauseTargetIfChanged(ctx, target, update)
	case "authorized":
		// An issued authorization with no registered process can still be revoked.
		// A consumed authorization is deliberately treated as in-doubt until the
		// activation transaction or executor observation resolves it.
		if facts.authorization == nil || *facts.authorization == "issued" {
			revoked, err := s.revokeUnlaunchedPauseTarget(ctx, operation, target)
			if err != nil {
				return err
			}
			if revoked {
				return nil
			}
		}
		update.State = "blocked"
		update.BlockerReason = stringPointer("launch_activation_in_doubt")
		return s.setPauseTargetIfChanged(ctx, target, update)
	case "running":
		if facts.liveProcesses == 0 {
			update.State = "blocked"
			update.BlockerReason = stringPointer("running_process_identity_missing")
			return s.setPauseTargetIfChanged(ctx, target, update)
		}
		if !facts.checkpointable {
			update.State = "blocked"
			update.BlockerReason = stringPointer("execution_not_checkpointable")
			return s.setPauseTargetIfChanged(ctx, target, update)
		}
		commandID, err := s.EnqueueSuspend(ctx, *facts.attemptID, "scope_pause", "scope "+operation.ScopeID+" admission closed")
		if err != nil {
			return err
		}
		update.CommandID = &commandID
		update.State = "quiescing"
		return s.setPauseTargetIfChanged(ctx, target, update)
	default:
		update.State = "blocked"
		update.BlockerReason = stringPointer("coordination_state_inconsistent")
		return s.setPauseTargetIfChanged(ctx, target, update)
	}
}

func (s *Store) loadPauseTargetFacts(ctx context.Context, target PauseTarget) (pauseTargetFacts, error) {
	var facts pauseTargetFacts
	var checkpointable int
	err := s.db.QueryRowContext(ctx, `SELECT checkpointable FROM execution_requests WHERE id=?`, target.ExecutionID).Scan(&checkpointable)
	if errors.Is(err, sql.ErrNoRows) {
		return facts, ErrNotFound
	}
	if err != nil {
		return facts, err
	}
	facts.checkpointable = checkpointable != 0
	var leaseAttempt sql.NullString
	if target.LeaseID != nil {
		var leaseID, leaseState string
		err = s.db.QueryRowContext(ctx, `SELECT id,state,attempt_id FROM leases WHERE id=? AND execution_id=?`, *target.LeaseID, target.ExecutionID).Scan(&leaseID, &leaseState, &leaseAttempt)
		if errors.Is(err, sql.ErrNoRows) {
			return facts, ErrNotFound
		}
		if err != nil {
			return facts, err
		}
		facts.leaseID, facts.leaseState = stringPointer(leaseID), stringPointer(leaseState)
	}
	attemptID := target.AttemptID
	if attemptID == nil {
		attemptID = nullStringPointer(leaseAttempt)
	}
	if attemptID != nil {
		var attemptState string
		var authorization sql.NullString
		err = s.db.QueryRowContext(ctx, `SELECT a.state,la.state,
			COALESCE((SELECT COUNT(*) FROM attempt_processes ap WHERE ap.attempt_id=a.id AND ap.exited_at IS NULL),0)
			FROM attempts a LEFT JOIN launch_authorizations la ON la.attempt_id=a.id
			WHERE a.id=? AND a.execution_id=?`, *attemptID, target.ExecutionID).Scan(&attemptState, &authorization, &facts.liveProcesses)
		if errors.Is(err, sql.ErrNoRows) {
			return facts, ErrNotFound
		}
		if err != nil {
			return facts, err
		}
		facts.attemptID, facts.attemptState = attemptID, stringPointer(attemptState)
		facts.authorization = nullStringPointer(authorization)
	}
	return facts, nil
}

// revokeUnlaunchedPauseTarget fences a coordination right in one transaction.
// It deliberately does not release the lease or infer process absence: provider
// Prepare may be in flight. The executor must acknowledge cleanup through
// ReleaseReservation before the pause target can become quiesced.
func (s *Store) revokeUnlaunchedPauseTarget(ctx context.Context, operation PauseOperation, target PauseTarget) (bool, error) {
	if target.LeaseID == nil {
		return false, nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var leaseID, leaseState string
	var attemptID, attemptState, authorization sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT l.id,l.state,a.id,a.state,la.state
		FROM leases l
		LEFT JOIN attempts a ON a.id=l.attempt_id
		LEFT JOIN launch_authorizations la ON la.attempt_id=a.id
		WHERE l.id=? AND l.execution_id=? AND l.state!='released'`, *target.LeaseID, target.ExecutionID).Scan(&leaseID, &leaseState, &attemptID, &attemptState, &authorization)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if leaseState != "reserved" && leaseState != "prepared" && leaseState != "revocation_requested" {
		return false, nil
	}
	if authorization.Valid && authorization.String == "consumed" {
		return false, nil
	}
	if attemptID.Valid {
		var live int
		if err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM attempt_processes WHERE attempt_id=? AND exited_at IS NULL`, attemptID.String).Scan(&live); err != nil {
			return false, err
		}
		if live != 0 || (attemptState.Valid && attemptState.String != "authorized") {
			return false, nil
		}
	}
	t := now()
	if _, err = tx.ExecContext(ctx, `UPDATE leases SET state='revocation_requested',revocation_requested_at=COALESCE(revocation_requested_at,?) WHERE id=? AND state IN('reserved','prepared','revocation_requested')`, t, leaseID); err != nil {
		return false, err
	}
	if attemptID.Valid {
		if _, err = tx.ExecContext(ctx, `UPDATE launch_authorizations SET state='revoked',revoked_at=COALESCE(revoked_at,?) WHERE attempt_id=? AND state='issued'`, t, attemptID.String); err != nil {
			return false, err
		}
	}
	var projectID string
	if err = tx.QueryRowContext(ctx, `SELECT project_scope_id FROM execution_requests WHERE id=?`, target.ExecutionID).Scan(&projectID); err != nil {
		return false, err
	}
	if err = appendCoordinationEventTx(ctx, tx, "coordination_revocation_requested", &projectID, "execution", target.ExecutionID, nil, map[string]any{"pause_operation_id": operation.ID, "lease_id": leaseID, "attempt_id": nullStringPointer(attemptID)}); err != nil {
		return false, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE pause_targets SET attempt_id=COALESCE(?,attempt_id),lease_id=?,state='revoking',blocker_reason=NULL,updated_at=? WHERE operation_id=? AND execution_id=?`, nullableNullString(attemptID), leaseID, t, operation.ID, target.ExecutionID); err != nil {
		return false, err
	}
	return true, tx.Commit()
}

func (s *Store) setPauseTargetIfChanged(ctx context.Context, current PauseTarget, update PauseTargetUpdate) error {
	if current.State == update.State && equalOptionalString(current.AttemptID, update.AttemptID) &&
		equalOptionalString(current.LeaseID, update.LeaseID) && equalOptionalString(current.CommandID, update.CommandID) &&
		equalOptionalString(current.BlockerReason, update.BlockerReason) {
		return nil
	}
	return s.SetPauseTarget(ctx, current.OperationID, current.ExecutionID, update)
}

func stringPointer(value string) *string { return &value }

func nullStringPointer(value sql.NullString) *string {
	if !value.Valid {
		return nil
	}
	return stringPointer(value.String)
}

func nullableNullString(value sql.NullString) any {
	if value.Valid {
		return value.String
	}
	return nil
}

func equalOptionalString(left, right *string) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}
