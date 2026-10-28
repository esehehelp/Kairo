package store

import (
	"context"
	"database/sql"
	"errors"
	"sort"

	"kairo/internal/id"
)

// EnsurePriorityPreemption runs a planning pass (planner.go) that may suspend
// lower-priority cooperative executions (checkpointable and preemptible)
// when, and only when, their leases are what stops a waiting execution from
// fitting on the node it is going to: GPUs, CPU, RAM or disk, a single
// execution or a gang. The fewest such leases are chosen; executors receive
// the attempt-scoped commands through normal polling.
//
// It returns the priority-preemption command IDs the pass relies on. A
// repeated call returns the same live command IDs; it never creates a second
// suspend command for an attempt. This method only coordinates transfer of
// execution rights. It never requeues an execution or interprets its exit.
func (s *Store) EnsurePriorityPreemption(ctx context.Context, executorID string) ([]string, error) {
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if ok, err := callerMayPlanTx(ctx, tx, executorID); err != nil || !ok {
		return nil, err
	}
	m, err := loadPlanModel(ctx, tx, s.policy, executorID)
	if err != nil {
		return nil, err
	}
	candidates, err := loadPlanCandidates(ctx, tx, m)
	if err != nil {
		return nil, err
	}
	p := newPlanner(m, true)
	var commandIDs []string
	err = p.order(candidates, func(c *planCandidate) (bool, error) {
		placement := p.place(c)
		if len(placement.victims) == 0 {
			return false, nil
		}
		victims := make(map[string]preemptionVictim, len(placement.victims))
		for _, l := range placement.victims {
			victims[l.attemptID] = preemptionVictim{executionID: l.executionID, projectID: l.projectID, attemptID: l.attemptID, leaseID: l.id, epoch: l.epoch}
		}
		ids, enqueueErr := enqueuePrioritySuspendsTx(ctx, tx, c.execution, victims)
		commandIDs = append(commandIDs, ids...)
		return false, enqueueErr
	})
	if err != nil {
		return nil, err
	}
	if len(commandIDs) == 0 {
		return nil, nil
	}
	return commandIDs, tx.Commit()
}

type preemptionVictim struct {
	executionID, projectID, attemptID, leaseID string
	epoch                                      int64
}

func enqueuePrioritySuspendsTx(ctx context.Context, tx *sql.Tx, waiting ExecutionRequest, victims map[string]preemptionVictim) ([]string, error) {
	attemptIDs := make([]string, 0, len(victims))
	for attemptID := range victims {
		attemptIDs = append(attemptIDs, attemptID)
	}
	sort.Strings(attemptIDs)
	commandIDs := make([]string, 0, len(attemptIDs))
	for _, attemptID := range attemptIDs {
		victim := victims[attemptID]
		var existingID, existingOrigin, existingState string
		err := tx.QueryRowContext(ctx, `SELECT id,origin,state FROM commands WHERE attempt_id=$1 ORDER BY created_at LIMIT 1`, attemptID).Scan(&existingID, &existingOrigin, &existingState)
		if err == nil {
			if existingOrigin == "priority_preemption" && existingState != "rejected" {
				commandIDs = append(commandIDs, existingID)
			}
			continue
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}
		commandID, stamp := id.New("cmd"), now()
		reason := "higher-priority execution waiting: " + waiting.ID
		if _, err = tx.ExecContext(ctx, `INSERT INTO commands(id,execution_id,attempt_id,lease_id,coordination_epoch,kind,origin,reason,state,created_at,updated_at) VALUES($1,$2,$3,$4,$5,'suspend','priority_preemption',$6,'pending',$7,$8)`, commandID, victim.executionID, victim.attemptID, victim.leaseID, victim.epoch, reason, stamp, stamp); err != nil {
			return nil, err
		}
		if err = appendCoordinationEventTx(ctx, tx, "suspend_requested", &victim.projectID, "command", commandID, &victim.epoch, map[string]any{"execution_id": victim.executionID, "attempt_id": victim.attemptID, "lease_id": victim.leaseID, "origin": "priority_preemption", "reason": reason, "waiting_execution_id": waiting.ID}); err != nil {
			return nil, err
		}
		commandIDs = append(commandIDs, commandID)
	}
	return commandIDs, nil
}
