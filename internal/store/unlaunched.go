package store

import (
	"context"
	"database/sql"
	"time"
)

// UnlaunchedLease is a lease of an executor that never got as far as a
// running attempt: reserved, prepared or revoked, without an attempt or with
// one whose launch was authorized but never activated.
type UnlaunchedLease struct {
	Lease Lease `json:"lease"`
	// AttemptID is the authorized attempt, if any.
	AttemptID string             `json:"attempt_id,omitempty"`
	Resources []ResourceInstance `json:"resources"`
}

// ListUnlaunchedLeases returns executorID's unlaunched leases created more
// than olderThan ago, except gang rank leases not delivered to it yet. An
// executor works through its reservations one at a time within a tick, so
// between ticks any such lease it does not hold in memory is one whose
// answer it never received (a ReserveNext or AuthorizeLaunch response lost
// on the way) or whose cleanup it gave up on: it releases them, and nothing
// else ever would short of an executor restart.
func (s *Store) ListUnlaunchedLeases(ctx context.Context, executorID string, olderThan time.Duration) ([]UnlaunchedLease, error) {
	cutoff := time.Now().UTC().Add(-olderThan).Format(time.RFC3339Nano)
	rows, err := s.db.QueryContext(ctx, `SELECT l.id,l.execution_id,l.executor_id,l.coordination_epoch,l.state,l.created_at,l.expires_at,COALESCE(l.attempt_id,'')
		FROM leases l LEFT JOIN attempts a ON a.id=l.attempt_id
		WHERE l.executor_id=$1 AND l.state IN('reserved','prepared','revocation_requested')
		AND (l.attempt_id IS NULL OR a.state='authorized')
		AND (l.created_at)::timestamptz<($2)::timestamptz
		AND NOT EXISTS(SELECT 1 FROM gang_placement_leases gpl WHERE gpl.lease_id=l.id AND gpl.delivered_at IS NULL)
		ORDER BY l.created_at,l.id`, executorID, cutoff)
	if err != nil {
		return nil, err
	}
	var out []UnlaunchedLease
	for rows.Next() {
		var u UnlaunchedLease
		var expires sql.NullString
		if err = rows.Scan(&u.Lease.ID, &u.Lease.ExecutionID, &u.Lease.ExecutorID, &u.Lease.CoordinationEpoch, &u.Lease.State, &u.Lease.CreatedAt, &expires, &u.AttemptID); err != nil {
			rows.Close()
			return nil, err
		}
		if expires.Valid {
			u.Lease.ExpiresAt = &expires.String
		}
		if u.AttemptID != "" {
			attemptID := u.AttemptID
			u.Lease.AttemptID = &attemptID
		}
		out = append(out, u)
	}
	if err = rows.Close(); err != nil {
		return nil, err
	}
	for i := range out {
		if out[i].Resources, err = s.listLeaseResources(ctx, out[i].Lease.ID); err != nil {
			return nil, err
		}
	}
	return out, nil
}
