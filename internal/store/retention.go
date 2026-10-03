package store

import (
	"context"
	"time"
)

// Resource observations describe the present: every reader (admission,
// preemption, quiescence, lease reconciliation, resource status) uses only
// the latest observation of each resource. Older rows are pruned so the
// database does not grow by one row per resource and provider poll forever.

// ObservationRetention is how long observations other than the latest of a
// resource are kept (for looking back at a recent incident).
var ObservationRetention = time.Hour

// pruneBatch bounds one delete so the single database connection is never
// held for long while a large backlog is removed.
const pruneBatch = 2000

// PruneObservations deletes observations older than olderThan, always keeping
// each resource's latest one. It deletes in small batches and returns how many
// rows it removed.
func (s *Store) PruneObservations(ctx context.Context, olderThan time.Duration) (int, error) {
	cutoff := time.Now().UTC().Add(-olderThan).Format(time.RFC3339Nano)
	removed := 0
	for {
		result, err := s.db.ExecContext(ctx, `DELETE FROM resource_observations WHERE id IN (
		 SELECT o.id FROM resource_observations o
		 WHERE julianday(o.observed_at) < julianday(?)
		 AND o.id < (SELECT MAX(x.id) FROM resource_observations x WHERE x.resource_id=o.resource_id)
		 LIMIT ?)`, cutoff, pruneBatch)
		if err != nil {
			return removed, err
		}
		n, err := result.RowsAffected()
		if err != nil {
			return removed, err
		}
		removed += int(n)
		if n < pruneBatch {
			return removed, nil
		}
		// Let the reconcilers and executors use the connection in between.
		select {
		case <-ctx.Done():
			return removed, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// Compact prunes observations and rewrites the database file to release the
// space deleted rows left behind. VACUUM needs the database to itself: run it
// with the daemon stopped (it fails with "database is locked" otherwise).
func (s *Store) Compact(ctx context.Context) (int, error) {
	removed, err := s.PruneObservations(ctx, ObservationRetention)
	if err != nil {
		return removed, err
	}
	_, err = s.db.ExecContext(ctx, `VACUUM`)
	return removed, err
}
