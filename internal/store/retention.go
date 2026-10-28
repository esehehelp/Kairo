package store

import (
	"context"
	"database/sql"
	"time"
)

// Resource observations describe the present: every reader (admission,
// preemption, quiescence, lease reconciliation, resource status) uses only
// the latest observation of each resource. Older rows are pruned so the
// database does not grow by one row per resource and provider poll forever.

// ObservationRetention is how long observations other than the latest of a
// resource are kept (for looking back at a recent incident).
var ObservationRetention = time.Hour

// pruneBatch bounds one delete so the decision lock (every write takes it)
// is never held for long while a large backlog is removed.
const pruneBatch = 2000

// PruneObservations deletes observations older than olderThan, always keeping
// each resource's latest one. It deletes in small batches and returns how many
// rows it removed.
func (s *Store) PruneObservations(ctx context.Context, olderThan time.Duration) (int, error) {
	cutoff := time.Now().UTC().Add(-olderThan).Format(time.RFC3339Nano)
	removed := 0
	for {
		result, err := s.exec(ctx, `DELETE FROM resource_observations WHERE id IN (
		 SELECT o.id FROM resource_observations o
		 WHERE (o.observed_at)::timestamptz < ($1)::timestamptz
		 AND o.id < (SELECT MAX(x.id) FROM resource_observations x WHERE x.resource_id=o.resource_id)
		 LIMIT $2)`, cutoff, pruneBatch)
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

// Device samples are a history (power, clocks and limits over time), read
// for a device's latest state and for looking back. Each provider poll adds a
// row per device it reports - about 5 MB a day per GPU and provider reporting
// it at the default 5 s poll - so they are kept for DeviceSampleRetention.
// The energy attributed to attempts is added up as samples arrive and does
// not need them afterwards.

// DeviceSampleRetention is how long device samples other than the latest of
// a device are kept.
var DeviceSampleRetention = 72 * time.Hour

// PruneDeviceSamples deletes device samples older than olderThan, always
// keeping each device's latest one (the next sample's energy is counted from
// it), device by device in small batches. It returns how many rows it
// removed.
func (s *Store) PruneDeviceSamples(ctx context.Context, olderThan time.Duration) (int, error) {
	cutoff := sampleStamp(time.Now().Add(-olderThan))
	type bound struct{ device, before string }
	var bounds []bound
	err := eachRow(ctx, s.db, `SELECT device,MAX(observed_at) FROM device_samples GROUP BY device`, nil, func(rows *sql.Rows) error {
		var b bound
		if err := rows.Scan(&b.device, &b.before); err != nil {
			return err
		}
		b.before = min(b.before, cutoff)
		bounds = append(bounds, b)
		return nil
	})
	if err != nil {
		return 0, err
	}
	removed := 0
	for _, b := range bounds {
		for {
			result, err := s.exec(ctx, `DELETE FROM device_samples WHERE id IN (SELECT id FROM device_samples WHERE device=$1 AND observed_at<$2 LIMIT $3)`, b.device, b.before, pruneBatch)
			if err != nil {
				return removed, err
			}
			n, err := result.RowsAffected()
			if err != nil {
				return removed, err
			}
			removed += int(n)
			if n < pruneBatch {
				break
			}
			select {
			case <-ctx.Done():
				return removed, ctx.Err()
			case <-time.After(50 * time.Millisecond):
			}
		}
	}
	return removed, nil
}

// Compact prunes observations and device samples and vacuums their tables,
// so the space of the deleted rows is reused and the planner's statistics
// are fresh. It runs alongside the daemon (autovacuum does the same in
// time).
func (s *Store) Compact(ctx context.Context) (int, error) {
	removed, err := s.PruneObservations(ctx, ObservationRetention)
	if err != nil {
		return removed, err
	}
	samples, err := s.PruneDeviceSamples(ctx, DeviceSampleRetention)
	removed += samples
	if err != nil {
		return removed, err
	}
	// VACUUM runs outside a transaction, and changes no row.
	_, err = s.db.ExecContext(ctx, `VACUUM (ANALYZE) resource_observations, device_samples`)
	return removed, err
}
