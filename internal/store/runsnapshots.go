package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"kairo/internal/id"
	"kairo/internal/machine"
)

// Run snapshots (README: Data stores, run snapshots): whatever a run Kairo
// launched writes is Kairo's to keep from the moment it is written, not only
// once the run has ended. While a run has not ended, its output directory is
// packed again and again, each time only the files changed since the run's
// previous snapshot: a data object of kind run-snapshot, source
// <run id>~<n>, packed (by its executor, from copies of the files still
// being written), copied to the durable stores and replicated like any. One
// is ordered every RunSnapshotInterval of the data policy, and as soon as
// the run publishes a checkpoint (a checkpoint command or a suspend
// acknowledged checkpointed) after its previous snapshot; one at a time per
// run. The directory itself stays the run's: a snapshot never deletes it
// (its pack goes once copied, the source settled kept), and the run's output
// object is packed whole when the run ends.

// ObjectRunSnapshot is the kind of a running run's snapshot.
const ObjectRunSnapshot = "run-snapshot"

// RunOfSnapshot is the run a snapshot's source id (<run>~<n>) names.
func RunOfSnapshot(sourceID string) string {
	if i := strings.LastIndex(sourceID, "~"); i > 0 {
		return sourceID[:i]
	}
	return sourceID
}

// orderRunSnapshotsTx orders the snapshots due of the runs whose output
// directories executorID holds.
func (s *Store) orderRunSnapshotsTx(ctx context.Context, tx *sql.Tx, declared []machine.Store, executorID string) error {
	every := s.data.RunSnapshotInterval
	if every <= 0 {
		return nil
	}
	cutoff := time.Now().UTC().Add(-every)
	type running struct {
		runID, storeID, root, path, nodeID, projectID, created, attemptID string
		last                                                              sql.NullString
		count                                                             int
		busy                                                              bool
	}
	var runs []running
	err := eachRow(ctx, tx, `SELECT o.run_id,o.store_id,o.root,o.path,x.node_id,e.project_scope_id,o.created_at,a.id,
		(SELECT d.created_at FROM data_objects d WHERE d.kind='run-snapshot' AND d.store_id=o.store_id AND left(d.source_id,length(o.run_id)+1)=o.run_id||'~' ORDER BY d.created_at::timestamptz DESC LIMIT 1),
		(SELECT COUNT(*) FROM data_objects d WHERE d.kind='run-snapshot' AND d.store_id=o.store_id AND left(d.source_id,length(o.run_id)+1)=o.run_id||'~'),
		EXISTS(SELECT 1 FROM data_objects d WHERE d.kind='run-snapshot' AND d.store_id=o.store_id AND left(d.source_id,length(o.run_id)+1)=o.run_id||'~' AND d.state='packing')
		FROM run_outputs o JOIN attempts a ON a.id=o.last_attempt_id JOIN execution_requests e ON e.id=a.execution_id JOIN executors x ON x.id=o.executor_id
		WHERE o.executor_id=$1 AND o.state='active'
		AND NOT EXISTS(SELECT 1 FROM data_objects d WHERE d.kind='output' AND d.source_id=o.run_id AND d.store_id=o.store_id)
		ORDER BY o.created_at,o.run_id`, []any{executorID}, func(rows *sql.Rows) error {
		var r running
		if err := rows.Scan(&r.runID, &r.storeID, &r.root, &r.path, &r.nodeID, &r.projectID, &r.created, &r.attemptID, &r.last, &r.count, &r.busy); err != nil {
			return err
		}
		runs = append(runs, r)
		return nil
	})
	if err != nil {
		return err
	}
	t := now()
	for _, r := range runs {
		if r.busy {
			continue
		}
		ended, err := runEndedTx(ctx, tx, r.runID)
		if err != nil {
			return err
		}
		if ended || outputOwnership(declared, r.nodeID, r.storeID, r.root, r.path, r.runID) != "" {
			continue
		}
		since := r.created
		if r.last.Valid {
			since = r.last.String
		}
		sinceTime, err := time.Parse(time.RFC3339Nano, since)
		if err != nil {
			return err
		}
		var state string
		if err = tx.QueryRowContext(ctx, `SELECT state FROM attempts WHERE id=$1`, r.attemptID).Scan(&state); err != nil {
			return err
		}
		var checkpointed bool
		// a checkpoint the run's latest attempt published since
		if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM commands c WHERE c.attempt_id=$1 AND c.state='checkpointed' AND c.updated_at::timestamptz>$2::text::timestamptz)`,
			r.attemptID, since).Scan(&checkpointed); err != nil {
			return err
		}
		periodic := (state == "running" || state == "quiescing") && sinceTime.Before(cutoff)
		if !periodic && !checkpointed {
			continue
		}
		objectID := id.New("obj")
		sourceID := fmt.Sprintf("%s~%d", r.runID, r.count+1)
		var sinceAt any
		if r.last.Valid {
			sinceAt = r.last.String
		}
		if err := checkMade(recObject, "packing"); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO data_objects(id,kind,source_id,store_id,executor_id,root,source_path,project_scope_id,state,lifecycle,snapshot_since,name,created_at,updated_at)
			VALUES($1,'run-snapshot',$2,$3,$4,$5,$6,$7,'packing','durable',$8,$9,$10,$11) ON CONFLICT DO NOTHING`,
			objectID, sourceID, r.storeID, executorID, r.root, r.path, r.projectID, sinceAt, "run-snapshots/"+sourceID, t, t); err != nil {
			return err
		}
		reason := "periodic"
		if checkpointed {
			reason = "checkpoint"
		}
		if err = appendCoordinationEventTx(ctx, tx, "pack_ordered", &r.projectID, "object", objectID, nil, map[string]any{"kind": ObjectRunSnapshot, "source_id": sourceID, "store_id": r.storeID, "path": r.path, "since": sinceAt, "reason": reason}); err != nil {
			return err
		}
	}
	return nil
}
