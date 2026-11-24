package store

import (
	"context"

	"kairo/internal/store/provenance"
)

// Code snapshots and images: the record is store/provenance's. The store
// keeps the names its callers (the API, the CLI, the scheduler and the
// orchestration) use.

type (
	SourceSnapshot = provenance.SourceSnapshot
	LaunchSource   = provenance.LaunchSource
)

// ErrSourceInvalid is a snapshot that does not hold what it says.
var ErrSourceInvalid = provenance.ErrSourceInvalid

// CheckSourceSnapshot checks what a snapshot says of itself.
func CheckSourceSnapshot(snap SourceSnapshot) error { return provenance.CheckSourceSnapshot(snap) }

// RecordSourceSnapshot records a snapshot the daemon now keeps.
func (s *Store) RecordSourceSnapshot(ctx context.Context, snap SourceSnapshot) error {
	return provenance.RecordSourceSnapshot(ctx, s.db, snap)
}

// SourceSnapshotByID is a snapshot with its file list and the tasks bound to
// it; ErrNotFound for none.
func (s *Store) SourceSnapshotByID(ctx context.Context, id string) (SourceSnapshot, error) {
	return provenance.SourceSnapshotByID(ctx, s.db, id)
}

// ListSourceSnapshots lists snapshots newest first (of one project when
// project is not ""), without their file lists.
func (s *Store) ListSourceSnapshots(ctx context.Context, project string, limit int) ([]SourceSnapshot, error) {
	return provenance.ListSourceSnapshots(ctx, s.db, project, listLimit(limit))
}

func launchSourceTx(ctx context.Context, q queryer, executionID string) (*LaunchSource, error) {
	return provenance.LaunchSourceOf(ctx, q, executionID)
}

func taskSourceTx(ctx context.Context, q queryer, taskScopeID string) (string, error) {
	return provenance.TaskSource(ctx, q, taskScopeID)
}

func sourceKnownTx(ctx context.Context, q queryer, id string) (bool, error) {
	return provenance.SourceKnown(ctx, q, id)
}

func taskImageTx(ctx context.Context, q queryer, taskScopeID string) (string, error) {
	return provenance.TaskImage(ctx, q, taskScopeID)
}

func launchImageTx(ctx context.Context, q queryer, executionID string) (string, error) {
	return provenance.LaunchImage(ctx, q, executionID)
}

// SourcesInUse are the snapshots of executions not yet terminal: what runs,
// or is to run, in them. An executor keeps those unpacked
// (executor/source.go); every executor is told all of them, whichever asks,
// since any of those executions may yet run on it.
func (s *Store) SourcesInUse(ctx context.Context, _ string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT source_snapshot_id FROM execution_requests WHERE source_snapshot_id IS NOT NULL AND state<>'terminal' ORDER BY source_snapshot_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}
