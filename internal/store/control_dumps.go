package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// The database's dump of an hourly control snapshot (README: Kairo keeps
// itself) is whole once a day, a base, and the difference from the latest
// base in between (dumpdelta): the base's chunk table is kept here for the
// deltas after it. A base serves only once its run kept it (exit 0, its
// output packed): a delta of a base no run kept could not be rebuilt.

// Dump kinds.
const (
	ControlDumpBase  = "base"
	ControlDumpDelta = "delta"
)

// controlBaseFor: a base serves this long; a delta that is more than
// controlDeltaShare of its base makes the next dump a base.
const (
	controlBaseFor    = 24 * time.Hour
	controlDeltaShare = 0.5
	controlRunKeptSQL = `EXISTS(SELECT 1 FROM attempts a WHERE a.execution_id=%[1]s AND a.exit_code=0)
		AND EXISTS(SELECT 1 FROM data_objects d WHERE d.kind='output' AND d.source_id=%[1]s AND d.dropped_at IS NULL AND d.state IN('packing','packed'))`
)

// DumpBase is a base the next dumps are the difference from.
type DumpBase struct {
	ExecutionID string
	// Task is its run's task (snapshot-<slot>): its output holds the base
	Task      string
	SHA256    string
	Bytes     int64
	Chunks    []byte
	CreatedAt time.Time
}

// CurrentDumpBase is the base the hourly snapshot at now diffs from: the
// latest whose run kept it, younger than controlBaseFor, and whose latest
// delta was no more than controlDeltaShare of it; nil when the dump is to be
// whole.
func (s *Store) CurrentDumpBase(ctx context.Context, now time.Time) (*DumpBase, error) {
	var b DumpBase
	var created string
	err := s.db.QueryRowContext(ctx, `SELECT c.execution_id,COALESCE(t.external_key,''),c.sha256,c.bytes,c.chunks,c.created_at FROM control_dumps c
		JOIN execution_requests e ON e.id=c.execution_id LEFT JOIN coordination_scopes t ON t.id=e.task_scope_id
		WHERE c.kind='base' AND `+fmt.Sprintf(controlRunKeptSQL, "c.execution_id")+`
		ORDER BY c.created_at DESC,c.execution_id DESC LIMIT 1`).Scan(&b.ExecutionID, &b.Task, &b.SHA256, &b.Bytes, &b.Chunks, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if b.CreatedAt, err = time.Parse(time.RFC3339Nano, created); err != nil {
		return nil, err
	}
	if now.Sub(b.CreatedAt) >= controlBaseFor {
		return nil, nil
	}
	var last sql.NullInt64
	if err := s.db.QueryRowContext(ctx, `SELECT new_bytes FROM control_dumps WHERE kind='delta' AND base_execution_id=$1 ORDER BY created_at DESC LIMIT 1`, b.ExecutionID).Scan(&last); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if last.Valid && float64(last.Int64) > controlDeltaShare*float64(b.Bytes) {
		return nil, nil
	}
	return &b, nil
}

// ControlDump is what an hourly snapshot's dump was: a base (with its chunk
// table) or a delta of BaseExecutionID.
type ControlDump struct {
	ExecutionID     string
	Kind            string
	BaseExecutionID string
	SHA256          string
	Bytes, NewBytes int64
	Chunks          []byte
}

// RecordControlDump records the dump handed to the hourly snapshot run
// d.ExecutionID (handed again, the last one counts).
func (s *Store) RecordControlDump(ctx context.Context, d ControlDump) error {
	if d.Kind != ControlDumpBase && d.Kind != ControlDumpDelta || (d.Kind == ControlDumpBase) != (d.BaseExecutionID == "") || (d.Kind == ControlDumpBase) != (d.Chunks != nil) {
		return fmt.Errorf("a control dump that is neither a base nor a delta of one: %s", d.Kind)
	}
	var base, chunks any
	if d.BaseExecutionID != "" {
		base = d.BaseExecutionID
	}
	if d.Chunks != nil {
		chunks = d.Chunks
	}
	_, err := s.exec(ctx, `INSERT INTO control_dumps(execution_id,kind,base_execution_id,sha256,bytes,new_bytes,chunks,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8)
		ON CONFLICT(execution_id) DO UPDATE SET kind=excluded.kind,base_execution_id=excluded.base_execution_id,sha256=excluded.sha256,bytes=excluded.bytes,
		new_bytes=excluded.new_bytes,chunks=excluded.chunks,created_at=excluded.created_at`,
		d.ExecutionID, d.Kind, base, d.SHA256, d.Bytes, d.NewBytes, chunks, now())
	return err
}
