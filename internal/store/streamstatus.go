package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"kairo/internal/loader"
)

// AttemptStream is what the loader of one stream an attempt reads said last
// (issue #43; executor/streamstatus.go): its status, and when Kairo heard it.
type AttemptStream struct {
	Stream     string        `json:"stream"`
	Status     loader.Status `json:"status"`
	ObservedAt string        `json:"observed_at,omitempty"`
}

// maxStreamRanks bounds the ranks of a reported status.
const maxStreamRanks = 4096

// RecordAttemptStreams keeps what the loaders of an attempt's streams said
// last, one row per stream (a later report replaces it). A stream the
// attempt does not read is refused.
func (s *Store) RecordAttemptStreams(ctx context.Context, attemptID string, streams []AttemptStream) error {
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var executionID string
	if err := tx.QueryRowContext(ctx, `SELECT execution_id FROM attempts WHERE id=$1`, attemptID).Scan(&executionID); err == sql.ErrNoRows {
		return ErrNotFound
	} else if err != nil {
		return err
	}
	decls, err := executionStreamsTx(ctx, tx, executionID)
	if err != nil {
		return err
	}
	declared := map[string]bool{}
	for _, d := range decls {
		declared[d.Name] = true
	}
	at := now()
	for _, st := range streams {
		if !declared[st.Stream] {
			return fmt.Errorf("%w: attempt %s reads no stream %q", ErrStreamReportRefused, attemptID, st.Stream)
		}
		if len(st.Status.Ranks) > maxStreamRanks || st.Status.World < 0 || st.Status.World > maxStreamRanks {
			return fmt.Errorf("%w: stream %q of %d ranks", ErrStreamReportRefused, st.Stream, len(st.Status.Ranks))
		}
		body, err := json.Marshal(st.Status)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO attempt_streams(attempt_id,stream,status_json,observed_at) VALUES($1,$2,$3,$4)
			ON CONFLICT(attempt_id,stream) DO UPDATE SET status_json=excluded.status_json,observed_at=excluded.observed_at`, attemptID, st.Stream, string(body), at); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// taskAttemptStreams are what the loaders of a task's attempts said last, by
// attempt, streams by name.
func (s *Store) taskAttemptStreams(ctx context.Context, taskScopeID string) (map[string][]AttemptStream, error) {
	out := map[string][]AttemptStream{}
	err := eachRow(ctx, s.db, `SELECT s.attempt_id,s.stream,s.status_json,s.observed_at FROM attempt_streams s JOIN attempts a ON a.id=s.attempt_id
		JOIN orchestration_task_executions te ON te.execution_id=a.execution_id WHERE te.task_scope_id=$1 ORDER BY s.attempt_id,s.stream`, []any{taskScopeID}, func(rows *sql.Rows) error {
		var attemptID, body string
		var st AttemptStream
		if err := rows.Scan(&attemptID, &st.Stream, &body, &st.ObservedAt); err != nil {
			return err
		}
		if json.Unmarshal([]byte(body), &st.Status) != nil {
			return nil
		}
		out[attemptID] = append(out[attemptID], st)
		return nil
	})
	return out, err
}
