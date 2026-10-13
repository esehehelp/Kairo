package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
)

// AttemptView is an attempt as read-only clients (dashboards, monitors) see
// it: the attempt row plus where its logs live on the daemon host.
type AttemptView struct {
	Attempt
	StdoutPath *string `json:"stdout_path,omitempty"`
	StderrPath *string `json:"stderr_path,omitempty"`
}

// attemptColumns reads an AttemptView (scanAttempt) from attempts a.
const attemptColumns = `a.id,a.execution_id,a.state,a.executor_id,a.coordination_epoch,a.pid,a.process_identity,a.exit_code,a.exit_signal,a.continuation_ref,a.progress_json,a.started_at,a.last_heartbeat_at,a.checkpointed_at,a.exited_at,a.quiesced_at,a.stdout_path,a.stderr_path`

// scanAttempt reads the extra destinations, then attemptColumns.
func scanAttempt(rows *sql.Rows, extra ...any) (AttemptView, error) {
	var v AttemptView
	var progress sql.NullString
	dest := append(extra, &v.ID, &v.ExecutionID, &v.State, &v.ExecutorID, &v.CoordinationEpoch, &v.PID, &v.ProcessIdentity, &v.ExitCode, &v.ExitSignal, &v.ContinuationRef, &progress, &v.StartedAt, &v.LastHeartbeatAt, &v.CheckpointedAt, &v.ExitedAt, &v.QuiescedAt, &v.StdoutPath, &v.StderrPath)
	if err := rows.Scan(dest...); err != nil {
		return AttemptView{}, err
	}
	if progress.Valid && json.Valid([]byte(progress.String)) {
		v.Progress = json.RawMessage(progress.String)
	}
	return v, nil
}

type AttemptFilter struct {
	ProjectScopeID string
	ExecutionID    string
	State          string
	Limit          int
}

// ListAttempts returns attempts newest first (by authorization time), with
// their last reported progress.
func (s *Store) ListAttempts(ctx context.Context, filter AttemptFilter) ([]AttemptView, error) {
	query := `SELECT ` + attemptColumns + ` FROM attempts a JOIN execution_requests e ON e.id=a.execution_id`
	conditions := make([]string, 0, 3)
	args := make([]any, 0, 4)
	if filter.ProjectScopeID != "" {
		conditions = append(conditions, `e.project_scope_id=?`)
		args = append(args, filter.ProjectScopeID)
	}
	if filter.ExecutionID != "" {
		conditions = append(conditions, `a.execution_id=?`)
		args = append(args, filter.ExecutionID)
	}
	if filter.State != "" {
		conditions = append(conditions, `a.state=?`)
		args = append(args, filter.State)
	}
	if len(conditions) > 0 {
		query += ` WHERE ` + strings.Join(conditions, ` AND `)
	}
	query += ` ORDER BY COALESCE(a.authorized_at,a.started_at) DESC,a.id LIMIT ?`
	limit := filter.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]AttemptView, 0)
	for rows.Next() {
		v, err := scanAttempt(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// AttemptLogPaths returns where an attempt's logs live on the daemon host, as
// recorded by its executor ("" for a stream with no recorded path), or
// ErrNotFound for an unknown attempt.
func (s *Store) AttemptLogPaths(ctx context.Context, attemptID string) (stdout, stderr string, err error) {
	var out, errPath sql.NullString
	err = s.db.QueryRowContext(ctx, `SELECT stdout_path,stderr_path FROM attempts WHERE id=?`, attemptID).Scan(&out, &errPath)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", ErrNotFound
	}
	if err != nil {
		return "", "", err
	}
	return out.String, errPath.String, nil
}
