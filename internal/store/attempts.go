package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
)

// AttemptView is an attempt as read-only clients (dashboards, monitors) see
// it: the attempt row plus where its logs live on the daemon host.
type AttemptView struct {
	Attempt
	StdoutPath *string `json:"stdout_path,omitempty"`
	StderrPath *string `json:"stderr_path,omitempty"`
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
	query := `SELECT a.id,a.execution_id,a.state,a.executor_id,a.coordination_epoch,a.pid,a.process_identity,a.exit_code,a.exit_signal,a.continuation_ref,a.progress_json,a.started_at,a.last_heartbeat_at,a.checkpointed_at,a.exited_at,a.quiesced_at,a.stdout_path,a.stderr_path
		FROM attempts a JOIN execution_requests e ON e.id=a.execution_id`
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
		var v AttemptView
		var progress sql.NullString
		if err = rows.Scan(&v.ID, &v.ExecutionID, &v.State, &v.ExecutorID, &v.CoordinationEpoch, &v.PID, &v.ProcessIdentity, &v.ExitCode, &v.ExitSignal, &v.ContinuationRef, &progress, &v.StartedAt, &v.LastHeartbeatAt, &v.CheckpointedAt, &v.ExitedAt, &v.QuiescedAt, &v.StdoutPath, &v.StderrPath); err != nil {
			return nil, err
		}
		if progress.Valid && json.Valid([]byte(progress.String)) {
			v.Progress = json.RawMessage(progress.String)
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
