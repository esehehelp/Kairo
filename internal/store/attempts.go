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
	// After is the cursor a page ended at (ListAttemptsPage): the page is
	// the attempts after it.
	After string
}

// ListAttempts returns attempts newest first (by authorization time), with
// their last reported progress.
func (s *Store) ListAttempts(ctx context.Context, filter AttemptFilter) ([]AttemptView, error) {
	out, _, err := s.ListAttemptsPage(ctx, filter)
	return out, err
}

// ListAttemptsPage lists attempts newest first (by authorization time, else
// start; those with neither last), up to the filter's limit (100 by default,
// at most 1000) after its cursor; next is the cursor to read the rest from,
// "" when there is no more.
func (s *Store) ListAttemptsPage(ctx context.Context, filter AttemptFilter) (out []AttemptView, next string, err error) {
	const key = `COALESCE(a.authorized_at,a.started_at)`
	query := `SELECT ` + key + `,` + attemptColumns + ` FROM attempts a JOIN execution_requests e ON e.id=a.execution_id`
	conditions := make([]string, 0, 3)
	args := make([]any, 0, 4)
	if filter.ProjectScopeID != "" {
		conditions = append(conditions, `e.project_scope_id=`+bind(&args, filter.ProjectScopeID))
	}
	if filter.ExecutionID != "" {
		conditions = append(conditions, `a.execution_id=`+bind(&args, filter.ExecutionID))
	}
	if filter.State != "" {
		conditions = append(conditions, `a.state=`+bind(&args, filter.State))
	}
	if filter.After != "" {
		after, err := parseListCursor(filter.After)
		if err != nil {
			return nil, "", err
		}
		id := bind(&args, after.id)
		if after.key == nil {
			conditions = append(conditions, `(`+key+` IS NULL AND a.id>`+id+`)`)
		} else {
			at := bind(&args, *after.key)
			conditions = append(conditions, `(`+key+`<`+at+` OR `+key+`=`+at+` AND a.id>`+id+` OR `+key+` IS NULL)`)
		}
	}
	if len(conditions) > 0 {
		query += ` WHERE ` + strings.Join(conditions, ` AND `)
	}
	query += ` ORDER BY ` + key + ` DESC NULLS LAST,a.id LIMIT `
	limit := listLimit(filter.Limit)
	query += bind(&args, limit+1)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	out = make([]AttemptView, 0)
	var keys []*string
	for rows.Next() {
		var at sql.NullString
		v, err := scanAttempt(rows, &at)
		if err != nil {
			return nil, "", err
		}
		out = append(out, v)
		if at.Valid {
			keys = append(keys, &at.String)
		} else {
			keys = append(keys, nil)
		}
	}
	if err = rows.Err(); err != nil {
		return nil, "", err
	}
	if len(out) > limit {
		out = out[:limit]
		next = listCursor{key: keys[limit-1], id: out[limit-1].ID}.String()
	}
	return out, next, nil
}

// AttemptLogPaths returns where an attempt's logs live on the daemon host, as
// recorded by its executor ("" for a stream with no recorded path), or
// ErrNotFound for an unknown attempt.
func (s *Store) AttemptLogPaths(ctx context.Context, attemptID string) (stdout, stderr string, err error) {
	var out, errPath sql.NullString
	err = s.db.QueryRowContext(ctx, `SELECT stdout_path,stderr_path FROM attempts WHERE id=$1`, attemptID).Scan(&out, &errPath)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", ErrNotFound
	}
	if err != nil {
		return "", "", err
	}
	return out.String, errPath.String, nil
}
