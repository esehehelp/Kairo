package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// Log problems: what an executor or a node agent saw go wrong with an
// attempt's logs while it ran. They are facts on the attempt (a coordination
// event and a row task show displays), never a reason to stop it.
const (
	// LogSpaceLow: the filesystem holding the attempt's logs has less free
	// space than the executor's minimum.
	LogSpaceLow = "log_space_low"
	// LogSpaceExhausted: it has no space left; the attempt's output is being
	// lost.
	LogSpaceExhausted = "log_space_exhausted"
	// LogForwardFailed: a node agent could not ship a log to the daemon; it
	// keeps retrying from where the daemon's copy ends.
	LogForwardFailed = "log_forward_failed"
)

// LogProblem is one problem with an attempt's logs. A report carries Problem,
// Stream ("stdout", "stderr" or "" for both), Detail and Resolved (the
// problem is over: a log that caught up again); the store keeps when it was
// first and last reported and when it was resolved.
type LogProblem struct {
	Problem     string  `json:"problem"`
	Stream      string  `json:"stream,omitempty"`
	Detail      string  `json:"detail"`
	Resolved    bool    `json:"resolved,omitempty"`
	Occurrences int     `json:"occurrences,omitempty"`
	FirstSeenAt string  `json:"first_seen_at,omitempty"`
	LastSeenAt  string  `json:"last_seen_at,omitempty"`
	ResolvedAt  *string `json:"resolved_at,omitempty"`
}

// RecordAttemptLogProblem records a report of a problem with an attempt's
// logs. A repeated report of the same problem and stream updates its detail,
// last report and resolution; an event is appended when the problem appears,
// is resolved, or comes back.
func (s *Store) RecordAttemptLogProblem(ctx context.Context, attemptID string, p LogProblem) error {
	switch p.Problem {
	case LogSpaceLow, LogSpaceExhausted, LogForwardFailed:
	default:
		return fmt.Errorf("unknown log problem %q", p.Problem)
	}
	if p.Stream != "" && p.Stream != "stdout" && p.Stream != "stderr" {
		return fmt.Errorf("log stream must be stdout, stderr or empty, not %q", p.Stream)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var projectID string
	err = tx.QueryRowContext(ctx, `SELECT e.project_scope_id FROM attempts a JOIN execution_requests e ON e.id=a.execution_id WHERE a.id=?`, attemptID).Scan(&projectID)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	var wasResolved sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT resolved_at FROM attempt_log_problems WHERE attempt_id=? AND problem=? AND stream=?`, attemptID, p.Problem, p.Stream).Scan(&wasResolved)
	existed := err == nil
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	t := now()
	var resolvedAt any
	if p.Resolved {
		resolvedAt = t
	}
	if existed {
		if wasResolved.Valid && p.Resolved {
			resolvedAt = wasResolved.String
		}
		_, err = tx.ExecContext(ctx, `UPDATE attempt_log_problems SET detail=CASE WHEN ?='' THEN detail ELSE ? END,occurrences=occurrences+1,last_seen_at=?,resolved_at=? WHERE attempt_id=? AND problem=? AND stream=?`, p.Detail, p.Detail, t, resolvedAt, attemptID, p.Problem, p.Stream)
	} else {
		_, err = tx.ExecContext(ctx, `INSERT INTO attempt_log_problems(attempt_id,problem,stream,detail,first_seen_at,last_seen_at,resolved_at) VALUES(?,?,?,?,?,?,?)`, attemptID, p.Problem, p.Stream, p.Detail, t, t, resolvedAt)
	}
	if err != nil {
		return err
	}
	event := ""
	switch {
	case p.Resolved && (!existed || !wasResolved.Valid):
		event = "attempt_log_problem_resolved"
		if !existed {
			// Reported only once it was over (a short shipping outage).
			event = "attempt_log_problem"
		}
	case !p.Resolved && (!existed || wasResolved.Valid):
		event = "attempt_log_problem"
	}
	if event != "" {
		payload := map[string]any{"problem": p.Problem, "stream": p.Stream, "detail": p.Detail, "resolved": p.Resolved}
		if err = appendCoordinationEventTx(ctx, tx, event, &projectID, "attempt", attemptID, nil, payload); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// taskLogProblems lists the log problems of a task's attempts, by attempt.
func (s *Store) taskLogProblems(ctx context.Context, taskScopeID string) (map[string][]LogProblem, error) {
	out := map[string][]LogProblem{}
	err := eachRow(ctx, s.db, `SELECT p.attempt_id,p.problem,p.stream,p.detail,p.occurrences,p.first_seen_at,p.last_seen_at,p.resolved_at
		FROM orchestration_task_executions te JOIN attempts a ON a.execution_id=te.execution_id JOIN attempt_log_problems p ON p.attempt_id=a.id
		WHERE te.task_scope_id=? ORDER BY p.first_seen_at,p.problem,p.stream`, []any{taskScopeID}, func(rows *sql.Rows) error {
		var attemptID string
		var p LogProblem
		if err := rows.Scan(&attemptID, &p.Problem, &p.Stream, &p.Detail, &p.Occurrences, &p.FirstSeenAt, &p.LastSeenAt, &p.ResolvedAt); err != nil {
			return err
		}
		p.Resolved = p.ResolvedAt != nil
		out[attemptID] = append(out[attemptID], p)
		return nil
	})
	return out, err
}
