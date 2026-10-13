package api

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"kairo/internal/orchestration"
	"kairo/internal/store"
)

// applyProject stores a ProjectSpec declaration; the daemon reconciles it.
// The answer names what changed, not the project's whole status.
func (s *Server) applyProject(w http.ResponseWriter, r *http.Request) {
	var manifest orchestration.Manifest
	if err := decode(r, &manifest); err != nil {
		writeError(w, err)
		return
	}
	validated, err := orchestration.Normalize(manifest)
	if err != nil {
		writeError(w, err)
		return
	}
	applied, err := s.Store.ApplyProject(r.Context(), validated)
	if err != nil {
		writeError(w, err)
		return
	}
	code := http.StatusCreated
	if applied.Idempotent {
		code = http.StatusOK
	}
	writeJSON(w, code, map[string]any{
		"project":     validated.Manifest.Project.Name,
		"spec_digest": validated.Digest,
		"idempotent":  applied.Idempotent,
		"created":     applied.Created,
		"unchanged":   applied.Unchanged,
	})
}

// getProject is a project's status. Optional filters, all of which must
// match: ?task=NAME (repeatable), ?match=PREFIX, ?state=s1,s2, ?live=1
// (pending or running) and ?since=RFC3339 (updated since). state_counts
// always counts every task.
func (s *Server) getProject(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	filter := store.TaskFilter{Names: query["task"], Prefix: query.Get("match")}
	for _, raw := range query["state"] {
		for _, state := range strings.Split(raw, ",") {
			if state = strings.TrimSpace(state); state != "" {
				filter.States = append(filter.States, state)
			}
		}
	}
	switch live := query.Get("live"); live {
	case "", "0", "false":
	case "1", "true":
		filter.Live = true
	default:
		writeError(w, fmt.Errorf("live must be 1 or 0, not %q", live))
		return
	}
	if since := query.Get("since"); since != "" {
		t, err := time.Parse(time.RFC3339, since)
		if err != nil {
			writeError(w, fmt.Errorf("since must be an RFC 3339 time: %w", err))
			return
		}
		filter.Since = t
	}
	status, err := s.Store.QueryProjectStatus(r.Context(), r.PathValue("project"), filter)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"project": status})
}

// getTask is one task in detail: its declaration, direct dependencies and
// dependents, admission gates, and every execution with its attempts (and
// their log sizes), gang ranks and what it waits for.
func (s *Server) getTask(w http.ResponseWriter, r *http.Request) {
	detail, err := s.Store.GetTaskDetail(r.Context(), r.PathValue("project"), r.PathValue("task"))
	if err != nil {
		writeError(w, err)
		return
	}
	for i := range detail.Executions {
		for j := range detail.Executions[i].Attempts {
			a := &detail.Executions[i].Attempts[j]
			a.StdoutBytes = s.logSize(a.StdoutPath)
			a.StderrBytes = s.logSize(a.StderrPath)
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"task": detail})
}

// cancelTask ends a task that has not started or is paused (store.CancelTask).
func (s *Server) cancelTask(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Actor  string `json:"actor"`
		DryRun bool   `json:"dry_run"`
	}
	if err := decodeOptional(r, &body); err != nil {
		writeError(w, err)
		return
	}
	if s.ObserveOnly && !body.DryRun {
		writeError(w, errObserveOnly)
		return
	}
	result, err := s.Store.CancelTask(r.Context(), r.PathValue("project"), r.PathValue("task"), actorOr(r, body.Actor), body.DryRun)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

// rerunTask runs a failed, stopped, cancelled or blocked task's declaration
// again (store.RerunTask).
func (s *Server) rerunTask(w http.ResponseWriter, r *http.Request) {
	if s.ObserveOnly {
		writeError(w, errObserveOnly)
		return
	}
	var body struct {
		Actor     string `json:"actor"`
		RequestID string `json:"request_id"`
	}
	if err := decodeOptional(r, &body); err != nil {
		writeError(w, err)
		return
	}
	result, err := s.Store.RerunTask(r.Context(), r.PathValue("project"), r.PathValue("task"), actorOr(r, body.Actor), strings.TrimSpace(body.RequestID))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}
