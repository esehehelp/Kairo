package api

import (
	"errors"
	"net/http"
	"strings"

	"kairo/internal/id"
	"kairo/internal/store"
)

func (s *Server) listScopes(w http.ResponseWriter, r *http.Request) {
	project, queue, task := r.URL.Query().Get("project"), r.URL.Query().Get("queue"), r.URL.Query().Get("task")
	if project == "" && (queue != "" || task != "") {
		writeError(w, errors.New("queue and task scope lookup requires a project"))
		return
	}
	if queue == "" && task != "" {
		writeError(w, errors.New("task scope lookup requires a queue"))
		return
	}
	if project != "" {
		set, err := s.Store.GetScopeByPath(r.Context(), store.ScopePath{Project: project, Queue: queue, Task: task})
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"scopes": []store.Scope{*mostSpecificScope(set)}})
		return
	}
	values, err := s.Store.ListScopes(r.Context(), store.ScopeFilter{
		Kind: r.URL.Query().Get("kind"), ParentID: r.URL.Query().Get("parent_id"),
	})
	if err != nil {
		writeError(w, err)
		return
	}
	if values == nil {
		values = []store.Scope{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"scopes": values})
}

func mostSpecificScope(scopes store.ScopeSet) *store.Scope {
	if scopes.Task != nil {
		return scopes.Task
	}
	if scopes.Queue != nil {
		return scopes.Queue
	}
	return &scopes.Project
}

func (s *Server) getScope(w http.ResponseWriter, r *http.Request) {
	value, err := s.Store.GetScope(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"scope": value})
}

func (s *Server) pauseScope(w http.ResponseWriter, r *http.Request) {
	var body struct {
		RequestID string `json:"request_id"`
		Actor     string `json:"actor"`
		// Force ends the captured executions instead of suspending them:
		// graceful stop, then the process trees are killed after GraceSeconds
		// (default 30).
		Force        bool   `json:"force,omitempty"`
		GraceSeconds *int   `json:"grace_seconds,omitempty"`
		Reason       string `json:"reason,omitempty"`
	}
	if err := decodeOptional(r, &body); err != nil {
		writeError(w, err)
		return
	}
	if strings.TrimSpace(body.RequestID) == "" {
		body.RequestID = id.New("api")
	}
	if !body.Force && (body.GraceSeconds != nil || body.Reason != "") {
		writeError(w, errors.New("grace_seconds and reason apply only to a forced pause"))
		return
	}
	actor := actorOr(r, body.Actor)
	var value store.PauseOperation
	var err error
	if body.Force {
		if s.ObserveOnly {
			writeError(w, errObserveOnly)
			return
		}
		grace := 30
		if body.GraceSeconds != nil {
			grace = *body.GraceSeconds
		}
		value, err = s.Store.ForcePauseScope(r.Context(), r.PathValue("id"), actor, body.RequestID, store.ForceStopSpec{GraceSeconds: grace, Reason: body.Reason})
	} else {
		value, err = s.Store.PauseScope(r.Context(), r.PathValue("id"), actor, body.RequestID)
	}
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"operation_id": value.ID, "operation": value})
}

func (s *Server) resumeScope(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Actor string `json:"actor"`
	}
	if err := decodeOptional(r, &body); err != nil {
		writeError(w, err)
		return
	}
	value, err := s.Store.ResumeScope(r.Context(), r.PathValue("id"), actorOr(r, body.Actor))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"gate": value})
}

func (s *Server) getPauseOperation(w http.ResponseWriter, r *http.Request) {
	value, err := s.Store.GetPauseOperation(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	targets, err := s.Store.ListPauseTargets(r.Context(), value.ID)
	if err != nil {
		writeError(w, err)
		return
	}
	if targets == nil {
		targets = []store.PauseTarget{}
	}
	response := map[string]any{"operation": value, "targets": targets}
	if value.Force != nil {
		stops, err := s.Store.ListForceStops(r.Context(), value.ID)
		if err != nil {
			writeError(w, err)
			return
		}
		response["force_stops"] = stops
	}
	writeJSON(w, http.StatusOK, response)
}
