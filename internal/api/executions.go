package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"kairo/internal/store"
)

type submitExecutionRequest struct {
	ClientRequestID      string   `json:"client_request_id"`
	Project              string   `json:"project"`
	Queue                string   `json:"queue,omitempty"`
	Task                 string   `json:"task,omitempty"`
	Argv                 []string `json:"argv"`
	CWD                  string   `json:"cwd"`
	Priority             int      `json:"priority"`
	Checkpointable       bool     `json:"checkpointable"`
	Preemptible          bool     `json:"preemptible"`
	InputContinuationRef *string  `json:"input_continuation_ref,omitempty"`
	Executor             struct {
		Labels map[string]string `json:"labels"`
	} `json:"executor"`
	Exclusive []store.ExclusiveRequest `json:"exclusive"`
	Capacity  store.CapacityRequest    `json:"capacity"`
}

func (s *Server) submitExecution(w http.ResponseWriter, r *http.Request) {
	var body submitExecutionRequest
	if err := decode(r, &body); err != nil {
		writeError(w, err)
		return
	}
	if body.Executor.Labels == nil {
		body.Executor.Labels = map[string]string{}
	}
	executor, err := json.Marshal(body.Executor)
	if err != nil {
		writeError(w, err)
		return
	}
	value, idempotent, err := s.Store.SubmitExecution(r.Context(), store.ExecutionSpec{
		ClientRequestID: body.ClientRequestID,
		Scope:           store.ScopePath{Project: body.Project, Queue: body.Queue, Task: body.Task},
		Argv:            body.Argv, CWD: body.CWD, ExecutorSelector: executor,
		Priority: body.Priority, Checkpointable: body.Checkpointable, Preemptible: body.Preemptible,
		InputContinuationRef: body.InputContinuationRef, Exclusive: body.Exclusive, Capacity: body.Capacity,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	status := http.StatusCreated
	if idempotent {
		status = http.StatusOK
	}
	writeJSON(w, status, map[string]any{"execution": value, "idempotent": idempotent})
}

func (s *Server) listExecutions(w http.ResponseWriter, r *http.Request) {
	filter := store.ExecutionFilter{State: r.URL.Query().Get("state"), Limit: queryInt(r, "limit", 100)}
	project, queue, task := r.URL.Query().Get("project"), r.URL.Query().Get("queue"), r.URL.Query().Get("task")
	if project == "" && (queue != "" || task != "") {
		writeError(w, errors.New("queue and task filters require a project"))
		return
	}
	if queue == "" && task != "" {
		writeError(w, errors.New("task filter requires a queue"))
		return
	}
	if scopeID := r.URL.Query().Get("scope_id"); scopeID != "" {
		filter.ScopeID = scopeID
	} else if project != "" {
		scopes, err := s.Store.GetScopeByPath(r.Context(), store.ScopePath{
			Project: project, Queue: queue, Task: task,
		})
		if err != nil {
			writeError(w, err)
			return
		}
		filter.ProjectScopeID = scopes.Project.ID
		filter.ScopeID = mostSpecificScope(scopes).ID
	}
	values, err := s.Store.ListExecutions(r.Context(), filter)
	if err != nil {
		writeError(w, err)
		return
	}
	if values == nil {
		values = []store.ExecutionRequest{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"executions": values})
}

func (s *Server) getExecution(w http.ResponseWriter, r *http.Request) {
	value, err := s.Store.GetExecution(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"execution": value})
}

func (s *Server) withdrawExecution(w http.ResponseWriter, r *http.Request) {
	if s.ObserveOnly {
		writeError(w, errObserveOnly)
		return
	}
	if err := s.Store.WithdrawExecution(r.Context(), r.PathValue("id")); err != nil {
		writeError(w, err)
		return
	}
	value, err := s.Store.GetExecution(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"execution": value})
}

// listAttempts is the read side of attempt progress: ?project=, ?execution_id=,
// ?state=, ?limit=. Newest attempts first.
func (s *Server) listAttempts(w http.ResponseWriter, r *http.Request) {
	filter := store.AttemptFilter{ExecutionID: r.URL.Query().Get("execution_id"), State: r.URL.Query().Get("state"), Limit: queryInt(r, "limit", 100)}
	if project := r.URL.Query().Get("project"); project != "" {
		scopes, err := s.Store.GetScopeByPath(r.Context(), store.ScopePath{Project: project})
		if err != nil {
			writeError(w, err)
			return
		}
		filter.ProjectScopeID = scopes.Project.ID
	}
	values, err := s.Store.ListAttempts(r.Context(), filter)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"attempts": values})
}

func (s *Server) listEvents(w http.ResponseWriter, r *http.Request) {
	filter := store.EventFilter{AfterSequence: queryInt64(r, "after", 0), Limit: queryInt(r, "limit", 100)}
	if project := r.URL.Query().Get("project"); project != "" {
		set, err := s.Store.GetScopeByPath(r.Context(), store.ScopePath{Project: project})
		if err != nil {
			writeError(w, err)
			return
		}
		filter.ProjectScopeID = set.Project.ID
	}
	values, err := s.Store.ListEvents(r.Context(), filter)
	if err != nil {
		writeError(w, err)
		return
	}
	if values == nil {
		values = []store.CoordinationEvent{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": values})
}
