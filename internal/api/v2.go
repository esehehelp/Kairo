package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"kairo/internal/id"
	"kairo/internal/store"
)

type submitExecutionRequest struct {
	SchemaVersion        int      `json:"schema_version"`
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
	if body.SchemaVersion != 2 {
		writeError(w, fmt.Errorf("schema_version must be 2, got %d", body.SchemaVersion))
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
	}
	if err := decodeOptional(r, &body); err != nil {
		writeError(w, err)
		return
	}
	if strings.TrimSpace(body.RequestID) == "" {
		body.RequestID = id.New("api")
	}
	value, err := s.Store.PauseScope(r.Context(), r.PathValue("id"), body.Actor, body.RequestID)
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
	value, err := s.Store.ResumeScope(r.Context(), r.PathValue("id"), body.Actor)
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
	writeJSON(w, http.StatusOK, map[string]any{"operation": value, "targets": targets})
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

func (s *Server) resourceStatus(w http.ResponseWriter, r *http.Request) {
	resources, err := s.Store.ListResourceStatus(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	claims, err := s.Store.ListClaims(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"resources": resources, "external_claims": claims})
}

func (s *Server) resourceAction(w http.ResponseWriter, r *http.Request) {
	action := r.PathValue("action")
	state, reason := "", ""
	switch action {
	case "enable":
		state = "enabled"
	case "reconcile":
		if s.ObserveOnly {
			writeError(w, errObserveOnly)
			return
		}
		var body struct {
			ConfirmProcessAbsent bool `json:"confirm_process_absent"`
		}
		if err := decode(r, &body); err != nil {
			writeError(w, err)
			return
		}
		if err := s.Store.ReconcileResource(r.Context(), r.PathValue("id"), body.ConfirmProcessAbsent); err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]bool{"ok": true})
		return
	case "quarantine":
		state = "quarantined"
		var body struct {
			Reason string `json:"reason"`
		}
		if err := decode(r, &body); err != nil {
			writeError(w, err)
			return
		}
		reason = body.Reason
	default:
		writeError(w, fmt.Errorf("unsupported resource action %q", action))
		return
	}
	if err := s.Store.SetResourceAdminState(r.Context(), r.PathValue("id"), state, reason); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]bool{"ok": true})
}

type workerContext struct {
	LeaseID           string
	CoordinationEpoch int64
}

func getWorkerContext(r *http.Request) (workerContext, error) {
	leaseID := r.Header.Get("Kairo-Lease-ID")
	epochText := r.Header.Get("Kairo-Coordination-Epoch")
	if leaseID == "" || epochText == "" {
		return workerContext{}, errors.New("Kairo-Lease-ID and Kairo-Coordination-Epoch headers are required")
	}
	epoch, err := strconv.ParseInt(epochText, 10, 64)
	if err != nil {
		return workerContext{}, errors.New("invalid coordination epoch")
	}
	return workerContext{LeaseID: leaseID, CoordinationEpoch: epoch}, nil
}

func (s *Server) workerPoll(w http.ResponseWriter, r *http.Request) {
	c, err := getWorkerContext(r)
	if err != nil {
		writeError(w, err)
		return
	}
	commands, err := s.Store.PollCommands(r.Context(), r.PathValue("id"), c.LeaseID, c.CoordinationEpoch)
	if err != nil {
		writeError(w, err)
		return
	}
	if commands == nil {
		commands = []store.Command{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"commands": commands})
}

func (s *Server) workerAck(w http.ResponseWriter, r *http.Request) {
	c, err := getWorkerContext(r)
	if err != nil {
		writeError(w, err)
		return
	}
	var body struct {
		Phase   string          `json:"phase"`
		Payload json.RawMessage `json:"payload"`
	}
	if err = decode(r, &body); err == nil {
		err = s.Store.AckCommand(r.Context(), r.PathValue("id"), c.LeaseID, c.CoordinationEpoch, r.PathValue("command"), body.Phase, body.Payload)
	}
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]bool{"ok": true})
}

func (s *Server) workerHeartbeat(w http.ResponseWriter, r *http.Request) {
	c, err := getWorkerContext(r)
	if err != nil {
		writeError(w, err)
		return
	}
	var body struct {
		Progress json.RawMessage `json:"progress"`
	}
	if err = decode(r, &body); err == nil {
		err = s.Store.Heartbeat(r.Context(), r.PathValue("id"), c.LeaseID, c.CoordinationEpoch, body.Progress)
	}
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]bool{"ok": true})
}

func (s *Server) workerProcess(w http.ResponseWriter, r *http.Request) {
	c, err := getWorkerContext(r)
	if err != nil {
		writeError(w, err)
		return
	}
	var body struct {
		Rank            int    `json:"rank"`
		PID             int    `json:"pid"`
		ProcessIdentity string `json:"process_identity"`
	}
	if err = decode(r, &body); err == nil {
		err = s.Store.RegisterProcess(r.Context(), r.PathValue("id"), c.LeaseID, c.CoordinationEpoch, body.Rank, body.PID, body.ProcessIdentity)
	}
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]bool{"ok": true})
}

func decodeOptional(r *http.Request, target any) error {
	if r.Body == nil || r.ContentLength == 0 {
		return nil
	}
	return decode(r, target)
}

func queryInt(r *http.Request, key string, fallback int) int {
	raw := r.URL.Query().Get(key)
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 0 {
		return fallback
	}
	return value
}

func queryInt64(r *http.Request, key string, fallback int64) int64 {
	raw := r.URL.Query().Get(key)
	if raw == "" {
		return fallback
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value < 0 {
		return fallback
	}
	return value
}
