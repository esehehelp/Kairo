package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"kairo/internal/plan"
	"kairo/internal/store"
)

func (s *Server) v1Status(w http.ResponseWriter, r *http.Request) {
	value, err := s.Store.Status(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}
func (s *Server) listQueues(w http.ResponseWriter, r *http.Request) {
	value, err := s.Store.ListQueues(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	if value == nil {
		value = []store.Queue{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"queues": value})
}
func (s *Server) getQueue(w http.ResponseWriter, r *http.Request) {
	q, err := s.Store.GetQueue(r.Context(), r.PathValue("project"), r.PathValue("queue"))
	if err != nil {
		writeError(w, err)
		return
	}
	tasks, err := s.Store.ListTasks(r.Context(), q.ID)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"queue": q, "tasks": tasks})
}
func (s *Server) applyPlan(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Source           string `json:"source"`
		ExpectedRevision *int   `json:"expected_revision"`
		Create           bool   `json:"create"`
		Actor            string `json:"actor"`
		RequestID        string `json:"request_id"`
	}
	if err := decode(r, &body); err != nil {
		writeError(w, err)
		return
	}
	if body.ExpectedRevision == nil {
		writeError(w, errors.New("expected_revision is required"))
		return
	}
	validated, err := plan.Parse([]byte(body.Source))
	if err != nil {
		writeError(w, err)
		return
	}
	if validated.Manifest.Project != r.PathValue("project") || validated.Manifest.Queue != r.PathValue("queue") {
		writeError(w, errors.New("manifest project/queue does not match URL"))
		return
	}
	result, err := s.Store.ApplyPlan(r.Context(), validated, store.ApplyOptions{ExpectedRevision: *body.ExpectedRevision, Create: body.Create, Actor: body.Actor, RequestID: body.RequestID})
	if err != nil {
		writeError(w, err)
		return
	}
	status := http.StatusOK
	if result.Created {
		status = http.StatusCreated
	}
	writeJSON(w, status, result)
}
func (s *Server) queueHistory(w http.ResponseWriter, r *http.Request) {
	q, err := s.Store.GetQueue(r.Context(), r.PathValue("project"), r.PathValue("queue"))
	if err != nil {
		writeError(w, err)
		return
	}
	history, err := s.Store.QueueHistory(r.Context(), q.ID)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"queue": q, "history": history})
}
func (s *Server) exportPlan(w http.ResponseWriter, r *http.Request) {
	q, err := s.Store.GetQueue(r.Context(), r.PathValue("project"), r.PathValue("queue"))
	if err != nil {
		writeError(w, err)
		return
	}
	history, err := s.Store.QueueHistory(r.Context(), q.ID)
	if err != nil {
		writeError(w, err)
		return
	}
	if len(history) == 0 {
		writeError(w, store.ErrNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/toml; charset=utf-8")
	_, _ = w.Write([]byte(history[0].SourceText))
}
func (s *Server) queueAction(w http.ResponseWriter, r *http.Request) {
	action := r.PathValue("action")
	state := ""
	if action == "pause" {
		state = "paused"
	} else if action == "resume" {
		state = "active"
	} else {
		writeError(w, fmt.Errorf("unsupported queue action %q", action))
		return
	}
	q, err := s.Store.GetQueue(r.Context(), r.PathValue("project"), r.PathValue("queue"))
	if err == nil {
		err = s.Store.SetQueueState(r.Context(), q.ID, state)
	}
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]bool{"ok": true})
}
func (s *Server) listTasks(w http.ResponseWriter, r *http.Request) {
	tasks, err := s.Store.ListTasks(r.Context(), r.URL.Query().Get("queue_id"))
	if err != nil {
		writeError(w, err)
		return
	}
	if tasks == nil {
		tasks = []store.Task{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"tasks": tasks})
}
func (s *Server) getTask(w http.ResponseWriter, r *http.Request) {
	task, err := s.Store.GetTask(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, task)
}
func (s *Server) taskAction(w http.ResponseWriter, r *http.Request) {
	command, err := s.Store.SetTaskState(r.Context(), r.PathValue("id"), r.PathValue("action"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"ok": true, "command_id": command})
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
	LeaseID           string `json:"lease_id"`
	CoordinationEpoch int64  `json:"coordination_epoch"`
}

func getWorkerContext(r *http.Request) (workerContext, error) {
	var value workerContext
	value.LeaseID = r.Header.Get("Kairo-Lease-ID")
	epoch := r.Header.Get("Kairo-Coordination-Epoch")
	if value.LeaseID == "" || epoch == "" {
		return value, errors.New("Kairo-Lease-ID and Kairo-Coordination-Epoch headers are required")
	}
	if _, err := fmt.Sscan(epoch, &value.CoordinationEpoch); err != nil {
		return value, errors.New("invalid coordination epoch")
	}
	return value, nil
}
func (s *Server) workerPoll(w http.ResponseWriter, r *http.Request) {
	c, err := getWorkerContext(r)
	if err != nil {
		writeError(w, err)
		return
	}
	commands, err := s.Store.PollCommandsV1(r.Context(), r.PathValue("id"), c.LeaseID, c.CoordinationEpoch)
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
		err = s.Store.AckCommandV1(r.Context(), r.PathValue("id"), c.LeaseID, c.CoordinationEpoch, r.PathValue("command"), body.Phase, body.Payload)
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
		err = s.Store.HeartbeatV1(r.Context(), r.PathValue("id"), c.LeaseID, c.CoordinationEpoch, body.Progress)
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
func (s *Server) workerTerminal(w http.ResponseWriter, r *http.Request) {
	c, err := getWorkerContext(r)
	if err != nil {
		writeError(w, err)
		return
	}
	var body struct {
		ExitCode     int    `json:"exit_code"`
		FailureClass string `json:"failure_class"`
	}
	if err = decode(r, &body); err == nil {
		err = s.Store.TerminalV1(r.Context(), r.PathValue("id"), c.LeaseID, c.CoordinationEpoch, body.ExitCode, strings.ToLower(body.FailureClass))
	}
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]bool{"ok": true})
}
