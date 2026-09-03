package api

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"kairo/internal/store"
)

type Server struct {
	Store  *store.Store
	Logger *slog.Logger
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.health)
	mux.HandleFunc("GET /v1/state", s.state)
	mux.HandleFunc("GET /v1/status", s.v1Status)
	mux.HandleFunc("GET /v1/queues", s.listQueues)
	mux.HandleFunc("GET /v1/queues/{project}/{queue}", s.getQueue)
	mux.HandleFunc("POST /v1/queues/{project}/{queue}/plan", s.applyPlan)
	mux.HandleFunc("GET /v1/queues/{project}/{queue}/history", s.queueHistory)
	mux.HandleFunc("GET /v1/queues/{project}/{queue}/export", s.exportPlan)
	mux.HandleFunc("POST /v1/queues/{project}/{queue}/{action}", s.queueAction)
	mux.HandleFunc("GET /v1/tasks", s.listTasks)
	mux.HandleFunc("GET /v1/tasks/{id}", s.getTask)
	mux.HandleFunc("POST /v1/tasks/{id}/{action}", s.taskAction)
	mux.HandleFunc("GET /v1/resources/status", s.resourceStatus)
	mux.HandleFunc("POST /v1/resources/{id}/{action}", s.resourceAction)
	mux.HandleFunc("POST /v1/resources", s.resources)
	mux.HandleFunc("POST /v1/resources/{id}/ready", s.resourceReady)
	mux.HandleFunc("POST /v1/workloads", s.workloads)
	mux.HandleFunc("GET /v1/workloads/{id}", s.workload)
	mux.HandleFunc("POST /v1/workloads/{id}/pause", s.pause)
	mux.HandleFunc("POST /v1/workloads/{id}/resume", s.resume)
	mux.HandleFunc("GET /v1/attempts/{id}/commands", s.pollCommands)
	mux.HandleFunc("POST /v1/attempts/{id}/commands/{command}/acks", s.ack)
	mux.HandleFunc("POST /v1/attempts/{id}/heartbeat", s.heartbeat)
	mux.HandleFunc("POST /v1/attempts/{id}/disposition", s.disposition)
	mux.HandleFunc("GET /v1/worker/attempts/{id}/commands", s.workerPoll)
	mux.HandleFunc("POST /v1/worker/attempts/{id}/commands/{command}/acks", s.workerAck)
	mux.HandleFunc("POST /v1/worker/attempts/{id}/heartbeat", s.workerHeartbeat)
	mux.HandleFunc("POST /v1/worker/attempts/{id}/processes", s.workerProcess)
	mux.HandleFunc("POST /v1/worker/attempts/{id}/terminal", s.workerTerminal)
	return s.logging(mux)
}

func (s *Server) logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, err error) {
	status := http.StatusBadRequest
	if errors.Is(err, store.ErrNotFound) {
		status = http.StatusNotFound
	} else if errors.Is(err, store.ErrSpecConflict) {
		status = http.StatusConflict
	} else if errors.Is(err, store.ErrRevisionConflict) || errors.Is(err, store.ErrStaleEpoch) {
		status = http.StatusConflict
	}
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

func decode(r *http.Request, target any) error {
	dec := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	dec.DisallowUnknownFields()
	return dec.Decode(target)
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) state(w http.ResponseWriter, r *http.Request) {
	snapshot, err := s.Store.Snapshot(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, snapshot)
}

func (s *Server) resources(w http.ResponseWriter, r *http.Request) {
	var resource store.Resource
	if err := decode(r, &resource); err != nil {
		writeError(w, err)
		return
	}
	if err := s.Store.AddResource(r.Context(), resource); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, resource)
}

func (s *Server) resourceReady(w http.ResponseWriter, r *http.Request) {
	if err := s.Store.SetResourceReady(r.Context(), r.PathValue("id")); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) workloads(w http.ResponseWriter, r *http.Request) {
	var spec store.WorkloadSpec
	if err := decode(r, &spec); err != nil {
		writeError(w, err)
		return
	}
	workload, created, err := s.Store.SubmitWorkload(r.Context(), spec)
	if err != nil {
		writeError(w, err)
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(w, status, map[string]any{"created": created, "workload": workload})
}

func (s *Server) workload(w http.ResponseWriter, r *http.Request) {
	workload, err := s.Store.GetWorkload(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, workload)
}

func (s *Server) pause(w http.ResponseWriter, r *http.Request) {
	commandID, err := s.Store.PauseWorkload(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"command_id": commandID})
}

func (s *Server) resume(w http.ResponseWriter, r *http.Request) {
	if err := s.Store.ResumeWorkload(r.Context(), r.PathValue("id")); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"ok": true})
}

func (s *Server) pollCommands(w http.ResponseWriter, r *http.Request) {
	commands, err := s.Store.PollCommands(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	if commands == nil {
		commands = []store.Command{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"commands": commands})
}

func (s *Server) ack(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Phase   string          `json:"phase"`
		Payload json.RawMessage `json:"payload"`
	}
	if err := decode(r, &body); err != nil {
		writeError(w, err)
		return
	}
	if err := s.Store.AckCommand(
		r.Context(), r.PathValue("id"), r.PathValue("command"), body.Phase, body.Payload,
	); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"ok": true})
}

func (s *Server) heartbeat(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Progress json.RawMessage `json:"progress"`
	}
	if err := decode(r, &body); err != nil {
		writeError(w, err)
		return
	}
	if err := s.Store.Heartbeat(r.Context(), r.PathValue("id"), body.Progress); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"ok": true})
}

func (s *Server) disposition(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Disposition string          `json:"disposition"`
		Payload     json.RawMessage `json:"payload"`
	}
	if err := decode(r, &body); err != nil {
		writeError(w, err)
		return
	}
	if err := s.Store.ReportDisposition(
		r.Context(), r.PathValue("id"), strings.ToLower(body.Disposition), body.Payload,
	); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"ok": true})
}
