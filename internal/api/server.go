package api

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"

	"kairo/internal/store"
)

type Server struct {
	Store  *store.Store
	Logger *slog.Logger
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.health)
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
