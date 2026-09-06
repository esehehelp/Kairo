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
	Store       *store.Store
	Logger      *slog.Logger
	ObserveOnly bool
}

var errObserveOnly = errors.New("operation is disabled in observe-only mode")

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.health)
	mux.HandleFunc("POST /v2/executions", s.submitExecution)
	mux.HandleFunc("GET /v2/executions", s.listExecutions)
	mux.HandleFunc("GET /v2/executions/{id}", s.getExecution)
	mux.HandleFunc("POST /v2/executions/{id}/withdraw", s.withdrawExecution)
	mux.HandleFunc("GET /v2/scopes", s.listScopes)
	mux.HandleFunc("GET /v2/scopes/{id}", s.getScope)
	mux.HandleFunc("POST /v2/scopes/{id}/pause", s.pauseScope)
	mux.HandleFunc("POST /v2/scopes/{id}/resume", s.resumeScope)
	mux.HandleFunc("GET /v2/pause-operations/{id}", s.getPauseOperation)
	mux.HandleFunc("GET /v2/events", s.listEvents)
	mux.HandleFunc("GET /v2/resources/status", s.resourceStatus)
	mux.HandleFunc("POST /v2/resources/{id}/{action}", s.resourceAction)
	mux.HandleFunc("GET /v2/worker/attempts/{id}/commands", s.workerPoll)
	mux.HandleFunc("POST /v2/worker/attempts/{id}/commands/{command}/acks", s.workerAck)
	mux.HandleFunc("POST /v2/worker/attempts/{id}/heartbeat", s.workerHeartbeat)
	mux.HandleFunc("POST /v2/worker/attempts/{id}/processes", s.workerProcess)
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
	} else if errors.Is(err, store.ErrIdempotencyConflict) || errors.Is(err, store.ErrStaleEpoch) || errors.Is(err, store.ErrExecutionStarted) {
		status = http.StatusConflict
	} else if errors.Is(err, store.ErrGateClosed) {
		status = http.StatusLocked
	} else if errors.Is(err, errObserveOnly) {
		status = http.StatusLocked
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
