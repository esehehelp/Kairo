package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"sync"

	"kairo/internal/store"
)

// Server is the daemon's HTTP API: one /api surface for operators, node
// agents and the processes of attempts, each authenticated by a bearer token
// (see auth.go and routes.go).
type Server struct {
	Store       *store.Store
	ObserveOnly bool
	// LogDirectory receives attempt logs shipped by node agents.
	LogDirectory string

	logMu sync.Mutex
}

var (
	errObserveOnly = errors.New("operation is disabled in observe-only mode")
	// ErrForbidden: the token is valid but its role or node does not allow this.
	ErrForbidden = errors.New("token is not allowed to do this")
)

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	for _, rt := range s.routes() {
		mux.HandleFunc(rt.method+" "+rt.pattern, s.authorize(rt.role, rt.handler))
	}
	return mux
}

// Errors travel as {"error": message, "code": code}; a code maps back to the
// store sentinel on the client side (ErrorForCode), so the node agent sees the
// same error values the store returns in-process.
var errorCodes = []struct {
	code   string
	status int
	err    error
}{
	{"unauthorized", http.StatusUnauthorized, store.ErrUnauthorized},
	{"forbidden", http.StatusForbidden, ErrForbidden},
	{"not_found", http.StatusNotFound, store.ErrNotFound},
	{"stale_epoch", http.StatusConflict, store.ErrStaleEpoch},
	{"execution_started", http.StatusConflict, store.ErrExecutionStarted},
	{"idempotency_conflict", http.StatusConflict, store.ErrIdempotencyConflict},
	{"orchestration_conflict", http.StatusConflict, store.ErrOrchestrationConflict},
	{"ownership_conflict", http.StatusConflict, store.ErrOwnershipConflict},
	{"gang_not_ready", http.StatusConflict, store.ErrGangNotReady},
	{"gang_aborted", http.StatusConflict, store.ErrGangAborted},
	{"gate_closed", http.StatusLocked, store.ErrGateClosed},
	{"observe_only", http.StatusLocked, errObserveOnly},
}

// ErrorForCode maps a wire code back to the store sentinel (nil if none).
func ErrorForCode(code string) error {
	for _, c := range errorCodes {
		if c.code == code {
			return c.err
		}
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, err error) {
	status, code := http.StatusBadRequest, "bad_request"
	for _, c := range errorCodes {
		if errors.Is(err, c.err) {
			status, code = c.status, c.code
			break
		}
	}
	writeJSON(w, status, map[string]string{"error": err.Error(), "code": code})
}

func decode(r *http.Request, target any) error {
	dec := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	dec.DisallowUnknownFields()
	return dec.Decode(target)
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

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}
