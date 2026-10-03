package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"sync"

	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"

	"kairo/internal/store"
)

// APIVersion is the version of the API contract, reported by GET /health. It
// changes only when a change would break an existing client.
const APIVersion = 1

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

// codeInternal marks a failure of the daemon rather than of the request:
// 503 when it is likely to pass (a busy database, a deadline), else 500.
// Clients treat both as transient.
const codeInternal = "internal"

func writeError(w http.ResponseWriter, err error) {
	status, code := errorStatus(err)
	writeJSON(w, status, map[string]string{"error": err.Error(), "code": code})
}

// errorStatus maps an error to its HTTP status and wire code: a store
// sentinel to its code, a database or context failure to "internal", and
// anything else (validation) to 400 bad_request.
func errorStatus(err error) (int, string) {
	for _, c := range errorCodes {
		if errors.Is(err, c.err) {
			return c.status, c.code
		}
	}
	var sqliteErr *sqlite.Error
	switch {
	case errors.As(err, &sqliteErr):
		// Extended result codes keep the primary code in the low byte.
		if primary := sqliteErr.Code() & 0xff; primary == sqlite3.SQLITE_BUSY || primary == sqlite3.SQLITE_LOCKED {
			return http.StatusServiceUnavailable, codeInternal
		}
		return http.StatusInternalServerError, codeInternal
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return http.StatusServiceUnavailable, codeInternal
	case errors.Is(err, sql.ErrConnDone), errors.Is(err, sql.ErrTxDone):
		return http.StatusInternalServerError, codeInternal
	}
	return http.StatusBadRequest, "bad_request"
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
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "api": APIVersion})
}
