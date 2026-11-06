package api

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// A request slower than SlowRequest is logged with its route and status,
// never its path; a fast one is not.
func TestSlowRequestsAreLogged(t *testing.T) {
	var logs bytes.Buffer
	s := &Server{SlowRequest: 50 * time.Millisecond, Logger: slog.New(slog.NewTextHandler(&logs, nil))}
	slow := s.timed("GET /api/attempts/{attempt}/commands", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(80 * time.Millisecond)
		w.WriteHeader(http.StatusTeapot)
	})
	fast := s.timed("GET /health", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("ok")) })
	fast(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/health", nil))
	if logs.Len() != 0 {
		t.Fatalf("a fast request was logged: %s", logs.String())
	}
	rec := httptest.NewRecorder()
	slow(rec, httptest.NewRequest(http.MethodGet, "/api/attempts/att_secretish/commands", nil))
	line := logs.String()
	if rec.Code != http.StatusTeapot || !strings.Contains(line, "slow request") || !strings.Contains(line, `route="GET /api/attempts/{attempt}/commands"`) ||
		!strings.Contains(line, "status=418") || strings.Contains(line, "att_secretish") {
		t.Fatalf("logged %q (status %d)", line, rec.Code)
	}
}
