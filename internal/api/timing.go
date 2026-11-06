package api

import (
	"log/slog"
	"net/http"
	"time"
)

// defaultSlowRequest is how long a request may take before it is logged: the
// SDK gives up on a request after 10 s, so a daemon this slow is one an
// attempt may see fail.
const defaultSlowRequest = 2 * time.Second

// timed logs, at WARN, a request that took SlowRequest or longer: its route
// (the pattern, never the path or a header, so no id or token), its status,
// how long it took and where it came from.
func (s *Server) timed(pattern string, next http.HandlerFunc) http.HandlerFunc {
	slow := s.SlowRequest
	if slow <= 0 {
		slow = defaultSlowRequest
	}
	return func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next(sw, r)
		if took := time.Since(started); took >= slow {
			logger := s.Logger
			if logger == nil {
				logger = slog.Default()
			}
			logger.Warn("slow request", "route", pattern, "status", sw.status, "seconds", took.Seconds(), "remote", r.RemoteAddr)
		}
	}
}

// statusWriter remembers the status a handler wrote.
type statusWriter struct {
	http.ResponseWriter
	status int
	wrote  bool
}

func (w *statusWriter) WriteHeader(status int) {
	if !w.wrote {
		w.status, w.wrote = status, true
	}
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusWriter) Write(p []byte) (int, error) {
	w.wrote = true
	return w.ResponseWriter.Write(p)
}

// Unwrap lets http.ResponseController reach the connection's writer.
func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
