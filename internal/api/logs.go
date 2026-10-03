package api

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"strconv"

	"kairo/internal/store"
)

const (
	// defaultLogLimit and maxLogLimit bound one GET /api/attempts/{id}/log.
	defaultLogLimit = 1 << 20
	maxLogLimit     = 8 << 20
)

// attemptLog serves a slice of an attempt's stdout or stderr log as raw
// bytes: ?stream=stdout|stderr (default stderr), ?offset= (default 0),
// ?limit= (default 1 MiB, at most 8 MiB). Only the path recorded for the
// attempt is ever read. Kairo-Log-Size carries the file's current size and
// Kairo-Log-Next-Offset where the next read continues; an offset at or past
// the end returns an empty body, so a follower polls from Next-Offset.
func (s *Server) attemptLog(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	stream := query.Get("stream")
	if stream == "" {
		stream = "stderr"
	}
	if stream != "stdout" && stream != "stderr" {
		writeError(w, fmt.Errorf("stream must be stdout or stderr, not %q", stream))
		return
	}
	offset, err := logQueryInt(query.Get("offset"), 0)
	if err != nil {
		writeError(w, fmt.Errorf("offset: %w", err))
		return
	}
	limit, err := logQueryInt(query.Get("limit"), defaultLogLimit)
	if err != nil {
		writeError(w, fmt.Errorf("limit: %w", err))
		return
	}
	limit = min(limit, maxLogLimit)
	attemptID := r.PathValue("id")
	stdoutPath, stderrPath, err := s.Store.AttemptLogPaths(r.Context(), attemptID)
	if err != nil {
		writeError(w, err)
		return
	}
	path := stdoutPath
	if stream == "stderr" {
		path = stderrPath
	}
	if path == "" {
		writeError(w, fmt.Errorf("%w: attempt %s has no %s log", store.ErrNotFound, attemptID, stream))
		return
	}
	f, err := os.Open(path)
	if errors.Is(err, fs.ErrNotExist) {
		// Recorded but not there yet: a node agent ships a log once it has
		// bytes (and every couple of seconds). An empty log so far.
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("Kairo-Log-Size", "0")
		w.Header().Set("Kairo-Log-Next-Offset", strconv.FormatInt(offset, 10))
		w.WriteHeader(http.StatusOK)
		return
	}
	if err != nil {
		writeError(w, err)
		return
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		writeError(w, err)
		return
	}
	// The log may grow while it is read: serve what existed at Stat, so the
	// size header and the body agree.
	size := info.Size()
	n := max(min(limit, size-offset), 0)
	body := make([]byte, n)
	if n > 0 {
		read, err := f.ReadAt(body, offset)
		if err != nil && !errors.Is(err, io.EOF) {
			writeError(w, err)
			return
		}
		body = body[:read]
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Kairo-Log-Size", strconv.FormatInt(size, 10))
	w.Header().Set("Kairo-Log-Next-Offset", strconv.FormatInt(offset+int64(len(body)), 10))
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

// logQueryInt parses a non-negative integer query value; unlike queryInt it
// refuses a malformed one rather than falling back.
func logQueryInt(raw string, fallback int64) (int64, error) {
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value < 0 {
		return 0, fmt.Errorf("%q is not a non-negative integer", raw)
	}
	return value, nil
}
