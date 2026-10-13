package api

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"kairo/internal/store"
)

const (
	// defaultLogLimit and maxLogLimit bound one GET /api/attempts/{id}/log.
	defaultLogLimit = 1 << 20
	maxLogLimit     = 8 << 20
)

// attemptLog serves a slice of an attempt's stdout or stderr log as raw
// bytes: ?stream=stdout|stderr (default stderr), then either ?offset=
// (default 0) and ?limit= (default 1 MiB, at most 8 MiB), or ?tail=N for the
// last N bytes (at most 8 MiB). Only the path recorded for the attempt is
// ever read, and only inside the daemon's log directory. X-Kairo-Log-Size
// carries the file's current size and X-Kairo-Log-Offset the offset of the
// first byte served; Kairo-Log-Size and Kairo-Log-Next-Offset (where the next
// read continues) are kept for older clients. An offset at or past the end
// returns an empty body, so a follower polls from the next offset. A recorded
// log that does not exist yet (a node agent ships one once it has bytes)
// reads as empty.
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
	tail, err := logQueryInt(query.Get("tail"), -1)
	if err != nil {
		writeError(w, fmt.Errorf("tail: %w", err))
		return
	}
	if tail >= 0 && (query.Has("offset") || query.Has("limit")) {
		writeError(w, errors.New("tail cannot be combined with offset or limit"))
		return
	}
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
	local, err := s.logFile(path)
	if err != nil {
		writeError(w, fmt.Errorf("%w: the %s log of attempt %s", err, stream, attemptID))
		return
	}
	var size int64
	var body []byte
	f, err := os.Open(local)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		// Recorded but not shipped yet: an empty log so far.
		if tail >= 0 {
			offset = 0
		}
	case err != nil:
		writeError(w, err)
		return
	default:
		defer f.Close()
		info, err := f.Stat()
		if err != nil {
			writeError(w, err)
			return
		}
		if !info.Mode().IsRegular() {
			writeError(w, fmt.Errorf("%w: the %s log of attempt %s is not a file", store.ErrNotFound, stream, attemptID))
			return
		}
		// The log may grow while it is read: serve what existed at Stat, so the
		// size header and the body agree.
		size = info.Size()
		if tail >= 0 {
			limit = min(tail, maxLogLimit)
			offset = max(size-limit, 0)
		}
		n := max(min(limit, size-offset), 0)
		body = make([]byte, n)
		if n > 0 {
			read, err := f.ReadAt(body, offset)
			if err != nil && !errors.Is(err, io.EOF) {
				writeError(w, err)
				return
			}
			body = body[:read]
		}
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Kairo-Log-Size", strconv.FormatInt(size, 10))
	w.Header().Set("X-Kairo-Log-Offset", strconv.FormatInt(offset, 10))
	w.Header().Set("Kairo-Log-Size", strconv.FormatInt(size, 10))
	w.Header().Set("Kairo-Log-Next-Offset", strconv.FormatInt(offset+int64(len(body)), 10))
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

// logFile resolves a recorded log path and refuses one outside the daemon's
// log directory (after following symbolic links), so a recorded path can
// never make the daemon serve another file.
func (s *Server) logFile(path string) (string, error) {
	if s.LogDirectory == "" {
		return "", fmt.Errorf("%w: the daemon has no log directory", store.ErrNotFound)
	}
	dir, err := filepath.Abs(s.LogDirectory)
	if err != nil {
		return "", err
	}
	dir = resolveLinks(dir)
	file, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	if resolved, err := filepath.EvalSymlinks(file); err == nil {
		file = resolved
	} else {
		file = filepath.Join(resolveLinks(filepath.Dir(file)), filepath.Base(file))
	}
	rel, err := filepath.Rel(dir, file)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return "", fmt.Errorf("%w: log path is outside the daemon's log directory", ErrForbidden)
	}
	return file, nil
}

// resolveLinks follows the symbolic links of an existing path; a path that
// does not exist is returned as is.
func resolveLinks(path string) string {
	if resolved, err := filepath.EvalSymlinks(path); err == nil {
		return resolved
	}
	return path
}

// logSize is the current size of a recorded log, or nil when it cannot be
// read (not recorded, outside the log directory, not shipped yet).
func (s *Server) logSize(path *string) *int64 {
	if path == nil || *path == "" {
		return nil
	}
	local, err := s.logFile(*path)
	if err != nil {
		return nil
	}
	info, err := os.Stat(local)
	if err != nil || !info.Mode().IsRegular() {
		return nil
	}
	size := info.Size()
	return &size
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
