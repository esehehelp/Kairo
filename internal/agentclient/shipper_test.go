package agentclient

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"kairo/internal/api"
	"kairo/internal/store"
)

// An attempt that wrote nothing (or could not: a full disk) still has its logs
// at the daemon, empty, rather than none.
func TestLogShipperShipsAnEmptyLog(t *testing.T) {
	st, ts, token, daemonLogs := newDaemon(t)
	c := New(ts.URL, token, nil)
	name := remoteAttempt(t, st, c).Attempt.ID + ".stdout.log"
	nodeLogs := t.TempDir()
	if err := os.WriteFile(filepath.Join(nodeLogs, name), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	(&LogShipper{Client: c, Dir: nodeLogs}).Sweep(context.Background())
	if info, err := os.Stat(filepath.Join(daemonLogs, name)); err != nil || info.Size() != 0 {
		t.Fatalf("daemon copy of an empty log: %v %v", info, err)
	}
}

// failingDaemon forwards to the daemon, but answers log appends with a
// daemon-side failure while failing is set.
func failingDaemon(t *testing.T, daemon string, failing *atomic.Bool) *httptest.Server {
	t.Helper()
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/agent/logs" && failing.Load() {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":"daemon log directory: write x: no space left on device","code":"internal"}`))
			return
		}
		forward, _ := http.NewRequest(r.Method, daemon+r.URL.Path, r.Body)
		forward.Header = r.Header
		resp, err := http.DefaultClient.Do(forward)
		if err != nil {
			t.Error(err)
			return
		}
		defer resp.Body.Close()
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
	}))
	t.Cleanup(proxy.Close)
	return proxy
}

// logProblemEvents are the log problem events of an attempt, as
// "event problem stream resolved: detail".
func logProblemEvents(t *testing.T, st *store.Store, attemptID string) []string {
	t.Helper()
	events, err := st.ListEvents(context.Background(), store.EventFilter{})
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range events {
		if e.AggregateID != attemptID || !strings.HasPrefix(e.EventType, "attempt_log_problem") {
			continue
		}
		var p store.LogProblem
		_ = json.Unmarshal(e.Payload, &p)
		resolved := "open"
		if p.Resolved {
			resolved = "resolved"
		}
		out = append(out, e.EventType+" "+p.Problem+" "+p.Stream+" "+resolved+": "+p.Detail)
	}
	return out
}

// Shipping that fails is retried from where the daemon's copy ends, so the
// log arrives whole once the daemon takes it again; a failure that lasts
// ReportAfter is recorded on the attempt, and so is the catching up. A shorter
// one leaves no trace.
func TestLogShipperRecordsALastingForwardingFailureAndResumes(t *testing.T) {
	st, ts, token, daemonLogs := newDaemon(t)
	var failing atomic.Bool
	c := New(failingDaemon(t, ts.URL, &failing).URL, token, nil)
	attemptID := remoteAttempt(t, st, c).Attempt.ID
	name := attemptID + ".stderr.log"
	nodeLogs := t.TempDir()
	path := filepath.Join(nodeLogs, name)
	if err := os.WriteFile(path, []byte("first\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 27, 10, 0, 0, 0, time.UTC)
	shipper := &LogShipper{Client: c, Dir: nodeLogs, Clock: func() time.Time { return now }}
	ctx := context.Background()

	// A brief outage: nothing recorded.
	failing.Store(true)
	shipper.Sweep(ctx)
	now = now.Add(5 * time.Second)
	failing.Store(false)
	shipper.Sweep(ctx)
	if got := logProblemEvents(t, st, attemptID); got != nil {
		t.Fatalf("a brief outage was recorded: %v", got)
	}

	// A lasting one, while the attempt goes on writing.
	failing.Store(true)
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.WriteString("second\n")
	shipper.Sweep(ctx)
	now = now.Add(31 * time.Second)
	shipper.Sweep(ctx)
	_, _ = f.WriteString("third\n")
	f.Close()
	now = now.Add(time.Minute)
	shipper.Sweep(ctx)
	failing.Store(false)
	shipper.Sweep(ctx)
	shipper.Sweep(ctx)

	body, _ := os.ReadFile(filepath.Join(daemonLogs, name))
	if string(body) != "first\nsecond\nthird\n" {
		t.Fatalf("daemon copy = %q", body)
	}
	want := []string{
		"attempt_log_problem log_forward_failed stderr open: agent API 500: daemon log directory: write x: no space left on device (retrying)",
		"attempt_log_problem_resolved log_forward_failed stderr resolved: agent API 500: daemon log directory: write x: no space left on device; caught up after 1m31s",
	}
	if got := logProblemEvents(t, st, attemptID); strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("events:\n%s", strings.Join(got, "\n"))
	}
}

// ShipAttempt ships one attempt's logs at once (the executor's flush at exit).
func TestShipAttemptShipsBothLogsOfTheAttempt(t *testing.T) {
	st, ts, token, daemonLogs := newDaemon(t)
	c := New(ts.URL, token, nil)
	attemptID := remoteAttempt(t, st, c).Attempt.ID
	nodeLogs := t.TempDir()
	for stream, body := range map[string]string{"stdout": "out\n", "stderr": "err\n"} {
		if err := os.WriteFile(filepath.Join(nodeLogs, attemptID+"."+stream+".log"), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	shipper := &LogShipper{Client: c, Dir: nodeLogs}
	if err := shipper.ShipAttempt(context.Background(), attemptID); err != nil {
		t.Fatal(err)
	}
	for stream, want := range map[string]string{"stdout": "out\n", "stderr": "err\n"} {
		if body, _ := os.ReadFile(filepath.Join(daemonLogs, attemptID+"."+stream+".log")); string(body) != want {
			t.Fatalf("%s at the daemon = %q", stream, body)
		}
	}
	// Waiting for a sweep in progress is bounded by the context.
	shipper.mu.Lock()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := shipper.ShipAttempt(ctx, attemptID)
	shipper.mu.Unlock()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("flush behind a stuck sweep: %v", err)
	}
}

// A daemon that cannot write its copy of a log (a full disk) answers with a
// daemon-side failure, which the agent retries, not a bad request.
func TestDaemonLogDirectoryFailureIsInternal(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "kairo.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	_, token, err := st.CreateAPIToken(context.Background(), "remote-agent", store.RoleNode, "remote")
	if err != nil {
		t.Fatal(err)
	}
	notADirectory := filepath.Join(t.TempDir(), "file")
	if err = os.WriteFile(notADirectory, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer((&api.Server{Store: st, LogDirectory: filepath.Join(notADirectory, "logs")}).Handler())
	t.Cleanup(ts.Close)
	c := New(ts.URL, token, nil)
	name := remoteAttempt(t, st, c).Attempt.ID + ".stderr.log"
	_, err = c.appendLog(context.Background(), name, 0, []byte("x"))
	var apiErr *Error
	if !errors.As(err, &apiErr) || apiErr.Status != http.StatusInternalServerError || !strings.HasPrefix(apiErr.Message, "daemon log directory: ") {
		t.Fatalf("append into a broken log directory: %v", err)
	}
}

// A node may record log problems only on its own attempts.
func TestLogProblemsAreBoundToTheNode(t *testing.T) {
	st, ts, token, _ := newDaemon(t)
	c := New(ts.URL, token, nil)
	attemptID := remoteAttempt(t, st, c).Attempt.ID
	if err := c.RecordAttemptLogProblem(context.Background(), attemptID, store.LogProblem{Problem: store.LogSpaceLow, Detail: "/srv has 10 MiB free"}); err != nil {
		t.Fatal(err)
	}
	_, other, err := st.CreateAPIToken(context.Background(), "other-agent", store.RoleNode, "other")
	if err != nil {
		t.Fatal(err)
	}
	forbidden(t, "log problem of another node's attempt", New(ts.URL, other, nil).RecordAttemptLogProblem(context.Background(), attemptID, store.LogProblem{Problem: store.LogSpaceLow}))
	if got := logProblemEvents(t, st, attemptID); len(got) != 1 || !strings.HasSuffix(got[0], ": /srv has 10 MiB free") {
		t.Fatalf("events: %v", got)
	}
}
