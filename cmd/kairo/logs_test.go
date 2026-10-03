package main

import (
	"bytes"
	"context"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// logAPI serves GET /api/attempts/att-1/log from a growing in-memory log,
// recording each request's query.
type logAPI struct {
	mu       sync.Mutex
	log      []byte
	requests []string
}

func (l *logAPI) append(data string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.log = append(l.log, data...)
}

func (l *logAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if r.Method != http.MethodGet || r.URL.Path != "/api/attempts/att-1/log" {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"attempt not found","code":"not_found"}`))
		return
	}
	l.requests = append(l.requests, r.URL.RawQuery)
	q := r.URL.Query()
	offset, _ := strconv.ParseInt(q.Get("offset"), 10, 64)
	limit, _ := strconv.ParseInt(q.Get("limit"), 10, 64)
	size := int64(len(l.log))
	end := min(offset+limit, size)
	body := []byte{}
	if offset < end {
		body = l.log[offset:end]
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Kairo-Log-Size", strconv.FormatInt(size, 10))
	w.Header().Set("Kairo-Log-Next-Offset", strconv.FormatInt(offset+int64(len(body)), 10))
	_, _ = w.Write(body)
}

func (l *logAPI) queries() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.requests...)
}

func TestLogsPrintsTheWholeLog(t *testing.T) {
	api := &logAPI{log: []byte("step 1\nstep 2\n")}
	server := fakeAPI(t, api)
	defer server.Close()
	out, err := captureStdout(t, func() error {
		return run([]string{"logs", "--api", server.URL, "--stream", "stdout", "att-1"})
	})
	if err != nil || out != "step 1\nstep 2\n" {
		t.Fatalf("logs: %q %v", out, err)
	}
	if got := api.queries(); len(got) != 1 || got[0] != "limit=1048576&offset=0&stream=stdout" {
		t.Fatalf("requests %v", got)
	}
}

func TestLogsTailFetchesOnlyTheLastBytes(t *testing.T) {
	api := &logAPI{log: []byte("0123456789")}
	server := fakeAPI(t, api)
	defer server.Close()
	out, err := captureStdout(t, func() error {
		return run([]string{"logs", "--api", server.URL, "--tail", "3", "att-1"})
	})
	if err != nil || out != "789" {
		t.Fatalf("tail: %q %v", out, err)
	}
	got := api.queries()
	if len(got) != 2 || got[0] != "limit=0&offset=0&stream=stderr" || got[1] != "limit=1048576&offset=7&stream=stderr" {
		t.Fatalf("requests %v", got)
	}
	// A tail longer than the log prints all of it.
	if out, err = captureStdout(t, func() error {
		return run([]string{"logs", "--api", server.URL, "--tail", "50", "att-1"})
	}); err != nil || out != "0123456789" {
		t.Fatalf("long tail: %q %v", out, err)
	}
}

func TestLogsFollowPollsFromTheNextOffset(t *testing.T) {
	saved := logFollowInterval
	logFollowInterval = 10 * time.Millisecond
	defer func() { logFollowInterval = saved }()
	api := &logAPI{log: []byte("a")}
	server := fakeAPI(t, api)
	defer server.Close()
	client, err := newAPIClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var out syncBuffer
	done := make(chan error, 1)
	go func() { done <- printLog(ctx, client, "att-1", "stderr", -1, true, &out) }()
	waitFor(t, func() bool { return out.String() == "a" })
	api.append("bc")
	waitFor(t, func() bool { return out.String() == "abc" })
	cancel()
	if err := <-done; err != context.Canceled {
		t.Fatalf("follow ended with %v", err)
	}
	for _, q := range api.queries()[1:] {
		if !strings.Contains(q, "offset=1&") && !strings.Contains(q, "offset=3&") {
			t.Fatalf("follow re-read from the start: %v", api.queries())
		}
	}
}

func TestLogsReportsAnUnknownAttempt(t *testing.T) {
	server := fakeAPI(t, &logAPI{})
	defer server.Close()
	err := run([]string{"logs", "--api", server.URL, "att-missing"})
	if err == nil || !strings.Contains(err.Error(), "HTTP 404 not_found") {
		t.Fatalf("unknown attempt: %v", err)
	}
}

func TestLogsValidatesArguments(t *testing.T) {
	isolateClientEnv(t)
	for _, args := range [][]string{
		{"logs"},
		{"logs", "a", "b"},
		{"logs", "--stream", "journal", "att-1"},
		{"logs", "--tail", "-5", "att-1"},
	} {
		if err := run(args); err == nil {
			t.Fatalf("%v accepted", args)
		}
	}
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
