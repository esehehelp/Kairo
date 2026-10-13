package main

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
)

// logsFixture: task "train" with execution #1 (att_1, exited) and #2
// (att_2, running); task "gang" with a two-rank execution.
func logsFixture() *contractAPI {
	f := newContractAPI()
	f.details["train"] = map[string]any{"name": "train", "queue": "gpu", "state": "running", "executions": []map[string]any{
		{"ordinal": 1, "execution_id": "exe_1", "state": "terminal", "attempt_id": "att_1",
			"attempts": []map[string]any{{"id": "att_1", "execution_id": "exe_1", "state": "exited"}}},
		{"ordinal": 2, "execution_id": "exe_2", "state": "started", "attempt_id": "att_2",
			"attempts": []map[string]any{{"id": "att_2", "execution_id": "exe_2", "state": "running"}}},
	}}
	f.details["gang"] = map[string]any{"name": "gang", "queue": "gpu", "state": "running", "executions": []map[string]any{
		{"ordinal": 1, "execution_id": "exe_g0", "state": "started", "attempt_id": "att_g0",
			"gang": []map[string]any{
				{"rank": 0, "execution_id": "exe_g0", "state": "started", "attempt_id": "att_g0"},
				{"rank": 1, "execution_id": "exe_g1", "state": "started", "attempt_id": "att_g1"},
			}},
	}}
	f.details["fresh"] = map[string]any{"name": "fresh", "queue": "gpu", "state": "pending", "executions": []any{}}
	f.setTask("fresh", map[string]any{"queue": "gpu", "state": "pending", "executions": []any{}})
	f.attempts["exe_2"] = []map[string]any{{"id": "att_2", "execution_id": "exe_2", "state": "running"}}
	var numbered strings.Builder
	for i := 1; i <= 100; i++ {
		fmt.Fprintf(&numbered, "line %d\n", i)
	}
	f.appendLog("att_2", "stdout", numbered.String())
	f.appendLog("att_2", "stderr", "warn\n")
	f.appendLog("att_1", "stdout", "first run\n")
	f.appendLog("att_1", "stderr", "")
	f.appendLog("att_g0", "stdout", "rank zero\n")
	f.appendLog("att_g1", "stdout", "rank one\n")
	return f
}

func TestTaskLogsTailsBothStreamsOfTheLatestAttempt(t *testing.T) {
	f := logsFixture()
	server := f.serve(t)
	out, err := captureStdout(t, func() error { return run([]string{"task", "logs", "--api", server.URL, "p", "train"}) })
	if err != nil {
		t.Fatal(err)
	}
	got := lines(out)
	if len(got) != 53 || got[0] != "==> stdout (att_2) <==" || got[1] != "line 51" || got[50] != "line 100" || got[51] != "==> stderr (att_2) <==" {
		t.Fatalf("output:\n%s", out)
	}
	if !strings.HasSuffix(out, "line 100\n==> stderr (att_2) <==\nwarn\n") {
		t.Fatalf("stderr part:\n%s", out)
	}
	for _, r := range f.seen() {
		if strings.Contains(r, "/log?") && !strings.Contains(r, "tail=") {
			t.Fatalf("tail fetch without tail=: %s", r)
		}
	}

	out, err = captureStdout(t, func() error {
		return run([]string{"task", "logs", "--api", server.URL, "p", "gpu", "train", "--tail", "3", "--stream", "stdout"})
	})
	if err != nil || out != "line 98\nline 99\nline 100\n" {
		t.Fatalf("--tail 3 --stream stdout: %v %q", err, out)
	}
	out, err = captureStdout(t, func() error {
		return run([]string{"task", "logs", "--api", server.URL, "p", "train", "--tail", "0", "--stream", "stdout"})
	})
	if err != nil || len(lines(out)) != 100 {
		t.Fatalf("--tail 0: %v %d lines", err, len(lines(out)))
	}
	out, err = captureStdout(t, func() error {
		return run([]string{"task", "logs", "--api", server.URL, "p", "train", "--attempt", "1"})
	})
	if err != nil || out != "==> stdout (att_1) <==\nfirst run\n==> stderr (att_1) <==\n" {
		t.Fatalf("--attempt 1: %v %q", err, out)
	}
	out, err = captureStdout(t, func() error {
		return run([]string{"task", "logs", "--api", server.URL, "p", "train", "--attempt", "all", "--stream", "stdout", "--tail", "1"})
	})
	if err != nil || out != "==> #1 stdout (att_1) <==\nfirst run\n==> #2 stdout (att_2) <==\nline 100\n" {
		t.Fatalf("--attempt all: %v %q", err, out)
	}
	if err := run([]string{"task", "logs", "--api", server.URL, "p", "train", "--attempt", "7"}); err == nil || !strings.Contains(err.Error(), "no execution #7 (it has: [#1 #2])") {
		t.Fatalf("--attempt 7: %v", err)
	}
	if err := run([]string{"task", "logs", "--api", server.URL, "p", "fresh"}); err == nil || !strings.Contains(err.Error(), "no attempt to show yet (task fresh is pending)") {
		t.Fatalf("no attempt: %v", err)
	}
	if err := run([]string{"task", "logs", "--api", server.URL, "p", "train", "-f", "--attempt", "all"}); err == nil || exitCode(err) != exitUsage {
		t.Fatalf("-f --attempt all: %v", err)
	}
}

func TestTaskLogsGangRank(t *testing.T) {
	server := logsFixture().serve(t)
	out, err := captureStdout(t, func() error {
		return run([]string{"task", "logs", "--api", server.URL, "p", "gang", "--stream", "stdout"})
	})
	if err != nil || out != "rank zero\n" {
		t.Fatalf("rank 0: %v %q", err, out)
	}
	out, err = captureStdout(t, func() error {
		return run([]string{"task", "logs", "--api", server.URL, "p", "gang", "--stream", "stdout", "--rank", "1"})
	})
	if err != nil || out != "rank one\n" {
		t.Fatalf("rank 1: %v %q", err, out)
	}
	if err := run([]string{"task", "logs", "--api", server.URL, "p", "gang", "--rank", "2"}); err == nil || !strings.Contains(err.Error(), "no gang rank 2") {
		t.Fatalf("rank 2: %v", err)
	}
	if err := run([]string{"task", "logs", "--api", server.URL, "p", "train", "--rank", "1"}); err == nil || !strings.Contains(err.Error(), "not a gang") {
		t.Fatalf("--rank on a plain task: %v", err)
	}
}

func TestTaskLogsTailGrowsUntilTheLinesAreCovered(t *testing.T) {
	f := logsFixture()
	long := strings.Repeat("x", 999)
	var text strings.Builder
	for i := 1; i <= 300; i++ {
		fmt.Fprintf(&text, "%03d%s\n", i, long)
	}
	f.logs["att_2"]["stdout"] = []byte(text.String())
	server := f.serve(t)
	out, err := captureStdout(t, func() error {
		return run([]string{"task", "logs", "--api", server.URL, "p", "train", "--stream", "stdout", "--tail", "100"})
	})
	got := lines(out)
	if err != nil || len(got) != 100 || !strings.HasPrefix(got[0], "201x") || !strings.HasPrefix(got[99], "300x") {
		t.Fatalf("--tail 100 of 1000-byte lines: %v, %d lines", err, len(got))
	}
	var tails []string
	for _, r := range f.seen() {
		if strings.Contains(r, "tail=") {
			tails = append(tails, r[strings.Index(r, "tail="):])
		}
	}
	if len(tails) != 2 || tails[0] != "tail=65536" || tails[1] != "tail=262144" {
		t.Fatalf("tail fetches: %v", tails)
	}
}

func TestTaskLogsFollowStopsWhenTheAttemptExited(t *testing.T) {
	f := logsFixture()
	clock := useFakeClock(t, base)
	clock.onSleep = func(n int) {
		switch n {
		case 1:
			f.appendLog("att_2", "stdout", "line 101\npartial")
		case 2:
			f.appendLog("att_2", "stderr", "oops\n")
		case 3:
			f.appendLog("att_2", "stdout", " end\n")
			f.mu.Lock()
			f.attempts["exe_2"] = []map[string]any{{"id": "att_2", "execution_id": "exe_2", "state": "exited", "exited_at": stamp(base)}}
			f.mu.Unlock()
		}
	}
	server := f.serve(t)
	out, err := captureStdout(t, func() error {
		return run([]string{"task", "logs", "--api", server.URL, "p", "train", "--tail", "1", "-f"})
	})
	if err != nil {
		t.Fatal(err)
	}
	want := "==> stdout (att_2) <==\nline 100\n==> stderr (att_2) <==\nwarn\n" +
		"==> stdout (att_2) <==\nline 101\npartial\n==> stderr (att_2) <==\noops\n==> stdout (att_2) <==\n end\n"
	if out != want {
		t.Fatalf("follow output:\n%q\nwant\n%q", out, want)
	}
	// exited at the 3rd poll (with output), quiet at the 4th and 5th.
	if clock.sleeps != 5 {
		t.Fatalf("polls: %d", clock.sleeps)
	}
	followed := 0
	for _, r := range f.seen() {
		if strings.Contains(r, "offset=") {
			followed++
		}
	}
	if followed != 10 {
		t.Fatalf("follow fetches: %d", followed)
	}
}

func TestTaskLogsFollowEndsCleanlyOnInterrupt(t *testing.T) {
	f := logsFixture()
	clock := useFakeClock(t, base)
	clock.stopAfter = 3
	server := f.serve(t)
	out, err := captureStdout(t, func() error {
		return run([]string{"task", "logs", "--api", server.URL, "p", "train", "--tail", "1", "--stream", "stderr", "--follow"})
	})
	if err != nil || out != "warn\n" {
		t.Fatalf("interrupted follow: %v %q", err, out)
	}
}

func TestTaskLogsFollowWaitsForTheFirstAttempt(t *testing.T) {
	f := logsFixture()
	clock := useFakeClock(t, base)
	clock.onSleep = func(n int) {
		if n == 2 {
			executions := []map[string]any{{"ordinal": 1, "execution_id": "exe_f", "state": "started", "attempt_id": "att_f"}}
			f.setTask("fresh", map[string]any{"state": "running", "executions": executions})
			f.mu.Lock()
			f.details["fresh"]["executions"] = executions
			f.attempts["exe_f"] = []map[string]any{{"id": "att_f", "execution_id": "exe_f", "state": "exited"}}
			f.logs["att_f"] = map[string][]byte{"stdout": []byte("hello\n")}
			f.mu.Unlock()
		}
	}
	server := f.serve(t)
	out, err := captureStdout(t, func() error {
		return run([]string{"task", "logs", "--api", server.URL, "p", "fresh", "--stream", "stdout", "-f"})
	})
	if err != nil || out != "hello\n" {
		t.Fatalf("follow before start: %v %q", err, out)
	}
	// While no attempt exists, the polls read the task's status, not its detail.
	details, polls := 0, 0
	for _, r := range f.seen() {
		switch r {
		case "GET /api/projects/p/tasks/fresh":
			details++
		case "GET /api/projects/p?task=fresh":
			polls++
		}
	}
	if details != 2 || polls != 2 {
		t.Fatalf("detail fetches %d, status polls %d: %v", details, polls, f.seen())
	}
}

func TestExecutionLogs(t *testing.T) {
	f := newContractAPI()
	f.attempts["exe_9"] = []map[string]any{{"id": "att_9", "execution_id": "exe_9", "state": "running", "started_at": stamp(base)}}
	f.appendLog("att_9", "stdout", "started\n")
	server := f.serve(t)
	for _, attempt := range []string{"latest", "1"} {
		out, err := captureStdout(t, func() error {
			return run([]string{"execution", "logs", "--api", server.URL, "exe_9", "--stream", "stdout", "--attempt", attempt})
		})
		if err != nil || out != "started\n" {
			t.Fatalf("--attempt %s: %v %q", attempt, err, out)
		}
	}
	out, err := captureStdout(t, func() error {
		return run([]string{"execution", "logs", "--api", server.URL, "exe_9", "--stream", "stdout", "--attempt", "all"})
	})
	if err != nil || out != "==> stdout (att_9) <==\nstarted\n" {
		t.Fatalf("all: %v %q", err, out)
	}
	if err := run([]string{"execution", "logs", "--api", server.URL, "exe_9", "--attempt", "2"}); err == nil || !strings.Contains(err.Error(), "has 1 attempt(s)") {
		t.Fatalf("--attempt 2: %v", err)
	}
	if !strings.Contains(strings.Join(f.seen(), "\n"), "GET /api/attempts?execution_id=exe_9") {
		t.Fatalf("requests: %v", f.seen())
	}
	if err := run([]string{"execution", "logs", "--api", server.URL, "exe_none"}); err == nil || !strings.Contains(err.Error(), "no attempt yet") {
		t.Fatalf("no attempts: %v", err)
	}
}

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
	serveLogBytes(w, r, l.log)
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
	if got := api.queries(); len(got) != 1 || got[0] != "limit=8388608&offset=0&stream=stdout" {
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
	if got := api.queries(); len(got) != 1 || got[0] != "stream=stderr&tail=3" {
		t.Fatalf("requests %v", got)
	}
	// A tail longer than the log prints all of it.
	if out, err = captureStdout(t, func() error {
		return run([]string{"logs", "--api", server.URL, "--tail", "50", "att-1"})
	}); err != nil || out != "0123456789" {
		t.Fatalf("long tail: %q %v", out, err)
	}
}

func TestLogsFollowPollsFromTheNextOffsetUntilInterrupted(t *testing.T) {
	api := &logAPI{log: []byte("a")}
	server := fakeAPI(t, api)
	defer server.Close()
	clock := useFakeClock(t, base)
	clock.onSleep = func(n int) {
		if n == 1 {
			api.append("bc")
		}
	}
	clock.stopAfter = 3
	out, err := captureStdout(t, func() error { return run([]string{"logs", "--api", server.URL, "-f", "att-1"}) })
	if err != nil || out != "abc" {
		t.Fatalf("follow: %v %q", err, out)
	}
	// The log did not grow at the second poll: kairo logs keeps following.
	want := "limit=8388608&offset=0&stream=stderr limit=8388608&offset=1&stream=stderr limit=8388608&offset=3&stream=stderr"
	if got := api.queries(); strings.Join(got, " ") != want {
		t.Fatalf("requests %v", got)
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
