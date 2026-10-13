package main

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

// waitFixture: a pending, b running, c succeeded a day ago.
func waitFixture() *contractAPI {
	f := newContractAPI()
	f.setTask("a", map[string]any{"queue": "q", "state": "pending", "updated_at": stamp(base.Add(-time.Hour)), "executions": []any{}})
	f.setTask("b", map[string]any{"queue": "q", "state": "running", "updated_at": stamp(base.Add(-time.Hour)),
		"executions": []map[string]any{{"ordinal": 1, "execution_id": "exe_b", "state": "started", "attempt_id": "att_b"}}})
	f.setTask("c", map[string]any{"queue": "q", "state": "succeeded", "updated_at": stamp(base.Add(-24 * time.Hour)),
		"executions": []map[string]any{{"ordinal": 1, "execution_id": "exe_c", "state": "terminal", "exit_code": 0}}})
	return f
}

func ended(state string, exit int) map[string]any {
	update := map[string]any{"state": state, "executions": []map[string]any{{"ordinal": 1, "execution_id": "exe", "state": "terminal", "exit_code": exit, "attempt_id": "att"}}}
	if state != "succeeded" {
		update["result"] = map[string]any{"reason": "nonzero or signaled exit", "exit_code": exit}
	}
	return update
}

func running(progress map[string]any) map[string]any {
	execution := map[string]any{"ordinal": 1, "execution_id": "exe", "state": "started", "attempt_id": "att"}
	if progress != nil {
		execution["progress"] = progress
	}
	return map[string]any{"state": "running", "executions": []map[string]any{execution}}
}

func TestTaskWaitPrintsTransitionsUntilAllSucceed(t *testing.T) {
	f := waitFixture()
	clock := useFakeClock(t, base)
	clock.onSleep = func(n int) {
		switch n {
		case 1:
			f.setTask("a", running(nil))
		case 2:
			f.setTask("b", ended("succeeded", 0))
		case 3:
			f.setTask("a", ended("succeeded", 0))
		}
	}
	server := f.serve(t)
	out, err := captureStdout(t, func() error { return run([]string{"task", "wait", "--api", server.URL, "p", "q", "a", "b"}) })
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"12:00:00 a pending",
		"12:00:00 b running",
		"12:00:10 a pending -> running",
		"12:00:20 b running -> succeeded",
		"12:00:30 a running -> succeeded",
		"12:00:30 DONE 2 task(s) succeeded",
	}
	if strings.Join(lines(out), "\n") != strings.Join(want, "\n") {
		t.Fatalf("output:\n%s", out)
	}
	if got := f.seen()[0]; got != "GET /api/projects/p?task=q&task=a&task=b" {
		t.Fatalf("poll: %s", got)
	}
}

func TestTaskWaitExitCodes(t *testing.T) {
	f := waitFixture()
	clock := useFakeClock(t, base)
	clock.onSleep = func(n int) {
		if n == 1 {
			f.setTask("b", ended("failed", 3))
		}
	}
	server := f.serve(t)
	out, err := captureStdout(t, func() error { return run([]string{"task", "wait", "--api", server.URL, "p", "b", "c"}) })
	if exitCode(err) != waitFailed || !strings.Contains(out, "12:00:10 b running -> failed exit 3\n") || !strings.HasSuffix(out, "12:00:10 DONE not succeeded: b (failed)\n") {
		t.Fatalf("failure: %v\n%s", err, out)
	}
	var stderr strings.Builder
	reportError(&stderr, err)
	if stderr.String() != "" {
		t.Fatalf("exit 1 printed %q", stderr.String())
	}

	clock = useFakeClock(t, base)
	out, err = captureStdout(t, func() error {
		return run([]string{"task", "wait", "--api", server.URL, "p", "a", "--timeout", "25s", "--interval", "10s"})
	})
	if exitCode(err) != waitTimeout || !strings.HasSuffix(out, "12:00:30 TIMEOUT after 25s: a (pending)\n") {
		t.Fatalf("timeout: %v\n%s", err, out)
	}

	for _, args := range [][]string{
		{"p", "nope"},
		{"p", "a", "nope"},
		{"other", "a"},
		{"p"},
		{"p", "--match", "zz"},
		{"p", "a", "--until", "soon"},
		{"p", "a", "--bogus"},
	} {
		_, err := captureStdout(t, func() error { return run(append([]string{"task", "wait", "--api", server.URL}, args...)) })
		if exitCode(err) != waitNotFound {
			t.Fatalf("%v: exit %d (%v)", args, exitCode(err), err)
		}
	}
}

func TestTaskWaitRidesOutAnOutage(t *testing.T) {
	f := waitFixture()
	clock := useFakeClock(t, base)
	clock.onSleep = func(n int) {
		switch n {
		case 1:
			f.mu.Lock()
			f.projectFailures, f.failureStatus = 2, http.StatusServiceUnavailable
			f.mu.Unlock()
		case 4:
			f.setTask("a", ended("succeeded", 0))
		}
	}
	server := f.serve(t)
	out, err := captureStdout(t, func() error { return run([]string{"task", "wait", "--api", server.URL, "p", "a"}) })
	want := []string{
		"12:00:00 a pending",
		"12:00:10 DOWN HTTP 503 internal: database is locked",
		"12:00:30 UP",
		"12:00:40 a pending -> succeeded",
		"12:00:40 DONE 1 task(s) succeeded",
	}
	if err != nil || strings.Join(lines(out), "\n") != strings.Join(want, "\n") {
		t.Fatalf("outage: %v\n%s", err, out)
	}
}

func TestTaskWaitMatchIncludesNewTasksAndUntilStarted(t *testing.T) {
	f := waitFixture()
	clock := useFakeClock(t, base)
	clock.onSleep = func(n int) {
		switch n {
		case 1:
			f.setTask("a2", map[string]any{"queue": "q", "state": "pending", "updated_at": stamp(base), "executions": []any{}})
			f.setTask("a", running(nil))
		case 2:
			f.setTask("a2", running(map[string]any{"current": 5, "total": 10, "unit": "steps"}))
		}
	}
	server := f.serve(t)
	out, err := captureStdout(t, func() error {
		return run([]string{"task", "wait", "--api", server.URL, "p", "--match", "a", "--until", "started"})
	})
	want := []string{
		"12:00:00 a pending",
		"12:00:10 a pending -> running",
		"12:00:10 a2 pending",
		"12:00:20 a2 pending -> running 5/10 steps",
		"12:00:20 DONE 2 task(s) started",
	}
	if err != nil || strings.Join(lines(out), "\n") != strings.Join(want, "\n") {
		t.Fatalf("match: %v\n%s", err, out)
	}
	if got := f.seen()[0]; got != "GET /api/projects/p?match=a" {
		t.Fatalf("poll: %s", got)
	}
}

func TestTaskWaitReportsAStallOnce(t *testing.T) {
	f := waitFixture()
	attempt := func(bytes int) map[string]any {
		return map[string]any{"name": "b", "queue": "q", "state": "running", "executions": []map[string]any{{"ordinal": 1, "execution_id": "exe_b", "state": "started",
			"attempts": []map[string]any{{"id": "att_b", "state": "running", "stdout_bytes": bytes, "stderr_bytes": 7}}}}}
	}
	f.details["b"] = attempt(100)
	clock := useFakeClock(t, base)
	clock.onSleep = func(n int) {
		f.mu.Lock()
		defer f.mu.Unlock()
		switch n {
		case 6:
			f.details["b"] = attempt(200)
		}
		if n == 12 {
			f.mu.Unlock()
			f.setTask("b", ended("succeeded", 0))
			f.mu.Lock()
		}
	}
	server := f.serve(t)
	out, err := captureStdout(t, func() error {
		return run([]string{"task", "wait", "--api", server.URL, "p", "b", "--stall", "30s"})
	})
	want := []string{
		"12:00:00 b running",
		"12:00:30 b STALL no log output for 30s",
		"12:01:30 b STALL no log output for 30s",
		"12:02:00 b running -> succeeded",
		"12:02:00 DONE 1 task(s) succeeded",
	}
	if err != nil || strings.Join(lines(out), "\n") != strings.Join(want, "\n") {
		t.Fatalf("stall: %v\n%s", err, out)
	}
}

func TestProjectWatchStreamsChangesAndExitsWhenIdle(t *testing.T) {
	f := waitFixture()
	clock := useFakeClock(t, base)
	set := func(name string, update map[string]any) {
		update["updated_at"] = stamp(clock.now)
		f.setTask(name, update)
	}
	set("b", running(map[string]any{"current": 1, "total": 10, "unit": "steps"}))
	f.setTask("b", map[string]any{"updated_at": stamp(base.Add(-time.Hour))})
	clock.onSleep = func(n int) {
		switch n {
		case 1:
			set("b", running(map[string]any{"current": 2, "total": 10, "unit": "steps"}))
		case 6:
			set("b", running(map[string]any{"current": 7, "total": 10, "unit": "steps"}))
		case 7:
			set("a", running(nil))
		case 8:
			set("b", ended("succeeded", 0))
		case 9:
			set("a", ended("failed", 1))
		}
	}
	server := f.serve(t)
	out, err := captureStdout(t, func() error { return run([]string{"project", "watch", "--api", server.URL, "p", "--exit-when-idle"}) })
	want := []string{
		"12:00:00 a pending",
		"12:00:00 b running 1/10 steps",
		"12:00:00 SUMMARY p: 1 running, 1 pending, 1 succeeded",
		"12:01:00 b PROGRESS 7/10 steps",
		"12:01:10 a pending -> running",
		"12:01:20 b running -> succeeded",
		"12:01:30 a running -> failed exit 1",
		"12:01:30 IDLE not succeeded: a (failed)",
	}
	if exitCode(err) != 1 || strings.Join(lines(out), "\n") != strings.Join(want, "\n") {
		t.Fatalf("watch: %v\n%s", err, out)
	}
	requests := f.seen()
	if !strings.Contains(requests[0], "live=1") || !strings.Contains(requests[1], "since=2026-10-13T11%3A59%3A50Z") {
		t.Fatalf("requests: %v", requests[:2])
	}
}

func TestProjectWatchSummariesAndIdleSuccess(t *testing.T) {
	f := waitFixture()
	clock := useFakeClock(t, base)
	clock.stopAfter = 3
	server := f.serve(t)
	out, err := captureStdout(t, func() error {
		return run([]string{"project", "watch", "--api", server.URL, "p", "--interval", "5m", "--match", "b"})
	})
	want := []string{
		"12:00:00 b running",
		"12:00:00 SUMMARY p: 1 running, 1 pending, 1 succeeded",
		"12:10:00 SUMMARY p: 1 running, 1 pending, 1 succeeded",
	}
	if err != nil || strings.Join(lines(out), "\n") != strings.Join(want, "\n") {
		t.Fatalf("summaries: %v\n%s", err, out)
	}

	f.setTask("a", ended("succeeded", 0))
	f.setTask("b", ended("succeeded", 0))
	useFakeClock(t, base)
	out, err = captureStdout(t, func() error { return run([]string{"project", "watch", "--api", server.URL, "p", "--exit-when-idle"}) })
	if err != nil || !strings.HasSuffix(out, "12:00:00 IDLE nothing pending or running\n") {
		t.Fatalf("idle: %v\n%s", err, out)
	}
}
