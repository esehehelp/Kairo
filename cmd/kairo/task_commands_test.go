package main

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var base = time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)

// listFixture is a project of five tasks around base.
func listFixture() *contractAPI {
	f := newContractAPI()
	f.setTask("train-a", map[string]any{"queue": "gpu", "state": "running", "updated_at": stamp(base.Add(-time.Hour)),
		"executions": []map[string]any{{"ordinal": 1, "execution_id": "exe_a", "reason": "initial", "state": "started", "attempt_id": "att_a",
			"attempt_started_at": stamp(base.Add(-time.Hour)), "progress": map[string]any{"current": 120, "total": 500, "unit": "steps"}}}})
	f.setTask("prep", map[string]any{"queue": "cpu", "state": "succeeded", "updated_at": stamp(base.Add(-2 * time.Hour)),
		"executions": []map[string]any{{"ordinal": 1, "execution_id": "exe_p", "state": "terminal", "exit_code": 0,
			"attempt_started_at": stamp(base.Add(-3 * time.Hour)), "exited_at": stamp(base.Add(-2*time.Hour - 15*time.Minute))}}})
	f.setTask("eval-b", map[string]any{"queue": "gpu", "state": "pending", "updated_at": stamp(base.Add(-30 * time.Minute)), "executions": []any{}})
	f.setTask("old", map[string]any{"queue": "cpu", "state": "failed", "updated_at": stamp(base.Add(-72 * time.Hour)),
		"result":     map[string]any{"reason": "nonzero or signaled exit", "exit_code": 1},
		"executions": []map[string]any{{"ordinal": 2, "execution_id": "exe_o", "state": "terminal", "exit_code": 1}}})
	f.setTask("bad", map[string]any{"queue": "cpu", "state": "failed", "updated_at": stamp(base.Add(-time.Hour)),
		"result":     map[string]any{"reason": "nonzero or signaled exit", "exit_code": 2},
		"executions": []map[string]any{{"ordinal": 1, "execution_id": "exe_b", "state": "terminal", "exit_code": 2}}})
	return f
}

func TestTaskListDefaultShowsLiveAndRecentSorted(t *testing.T) {
	useFakeClock(t, base)
	f := listFixture()
	server := f.serve(t)
	out, err := captureStdout(t, func() error { return run([]string{"task", "list", "p", "--api", server.URL}) })
	if err != nil {
		t.Fatal(err)
	}
	got := lines(out)
	if len(got) != 6 {
		t.Fatalf("output:\n%s", out)
	}
	if fields := strings.Fields(got[0]); strings.Join(fields, " ") != "NAME QUEUE STATE EXEC STARTED ELAPSED NOTE" {
		t.Fatalf("header: %q", got[0])
	}
	order := []string{"train-a", "eval-b", "bad", "prep"}
	for i, name := range order {
		if !strings.HasPrefix(got[i+1], name+" ") {
			t.Fatalf("row %d is not %s:\n%s", i+1, name, out)
		}
	}
	if fields := strings.Fields(got[1]); strings.Join(fields, " ") != "train-a gpu running #1 10-13 11:00 1h0m 120/500 steps" {
		t.Fatalf("running row: %q", got[1])
	}
	if fields := strings.Fields(got[2]); strings.Join(fields, " ") != "eval-b gpu pending - - -" {
		t.Fatalf("pending row: %q", got[2])
	}
	if fields := strings.Fields(got[3]); strings.Join(fields, " ") != "bad cpu failed #1 - - exit 2" {
		t.Fatalf("failed row: %q", got[3])
	}
	if fields := strings.Fields(got[4]); strings.Join(fields, " ") != "prep cpu succeeded #1 10-13 09:00 45m0s" {
		t.Fatalf("succeeded row: %q", got[4])
	}
	if got[5] != "p: 1 running, 1 pending, 1 succeeded, 2 failed (4 shown)" {
		t.Fatalf("summary: %q", got[5])
	}
	requests := f.seen()
	if len(requests) != 2 || !strings.Contains(requests[0], "live=1") || !strings.Contains(requests[1], "since=2026-10-12T12%3A00%3A00Z") {
		t.Fatalf("requests: %v", requests)
	}
}

func TestTaskListFiltersAllAndJSON(t *testing.T) {
	useFakeClock(t, base)
	f := listFixture()
	server := f.serve(t)
	out, err := captureStdout(t, func() error { return run([]string{"task", "list", "--api", server.URL, "p", "--all"}) })
	if err != nil || len(lines(out)) != 7 || !strings.Contains(out, "\nold ") {
		t.Fatalf("--all: %v\n%s", err, out)
	}
	out, err = captureStdout(t, func() error {
		return run([]string{"task", "list", "--api", server.URL, "p", "--state", "failed", "--match", "o"})
	})
	if err != nil || len(lines(out)) != 3 || !strings.HasPrefix(lines(out)[1], "old ") || !strings.Contains(out, "exit 1") {
		t.Fatalf("--state --match: %v\n%s", err, out)
	}
	if last := f.seen()[len(f.seen())-1]; !strings.Contains(last, "state=failed") || !strings.Contains(last, "match=o") {
		t.Fatalf("filters not sent: %s", last)
	}
	out, err = captureStdout(t, func() error { return run([]string{"task", "list", "--api", server.URL, "p", "--live", "--json"}) })
	if err != nil {
		t.Fatal(err)
	}
	var parsed struct {
		Project struct {
			Name  string           `json:"name"`
			Tasks []map[string]any `json:"tasks"`
		} `json:"project"`
	}
	if err := json.Unmarshal([]byte(out), &parsed); err != nil || parsed.Project.Name != "p" || len(parsed.Project.Tasks) != 2 || parsed.Project.Tasks[0]["name"] != "train-a" {
		t.Fatalf("--json: %v %s", err, out)
	}
	out, err = captureStdout(t, func() error { return run([]string{"task", "list", "--api", server.URL, "p", "--since", "90m"}) })
	if err != nil || len(lines(out)) != 5 { // train-a, eval-b, bad (header, summary)
		t.Fatalf("--since: %v\n%s", err, out)
	}
	if err := run([]string{"task", "list", "--api", server.URL, "p", "--since", "yesterday"}); err == nil || exitCode(err) != exitUsage {
		t.Fatalf("bad --since: %v", err)
	}
	if err := run([]string{"task", "list", "--api", server.URL, "nope"}); err == nil || !strings.Contains(err.Error(), "project nope not found") {
		t.Fatalf("unknown project: %v", err)
	}
}

func TestTaskPauseResolvesTheQueue(t *testing.T) {
	f := listFixture()
	var scopeQuery string
	f.fallback = func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/scopes":
			scopeQuery = r.URL.RawQuery
			writeJSONResponse(w, http.StatusOK, map[string]any{"scopes": []map[string]string{{"id": "scp_t"}}})
		case "/api/scopes/scp_t/resume":
			writeJSONResponse(w, http.StatusOK, map[string]any{"gate": map[string]any{"state": "open"}})
		default:
			http.NotFound(w, r)
		}
	}
	server := f.serve(t)
	if _, err := captureStdout(t, func() error { return run([]string{"task", "resume", "--api", server.URL, "p", "train-a"}) }); err != nil {
		t.Fatal(err)
	}
	if scopeQuery != "project=p&queue=gpu&task=train-a" {
		t.Fatalf("scope query: %s", scopeQuery)
	}
	if first := f.seen()[0]; first != "GET /api/projects/p?task=train-a" {
		t.Fatalf("lookup: %s", first)
	}
	// Flags may follow the arguments.
	if _, err := captureStdout(t, func() error { return run([]string{"task", "resume", "p", "gpu", "train-a", "--api", server.URL}) }); err != nil {
		t.Fatal(err)
	}
	err := run([]string{"task", "resume", "--api", server.URL, "p", "nope"})
	if err == nil || err.Error() != "task nope not found in project p; give PROJECT QUEUE TASK for tasks submitted without a ProjectSpec" {
		t.Fatalf("unknown task: %v", err)
	}
}

func showFixture() *contractAPI {
	f := newContractAPI()
	f.details["train"] = map[string]any{
		"name": "train", "queue": "gpu", "policy": "RunToCompletion@v1", "state": "running", "spec_digest": "0123456789abcdef0123",
		"depends_on": []string{"prep"}, "created_at": stamp(base.Add(-5 * time.Hour)), "updated_at": stamp(base.Add(-time.Hour)),
		"spec": map[string]any{"name": "train", "queue": "gpu", "policy": "RunToCompletion@v1", "depends_on": []string{"prep"},
			"execution": map[string]any{"argv": []string{"python", "train.py", "--name", "a b"}, "cwd": `D:\work`, "priority": 5,
				"checkpointable": true, "executor": map[string]any{"labels": map[string]string{"os": "windows", "gpu": "yes"}},
				"exclusive": []map[string]any{{"kind": "gpu", "count": 1}}, "capacity": map[string]any{"cpu_millis": 4000, "ram_bytes": 16 << 30},
				"env": map[string]string{"HF_HOME": `D:\hf`}}},
		"dependencies": []map[string]string{{"name": "prep", "state": "succeeded"}},
		"dependents":   []map[string]string{{"name": "eval", "state": "pending"}},
		"gates":        []map[string]string{{"scope": "task", "state": "open"}, {"scope": "project", "state": "open"}},
		"executions": []map[string]any{
			{"ordinal": 1, "execution_id": "exe_1", "reason": "initial", "state": "terminal", "exit_code": 1, "terminal_cause": "process_exit",
				"attempt_started_at": stamp(base.Add(-4 * time.Hour)), "exited_at": stamp(base.Add(-3 * time.Hour)),
				"attempts": []map[string]any{{"id": "att_1", "execution_id": "exe_1", "state": "exited", "executor_id": "local-gpu", "node_id": "pc1",
					"exit_code": 1, "started_at": stamp(base.Add(-4 * time.Hour)), "exited_at": stamp(base.Add(-3 * time.Hour)),
					"stdout_path": `D:\logs\att_1.out`, "stdout_bytes": 2048, "stderr_path": `D:\logs\att_1.err`, "stderr_bytes": 10}}},
			{"ordinal": 2, "execution_id": "exe_2", "reason": "retry", "state": "waiting",
				"gang": []map[string]any{{"rank": 0, "execution_id": "exe_2", "state": "waiting"}, {"rank": 1, "execution_id": "exe_2r1", "state": "authorized", "node_id": "pve0"}},
				"waiting": []map[string]any{
					{"reason_code": "external_claim", "detail": map[string]any{"resource_id": "GPU-1234", "pid": 4242}, "created_at": stamp(base.Add(-10 * time.Minute))},
					{"reason_code": "scope_gate_closed", "scope": "queue"},
				}},
		},
	}
	return f
}

func TestTaskShowRendersTheDetail(t *testing.T) {
	useFakeClock(t, base)
	f := showFixture()
	server := f.serve(t)
	out, err := captureStdout(t, func() error { return run([]string{"task", "show", "--api", server.URL, "p", "train"}) })
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"task:        train\n",
		"project:     p  queue: gpu  policy: RunToCompletion@v1\n",
		"digest:      0123456789ab\n",
		`argv:        python train.py --name "a b"` + "\n",
		"cwd:         D:\\work\n",
		"priority:    5  checkpointable: yes  preemptible: no\n",
		"executor:    gpu=yes os=windows\n",
		"resources:   gpu x1; cpu 4000m; ram 16.0 GiB\n",
		"env:         HF_HOME=D:\\hf\n",
		"depends on:  prep (succeeded)\n",
		"dependents:  eval (pending)\n",
		"gates:       task open, project open\n",
		"execution #1 (initial)  terminal  exit 1  [process_exit]\n",
		"  time:       started 2026-10-13 08:00:00  ended 2026-10-13 09:00:00 (1h0m)\n",
		"  attempt:    att_1 exited node pc1 executor local-gpu exit 1\n",
		"  stdout:     D:\\logs\\att_1.out (2.0 KiB)\n",
		"  stderr:     D:\\logs\\att_1.err (10 B)\n",
		"execution #2 (retry)  waiting\n",
		"  gang:       rank 1 exe_2r1 authorized node pve0\n",
		"  waiting:    external_claim on GPU-1234 (pid=4242) (since 10-13 11:50)\n",
		"  waiting:    scope_gate_closed: the queue is paused\n",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in:\n%s", want, out)
		}
	}
	out, err = captureStdout(t, func() error { return run([]string{"task", "show", "--api", server.URL, "p", "gpu", "train", "--json"}) })
	if err != nil || !strings.HasPrefix(out, `{"task":`) {
		t.Fatalf("--json: %v %q", err, out)
	}
	err = run([]string{"task", "show", "--api", server.URL, "p", "nope"})
	if err == nil || !strings.Contains(err.Error(), "task nope not found in project p") {
		t.Fatalf("unknown task: %v", err)
	}
}

func TestTaskCancelUsesTheCancelRoute(t *testing.T) {
	f := newContractAPI()
	var bodies []map[string]any
	f.cancel = func(task string, body map[string]any) (int, any) {
		bodies = append(bodies, body)
		if task == "live" {
			return http.StatusConflict, map[string]string{"error": "task live is running: pause it first or end it with task pause --force", "code": "orchestration_conflict"}
		}
		return http.StatusOK, map[string]any{"task": task, "state": "cancelled", "withdrawn": []string{"exe_1", "exe_2"}, "dry_run": body["dry_run"]}
	}
	server := f.serve(t)
	out, err := captureStdout(t, func() error { return run([]string{"task", "cancel", "--api", server.URL, "p", "t"}) })
	if err != nil || out != "withdrawn exe_1\nwithdrawn exe_2\ncancelled t\n" {
		t.Fatalf("cancel: %v %q", err, out)
	}
	out, err = captureStdout(t, func() error { return run([]string{"task", "cancel", "--api", server.URL, "--dry-run", "p", "q", "t"}) })
	if err != nil || out != "would withdraw exe_1\nwould withdraw exe_2\nwould cancel t\n" {
		t.Fatalf("dry run: %v %q", err, out)
	}
	if bodies[0]["dry_run"] != false || bodies[1]["dry_run"] != true || bodies[0]["actor"] == "" {
		t.Fatalf("bodies: %v", bodies)
	}
	err = run([]string{"task", "cancel", "--api", server.URL, "p", "live"})
	if err == nil || !strings.Contains(err.Error(), "HTTP 409") || !strings.Contains(err.Error(), "pause --force") {
		t.Fatalf("running task: %v", err)
	}
}

func TestTaskRerun(t *testing.T) {
	f := newContractAPI()
	f.rerun = func(task string, body map[string]any) (int, any) {
		if body["request_id"] == "" || body["actor"] == "" {
			return http.StatusBadRequest, map[string]string{"error": "missing fields"}
		}
		if task == "done" {
			return http.StatusConflict, map[string]string{"error": "task done succeeded; nothing to rerun", "code": "orchestration_conflict"}
		}
		return http.StatusOK, map[string]any{"task": task, "state": "pending", "ordinal": 3, "unblocked": []string{"eval", "report"}}
	}
	server := f.serve(t)
	out, err := captureStdout(t, func() error { return run([]string{"task", "rerun", "--api", server.URL, "p", "train"}) })
	if err != nil || out != "rerun train (execution #3)\nunblocked eval\nunblocked report\n" {
		t.Fatalf("rerun: %v %q", err, out)
	}
	if err := run([]string{"task", "rerun", "--api", server.URL, "p", "done"}); err == nil || !strings.Contains(err.Error(), "nothing to rerun") {
		t.Fatalf("succeeded task: %v", err)
	}
}

func TestUnknownRouteHintsAtAnOlderDaemon(t *testing.T) {
	server := newContractAPI().serve(t) // no rerun handler: the mux's plain 404
	err := run([]string{"task", "rerun", "--api", server.URL, "p", "train"})
	if err == nil || !strings.Contains(err.Error(), "the daemon may be older than this CLI (cli ") || !strings.Contains(err.Error(), "restart it from the same build") {
		t.Fatalf("older daemon: %v", err)
	}
	// A JSON not_found is about the entity, not the route.
	err = run([]string{"task", "show", "--api", server.URL, "p", "train"})
	if err == nil || strings.Contains(err.Error(), "older") {
		t.Fatalf("missing task: %v", err)
	}
}

func TestVersionComparesCLIAndDaemon(t *testing.T) {
	f := newContractAPI()
	f.version = "abc123"
	server := f.serve(t)
	out, err := captureStdout(t, func() error { return run([]string{"version", "--api", server.URL}) })
	if err != nil || !strings.HasPrefix(out, "kairo cli ") || !strings.Contains(out, "\ndaemon abc123\n") {
		t.Fatalf("version: %v %q", err, out)
	}
	out, err = captureStdout(t, func() error { return run([]string{"version", "--api", "http://127.0.0.1:1"}) })
	if err != nil || !strings.Contains(out, "daemon unreachable: ") {
		t.Fatalf("unreachable: %v %q", err, out)
	}
}

const applySpec = `schema_version = 1
[project]
name = "p"
[[tasks]]
name = "t"
queue = "q"
depends_on = []
policy = "RunToCompletion@v1"
[tasks.execution]
schema_version = 2
argv = ["worker"]
cwd = "/work"
checkpointable = false
preemptible = false
[[tasks]]
name = "u"
queue = "q"
depends_on = ["t"]
policy = "RunToCompletion@v1"
[tasks.execution]
schema_version = 2
argv = ["worker"]
cwd = "/work"
checkpointable = false
preemptible = false
`

func TestProjectApplyAndValidateSummaries(t *testing.T) {
	f := newContractAPI()
	reply := map[string]any{"project": "p", "spec_digest": "ff", "idempotent": false, "created": []string{"u"}, "unchanged": 1}
	status := http.StatusCreated
	f.apply = func(map[string]any) (int, any) { return status, reply }
	server := f.serve(t)
	path := filepath.Join(t.TempDir(), "p.toml")
	if err := os.WriteFile(path, []byte(applySpec), 0o600); err != nil {
		t.Fatal(err)
	}
	out, err := captureStdout(t, func() error { return run([]string{"project", "apply", "--api", server.URL, path}) })
	if err != nil || out != "applied p: 1 new task(s), 1 unchanged\n  + u\n" {
		t.Fatalf("apply: %v %q", err, out)
	}
	reply = map[string]any{"project": "p", "spec_digest": "ff", "idempotent": true, "created": []string{}, "unchanged": 2}
	out, err = captureStdout(t, func() error { return run([]string{"project", "apply", "--api", server.URL, path}) })
	if err != nil || out != "applied p: no changes (idempotent)\n" {
		t.Fatalf("idempotent apply: %v %q", err, out)
	}
	out, err = captureStdout(t, func() error { return run([]string{"project", "apply", "--json", "--api", server.URL, path}) })
	if err != nil || !strings.Contains(out, `"idempotent":true`) {
		t.Fatalf("apply --json: %v %q", err, out)
	}
	status, reply = http.StatusConflict, map[string]any{"error": "task t was declared before with different content", "code": "orchestration_conflict"}
	_, err = captureStdout(t, func() error { return run([]string{"project", "apply", "--api", server.URL, path}) })
	if err == nil || !strings.Contains(err.Error(), "task t was declared before") || !strings.Contains(err.Error(), "hint: a task's content is immutable once declared") {
		t.Fatalf("conflict: %v", err)
	}

	out, err = captureStdout(t, func() error { return run([]string{"project", "validate", path}) })
	if err != nil || !strings.HasPrefix(out, "ok p: 2 tasks, digest ") || len(strings.TrimSpace(strings.TrimPrefix(out, "ok p: 2 tasks, digest "))) != 12 {
		t.Fatalf("validate: %v %q", err, out)
	}
	out, err = captureStdout(t, func() error { return run([]string{"project", "validate", "--json", path}) })
	if err != nil || !strings.Contains(out, `"spec_digest"`) || !strings.Contains(out, `"tasks"`) {
		t.Fatalf("validate --json: %v %q", err, out)
	}
}

func TestProjectStatusPassesFilters(t *testing.T) {
	useFakeClock(t, base)
	f := listFixture()
	server := f.serve(t)
	out, err := captureStdout(t, func() error {
		return run([]string{"project", "status", "--api", server.URL, "p", "--task", "bad", "--task", "old", "--state", "failed", "--live", "--match", "b", "--since", "1h"})
	})
	if err != nil || !strings.HasPrefix(out, `{"project":`) {
		t.Fatalf("status: %v %q", err, out)
	}
	want := "GET /api/projects/p?live=1&match=b&since=2026-10-13T11%3A00%3A00Z&state=failed&task=bad&task=old"
	if got := f.seen()[0]; got != want {
		t.Fatalf("request %s, want %s", got, want)
	}
}
