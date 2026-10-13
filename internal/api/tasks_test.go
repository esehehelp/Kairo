package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"kairo/internal/buildinfo"
	"kairo/internal/store"
)

// twoTaskBody declares train and, after it, export.
func twoTaskBody() map[string]any {
	body := projectBody("uv", "run", "train")
	train := body["tasks"].([]any)[0].(map[string]any)
	export := map[string]any{}
	for k, v := range train {
		export[k] = v
	}
	export["name"], export["depends_on"] = "export", []any{"train"}
	body["tasks"] = []any{train, export}
	return body
}

func TestProjectApplyAnswersCompactly(t *testing.T) {
	d := newTestDaemon(t, false)
	var applied struct {
		Project    string   `json:"project"`
		SpecDigest string   `json:"spec_digest"`
		Idempotent bool     `json:"idempotent"`
		Created    []string `json:"created"`
		Unchanged  *int     `json:"unchanged"`
	}
	status, raw := d.call(t, http.MethodPost, "/api/projects", d.admin, projectBody("uv", "run", "train"))
	if status != http.StatusCreated || json.Unmarshal(raw, &applied) != nil || applied.Project != "neo-ime" || applied.SpecDigest == "" || applied.Idempotent || strings.Join(applied.Created, ",") != "train" || applied.Unchanged == nil || *applied.Unchanged != 0 {
		t.Fatalf("apply: %d %s", status, raw)
	}
	if bytes.Contains(raw, []byte(`"tasks"`)) {
		t.Fatalf("apply answered the whole status: %s", raw)
	}
	status, raw = d.call(t, http.MethodPost, "/api/projects", d.admin, twoTaskBody())
	if status != http.StatusCreated || json.Unmarshal(raw, &applied) != nil || strings.Join(applied.Created, ",") != "export" || *applied.Unchanged != 1 {
		t.Fatalf("second apply: %d %s", status, raw)
	}
	status, raw = d.call(t, http.MethodPost, "/api/projects", d.admin, twoTaskBody())
	if status != http.StatusOK || json.Unmarshal(raw, &applied) != nil || !applied.Idempotent || len(applied.Created) != 0 || *applied.Unchanged != 2 {
		t.Fatalf("idempotent apply: %d %s", status, raw)
	}
	changed := twoTaskBody()
	for _, task := range changed["tasks"].([]any) {
		task.(map[string]any)["execution"].(map[string]any)["cwd"] = "/elsewhere"
	}
	status, raw = d.call(t, http.MethodPost, "/api/projects", d.admin, changed)
	if e := expectError(t, "conflicting apply", status, raw, http.StatusConflict, "orchestration_conflict"); !strings.Contains(e.Error, "neo-ime/export, neo-ime/train") {
		t.Fatalf("conflict does not name every task: %s", e.Error)
	}
}

func TestProjectStatusFiltersOverTheAPI(t *testing.T) {
	d := newTestDaemon(t, false)
	if status, raw := d.call(t, http.MethodPost, "/api/projects", d.admin, twoTaskBody()); status != http.StatusCreated {
		t.Fatalf("apply: %d %s", status, raw)
	}
	if err := d.store.ReconcileProjectTasks(t.Context()); err != nil {
		t.Fatal(err)
	}
	read := d.token(t, "dashboard", store.RoleRead, "")
	names := func(query string) string {
		t.Helper()
		status, raw := d.call(t, http.MethodGet, "/api/projects/neo-ime"+query, read, nil)
		var body struct {
			Project store.ProjectStatus `json:"project"`
		}
		if status != http.StatusOK || json.Unmarshal(raw, &body) != nil {
			t.Fatalf("%s: %d %s", query, status, raw)
		}
		if body.Project.StateCounts["running"] != 1 || body.Project.StateCounts["pending"] != 1 {
			t.Fatalf("%s: state counts %+v", query, body.Project.StateCounts)
		}
		var out []string
		for _, task := range body.Project.Tasks {
			out = append(out, task.Name)
		}
		return strings.Join(out, ",")
	}
	for query, want := range map[string]string{
		"":                                   "export,train",
		"?task=train&task=export":            "export,train",
		"?match=ex":                          "export",
		"?state=running":                     "train",
		"?state=pending,running&live=1":      "export,train",
		"?live=1&task=train":                 "train",
		"?since=2000-01-01T00:00:00Z":        "export,train",
		"?since=2999-01-01T09:00:00%2B09:00": "",
	} {
		if got := names(query); got != want {
			t.Fatalf("%s: %q, want %q", query, got, want)
		}
	}
	for _, query := range []string{"?since=yesterday", "?state=bogus", "?live=maybe"} {
		status, raw := d.call(t, http.MethodGet, "/api/projects/neo-ime"+query, read, nil)
		expectError(t, query, status, raw, http.StatusBadRequest, "bad_request")
	}
	status, raw := d.call(t, http.MethodGet, "/api/projects/missing?live=1", read, nil)
	expectError(t, "unknown project", status, raw, http.StatusNotFound, "not_found")
}

func TestTaskDetailOverTheAPI(t *testing.T) {
	d := newTestDaemon(t, false)
	if status, raw := d.call(t, http.MethodPost, "/api/projects", d.admin, twoTaskBody()); status != http.StatusCreated {
		t.Fatalf("apply: %d %s", status, raw)
	}
	ctx := t.Context()
	if err := d.store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	launch := d.launchNext(t)
	stdout := filepath.Join(d.logs, launch.Attempt.ID+".stdout.log")
	if err := os.WriteFile(stdout, []byte("hello\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := d.store.SetAttemptLogPaths(ctx, launch.Attempt.ID, launch.Lease.CoordinationEpoch, stdout, filepath.Join(d.logs, launch.Attempt.ID+".stderr.log")); err != nil {
		t.Fatal(err)
	}
	status, raw := d.call(t, http.MethodGet, "/api/projects/neo-ime/tasks/train", d.token(t, "dashboard", store.RoleRead, ""), nil)
	var body struct {
		Task struct {
			Name       string          `json:"name"`
			State      string          `json:"state"`
			Spec       json.RawMessage `json:"spec"`
			Dependents []store.TaskRef `json:"dependents"`
			Gates      []store.TaskGate
			Executions []struct {
				ExecutionID string                `json:"execution_id"`
				AttemptID   string                `json:"attempt_id"`
				Attempts    []store.AttemptDetail `json:"attempts"`
			} `json:"executions"`
		} `json:"task"`
	}
	if status != http.StatusOK || json.Unmarshal(raw, &body) != nil {
		t.Fatalf("detail: %d %s", status, raw)
	}
	task := body.Task
	if task.Name != "train" || task.State != "running" || !bytes.Contains(task.Spec, []byte(`"queue":"training"`)) || len(task.Dependents) != 1 || task.Dependents[0].Name != "export" || len(task.Gates) != 3 {
		t.Fatalf("detail: %s", raw)
	}
	if len(task.Executions) != 1 || task.Executions[0].AttemptID != launch.Attempt.ID || len(task.Executions[0].Attempts) != 1 {
		t.Fatalf("executions: %s", raw)
	}
	attempt := task.Executions[0].Attempts[0]
	if attempt.StdoutBytes == nil || *attempt.StdoutBytes != 6 || attempt.StderrBytes != nil || attempt.NodeID == nil || *attempt.NodeID != "node" {
		t.Fatalf("attempt: %s", raw)
	}
	status, raw = d.call(t, http.MethodGet, "/api/projects/neo-ime/tasks/missing", d.admin, nil)
	expectError(t, "unknown task", status, raw, http.StatusNotFound, "not_found")
}

// launchNext launches the next waiting execution on executor "executor".
func (d *testDaemon) launchNext(t *testing.T) *store.Launch {
	t.Helper()
	ctx := context.Background()
	if err := d.store.UpsertNode(ctx, store.Node{ID: "node", Name: "node", OS: "linux", Architecture: "amd64", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := d.store.UpsertExecutor(ctx, store.Executor{ID: "executor", NodeID: "node", Kind: "local", Attributes: json.RawMessage(`{}`), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	reservation, err := d.store.ReserveNext(ctx, "executor")
	if err != nil || reservation == nil {
		t.Fatalf("reserve: %+v %v", reservation, err)
	}
	if err = d.store.MarkLeasePrepared(ctx, reservation.Lease.ID, reservation.Lease.CoordinationEpoch); err != nil {
		t.Fatal(err)
	}
	launch, err := d.store.AuthorizeLaunch(ctx, reservation)
	if err != nil {
		t.Fatal(err)
	}
	if err = d.store.ActivateLaunch(ctx, launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch, launch.AuthorizationToken, 100, "launcher:100"); err != nil {
		t.Fatal(err)
	}
	return launch
}

func TestTaskCancelAndRerunOverTheAPI(t *testing.T) {
	d := newTestDaemon(t, false)
	if status, raw := d.call(t, http.MethodPost, "/api/projects", d.admin, twoTaskBody()); status != http.StatusCreated {
		t.Fatalf("apply: %d %s", status, raw)
	}
	ctx := t.Context()
	if err := d.store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	var cancelled store.CancelResult
	status, raw := d.call(t, http.MethodPost, "/api/projects/neo-ime/tasks/train/cancel", d.admin, map[string]any{"dry_run": true})
	if status != http.StatusOK || json.Unmarshal(raw, &cancelled) != nil || !cancelled.DryRun || len(cancelled.Withdrawn) != 1 {
		t.Fatalf("dry run: %d %s", status, raw)
	}
	status, raw = d.call(t, http.MethodPost, "/api/projects/neo-ime/tasks/train/cancel", d.admin, nil)
	if status != http.StatusOK || json.Unmarshal(raw, &cancelled) != nil || cancelled.DryRun || cancelled.State != "cancelled" || cancelled.Task != "train" || len(cancelled.Withdrawn) != 1 {
		t.Fatalf("cancel: %d %s", status, raw)
	}
	status, raw = d.call(t, http.MethodGet, "/api/projects/neo-ime/tasks/train", d.admin, nil)
	if status != http.StatusOK || !bytes.Contains(raw, []byte(`"actor":"operator"`)) {
		t.Fatalf("cancel did not record the token as actor: %s", raw)
	}
	if status, raw = d.call(t, http.MethodPost, "/api/projects/neo-ime/tasks/train/cancel", d.admin, map[string]any{"actor": "again"}); status != http.StatusOK {
		t.Fatalf("repeated cancel: %d %s", status, raw)
	}
	status, raw = d.call(t, http.MethodPost, "/api/projects/neo-ime/tasks/missing/cancel", d.admin, nil)
	expectError(t, "cancel of an unknown task", status, raw, http.StatusNotFound, "not_found")

	var rerun store.RerunResult
	status, raw = d.call(t, http.MethodPost, "/api/projects/neo-ime/tasks/train/rerun", d.admin, map[string]any{"request_id": "r1"})
	if status != http.StatusOK || json.Unmarshal(raw, &rerun) != nil || rerun.State != "pending" || rerun.Ordinal != 1 || rerun.Task != "train" {
		t.Fatalf("rerun: %d %s", status, raw)
	}
	first := string(raw)
	if status, raw = d.call(t, http.MethodPost, "/api/projects/neo-ime/tasks/train/rerun", d.admin, map[string]any{"request_id": "r1"}); status != http.StatusOK || string(raw) != first {
		t.Fatalf("repeated rerun: %d %s, want %s", status, raw, first)
	}
	status, raw = d.call(t, http.MethodPost, "/api/projects/neo-ime/tasks/train/rerun", d.admin, map[string]any{"request_id": "r2"})
	if e := expectError(t, "rerun of a pending task", status, raw, http.StatusConflict, "task_conflict"); e.Error != "task train is pending; only failed, stopped, cancelled or blocked tasks can be rerun" {
		t.Fatalf("rerun conflict: %s", e.Error)
	}
	if err := d.store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	status, raw = d.call(t, http.MethodPost, "/api/projects/neo-ime/tasks/train/cancel", d.admin, nil)
	if status != http.StatusOK {
		t.Fatalf("cancel of the rerun: %d %s", status, raw)
	}
	// The rerun's execution was withdrawn: nothing waits any more.
	if status, raw = d.call(t, http.MethodGet, "/api/executions?state=waiting", d.admin, nil); status != http.StatusOK || !bytes.Contains(raw, []byte(`"executions":[]`)) {
		t.Fatalf("waiting executions: %d %s", status, raw)
	}
}

func TestTaskCancelStartedExecutionConflicts(t *testing.T) {
	d := newTestDaemon(t, false)
	if status, raw := d.call(t, http.MethodPost, "/api/projects", d.admin, projectBody("uv", "run", "train")); status != http.StatusCreated {
		t.Fatalf("apply: %d %s", status, raw)
	}
	if err := d.store.ReconcileProjectTasks(t.Context()); err != nil {
		t.Fatal(err)
	}
	launch := d.launchNext(t)
	status, raw := d.call(t, http.MethodPost, "/api/projects/neo-ime/tasks/train/cancel", d.admin, nil)
	e := expectError(t, "cancel of a running task", status, raw, http.StatusConflict, "task_conflict")
	if want := "execution " + launch.Execution.ID + " of neo-ime/training/train is running (started); pause the task first, or end it with task pause --force"; e.Error != want {
		t.Fatalf("error %q, want %q", e.Error, want)
	}
}

func TestObserveOnlyRefusesTaskCancelButNotItsDryRun(t *testing.T) {
	d := newTestDaemon(t, true)
	if status, raw := d.call(t, http.MethodPost, "/api/projects", d.admin, projectBody("uv", "run", "train")); status != http.StatusCreated {
		t.Fatalf("apply: %d %s", status, raw)
	}
	if status, raw := d.call(t, http.MethodPost, "/api/projects/neo-ime/tasks/train/cancel", d.admin, map[string]any{"dry_run": true}); status != http.StatusOK {
		t.Fatalf("dry run: %d %s", status, raw)
	}
	if status, _ := d.call(t, http.MethodPost, "/api/projects/neo-ime/tasks/train/cancel", d.admin, nil); status != http.StatusLocked {
		t.Fatalf("observe-only cancel: %d", status)
	}
}

func TestObserveOnlyRefusesTaskRerun(t *testing.T) {
	d := newTestDaemon(t, true)
	if status, raw := d.call(t, http.MethodPost, "/api/projects", d.admin, projectBody("uv", "run", "train")); status != http.StatusCreated {
		t.Fatalf("apply: %d %s", status, raw)
	}
	status, raw := d.call(t, http.MethodPost, "/api/projects/neo-ime/tasks/train/rerun", d.admin, map[string]any{"request_id": "r1"})
	expectError(t, "observe-only rerun", status, raw, http.StatusLocked, "observe_only")
}

func TestHealthAndWhoAmIReportTheBuild(t *testing.T) {
	d := newTestDaemon(t, false)
	want := []byte(`"version":"` + buildinfo.Version() + `"`)
	for _, c := range []struct{ path, token string }{{"/health", ""}, {"/api/whoami", d.admin}} {
		if status, raw := d.call(t, http.MethodGet, c.path, c.token, nil); status != http.StatusOK || !bytes.Contains(raw, want) {
			t.Fatalf("%s: %d %s", c.path, status, raw)
		}
	}
}
