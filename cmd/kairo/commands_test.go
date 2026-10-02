package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestProjectPauseWaitsForDurableOperationByDefault(t *testing.T) {
	var mu sync.Mutex
	var requests []string
	server := fakeAPI(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests = append(requests, r.Method+" "+r.URL.Path)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/scopes":
			_ = json.NewEncoder(w).Encode(map[string]any{"scopes": []map[string]string{{"id": "scp_project"}}})
		case "/api/scopes/scp_project/pause":
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{"operation_id": "pop_one"})
		case "/api/pause-operations/pop_one":
			_ = json.NewEncoder(w).Encode(map[string]any{"operation": map[string]string{"state": "quiesced"}, "targets": []any{}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	if err := projectCommand([]string{"pause", "--api", server.URL, "--timeout", time.Second.String(), "p"}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	want := []string{
		"GET /api/scopes",
		"POST /api/scopes/scp_project/pause",
		"GET /api/pause-operations/pop_one",
	}
	if len(requests) != len(want) {
		t.Fatalf("request sequence = %v, want %v", requests, want)
	}
	for i := range want {
		if requests[i] != want[i] {
			t.Fatalf("request sequence = %v, want %v", requests, want)
		}
	}
}

func TestProjectResumeOnlyOpensGate(t *testing.T) {
	var mu sync.Mutex
	var requests []string
	server := fakeAPI(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests = append(requests, r.Method+" "+r.URL.Path)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/scopes":
			_ = json.NewEncoder(w).Encode(map[string]any{"scopes": []map[string]string{{"id": "scp_project"}}})
		case "/api/scopes/scp_project/resume":
			_ = json.NewEncoder(w).Encode(map[string]any{"gate": map[string]any{"state": "open", "generation": 3}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	if err := projectCommand([]string{"resume", "--api", server.URL, "p"}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	want := []string{"GET /api/scopes", "POST /api/scopes/scp_project/resume"}
	if len(requests) != len(want) {
		t.Fatalf("request sequence = %v, want %v", requests, want)
	}
	for i := range want {
		if requests[i] != want[i] {
			t.Fatalf("request sequence = %v, want %v", requests, want)
		}
	}
}

func TestProjectApplyUsesOrchestrationContract(t *testing.T) {
	var gotMethod, gotPath string
	server := fakeAPI(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath = r.Method, r.URL.Path
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		var declaration map[string]any
		if err := json.Unmarshal(body, &declaration); err != nil {
			t.Error(err)
		}
		if declaration["schema_version"] != float64(1) {
			t.Errorf("declaration=%v", declaration)
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("{}\n"))
	}))
	defer server.Close()

	path := filepath.Join(t.TempDir(), "project.toml")
	source := `schema_version = 1
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
`
	if err := os.WriteFile(path, []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := projectCommand([]string{"apply", "--api", server.URL, path}); err != nil {
		t.Fatal(err)
	}
	if gotMethod != http.MethodPost || gotPath != "/api/projects" {
		t.Fatalf("request=%s %s", gotMethod, gotPath)
	}
}

func TestTaskCancelWithdrawsEveryExecutionNotStarted(t *testing.T) {
	var mu sync.Mutex
	var posts []string
	server := fakeAPI(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/executions":
			if r.URL.Query().Get("task") != "t" || r.URL.Query().Get("queue") != "q" {
				http.Error(w, "bad filter", http.StatusBadRequest)
				return
			}
			started := "2026-01-01T00:00:00Z"
			_ = json.NewEncoder(w).Encode(map[string]any{"executions": []map[string]any{
				{"id": "exe_done", "state": "terminal", "started_at": started},
				{"id": "exe_rank0", "state": "waiting"},
				{"id": "exe_rank1", "state": "waiting"},
			}})
		case r.Method == http.MethodPost:
			mu.Lock()
			posts = append(posts, r.URL.Path)
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	if err := taskCommand([]string{"cancel", "--api", server.URL, "p", "q", "t"}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	want := []string{"/api/executions/exe_rank0/withdraw", "/api/executions/exe_rank1/withdraw"}
	if len(posts) != len(want) || posts[0] != want[0] || posts[1] != want[1] {
		t.Fatalf("withdraw posts = %v, want %v", posts, want)
	}
}

func TestTaskCancelRefusesARunningExecution(t *testing.T) {
	posted := false
	server := fakeAPI(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost {
			posted = true
		}
		started := "2026-01-01T00:00:00Z"
		_ = json.NewEncoder(w).Encode(map[string]any{"executions": []map[string]any{
			{"id": "exe_waiting", "state": "waiting"},
			{"id": "exe_live", "state": "started", "started_at": started},
		}})
	}))
	defer server.Close()

	if err := taskCommand([]string{"cancel", "--api", server.URL, "p", "q", "t"}); err == nil {
		t.Fatal("task cancel withdrew around a running execution")
	}
	if posted {
		t.Fatal("task cancel posted a withdraw while an execution was running")
	}
}

// pauseServer answers a task pause; the operation ends in finalState.
func pauseServer(t *testing.T, bodies *[]map[string]any, operation map[string]any) *httptest.Server {
	t.Helper()
	var mu sync.Mutex
	return fakeAPI(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/scopes":
			_ = json.NewEncoder(w).Encode(map[string]any{"scopes": []map[string]string{{"id": "scp_task"}}})
		case "/api/scopes/scp_task/pause":
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			mu.Lock()
			*bodies = append(*bodies, body)
			mu.Unlock()
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{"operation_id": "pop_one"})
		case "/api/pause-operations/pop_one":
			_ = json.NewEncoder(w).Encode(operation)
		default:
			http.NotFound(w, r)
		}
	}))
}

func TestTaskPauseForceSendsTheForceStop(t *testing.T) {
	var bodies []map[string]any
	server := pauseServer(t, &bodies, map[string]any{
		"operation":   map[string]any{"state": "quiesced"},
		"targets":     []any{},
		"force_stops": []map[string]any{{"execution_id": "exe_1", "state": "terminated", "detail": map[string]any{"killed": true}}},
	})
	defer server.Close()
	if err := taskCommand([]string{"pause", "--api", server.URL, "-f", "--grace", "90s", "--reason", "superseded", "--timeout", "1s", "p", "q", "t"}); err != nil {
		t.Fatal(err)
	}
	if err := taskCommand([]string{"pause", "--api", server.URL, "--timeout", "1s", "p", "q", "t"}); err != nil {
		t.Fatal(err)
	}
	if len(bodies) != 2 {
		t.Fatalf("bodies: %v", bodies)
	}
	forced, plain := bodies[0], bodies[1]
	if forced["force"] != true || forced["grace_seconds"] != float64(90) || forced["reason"] != "superseded" {
		t.Fatalf("forced body: %v", forced)
	}
	// a plain pause sends only the actor and request id
	for _, key := range []string{"force", "grace_seconds", "reason"} {
		if _, ok := plain[key]; ok {
			t.Fatalf("plain pause sent %s: %v", key, plain)
		}
	}
}

func TestTaskPauseForceFlagValidation(t *testing.T) {
	for _, args := range [][]string{
		{"pause", "--grace", "10s", "p", "q", "t"},
		{"pause", "--reason", "x", "p", "q", "t"},
		{"pause", "--force", "--grace", "2h", "p", "q", "t"},
		{"resume", "--force", "p", "q", "t"},
	} {
		if err := taskCommand(append([]string{args[0], "--api", "http://127.0.0.1:1"}, args[1:]...)); err == nil || !strings.Contains(err.Error(), "--") {
			t.Fatalf("%v: %v", args, err)
		}
	}
}

func TestBlockedPauseNamesTheBlocker(t *testing.T) {
	var bodies []map[string]any
	server := pauseServer(t, &bodies, map[string]any{
		"operation": map[string]any{"state": "blocked"},
		"targets":   []map[string]any{{"execution_id": "exe_1", "state": "blocked", "blocker_reason": "force_stop_not_picked_up"}},
	})
	defer server.Close()
	err := taskCommand([]string{"pause", "--api", server.URL, "--force", "--timeout", "1s", "p", "q", "t"})
	if err == nil || !strings.Contains(err.Error(), "exe_1: force_stop_not_picked_up") || !strings.Contains(err.Error(), "check that node's agent log") {
		t.Fatalf("error = %v", err)
	}
}
