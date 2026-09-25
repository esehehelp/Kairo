package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestProjectPauseWaitsForDurableOperationByDefault(t *testing.T) {
	var mu sync.Mutex
	var requests []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests = append(requests, r.Method+" "+r.URL.Path)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v2/scopes":
			_ = json.NewEncoder(w).Encode(map[string]any{"scopes": []map[string]string{{"id": "scp_project"}}})
		case "/v2/scopes/scp_project/pause":
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{"operation_id": "pop_one"})
		case "/v2/pause-operations/pop_one":
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
		"GET /v2/scopes",
		"POST /v2/scopes/scp_project/pause",
		"GET /v2/pause-operations/pop_one",
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
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests = append(requests, r.Method+" "+r.URL.Path)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v2/scopes":
			_ = json.NewEncoder(w).Encode(map[string]any{"scopes": []map[string]string{{"id": "scp_project"}}})
		case "/v2/scopes/scp_project/resume":
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
	want := []string{"GET /v2/scopes", "POST /v2/scopes/scp_project/resume"}
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
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	if gotMethod != http.MethodPost || gotPath != "/orchestration/v1/project-specs" {
		t.Fatalf("request=%s %s", gotMethod, gotPath)
	}
}
