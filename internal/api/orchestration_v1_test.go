package api

import (
	"encoding/json"
	"net/http"
	"testing"

	"kairo/internal/store"
)

func TestProjectDeclarationAPIIsSeparateFromExecutionV2(t *testing.T) {
	_, server := newTestServer(t)
	body := map[string]any{
		"schema_version": 1,
		"project":        map[string]any{"name": "neo-ime"},
		"tasks": []any{
			map[string]any{
				"name":       "train",
				"queue":      "training",
				"depends_on": []any{},
				"policy":     "RunToCompletion@v1",
				"execution": map[string]any{
					"schema_version": 2,
					"argv":           []any{"uv", "run", "train"},
					"cwd":            "/work",
					"checkpointable": false,
					"preemptible":    false,
					"executor":       map[string]any{"labels": map[string]string{}},
					"exclusive":      []any{},
					"capacity":       map[string]any{},
				},
			},
		},
	}
	response, raw := callJSON(t, http.MethodPost, server.URL+"/orchestration/v1/project-specs", body)
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusCreated || payload["spec_digest"] == "" {
		t.Fatalf("apply status=%d payload=%v", response.StatusCode, payload)
	}
	response, raw = callJSON(t, http.MethodPost, server.URL+"/orchestration/v1/project-specs", body)
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || payload["idempotent"] != true {
		t.Fatalf("idempotent apply status=%d payload=%v", response.StatusCode, payload)
	}
	response, raw = callJSON(t, http.MethodGet, server.URL+"/orchestration/v1/projects/neo-ime", nil)
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatal(err)
	}
	project, ok := payload["project"].(map[string]any)
	if response.StatusCode != http.StatusOK || !ok || len(project["tasks"].([]any)) != 1 {
		t.Fatalf("status=%d payload=%v", response.StatusCode, payload)
	}
}

func TestProjectDeclarationRejectsTaskMutation(t *testing.T) {
	_, server := newTestServer(t)
	body := map[string]any{
		"schema_version": 1,
		"project":        map[string]any{"name": "p"},
		"tasks": []any{map[string]any{
			"name": "t", "queue": "q", "depends_on": []any{}, "policy": "RunToCompletion@v1",
			"execution": map[string]any{"schema_version": 2, "argv": []any{"one"}, "cwd": "/work", "checkpointable": false, "preemptible": false},
		}},
	}
	response, _ := callJSON(t, http.MethodPost, server.URL+"/orchestration/v1/project-specs", body)
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("first apply status=%d", response.StatusCode)
	}
	body["tasks"].([]any)[0].(map[string]any)["execution"].(map[string]any)["argv"] = []any{"changed"}
	response, raw := callJSON(t, http.MethodPost, server.URL+"/orchestration/v1/project-specs", body)
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusConflict || payload["error"] == "" {
		t.Fatalf("mutation status=%d payload=%v", response.StatusCode, payload)
	}
}

func TestProjectOrchestrationStatusRequiresDeclaration(t *testing.T) {
	st, server := newTestServer(t)
	if _, err := st.EnsureScopePath(t.Context(), store.ScopePath{Project: "raw-v3-project", Queue: "q", Task: "t"}); err != nil {
		t.Fatal(err)
	}
	response, _ := callJSON(t, http.MethodGet, server.URL+"/orchestration/v1/projects/raw-v3-project", nil)
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("status=%d", response.StatusCode)
	}
}
