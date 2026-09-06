package api

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"kairo/internal/store"
)

func newTestServer(t *testing.T) (*store.Store, *httptest.Server) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "api.db"))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer((&Server{Store: st}).Handler())
	t.Cleanup(func() {
		server.Close()
		st.Close()
	})
	return st, server
}

func callJSON(t *testing.T, method, target string, body any) (*http.Response, []byte) {
	t.Helper()
	var encoded []byte
	if body != nil {
		var err error
		encoded, err = json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
	}
	req, err := http.NewRequest(method, target, bytes.NewReader(encoded))
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	return response, payload
}

func TestExecutionSubmissionIsIdempotentAndCreatesScopes(t *testing.T) {
	_, server := newTestServer(t)
	body := map[string]any{
		"schema_version": 2, "client_request_id": "train/one", "project": "llm-develop",
		"queue": "training", "task": "model", "argv": []string{"python", "train.py"},
		"cwd": "/work", "checkpointable": true, "preemptible": true,
		"executor": map[string]any{"labels": map[string]string{"environment": "wsl2"}},
	}
	response, payload := callJSON(t, http.MethodPost, server.URL+"/v2/executions", body)
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("submit returned %d: %s", response.StatusCode, payload)
	}
	var first struct {
		Execution struct {
			ID string `json:"id"`
		} `json:"execution"`
		Idempotent bool `json:"idempotent"`
	}
	if err := json.Unmarshal(payload, &first); err != nil {
		t.Fatal(err)
	}
	if first.Execution.ID == "" || first.Idempotent {
		t.Fatalf("unexpected first submission: %s", payload)
	}
	response, payload = callJSON(t, http.MethodPost, server.URL+"/v2/executions", body)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("repeat returned %d: %s", response.StatusCode, payload)
	}
	var repeated struct {
		Execution struct {
			ID string `json:"id"`
		} `json:"execution"`
		Idempotent bool `json:"idempotent"`
	}
	if err := json.Unmarshal(payload, &repeated); err != nil {
		t.Fatal(err)
	}
	if !repeated.Idempotent || repeated.Execution.ID != first.Execution.ID {
		t.Fatalf("submission was not idempotent: %s", payload)
	}

	response, payload = callJSON(t, http.MethodGet, server.URL+"/v2/scopes?project=llm-develop&queue=training&task=model", nil)
	if response.StatusCode != http.StatusOK || !bytes.Contains(payload, []byte(`"kind":"task"`)) {
		t.Fatalf("scope lookup returned %d: %s", response.StatusCode, payload)
	}
}

func TestExecutionWithdrawIsOnlyPreStartRevocation(t *testing.T) {
	_, server := newTestServer(t)
	body := map[string]any{
		"schema_version": 2, "client_request_id": "one", "project": "p",
		"argv": []string{"run"}, "cwd": ".", "executor": map[string]any{"labels": map[string]string{}},
	}
	_, payload := callJSON(t, http.MethodPost, server.URL+"/v2/executions", body)
	var submitted struct {
		Execution struct {
			ID string `json:"id"`
		} `json:"execution"`
	}
	if err := json.Unmarshal(payload, &submitted); err != nil {
		t.Fatal(err)
	}
	response, payload := callJSON(t, http.MethodPost, server.URL+"/v2/executions/"+submitted.Execution.ID+"/withdraw", nil)
	if response.StatusCode != http.StatusAccepted || !bytes.Contains(payload, []byte(`"terminal_cause":"withdrawn_before_start"`)) {
		t.Fatalf("withdraw returned %d: %s", response.StatusCode, payload)
	}
}

func TestScopePauseReturnsDurableOperationID(t *testing.T) {
	st, server := newTestServer(t)
	scopes, err := st.EnsureScopePath(t.Context(), store.ScopePath{Project: "p"})
	if err != nil {
		t.Fatal(err)
	}
	response, payload := callJSON(t, http.MethodPost, server.URL+"/v2/scopes/"+scopes.Project.ID+"/pause", map[string]string{"actor": "test", "request_id": "pause-one"})
	if response.StatusCode != http.StatusAccepted || !bytes.Contains(payload, []byte(`"operation_id":"pop_`)) {
		t.Fatalf("pause returned %d: %s", response.StatusCode, payload)
	}
}

func TestResumeOnlyOpensAdmissionGate(t *testing.T) {
	st, server := newTestServer(t)
	scopes, err := st.EnsureScopePath(t.Context(), store.ScopePath{Project: "p"})
	if err != nil {
		t.Fatal(err)
	}
	operation, err := st.PauseScope(t.Context(), scopes.Project.ID, "test", "pause-one")
	if err != nil {
		t.Fatal(err)
	}
	response, payload := callJSON(t, http.MethodPost, server.URL+"/v2/scopes/"+scopes.Project.ID+"/resume", map[string]string{"actor": "test"})
	if response.StatusCode != http.StatusOK || !bytes.Contains(payload, []byte(`"state":"open"`)) {
		t.Fatalf("resume returned %d: %s", response.StatusCode, payload)
	}
	unchanged, err := st.GetPauseOperation(t.Context(), operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if unchanged.State != "requested" {
		t.Fatalf("resume rewrote pause operation state to %q", unchanged.State)
	}
}

func TestScopeFiltersRequireCompletePath(t *testing.T) {
	_, server := newTestServer(t)
	for _, path := range []string{
		"/v2/scopes?queue=q",
		"/v2/scopes?project=p&task=t",
		"/v2/executions?queue=q",
		"/v2/executions?project=p&task=t",
	} {
		response, payload := callJSON(t, http.MethodGet, server.URL+path, nil)
		if response.StatusCode != http.StatusBadRequest {
			t.Fatalf("incomplete scope path %s returned %d: %s", path, response.StatusCode, payload)
		}
	}
}

func TestV1AndPlanRoutesAreNotRegistered(t *testing.T) {
	_, server := newTestServer(t)
	for _, path := range []string{
		"/v1/status", "/v1/queues", "/v1/tasks", "/v1/resources/status",
		"/v1/worker/attempts/att0/commands", "/v2/queues/p/q/plan",
		"/v2/worker/attempts/att0/terminal",
	} {
		response, _ := callJSON(t, http.MethodGet, server.URL+path, nil)
		if response.StatusCode != http.StatusNotFound {
			t.Fatalf("removed route %s returned %d", path, response.StatusCode)
		}
	}
}

func TestWorkerAPIRequiresLeaseAndEpoch(t *testing.T) {
	_, server := newTestServer(t)
	response, payload := callJSON(t, http.MethodPost, server.URL+"/v2/worker/attempts/att0/heartbeat", map[string]any{"progress": map[string]any{}})
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("missing worker fencing headers returned %d: %s", response.StatusCode, payload)
	}
}

func TestObserveOnlyRejectsActuatingEndpoints(t *testing.T) {
	st, server := newTestServer(t)
	server.Close()
	httpServer := httptest.NewServer((&Server{Store: st, ObserveOnly: true}).Handler())
	defer httpServer.Close()
	body := map[string]any{
		"schema_version": 2, "client_request_id": "observe-only-actuation", "project": "llm-develop",
		"queue": "training", "task": "model", "argv": []string{"python", "train.py"},
		"cwd": "/work", "checkpointable": true, "preemptible": true,
		"executor": map[string]any{"labels": map[string]string{"environment": "wsl2"}},
	}
	response, payload := callJSON(t, http.MethodPost, httpServer.URL+"/v2/executions", body)
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("submit status=%d payload=%s", response.StatusCode, payload)
	}
	var submitted struct {
		Execution store.ExecutionRequest `json:"execution"`
	}
	if err := json.Unmarshal(payload, &submitted); err != nil {
		t.Fatal(err)
	}
	response, _ = callJSON(t, http.MethodPost, httpServer.URL+"/v2/executions/"+submitted.Execution.ID+"/withdraw", nil)
	if response.StatusCode != http.StatusLocked {
		t.Fatalf("observe-only withdraw status=%d", response.StatusCode)
	}
	response, _ = callJSON(t, http.MethodPost, httpServer.URL+"/v2/resources/gpu-0/reconcile", map[string]bool{"confirm_process_absent": true})
	if response.StatusCode != http.StatusLocked {
		t.Fatalf("observe-only reconcile status=%d", response.StatusCode)
	}
}
