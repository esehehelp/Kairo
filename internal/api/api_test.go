package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"kairo/internal/store"
)

type testDaemon struct {
	store  *store.Store
	server *httptest.Server
	admin  string
}

func newTestDaemon(t *testing.T, observeOnly bool) *testDaemon {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "api.db"))
	if err != nil {
		t.Fatal(err)
	}
	_, admin, err := st.CreateAPIToken(context.Background(), "operator", store.RoleAdmin, "")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer((&Server{Store: st, ObserveOnly: observeOnly}).Handler())
	t.Cleanup(func() {
		server.Close()
		st.Close()
	})
	return &testDaemon{store: st, server: server, admin: admin}
}

func (d *testDaemon) token(t *testing.T, name, role, node string) string {
	t.Helper()
	_, token, err := d.store.CreateAPIToken(context.Background(), name, role, node)
	if err != nil {
		t.Fatal(err)
	}
	return token
}

// workerToken authorizes one attempt and returns its token.
func (d *testDaemon) workerToken(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	if err := d.store.UpsertNode(ctx, store.Node{ID: "node", Name: "node", OS: "linux", Architecture: "amd64", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := d.store.UpsertExecutor(ctx, store.Executor{ID: "executor", NodeID: "node", Kind: "local", Attributes: json.RawMessage(`{}`), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := d.store.SubmitExecution(ctx, store.ExecutionSpec{ClientRequestID: "worker", Scope: store.ScopePath{Project: "p"}, Argv: []string{"run"}, CWD: ".", ExecutorSelector: json.RawMessage(`{"labels":{}}`)}); err != nil {
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
	return launch.WorkerToken
}

func (d *testDaemon) call(t *testing.T, method, path, token string, body any) (int, []byte) {
	t.Helper()
	var encoded []byte
	if body != nil {
		var err error
		if encoded, err = json.Marshal(body); err != nil {
			t.Fatal(err)
		}
	}
	req, err := http.NewRequest(method, d.server.URL+path, bytes.NewReader(encoded))
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response.StatusCode, payload
}

// Every route requires a token except /health, and each token role reaches
// exactly the routes of its role (an admin token also those of read).
func TestEveryRouteEnforcesItsRole(t *testing.T) {
	d := newTestDaemon(t, false)
	tokens := map[string]string{
		roleAdmin:  d.admin,
		roleRead:   d.token(t, "dashboard", store.RoleRead, ""),
		roleNode:   d.token(t, "agent", store.RoleNode, "node"),
		roleWorker: d.workerToken(t),
	}
	for _, rt := range (&Server{}).routes() {
		path := strings.NewReplacer("{id}", "x", "{project}", "x", "{command}", "x").Replace(rt.pattern)
		status, payload := d.call(t, rt.method, path, "", nil)
		if rt.role == roleNone {
			if status != http.StatusOK {
				t.Fatalf("%s %s without token: %d %s", rt.method, rt.pattern, status, payload)
			}
			continue
		}
		if status != http.StatusUnauthorized || !bytes.Contains(payload, []byte(`"code":"unauthorized"`)) {
			t.Fatalf("%s %s without token: %d %s", rt.method, rt.pattern, status, payload)
		}
		if status, _ = d.call(t, rt.method, path, "kairo_admin_unknown", nil); status != http.StatusUnauthorized {
			t.Fatalf("%s %s with an unknown token: %d", rt.method, rt.pattern, status)
		}
		for role, token := range tokens {
			status, payload = d.call(t, rt.method, path, token, nil)
			allowed := rt.role == roleAny || rt.role == role || (rt.role == roleRead && role == roleAdmin)
			denied := status == http.StatusUnauthorized || status == http.StatusForbidden
			if allowed == denied {
				t.Fatalf("%s %s with a %s token: %d %s", rt.method, rt.pattern, role, status, payload)
			}
		}
	}
}

func TestOldPrefixesAreGone(t *testing.T) {
	d := newTestDaemon(t, false)
	for _, path := range []string{
		"/v2/executions", "/v2/events", "/v2/scopes", "/v2/worker/attempts/att0/commands",
		"/v3/agent/register", "/orchestration/v1/projects/p", "/v1/status",
	} {
		if status, _ := d.call(t, http.MethodGet, path, d.admin, nil); status != http.StatusNotFound && status != http.StatusMethodNotAllowed {
			t.Fatalf("old route %s returned %d", path, status)
		}
	}
}

func TestWhoAmI(t *testing.T) {
	d := newTestDaemon(t, false)
	status, payload := d.call(t, http.MethodGet, "/api/whoami", d.admin, nil)
	if status != http.StatusOK || !bytes.Contains(payload, []byte(`"role":"admin"`)) || !bytes.Contains(payload, []byte(`"name":"operator"`)) {
		t.Fatalf("whoami: %d %s", status, payload)
	}
	node := d.token(t, "agent", store.RoleNode, "pve0")
	if _, payload = d.call(t, http.MethodGet, "/api/whoami", node, nil); !bytes.Contains(payload, []byte(`"node_id":"pve0"`)) {
		t.Fatalf("node whoami: %s", payload)
	}
}

func TestRevokedTokenIsRefusedImmediately(t *testing.T) {
	d := newTestDaemon(t, false)
	token := d.token(t, "temporary", store.RoleRead, "")
	if status, _ := d.call(t, http.MethodGet, "/api/executions", token, nil); status != http.StatusOK {
		t.Fatalf("before revoke: %d", status)
	}
	if err := d.store.RevokeAPIToken(context.Background(), "temporary"); err != nil {
		t.Fatal(err)
	}
	if status, _ := d.call(t, http.MethodGet, "/api/executions", token, nil); status != http.StatusUnauthorized {
		t.Fatalf("after revoke: %d", status)
	}
}

func submitBody(requestID string) map[string]any {
	return map[string]any{
		"client_request_id": requestID, "project": "llm-develop",
		"queue": "training", "task": "model", "argv": []string{"python", "train.py"},
		"cwd": "/work", "checkpointable": true, "preemptible": true,
		"executor": map[string]any{"labels": map[string]string{"environment": "wsl2"}},
	}
}

func TestExecutionSubmissionIsIdempotentAndCreatesScopes(t *testing.T) {
	d := newTestDaemon(t, false)
	body := submitBody("train/one")
	status, payload := d.call(t, http.MethodPost, "/api/executions", d.admin, body)
	if status != http.StatusCreated {
		t.Fatalf("submit returned %d: %s", status, payload)
	}
	var first struct {
		Execution  store.ExecutionRequest `json:"execution"`
		Idempotent bool                   `json:"idempotent"`
	}
	if err := json.Unmarshal(payload, &first); err != nil || first.Execution.ID == "" || first.Idempotent {
		t.Fatalf("unexpected first submission: %s", payload)
	}
	status, payload = d.call(t, http.MethodPost, "/api/executions", d.admin, body)
	var repeated struct {
		Execution  store.ExecutionRequest `json:"execution"`
		Idempotent bool                   `json:"idempotent"`
	}
	if status != http.StatusOK || json.Unmarshal(payload, &repeated) != nil || !repeated.Idempotent || repeated.Execution.ID != first.Execution.ID {
		t.Fatalf("submission was not idempotent: %d %s", status, payload)
	}
	status, payload = d.call(t, http.MethodGet, "/api/scopes?project=llm-develop&queue=training&task=model", d.admin, nil)
	if status != http.StatusOK || !bytes.Contains(payload, []byte(`"kind":"task"`)) {
		t.Fatalf("scope lookup returned %d: %s", status, payload)
	}
	// The request format carries no schema_version any more.
	body["schema_version"] = 2
	if status, payload = d.call(t, http.MethodPost, "/api/executions", d.admin, body); status != http.StatusBadRequest {
		t.Fatalf("schema_version accepted: %d %s", status, payload)
	}
}

func TestExecutionWithdrawIsOnlyPreStartRevocation(t *testing.T) {
	d := newTestDaemon(t, false)
	_, payload := d.call(t, http.MethodPost, "/api/executions", d.admin, submitBody("one"))
	var submitted struct {
		Execution store.ExecutionRequest `json:"execution"`
	}
	if err := json.Unmarshal(payload, &submitted); err != nil {
		t.Fatal(err)
	}
	status, payload := d.call(t, http.MethodPost, "/api/executions/"+submitted.Execution.ID+"/withdraw", d.admin, nil)
	if status != http.StatusAccepted || !bytes.Contains(payload, []byte(`"terminal_cause":"withdrawn_before_start"`)) {
		t.Fatalf("withdraw returned %d: %s", status, payload)
	}
}

func TestScopePauseRecordsTheTokenAsActor(t *testing.T) {
	d := newTestDaemon(t, false)
	scopes, err := d.store.EnsureScopePath(t.Context(), store.ScopePath{Project: "p"})
	if err != nil {
		t.Fatal(err)
	}
	status, payload := d.call(t, http.MethodPost, "/api/scopes/"+scopes.Project.ID+"/pause", d.admin, map[string]string{"request_id": "pause-one"})
	if status != http.StatusAccepted || !bytes.Contains(payload, []byte(`"operation_id":"pop_`)) || !bytes.Contains(payload, []byte(`"actor":"operator"`)) {
		t.Fatalf("pause returned %d: %s", status, payload)
	}
}

func TestResumeOnlyOpensAdmissionGate(t *testing.T) {
	d := newTestDaemon(t, false)
	scopes, err := d.store.EnsureScopePath(t.Context(), store.ScopePath{Project: "p"})
	if err != nil {
		t.Fatal(err)
	}
	operation, err := d.store.PauseScope(t.Context(), scopes.Project.ID, "test", "pause-one")
	if err != nil {
		t.Fatal(err)
	}
	status, payload := d.call(t, http.MethodPost, "/api/scopes/"+scopes.Project.ID+"/resume", d.admin, map[string]string{"actor": "test"})
	if status != http.StatusOK || !bytes.Contains(payload, []byte(`"state":"open"`)) {
		t.Fatalf("resume returned %d: %s", status, payload)
	}
	unchanged, err := d.store.GetPauseOperation(t.Context(), operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if unchanged.State != "requested" {
		t.Fatalf("resume rewrote pause operation state to %q", unchanged.State)
	}
}

func TestScopeFiltersRequireCompletePath(t *testing.T) {
	d := newTestDaemon(t, false)
	for _, path := range []string{
		"/api/scopes?queue=q",
		"/api/scopes?project=p&task=t",
		"/api/executions?queue=q",
		"/api/executions?project=p&task=t",
	} {
		if status, payload := d.call(t, http.MethodGet, path, d.admin, nil); status != http.StatusBadRequest {
			t.Fatalf("incomplete scope path %s returned %d: %s", path, status, payload)
		}
	}
}

// The worker API acts for the attempt its token was issued to.
func TestWorkerRoutesUseTheAttemptToken(t *testing.T) {
	d := newTestDaemon(t, false)
	token := d.workerToken(t)
	status, payload := d.call(t, http.MethodPost, "/api/worker/heartbeat", token, map[string]any{"progress": map[string]any{"step": 3}})
	if status != http.StatusAccepted {
		t.Fatalf("heartbeat: %d %s", status, payload)
	}
	if status, payload = d.call(t, http.MethodPost, "/api/worker/processes", token, map[string]any{"rank": 0, "pid": 200, "process_identity": "worker:200"}); status != http.StatusAccepted {
		t.Fatalf("process: %d %s", status, payload)
	}
	if status, payload = d.call(t, http.MethodGet, "/api/worker/commands", token, nil); status != http.StatusOK || !bytes.Contains(payload, []byte(`"commands":[]`)) {
		t.Fatalf("commands: %d %s", status, payload)
	}
	status, payload = d.call(t, http.MethodGet, "/api/attempts", d.admin, nil)
	if status != http.StatusOK || !bytes.Contains(payload, []byte(`"step":3`)) {
		t.Fatalf("progress not recorded: %d %s", status, payload)
	}
	if status, _ = d.call(t, http.MethodPost, "/api/worker/heartbeat", "kairo_worker_forged", map[string]any{"progress": map[string]any{}}); status != http.StatusUnauthorized {
		t.Fatalf("forged worker token: %d", status)
	}
}

func TestObserveOnlyRejectsActuatingEndpoints(t *testing.T) {
	d := newTestDaemon(t, true)
	status, payload := d.call(t, http.MethodPost, "/api/executions", d.admin, submitBody("observe-only-actuation"))
	if status != http.StatusCreated {
		t.Fatalf("submit status=%d payload=%s", status, payload)
	}
	var submitted struct {
		Execution store.ExecutionRequest `json:"execution"`
	}
	if err := json.Unmarshal(payload, &submitted); err != nil {
		t.Fatal(err)
	}
	if status, _ = d.call(t, http.MethodPost, "/api/executions/"+submitted.Execution.ID+"/withdraw", d.admin, nil); status != http.StatusLocked {
		t.Fatalf("observe-only withdraw status=%d", status)
	}
	if status, _ = d.call(t, http.MethodPost, "/api/resources/gpu-0/reconcile", d.admin, map[string]bool{"confirm_process_absent": true}); status != http.StatusLocked {
		t.Fatalf("observe-only reconcile status=%d", status)
	}
}

func TestForcedScopePause(t *testing.T) {
	d := newTestDaemon(t, false)
	scopes, err := d.store.EnsureScopePath(t.Context(), store.ScopePath{Project: "p", Queue: "q", Task: "t"})
	if err != nil {
		t.Fatal(err)
	}
	path := "/api/scopes/" + scopes.Task.ID + "/pause"
	if status, payload := d.call(t, http.MethodPost, path, d.admin, map[string]any{"actor": "test", "request_id": "plain", "grace_seconds": 5}); status != http.StatusBadRequest {
		t.Fatalf("grace without force: %d %s", status, payload)
	}
	status, payload := d.call(t, http.MethodPost, path, d.admin, map[string]any{"actor": "test", "request_id": "forced", "force": true, "grace_seconds": 5, "reason": "superseded"})
	var started struct {
		OperationID string               `json:"operation_id"`
		Operation   store.PauseOperation `json:"operation"`
	}
	if status != http.StatusAccepted || json.Unmarshal(payload, &started) != nil || started.Operation.Force == nil || started.Operation.Force.GraceSeconds != 5 || started.Operation.Force.Reason != "superseded" {
		t.Fatalf("forced pause: %d %s", status, payload)
	}
	status, payload = d.call(t, http.MethodGet, "/api/pause-operations/"+started.OperationID, d.admin, nil)
	if status != http.StatusOK || !bytes.Contains(payload, []byte(`"force_stops":[]`)) || !bytes.Contains(payload, []byte(`"grace_seconds":5`)) {
		t.Fatalf("operation: %d %s", status, payload)
	}
	status, payload = d.call(t, http.MethodPost, path, d.admin, map[string]any{"request_id": "forced-default", "force": true})
	if status != http.StatusAccepted || !bytes.Contains(payload, []byte(`"grace_seconds":30`)) {
		t.Fatalf("default grace: %d %s", status, payload)
	}
	observe := newTestDaemon(t, true)
	observeScopes, err := observe.store.EnsureScopePath(t.Context(), store.ScopePath{Project: "p", Queue: "q", Task: "t"})
	if err != nil {
		t.Fatal(err)
	}
	if status, payload = observe.call(t, http.MethodPost, "/api/scopes/"+observeScopes.Task.ID+"/pause", observe.admin, map[string]any{"request_id": "forced-observe", "force": true}); status != http.StatusLocked {
		t.Fatalf("observe-only forced pause: %d %s", status, payload)
	}
}

func projectBody(argv ...any) map[string]any {
	return map[string]any{
		"schema_version": 1,
		"project":        map[string]any{"name": "neo-ime"},
		"tasks": []any{map[string]any{
			"name": "train", "queue": "training", "depends_on": []any{}, "policy": "RunToCompletion@v1",
			"execution": map[string]any{
				"schema_version": 2, "argv": argv, "cwd": "/work", "checkpointable": false, "preemptible": false,
				"executor": map[string]any{"labels": map[string]string{}}, "exclusive": []any{}, "capacity": map[string]any{},
			},
		}},
	}
}

func TestProjectApplyIsIdempotentAndImmutable(t *testing.T) {
	d := newTestDaemon(t, false)
	status, raw := d.call(t, http.MethodPost, "/api/projects", d.admin, projectBody("uv", "run", "train"))
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil || status != http.StatusCreated || payload["spec_digest"] == "" {
		t.Fatalf("apply status=%d payload=%s", status, raw)
	}
	status, raw = d.call(t, http.MethodPost, "/api/projects", d.admin, projectBody("uv", "run", "train"))
	if err := json.Unmarshal(raw, &payload); err != nil || status != http.StatusOK || payload["idempotent"] != true {
		t.Fatalf("idempotent apply status=%d payload=%s", status, raw)
	}
	status, raw = d.call(t, http.MethodGet, "/api/projects/neo-ime", d.admin, nil)
	if status != http.StatusOK || !bytes.Contains(raw, []byte(`"name":"train"`)) {
		t.Fatalf("status=%d payload=%s", status, raw)
	}
	status, raw = d.call(t, http.MethodPost, "/api/projects", d.admin, projectBody("changed"))
	if status != http.StatusConflict || !bytes.Contains(raw, []byte(`"code":"orchestration_conflict"`)) {
		t.Fatalf("mutation status=%d payload=%s", status, raw)
	}
}

func TestProjectStatusRequiresDeclaration(t *testing.T) {
	d := newTestDaemon(t, false)
	if _, err := d.store.EnsureScopePath(t.Context(), store.ScopePath{Project: "undeclared", Queue: "q", Task: "t"}); err != nil {
		t.Fatal(err)
	}
	if status, _ := d.call(t, http.MethodGet, "/api/projects/undeclared", d.admin, nil); status != http.StatusNotFound {
		t.Fatalf("status=%d", status)
	}
}
