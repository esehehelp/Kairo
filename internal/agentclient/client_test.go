package agentclient

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"kairo/internal/api"
	"kairo/internal/executor"
	"kairo/internal/provider"
	"kairo/internal/store"
	"kairo/internal/tlsutil"
)

// TestHelperProcess is the attempt launched by the end-to-end test: it
// reports progress with the attempt token it was launched with, verifying the
// daemon with the CA it was handed. (Its switch is not a KAIRO_ variable: the
// executor keeps the parent's KAIRO_* variables from attempts.)
func TestHelperProcess(t *testing.T) {
	if os.Getenv("AGENTCLIENT_TEST_HELPER") != "1" {
		return
	}
	fmt.Fprintln(os.Stderr, "hello from remote attempt")
	caPEM, err := tlsutil.CAFromEnv(os.Getenv("KAIRO_API_CA"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "KAIRO_API_CA:", err)
		os.Exit(3)
	}
	tlsConfig, _ := tlsutil.ClientConfig(caPEM)
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: tlsConfig}}
	req, _ := http.NewRequest(http.MethodPost, os.Getenv("KAIRO_API_URL")+"/api/worker/heartbeat", strings.NewReader(`{"progress":{"unit":"step","current":7}}`))
	req.Header.Set("Authorization", "Bearer "+os.Getenv("KAIRO_ATTEMPT_TOKEN"))
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil || resp.StatusCode != http.StatusAccepted {
		fmt.Fprintln(os.Stderr, "heartbeat:", resp, err)
		os.Exit(4)
	}
	os.Exit(0)
}

// tlsDaemon serves the API over TLS with a fresh CA; it returns the server
// and the CA certificate.
func tlsDaemon(t *testing.T, st *store.Store, logDir string) (*httptest.Server, []byte) {
	t.Helper()
	dir := t.TempDir()
	if err := tlsutil.InitCA(dir); err != nil {
		t.Fatal(err)
	}
	if err := tlsutil.IssueServer(dir, []string{"127.0.0.1"}); err != nil {
		t.Fatal(err)
	}
	cert, err := tls.LoadX509KeyPair(filepath.Join(dir, tlsutil.ServerCertFile), filepath.Join(dir, tlsutil.ServerKeyFile))
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewUnstartedServer((&api.Server{Store: st, LogDirectory: logDir}).Handler())
	ts.TLS = tlsutil.ServerConfig()
	ts.TLS.Certificates = []tls.Certificate{cert}
	ts.StartTLS()
	t.Cleanup(ts.Close)
	caPEM, err := os.ReadFile(filepath.Join(dir, tlsutil.CACertFile))
	if err != nil {
		t.Fatal(err)
	}
	return ts, caPEM
}

type fakeGPU struct{ node string }

func (p *fakeGPU) ID() string { return "remote-gpu" }
func (p *fakeGPU) Observe(context.Context) (provider.Snapshot, error) {
	stamp := time.Now().UTC()
	total, free := int64(12_000), int64(12_000)
	binding := json.RawMessage(`{"cuda_index":"0"}`)
	return provider.Snapshot{
		Resources:    []store.ResourceInstance{{ID: p.node + "-gpu0", NodeID: p.node, ProviderID: "remote-gpu", Kind: "gpu", StableIdentity: "GPU-remote-0", Binding: binding, Attributes: json.RawMessage(`{}`), AdminState: "enabled"}},
		Observations: []store.Observation{{ResourceID: p.node + "-gpu0", ObservedAt: stamp.Format(time.RFC3339Nano), ValidUntil: stamp.Add(time.Minute).Format(time.RFC3339Nano), TotalBytes: &total, FreeBytes: &free, Evidence: json.RawMessage(`{}`)}},
	}, nil
}
func (p *fakeGPU) Prepare(context.Context, store.ResourceInstance) error { return nil }
func (p *fakeGPU) Release(context.Context, store.ResourceInstance) error { return nil }

// newDaemon serves the API with a node token for node "remote".
func newDaemon(t *testing.T) (*store.Store, *httptest.Server, string, string) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "kairo.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	_, token, err := st.CreateAPIToken(context.Background(), "remote-agent", store.RoleNode, "remote")
	if err != nil {
		t.Fatal(err)
	}
	logDir := t.TempDir()
	ts := httptest.NewServer((&api.Server{Store: st, LogDirectory: logDir}).Handler())
	t.Cleanup(ts.Close)
	return st, ts, token, logDir
}

func registerRemote(t *testing.T, c *Client) {
	t.Helper()
	if _, err := c.Register(context.Background(), store.Node{ID: "remote", Name: "remote", OS: "linux", Architecture: "amd64"},
		[]store.Executor{{ID: "remote-exec", Kind: "linux", Attributes: json.RawMessage(`{"environment":"remote-test"}`), Enabled: true}},
		[]Provider{{ID: "remote-gpu", Kind: "nvidia"}}); err != nil {
		t.Fatal(err)
	}
}

// remoteAttempt authorizes one attempt on the remote executor and returns it.
func remoteAttempt(t *testing.T, st *store.Store, c *Client) *store.Launch {
	t.Helper()
	ctx := context.Background()
	registerRemote(t, c)
	selector, _ := json.Marshal(map[string]any{"labels": map[string]string{"environment": "remote-test"}})
	if _, _, err := st.SubmitExecution(ctx, store.ExecutionSpec{ClientRequestID: "remote-attempt", Scope: store.ScopePath{Project: "pilot"}, Argv: []string{"run"}, CWD: ".", ExecutorSelector: selector}); err != nil {
		t.Fatal(err)
	}
	reservation, err := c.ReserveNext(ctx, "remote-exec")
	if err != nil || reservation == nil {
		t.Fatalf("reserve: %+v %v", reservation, err)
	}
	if err = c.MarkLeasePrepared(ctx, reservation.Lease.ID, reservation.Lease.CoordinationEpoch); err != nil {
		t.Fatal(err)
	}
	launch, err := c.AuthorizeLaunch(ctx, reservation)
	if err != nil {
		t.Fatal(err)
	}
	return launch
}

func forbidden(t *testing.T, name string, err error) {
	t.Helper()
	if !errors.Is(err, api.ErrForbidden) {
		t.Fatalf("%s: expected forbidden, got %v", name, err)
	}
}

func TestAgentAPIRequiresANodeToken(t *testing.T) {
	st, ts, _, _ := newDaemon(t)
	_, err := New(ts.URL, "kairo_node_wrong", nil).Register(context.Background(), store.Node{ID: "remote", Name: "remote"}, nil, nil)
	if !errors.Is(err, store.ErrUnauthorized) {
		t.Fatalf("expected unauthorized, got %v", err)
	}
	_, admin, err := st.CreateAPIToken(context.Background(), "operator", store.RoleAdmin, "")
	if err != nil {
		t.Fatal(err)
	}
	_, err = New(ts.URL, admin, nil).Register(context.Background(), store.Node{ID: "remote", Name: "remote"}, nil, nil)
	forbidden(t, "operator token on the agent API", err)
}

// A node token only acts for its own node: it cannot register as another
// node, nor touch another node's executors, leases, attempts or logs.
func TestNodeTokenIsBoundToItsNode(t *testing.T) {
	st, ts, token, _ := newDaemon(t)
	ctx := context.Background()
	c := New(ts.URL, token, nil)
	launch := remoteAttempt(t, st, c)
	_, otherToken, err := st.CreateAPIToken(ctx, "other-agent", store.RoleNode, "other")
	if err != nil {
		t.Fatal(err)
	}
	other := New(ts.URL, otherToken, nil)
	_, err = other.Register(ctx, store.Node{ID: "remote", Name: "remote", OS: "linux", Architecture: "amd64"}, nil, nil)
	forbidden(t, "register as another node", err)
	_, err = other.ReserveNext(ctx, "remote-exec")
	forbidden(t, "reserve for another node's executor", err)
	_, err = other.ForceStopOrders(ctx, "remote-exec")
	forbidden(t, "read another node's force stops", err)
	forbidden(t, "ack another node's attempt", other.AckForceStop(ctx, launch.Attempt.ID, "signalled", nil))
	forbidden(t, "terminal for another node's attempt", other.RecordTerminal(ctx, launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch, 0, ""))
	forbidden(t, "release another node's lease", other.ReleaseReservation(ctx, launch.Lease.ID, launch.Lease.CoordinationEpoch, "x"))
	forbidden(t, "observe with another node's provider", other.ApplyObservationBatch(ctx, "remote-gpu", nil, nil, nil))
	_, err = other.appendLog(ctx, launch.Attempt.ID+".stderr.log", 0, []byte("x"))
	forbidden(t, "append to another node's log", err)
	// Registering its own node with an executor id taken by another node is
	// refused by the store.
	if _, err = other.Register(ctx, store.Node{ID: "other", Name: "other", OS: "linux", Architecture: "amd64"},
		[]store.Executor{{ID: "remote-exec", Kind: "linux", Enabled: true}}, nil); !errors.Is(err, store.ErrOwnershipConflict) {
		t.Fatalf("executor takeover: %v", err)
	}
	if node, err := st.ExecutorNode(ctx, "remote-exec"); err != nil || node != "remote" {
		t.Fatalf("executor moved to %q (%v)", node, err)
	}
}

func TestSentinelErrorsRoundTrip(t *testing.T) {
	_, ts, token, _ := newDaemon(t)
	c := New(ts.URL, token, nil)
	err := c.SetAttemptLogPaths(context.Background(), "att_missing", 1, "att_missing.stdout.log", "att_missing.stderr.log")
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected store.ErrNotFound through the wire, got %v", err)
	}
}

func TestLogAppendIsIdempotentAndReportsGaps(t *testing.T) {
	st, ts, token, logDir := newDaemon(t)
	c := New(ts.URL, token, nil)
	ctx := context.Background()
	name := remoteAttempt(t, st, c).Attempt.ID + ".stderr.log"
	if size, err := c.appendLog(ctx, name, 0, []byte("abc")); err != nil || size != 3 {
		t.Fatalf("first append: size=%d err=%v", size, err)
	}
	// A retry that overlaps what the daemon already has only adds the tail.
	if size, err := c.appendLog(ctx, name, 0, []byte("abcdef")); err != nil || size != 6 {
		t.Fatalf("overlapping append: size=%d err=%v", size, err)
	}
	var gap *GapError
	if _, err := c.appendLog(ctx, name, 10, []byte("z")); !errors.As(err, &gap) || gap.Size != 6 {
		t.Fatalf("expected gap at 6, got %v", err)
	}
	if _, err := c.appendLog(ctx, "../escape.stderr.log", 0, []byte("z")); err == nil {
		t.Fatal("path traversal must be rejected")
	}
	if _, err := c.appendLog(ctx, "att_unknown.stderr.log", 0, []byte("z")); err == nil {
		t.Fatal("a log of an unknown attempt must be rejected")
	}
	body, _ := os.ReadFile(filepath.Join(logDir, name))
	if string(body) != "abcdef" {
		t.Fatalf("daemon log = %q", body)
	}
}

// A remote executor driven only through the agent API over TLS reserves the
// GPU, launches, exits, proves quiescence and releases the lease; the attempt
// reports progress with its own token, and its log reaches the daemon's log
// directory.
func TestRemoteExecutorRunsAttemptToRelease(t *testing.T) {
	st, _, token, daemonLogs := newDaemon(t)
	ts, caPEM := tlsDaemon(t, st, daemonLogs)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	tlsConfig, err := tlsutil.ClientConfig(caPEM)
	if err != nil {
		t.Fatal(err)
	}
	apiCA, err := tlsutil.CAEnv(caPEM)
	if err != nil {
		t.Fatal(err)
	}
	c := New(ts.URL, token, tlsConfig)
	registerRemote(t, c)
	selector, _ := json.Marshal(map[string]any{"labels": map[string]string{"environment": "remote-test"}})
	if _, _, err := st.SubmitExecution(ctx, store.ExecutionSpec{
		ClientRequestID:  "remote-1",
		Scope:            store.ScopePath{Project: "pilot", Queue: "remote", Task: "hello"},
		Argv:             []string{os.Args[0], "-test.run=^TestHelperProcess$"},
		CWD:              t.TempDir(),
		ExecutorSelector: selector,
		Exclusive:        []store.ExclusiveRequest{{Kind: "gpu", Count: 1}},
	}); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AGENTCLIENT_TEST_HELPER", "1")

	nodeLogs := t.TempDir()
	exec := executor.NewLocal("remote-exec", c, nodeLogs, nil)
	exec.NodeID = "remote"
	exec.Kind = "linux"
	exec.Attributes = map[string]string{"environment": "remote-test"}
	exec.APIURL = ts.URL
	exec.APICA = apiCA
	exec.Interval = 50 * time.Millisecond
	exec.ObservationInterval = 50 * time.Millisecond
	exec.Providers = map[string]provider.Provider{"remote-gpu": &fakeGPU{node: "remote"}}
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	go func() { _ = exec.Run(runCtx) }()

	for {
		executions, err := st.ListExecutions(ctx, store.ExecutionFilter{})
		if err != nil {
			t.Fatal(err)
		}
		resources, err := st.ListResourceStatus(ctx)
		if err != nil {
			t.Fatal(err)
		}
		released := len(resources) == 1 && resources[0].DerivedState == "available"
		if len(executions) == 1 && executions[0].State == "terminal" && released {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("attempt did not run to release: executions=%+v resources=%+v", executions, resources)
		}
		time.Sleep(100 * time.Millisecond)
	}
	stop()
	attempts, err := st.ListAttempts(ctx, store.AttemptFilter{})
	if err != nil || len(attempts) != 1 || attempts[0].ExitCode == nil || *attempts[0].ExitCode != 0 || !strings.Contains(string(attempts[0].Progress), `"current":7`) {
		t.Fatalf("attempt did not report with its token: %+v %v", attempts, err)
	}

	shipper := &LogShipper{Client: c, Dir: nodeLogs}
	shipper.Sweep(ctx)
	matches, _ := filepath.Glob(filepath.Join(daemonLogs, "*.stderr.log"))
	if len(matches) != 1 {
		t.Fatalf("expected one shipped stderr log, got %v", matches)
	}
	body, _ := os.ReadFile(matches[0])
	if !strings.Contains(string(body), "hello from remote attempt") {
		t.Fatalf("shipped log = %q", body)
	}
}

// Run ships a log that appears and grows after the shipper started.
func TestLogShipperRunShipsGrowingFile(t *testing.T) {
	st, ts, token, daemonLogs := newDaemon(t)
	c := New(ts.URL, token, nil)
	name := remoteAttempt(t, st, c).Attempt.ID + ".stderr.log"
	nodeLogs := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	shipper := &LogShipper{Client: c, Dir: nodeLogs, Interval: 50 * time.Millisecond}
	go func() { _ = shipper.Run(ctx) }()
	time.Sleep(120 * time.Millisecond)
	path := filepath.Join(nodeLogs, name)
	if err := os.WriteFile(path, []byte("first\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	waitFor := func(want string) {
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			body, _ := os.ReadFile(filepath.Join(daemonLogs, name))
			if string(body) == want {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		body, _ := os.ReadFile(filepath.Join(daemonLogs, name))
		t.Fatalf("daemon log = %q, want %q", body, want)
	}
	waitFor("first\n")
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.WriteString("second\n")
	f.Close()
	waitFor("first\nsecond\n")
}

func TestForceStopCallsRoundTrip(t *testing.T) {
	_, ts, token, _ := newDaemon(t)
	c := New(ts.URL, token, nil)
	ctx := context.Background()
	registerRemote(t, c)
	if orders, err := c.ForceStopOrders(ctx, "remote-exec"); err != nil || len(orders) != 0 {
		t.Fatalf("orders: %+v %v", orders, err)
	}
	if err := c.AckForceStop(ctx, "att_missing", "signalled", nil); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("ack of an unknown force stop: %v", err)
	}
}

// Against an observe-only daemon an agent's polls find nothing to do and
// actuation is refused, as for an in-process executor that only observes.
func TestObserveOnlyDaemonGivesAgentsNothingToDo(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "kairo.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	_, token, err := st.CreateAPIToken(ctx, "remote-agent", store.RoleNode, "remote")
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer((&api.Server{Store: st, ObserveOnly: true}).Handler())
	defer ts.Close()
	c := New(ts.URL, token, nil)
	registerRemote(t, c)
	selector, _ := json.Marshal(map[string]any{"labels": map[string]string{"environment": "remote-test"}})
	if _, _, err = st.SubmitExecution(ctx, store.ExecutionSpec{ClientRequestID: "observe", Scope: store.ScopePath{Project: "pilot"}, Argv: []string{"run"}, CWD: ".", ExecutorSelector: selector}); err != nil {
		t.Fatal(err)
	}
	if reservation, err := c.ReserveNext(ctx, "remote-exec"); err != nil || reservation != nil {
		t.Fatalf("observe-only reserve: %+v %v", reservation, err)
	}
	if commands, err := c.EnsurePriorityPreemption(ctx, "remote-exec"); err != nil || len(commands) != 0 {
		t.Fatalf("observe-only preemption: %v %v", commands, err)
	}
	snap, _ := (&fakeGPU{node: "remote"}).Observe(ctx)
	if err := c.ApplyObservationBatch(ctx, "remote-gpu", snap.Resources, snap.Observations, snap.Claims); err != nil {
		t.Fatalf("observe-only observation: %v", err)
	}
	executions, err := st.ListExecutions(ctx, store.ExecutionFilter{})
	if err != nil || len(executions) != 1 || executions[0].State != "waiting" {
		t.Fatalf("observe-only daemon changed an execution: %+v %v", executions, err)
	}
	var apiErr *Error
	if err := c.MarkQuarantineTerminated(ctx, "att_missing"); !errors.Is(err, store.ErrNotFound) && !(errors.As(err, &apiErr) && apiErr.Status == 423) {
		t.Fatalf("observe-only actuation: %v", err)
	}
}

// The client verifies the daemon's certificate against the configured CA.
func TestClientTrustsOnlyTheDaemonCA(t *testing.T) {
	st, _, token, _ := newDaemon(t)
	ts, caPEM := tlsDaemon(t, st, "")
	trusted, err := tlsutil.ClientConfig(caPEM)
	if err != nil {
		t.Fatal(err)
	}
	registerRemote(t, New(ts.URL, token, trusted))
	if _, err := New(ts.URL, token, nil).Register(context.Background(), store.Node{ID: "remote", Name: "remote", OS: "linux", Architecture: "amd64"}, nil, nil); err == nil {
		t.Fatal("a client without the daemon CA must not connect")
	}
}

// The node token never follows a redirect.
func TestClientDoesNotFollowRedirects(t *testing.T) {
	var leaked bool
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		leaked = r.Header.Get("Authorization") != ""
	}))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+r.URL.Path, http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()
	_, err := New(redirect.URL, "kairo_node_secret", nil).Register(context.Background(), store.Node{ID: "remote", Name: "remote"}, nil, nil)
	if err == nil || leaked {
		t.Fatalf("redirect followed: err=%v leaked=%v", err, leaked)
	}
}

// A log the daemon refuses for good (no such attempt) is not read and sent
// again on every sweep.
func TestLogShipperGivesUpOnRefusedLogs(t *testing.T) {
	_, ts, token, _ := newDaemon(t)
	var appends int
	counting := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/agent/logs" {
			appends++
		}
		proxy, _ := http.NewRequest(r.Method, ts.URL+r.URL.Path, r.Body)
		proxy.Header = r.Header
		resp, err := http.DefaultClient.Do(proxy)
		if err != nil {
			t.Error(err)
			return
		}
		defer resp.Body.Close()
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
	}))
	defer counting.Close()
	nodeLogs := t.TempDir()
	if err := os.WriteFile(filepath.Join(nodeLogs, "att_orphan.stderr.log"), []byte("old run\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	shipper := &LogShipper{Client: New(counting.URL, token, nil), Dir: nodeLogs}
	for range 3 {
		shipper.Sweep(context.Background())
	}
	if appends != 1 {
		t.Fatalf("orphan log sent %d times", appends)
	}
}
