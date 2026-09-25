package agentclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
)

const testToken = "s3cret"

// TestHelperProcess is the attempt launched by the end-to-end test.
func TestHelperProcess(t *testing.T) {
	if os.Getenv("KAIRO_AGENT_HELPER") != "1" {
		return
	}
	fmt.Fprintln(os.Stderr, "hello from remote attempt")
	os.Exit(0)
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

func newDaemon(t *testing.T) (*store.Store, *httptest.Server, string) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "kairo.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	logDir := t.TempDir()
	ts := httptest.NewServer((&api.Server{Store: st, AgentToken: testToken, LogDirectory: logDir}).Handler())
	t.Cleanup(ts.Close)
	return st, ts, logDir
}

func TestAgentAPIRequiresToken(t *testing.T) {
	_, ts, _ := newDaemon(t)
	bad := New(ts.URL, "wrong")
	_, err := bad.Register(context.Background(), store.Node{ID: "n", Name: "n"}, nil, nil)
	var apiErr *Error
	if !errors.As(err, &apiErr) || apiErr.Status != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %v", err)
	}

	st, err := store.Open(filepath.Join(t.TempDir(), "kairo.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	closed := httptest.NewServer((&api.Server{Store: st}).Handler())
	defer closed.Close()
	resp, err := http.Post(closed.URL+"/v3/agent/register", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("agent API must not be mounted without a token, got %d", resp.StatusCode)
	}
}

func TestSentinelErrorsRoundTrip(t *testing.T) {
	_, ts, _ := newDaemon(t)
	c := New(ts.URL, testToken)
	err := c.SetAttemptLogPaths(context.Background(), "att_missing", 1, "x.stdout.log", "x.stderr.log")
	if !errors.Is(err, store.ErrStaleEpoch) {
		t.Fatalf("expected store.ErrStaleEpoch through the wire, got %v", err)
	}
}

func TestLogAppendIsIdempotentAndReportsGaps(t *testing.T) {
	_, ts, logDir := newDaemon(t)
	c := New(ts.URL, testToken)
	ctx := context.Background()
	if size, err := c.appendLog(ctx, "att_x.stderr.log", 0, []byte("abc")); err != nil || size != 3 {
		t.Fatalf("first append: size=%d err=%v", size, err)
	}
	// A retry that overlaps what the daemon already has only adds the tail.
	if size, err := c.appendLog(ctx, "att_x.stderr.log", 0, []byte("abcdef")); err != nil || size != 6 {
		t.Fatalf("overlapping append: size=%d err=%v", size, err)
	}
	var gap *GapError
	if _, err := c.appendLog(ctx, "att_x.stderr.log", 10, []byte("z")); !errors.As(err, &gap) || gap.Size != 6 {
		t.Fatalf("expected gap at 6, got %v", err)
	}
	if _, err := c.appendLog(ctx, "../escape.stderr.log", 0, []byte("z")); err == nil {
		t.Fatal("path traversal must be rejected")
	}
	body, _ := os.ReadFile(filepath.Join(logDir, "att_x.stderr.log"))
	if string(body) != "abcdef" {
		t.Fatalf("daemon log = %q", body)
	}
}

// A remote executor driven only through the agent API reserves the GPU,
// launches, exits, proves quiescence and releases the lease; its log reaches
// the daemon's log directory.
func TestRemoteExecutorRunsAttemptToRelease(t *testing.T) {
	st, ts, daemonLogs := newDaemon(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	c := New(ts.URL, testToken)
	if _, err := c.Register(ctx, store.Node{ID: "remote", Name: "remote", OS: "linux", Architecture: "amd64"},
		[]store.Executor{{ID: "remote-exec", Kind: "linux", Attributes: json.RawMessage(`{"environment":"remote-test"}`), Enabled: true}},
		[]Provider{{ID: "remote-gpu", Kind: "nvidia"}}); err != nil {
		t.Fatal(err)
	}
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
	t.Setenv("KAIRO_AGENT_HELPER", "1")

	nodeLogs := t.TempDir()
	exec := executor.NewLocal("remote-exec", c, nodeLogs, nil)
	exec.NodeID = "remote"
	exec.Kind = "linux"
	exec.Attributes = map[string]string{"environment": "remote-test"}
	exec.APIURL = ts.URL
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
	_, ts, daemonLogs := newDaemon(t)
	nodeLogs := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	shipper := &LogShipper{Client: New(ts.URL, testToken), Dir: nodeLogs, Interval: 50 * time.Millisecond}
	go func() { _ = shipper.Run(ctx) }()
	time.Sleep(120 * time.Millisecond)
	path := filepath.Join(nodeLogs, "att_run.stderr.log")
	if err := os.WriteFile(path, []byte("first\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	waitFor := func(want string) {
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			body, _ := os.ReadFile(filepath.Join(daemonLogs, "att_run.stderr.log"))
			if string(body) == want {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		body, _ := os.ReadFile(filepath.Join(daemonLogs, "att_run.stderr.log"))
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
