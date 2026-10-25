package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"path/filepath"
	"strings"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"kairo/internal/machine"
	"kairo/internal/provider"
	"kairo/internal/store"
)

type observationProvider struct {
	snapshot     provider.Snapshot
	prepareCalls int
}

func (p *observationProvider) ID() string { return "gpu-provider" }
func (p *observationProvider) Observe(context.Context) (provider.Snapshot, error) {
	return p.snapshot, nil
}
func (p *observationProvider) Prepare(context.Context, store.ResourceInstance) error {
	p.prepareCalls++
	return nil
}
func (p *observationProvider) Release(context.Context, store.ResourceInstance) error { return nil }

func TestObserveOnlyRecordsExternalActivityWithoutAllocating(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(filepath.Join(t.TempDir(), "kairo.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err = st.UpsertNode(ctx, store.Node{ID: "n", Name: "n", OS: "windows", Architecture: "amd64", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err = st.UpsertExecutor(ctx, store.Executor{ID: "e", NodeID: "n", Kind: "windows", Attributes: json.RawMessage(`{"environment":"windows"}`), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err = st.UpsertProvider(ctx, "gpu-provider", "n", "nvidia", nil); err != nil {
		t.Fatal(err)
	}
	if _, err = st.ApplyMachines(ctx, machine.Spec{Nodes: []machine.Node{{ID: "n", GPUs: []string{"GPU-0"}}}}, false); err != nil {
		t.Fatal(err)
	}
	if _, _, err = st.SubmitExecution(ctx, store.ExecutionSpec{
		ClientRequestID: "observe-only",
		Scope:           store.ScopePath{Project: "pilot", Queue: "observe-only", Task: "waiting"},
		Argv:            []string{"train"},
		CWD:             ".",
		Exclusive:       []store.ExclusiveRequest{{Kind: "gpu", Count: 1}},
	}); err != nil {
		t.Fatal(err)
	}

	stamp := time.Now().UTC()
	total, free := int64(24_000), int64(8_000)
	identity := "pid:4242:start:unmanaged"
	p := &observationProvider{snapshot: provider.Snapshot{
		Resources:    []store.ResourceInstance{{ID: "gpu0", NodeID: "n", ProviderID: "gpu-provider", Kind: "gpu", StableIdentity: "GPU-0", AdminState: "enabled"}},
		Observations: []store.Observation{{ResourceID: "gpu0", ObservedAt: stamp.Format(time.RFC3339Nano), ValidUntil: stamp.Add(time.Minute).Format(time.RFC3339Nano), TotalBytes: &total, FreeBytes: &free}},
		Claims:       []store.ExternalClaim{{ID: "unmanaged-pretrain", ResourceID: "gpu0", ClaimKind: "external_process", ProcessIdentity: &identity}},
	}}
	executor := NewLocal("e", st, t.TempDir(), nil)
	executor.NodeID = "n"
	executor.Kind = "windows"
	executor.Attributes = map[string]string{"environment": "windows"}
	executor.ObserveOnly = true
	executor.Providers = map[string]provider.Provider{"gpu-provider": p}

	if err = executor.tick(ctx); err != nil {
		t.Fatal(err)
	}
	status, err := st.ListResourceStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	claims, err := st.ListClaims(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(status) != 1 || status[0].DerivedState != "conflicted" || len(claims) != 1 {
		t.Fatalf("unmanaged execution observation was not retained: resources=%+v claims=%+v", status, claims)
	}
	executions, err := st.ListExecutions(ctx, store.ExecutionFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(executions) != 1 || executions[0].State != "waiting" || p.prepareCalls != 0 {
		t.Fatalf("observe-only mode allocated execution: executions=%+v prepares=%d", executions, p.prepareCalls)
	}
}

func TestGangEnvTellsTheRankItsPlaceAndPinsTheInterconnect(t *testing.T) {
	e := &Local{Attributes: map[string]string{"interconnect_ifname": "eth1"}}
	if env := e.gangEnv(nil); env != nil {
		t.Fatalf("non-gang env: %v", env)
	}
	env := e.gangEnv(&store.GangLaunch{GangID: "gng_1", Rank: 1, Size: 2, MasterAddr: "10.77.10.2", MasterPort: 29500})
	want := map[string]bool{"RANK=1": true, "WORLD_SIZE=2": true, "MASTER_ADDR=10.77.10.2": true, "MASTER_PORT=29500": true,
		"KAIRO_GANG_RANK=1": true, "NCCL_SOCKET_IFNAME=eth1": true, "GLOO_SOCKET_IFNAME=eth1": true}
	for _, v := range env {
		delete(want, v)
	}
	if len(want) != 0 {
		t.Fatalf("missing %v in %v", want, env)
	}
}

// syncBuffer collects log lines written by the step watcher's goroutine.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// hangingProvider's observation does not return until release is closed, as
// an observation that cannot be cancelled does not.
type hangingProvider struct {
	calls   atomic.Int32
	release chan struct{}
}

func (p *hangingProvider) ID() string { return "gpu-provider" }
func (p *hangingProvider) Observe(context.Context) (provider.Snapshot, error) {
	p.calls.Add(1)
	<-p.release
	return provider.Snapshot{}, nil
}
func (p *hangingProvider) Prepare(context.Context, store.ResourceInstance) error { return nil }
func (p *hangingProvider) Release(context.Context, store.ResourceInstance) error { return nil }

// openProviderStore returns a store with the node and provider that an
// observation is recorded against.
func openProviderStore(t *testing.T) *store.Store {
	t.Helper()
	ctx := context.Background()
	st, err := store.Open(filepath.Join(t.TempDir(), "kairo.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if err = st.UpsertNode(ctx, store.Node{ID: "n", Name: "n", OS: "windows", Architecture: "amd64", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err = st.UpsertProvider(ctx, "gpu-provider", "n", "nvidia", nil); err != nil {
		t.Fatal(err)
	}
	return st
}

func waitFor(t *testing.T, what string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// A provider observation that never returns fails within the call timeout and
// the tick goes on; no second observation starts while the abandoned one is
// outstanding.
func TestHungProviderObservationFailsWithinItsTimeoutAndIsNotRepeated(t *testing.T) {
	ctx := context.Background()
	st := openProviderStore(t)
	p := &hangingProvider{release: make(chan struct{})}
	var logs bytes.Buffer
	executor := NewLocal("e", st, t.TempDir(), slog.New(slog.NewTextHandler(&logs, nil)))
	executor.ObserveOnly = true
	executor.ObservationInterval = time.Nanosecond
	executor.ProviderCallTimeout = 20 * time.Millisecond
	executor.Providers = map[string]provider.Provider{"gpu-provider": p}

	start := time.Now()
	if err := executor.tick(ctx); err != nil {
		t.Fatalf("a hung observation must not fail the tick: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Fatalf("tick waited %s for a hung observation", elapsed)
	}
	if !strings.Contains(logs.String(), "provider observation failed") {
		t.Fatalf("hung observation not reported: %s", logs.String())
	}
	if err := executor.tick(ctx); err != nil {
		t.Fatal(err)
	}
	// Another executor of the daemon shares the provider and must wait too.
	other := NewLocal("f", st, t.TempDir(), slog.New(slog.NewTextHandler(&logs, nil)))
	other.ObserveOnly = true
	other.ObservationInterval = time.Nanosecond
	other.Providers = map[string]provider.Provider{"gpu-provider": p}
	if err := other.tick(ctx); err != nil {
		t.Fatal(err)
	}
	if n := p.calls.Load(); n != 1 {
		t.Fatalf("second observation started while the first is outstanding: %d calls", n)
	}
	close(p.release)
	waitFor(t, "the abandoned observation to return", func() bool {
		abandonedMu.Lock()
		defer abandonedMu.Unlock()
		return abandonedCalls["gpu-provider"] == nil
	})
	if err := executor.tick(ctx); err != nil {
		t.Fatal(err)
	}
	if n := p.calls.Load(); n != 2 {
		t.Fatalf("observation not retried once the call returned: %d calls", n)
	}
}

// slowProvider takes a while to observe and records how many observations ran
// at once.
type slowProvider struct {
	running, most, calls atomic.Int32
}

func (p *slowProvider) ID() string { return "gpu-provider" }
func (p *slowProvider) Observe(context.Context) (provider.Snapshot, error) {
	p.calls.Add(1)
	n := p.running.Add(1)
	for {
		m := p.most.Load()
		if n <= m || p.most.CompareAndSwap(m, n) {
			break
		}
	}
	time.Sleep(100 * time.Millisecond)
	p.running.Add(-1)
	return provider.Snapshot{}, nil
}
func (p *slowProvider) Prepare(context.Context, store.ResourceInstance) error { return nil }
func (p *slowProvider) Release(context.Context, store.ResourceInstance) error { return nil }

// The executors of a daemon share their providers and tick on the same
// cadence: calls that overlap while running normally both succeed (only an
// abandoned call holds off the next one).
func TestOverlappingProviderCallsFromTwoExecutorsBothSucceed(t *testing.T) {
	ctx := context.Background()
	st := openProviderStore(t)
	p := &slowProvider{}
	var logs bytes.Buffer
	var mu sync.Mutex
	logger := slog.New(slog.NewTextHandler(&lockedWriter{w: &logs, mu: &mu}, nil))
	executors := []*Local{NewLocal("e", st, t.TempDir(), logger), NewLocal("f", st, t.TempDir(), logger)}
	errs := make(chan error, len(executors))
	for _, x := range executors {
		x.ObserveOnly = true
		x.ObservationInterval = time.Nanosecond
		x.Providers = map[string]provider.Provider{"gpu-provider": p}
		go func(x *Local) { errs <- x.tick(ctx) }(x)
	}
	for range executors {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	out := logs.String()
	mu.Unlock()
	if strings.Contains(out, "provider observation failed") {
		t.Fatalf("an overlapping observation failed: %s", out)
	}
	if n, most := p.calls.Load(), p.most.Load(); n != 2 || most != 2 {
		t.Fatalf("want both observations to run, at once: %d calls, at most %d together", n, most)
	}
}

type lockedWriter struct {
	w  io.Writer
	mu *sync.Mutex
}

func (l *lockedWriter) Write(b []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(b)
}

// A step that runs past StallThreshold is reported while it runs, and its end
// is reported once it finishes.
func TestStalledStepIsReportedWhileItRunsAndResumedAfter(t *testing.T) {
	ctx := t.Context()
	st := openProviderStore(t)
	p := &hangingProvider{release: make(chan struct{})}
	var logs syncBuffer
	executor := NewLocal("e", st, t.TempDir(), slog.New(slog.NewTextHandler(&logs, nil)))
	executor.ObserveOnly = true
	executor.ObservationInterval = time.Nanosecond
	executor.StallThreshold = 20 * time.Millisecond
	executor.ProviderCallTimeout = time.Minute
	executor.Providers = map[string]provider.Provider{"gpu-provider": p}
	go executor.watchSteps(ctx)

	done := make(chan error, 1)
	go func() { done <- executor.tick(ctx) }()
	waitFor(t, "the stall warning", func() bool { return strings.Contains(logs.String(), "executor step stalled") })
	close(p.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	out := logs.String()
	if !strings.Contains(out, "level=WARN") || !strings.Contains(out, "executor_id=e") || !strings.Contains(out, "step=observe:gpu-provider") || !strings.Contains(out, "age_seconds=") {
		t.Fatalf("stall warning does not name the executor and step: %s", out)
	}
	if !strings.Contains(out, "level=INFO") || !strings.Contains(out, "executor step resumed") {
		t.Fatalf("resumption of the stalled step not reported: %s", out)
	}
}
