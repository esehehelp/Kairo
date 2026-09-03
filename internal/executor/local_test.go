package executor

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"kairo/internal/plan"
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
	manifest, err := plan.Parse([]byte(`schema_version=1
project="pilot"
queue="observe-only"
[[tasks]]
key="waiting"
argv=["train"]
cwd="."
[[tasks.exclusive]]
kind="gpu"
count=1
`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = st.ApplyPlan(ctx, manifest, store.ApplyOptions{ExpectedRevision: 0, Create: true, Actor: "test", RequestID: "observe-only"}); err != nil {
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
	status, err := st.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(status.Resources) != 1 || status.Resources[0].DerivedState != "conflicted" || len(status.Claims) != 1 {
		t.Fatalf("unmanaged workload observation was not retained: %+v", status)
	}
	if len(status.Leases) != 0 || len(status.Attempts) != 0 || p.prepareCalls != 0 {
		t.Fatalf("observe-only mode allocated work: leases=%+v attempts=%+v prepares=%d", status.Leases, status.Attempts, p.prepareCalls)
	}
}
