package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestAPITokenLifecycle(t *testing.T) {
	store := openCoordinationStore(t)
	ctx := context.Background()
	admin, plain, err := store.CreateAPIToken(ctx, "operator", RoleAdmin, "")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(plain, "kairo_admin_") || admin.Role != RoleAdmin || admin.NodeID != nil {
		t.Fatalf("token %q %+v", plain, admin)
	}
	got, err := store.AuthenticateAPIToken(ctx, plain)
	if err != nil || got.ID != admin.ID || got.Name != "operator" {
		t.Fatalf("authenticate: %+v %v", got, err)
	}
	var stored string
	if err = store.db.QueryRow(`SELECT token_hash FROM api_tokens WHERE id=?`, admin.ID).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(stored, plain) || stored != hashToken(plain) {
		t.Fatal("token must be stored as its hash only")
	}
	if _, _, err = store.CreateAPIToken(ctx, "operator", RoleRead, ""); err == nil {
		t.Fatal("two active tokens with one name")
	}
	for _, bad := range []struct{ role, node string }{{RoleNode, ""}, {RoleAdmin, "node"}, {"root", ""}} {
		if _, _, err = store.CreateAPIToken(ctx, "bad-"+bad.role, bad.role, bad.node); err == nil {
			t.Fatalf("accepted role %q node %q", bad.role, bad.node)
		}
	}
	node, nodePlain, err := store.CreateAPIToken(ctx, "pve0", RoleNode, "pve0-node")
	if err != nil || node.NodeID == nil || *node.NodeID != "pve0-node" || !strings.HasPrefix(nodePlain, "kairo_node_") {
		t.Fatalf("node token: %+v %v", node, err)
	}
	if _, err = store.AuthenticateAPIToken(ctx, plain+"x"); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("unknown token: %v", err)
	}
	if _, err = store.AuthenticateAPIToken(ctx, ""); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("empty token: %v", err)
	}
	if err = store.RevokeAPIToken(ctx, "operator"); err != nil {
		t.Fatal(err)
	}
	if _, err = store.AuthenticateAPIToken(ctx, plain); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("revoked token still authenticates: %v", err)
	}
	if err = store.RevokeAPIToken(ctx, "operator"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second revoke: %v", err)
	}
	// The name is free again once its token is revoked.
	if _, _, err = store.CreateAPIToken(ctx, "operator", RoleAdmin, ""); err != nil {
		t.Fatal(err)
	}
	tokens, err := store.ListAPITokens(ctx)
	if err != nil || len(tokens) != 3 || tokens[0].RevokedAt == nil || tokens[0].LastUsedAt == nil {
		t.Fatalf("list: %+v %v", tokens, err)
	}
}

func TestWorkerTokenFollowsTheAttempt(t *testing.T) {
	store, _, launch := startCooperativeExecution(t, "worker-token")
	ctx := context.Background()
	if !strings.HasPrefix(launch.WorkerToken, "kairo_worker_") {
		t.Fatalf("worker token %q", launch.WorkerToken)
	}
	identity, err := store.AuthenticateWorkerToken(ctx, launch.WorkerToken)
	if err != nil {
		t.Fatal(err)
	}
	if identity.AttemptID != launch.Attempt.ID || identity.LeaseID != launch.Lease.ID || identity.Epoch != launch.Lease.CoordinationEpoch {
		t.Fatalf("identity %+v for launch %+v", identity, launch)
	}
	// An operator token is not a worker token and vice versa.
	if _, err = store.AuthenticateAPIToken(ctx, launch.WorkerToken); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("worker token accepted as API token: %v", err)
	}
	if _, err = store.AuthenticateWorkerToken(ctx, launch.AuthorizationToken); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("launch token accepted as worker token: %v", err)
	}
	if err = store.Heartbeat(ctx, identity.AttemptID, identity.LeaseID, identity.Epoch, json.RawMessage(`{"step":1}`)); err != nil {
		t.Fatal(err)
	}
	// Exited but not yet quiesced: the attempt's other processes may still report.
	if err = store.RecordTerminal(ctx, launch.Attempt.ID, launch.Lease.ID, identity.Epoch, 0, ""); err != nil {
		t.Fatal(err)
	}
	if _, err = store.AuthenticateWorkerToken(ctx, launch.WorkerToken); err != nil {
		t.Fatalf("exited attempt: %v", err)
	}
	refreshSchedulerGPU(t, store)
	if err = store.FinalizeQuiescence(ctx, launch.Attempt.ID, launch.Lease.ID, identity.Epoch); err != nil {
		t.Fatal(err)
	}
	if _, err = store.AuthenticateWorkerToken(ctx, launch.WorkerToken); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("quiesced attempt's token still valid: %v", err)
	}
}

func TestWorkerTokenIsFencedWithItsAttempt(t *testing.T) {
	store, _, launch := startCooperativeExecution(t, "worker-token-fenced")
	ctx := context.Background()
	// A daemon restart treats the executor's attempts as lost and bumps their
	// epoch: the token issued under the old epoch no longer authenticates.
	if _, err := store.MarkExecutorUnknown(ctx, "executor"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AuthenticateWorkerToken(ctx, launch.WorkerToken); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("token of a lost attempt: %v", err)
	}
}

func TestNodesCannotTakeOverEachOthersObjects(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	ctx := context.Background()
	if err := store.UpsertNode(ctx, Node{ID: "other", Name: "other", OS: "linux", Architecture: "amd64", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertExecutor(ctx, Executor{ID: "executor", NodeID: "other", Kind: "local", Enabled: true}); !errors.Is(err, ErrOwnershipConflict) {
		t.Fatalf("executor moved to another node: %v", err)
	}
	if err := store.UpsertProvider(ctx, "gpu-provider", "other", "nvidia", nil); !errors.Is(err, ErrOwnershipConflict) {
		t.Fatalf("provider moved to another node: %v", err)
	}
	// Re-registering on the owning node still refreshes the row.
	if err := store.UpsertExecutor(ctx, Executor{ID: "executor", NodeID: "node", Kind: "local", Attributes: json.RawMessage(`{"x":"1"}`), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if node, err := store.ExecutorNode(ctx, "executor"); err != nil || node != "node" {
		t.Fatalf("executor node %q %v", node, err)
	}
	if err := store.UpsertProvider(ctx, "other-provider", "other", "nvidia", nil); err != nil {
		t.Fatal(err)
	}
	stamp := time.Now().UTC()
	observation := Observation{ResourceID: "gpu-0", ObservedAt: stamp.Format(time.RFC3339Nano), ValidUntil: stamp.Add(time.Minute).Format(time.RFC3339Nano)}
	if err := store.ApplyObservationBatch(ctx, "other-provider", nil, []Observation{observation}, nil); !errors.Is(err, ErrOwnershipConflict) {
		t.Fatalf("observation for another provider's resource: %v", err)
	}
	if err := store.ApplyObservationBatch(ctx, "other-provider", nil, nil, []ExternalClaim{{ResourceID: "gpu-0", ClaimKind: "external_process"}}); !errors.Is(err, ErrOwnershipConflict) {
		t.Fatalf("claim on another provider's resource: %v", err)
	}
	stolen := ResourceInstance{ID: "gpu-0", NodeID: "other", ProviderID: "other-provider", Kind: "gpu", StableIdentity: "GPU-0"}
	if err := store.ApplyObservationBatch(ctx, "other-provider", []ResourceInstance{stolen}, nil, nil); !errors.Is(err, ErrOwnershipConflict) {
		t.Fatalf("resource moved to another provider: %v", err)
	}
	foreign := ResourceInstance{ID: "gpu-9", NodeID: "node", ProviderID: "other-provider", Kind: "gpu", StableIdentity: "GPU-9"}
	if err := store.ApplyObservationBatch(ctx, "other-provider", []ResourceInstance{foreign}, nil, nil); !errors.Is(err, ErrOwnershipConflict) {
		t.Fatalf("provider reported a resource of another node: %v", err)
	}
	if err := store.ApplyObservationBatch(ctx, "gpu-provider", nil, []Observation{observation}, nil); err != nil {
		t.Fatalf("owning provider's observation: %v", err)
	}
}

func TestNodeOfCoordinationObjects(t *testing.T) {
	store, _, launch := startCooperativeExecution(t, "node-of")
	ctx := context.Background()
	for name, lookup := range map[string]func() (string, error){
		"executor": func() (string, error) { return store.ExecutorNode(ctx, "executor") },
		"provider": func() (string, error) { return store.ProviderNode(ctx, "gpu-provider") },
		"lease":    func() (string, error) { return store.LeaseNode(ctx, launch.Lease.ID) },
		"attempt":  func() (string, error) { return store.AttemptNode(ctx, launch.Attempt.ID) },
	} {
		if node, err := lookup(); err != nil || node != "node" {
			t.Fatalf("%s: %q %v", name, node, err)
		}
	}
	if _, err := store.AttemptNode(ctx, "att_missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing attempt: %v", err)
	}
}
