package store

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"kairo/internal/machine"
)

// declareTestGPU declares one GPU for a node, keeping the other declarations.
func declareTestGPU(t testing.TB, store *Store, uuid, nodeID string) {
	t.Helper()
	if _, err := store.testExec(`DELETE FROM machine_gpus WHERE uuid=$1`, uuid); err != nil {
		t.Fatal(err)
	}
	if _, err := store.testExec(`INSERT INTO machine_gpus(uuid,node_id,declared_at) VALUES($1,$2,$3)`, uuid, nodeID, now()); err != nil {
		t.Fatal(err)
	}
}

// gpuPoll is a provider poll that reports one GPU, fresh, with a claim.
func gpuPoll(t *testing.T, store *Store, providerID, nodeID, uuid string, claim bool) error {
	t.Helper()
	stamp := time.Now().UTC()
	total, free := int64(12<<30), int64(11<<30)
	id := "gpu-" + uuid
	resources := []ResourceInstance{{ID: id, NodeID: nodeID, ProviderID: providerID, Kind: "gpu", StableIdentity: uuid, Binding: json.RawMessage(`{}`)}}
	observations := []Observation{{ResourceID: id, ObservedAt: stamp.Format(time.RFC3339Nano), ValidUntil: stamp.Add(time.Minute).Format(time.RFC3339Nano), TotalBytes: &total, FreeBytes: &free}}
	var claims []ExternalClaim
	if claim {
		claims = []ExternalClaim{{ID: "claim-" + id + "-unattributed", ResourceID: id, ClaimKind: "unattributed_activity"}}
	}
	return store.ApplyObservationBatch(context.Background(), providerID, resources, observations, claims)
}

// setupTwoGPUNodes registers nodes "a" and "b", each with an nvidia provider.
func setupTwoGPUNodes(t *testing.T, store *Store) {
	t.Helper()
	ctx := context.Background()
	for _, node := range []string{"a", "b"} {
		if err := store.UpsertNode(ctx, Node{ID: node, Name: node, OS: "linux", Architecture: "amd64", Enabled: true}); err != nil {
			t.Fatal(err)
		}
		if err := store.UpsertProvider(ctx, node+"-gpu", node, "nvidia", nil); err != nil {
			t.Fatal(err)
		}
	}
}

func resourceByID(t *testing.T, store *Store, id string) ResourceInstance {
	t.Helper()
	resources, err := store.ListResourceStatus(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range resources {
		if r.ID == id {
			return r
		}
	}
	t.Fatalf("resource %s not registered: %+v", id, resources)
	return ResourceInstance{}
}

func TestUndeclaredGPUIsReportedNotRegisteredAndTheRestOfThePollApplies(t *testing.T) {
	store := openCoordinationStore(t)
	setupTwoGPUNodes(t, store)
	ctx := context.Background()
	if _, err := store.ApplyMachines(ctx, machine.Spec{Nodes: []machine.Node{{ID: "a", GPUs: []string{"GPU-1"}}}}, false); err != nil {
		t.Fatal(err)
	}
	stamp := time.Now().UTC()
	total, free := int64(12<<30), int64(11<<30)
	observe := func(id string) Observation {
		return Observation{ResourceID: id, ObservedAt: stamp.Format(time.RFC3339Nano), ValidUntil: stamp.Add(time.Minute).Format(time.RFC3339Nano), TotalBytes: &total, FreeBytes: &free}
	}
	// One poll reports a declared GPU and an undeclared one.
	resources := []ResourceInstance{
		{ID: "gpu-GPU-1", NodeID: "a", ProviderID: "a-gpu", Kind: "gpu", StableIdentity: "GPU-1", Binding: json.RawMessage(`{}`)},
		{ID: "gpu-GPU-2", NodeID: "a", ProviderID: "a-gpu", Kind: "gpu", StableIdentity: "GPU-2", Binding: json.RawMessage(`{}`)},
	}
	claims := []ExternalClaim{{ID: "claim-2", ResourceID: "gpu-GPU-2", ClaimKind: "unattributed_activity"}}
	if err := store.ApplyObservationBatch(ctx, "a-gpu", resources, []Observation{observe("gpu-GPU-1"), observe("gpu-GPU-2")}, claims); err != nil {
		t.Fatal(err)
	}
	if r := resourceByID(t, store, "gpu-GPU-1"); r.DerivedState != "available" || r.DeclaredNodeID == nil || *r.DeclaredNodeID != "a" {
		t.Fatalf("declared GPU: %+v", r)
	}
	all, _ := store.ListResourceStatus(ctx)
	if len(all) != 1 {
		t.Fatalf("undeclared GPU was registered: %+v", all)
	}
	machines, err := store.ListMachines(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(machines.Undeclared) != 1 || machines.Undeclared[0].UUID != "GPU-2" || machines.Undeclared[0].NodeID != "a" || machines.Undeclared[0].DeclaredNodeID != nil {
		t.Fatalf("undeclared GPUs: %+v", machines.Undeclared)
	}
	if len(machines.GPUs) != 1 || machines.GPUs[0].State != "observed" {
		t.Fatalf("declared GPUs: %+v", machines.GPUs)
	}
	// Once the GPU is gone from the poll it is no longer reported.
	if err := gpuPoll(t, store, "a-gpu", "a", "GPU-1", false); err != nil {
		t.Fatal(err)
	}
	if machines, _ = store.ListMachines(ctx); len(machines.Undeclared) != 0 {
		t.Fatalf("a GPU gone from the poll is still reported: %+v", machines.Undeclared)
	}
}

func TestMovedGPUIsTakenOverOnlyOnceDeclaredForItsNewNode(t *testing.T) {
	store := openCoordinationStore(t)
	setupTwoGPUNodes(t, store)
	ctx := context.Background()
	if _, err := store.ApplyMachines(ctx, machine.Spec{Nodes: []machine.Node{{ID: "a", GPUs: []string{"GPU-X"}}}}, false); err != nil {
		t.Fatal(err)
	}
	if err := gpuPoll(t, store, "a-gpu", "a", "GPU-X", true); err != nil {
		t.Fatal(err)
	}
	// The card is moved to node b: b reports it before the declaration says so.
	if err := gpuPoll(t, store, "b-gpu", "b", "GPU-X", false); err != nil {
		t.Fatalf("a moved GPU failed the poll of its new node: %v", err)
	}
	if r := resourceByID(t, store, "gpu-GPU-X"); r.NodeID != "a" {
		t.Fatalf("GPU moved without a declaration: %+v", r)
	}
	machines, _ := store.ListMachines(ctx)
	if len(machines.Undeclared) != 1 || machines.Undeclared[0].DeclaredNodeID == nil || *machines.Undeclared[0].DeclaredNodeID != "a" {
		t.Fatalf("undeclared GPUs: %+v", machines.Undeclared)
	}

	result, err := store.ApplyMachines(ctx, machine.Spec{Nodes: []machine.Node{{ID: "a"}, {ID: "b", GPUs: []string{"GPU-X"}}}}, true)
	if err != nil || !result.DryRun || len(result.Changes) != 1 || result.Changes[0] != (MachineChange{UUID: "GPU-X", From: "a", To: "b"}) {
		t.Fatalf("dry run: %+v %v", result, err)
	}
	if machines, _ = store.ListMachines(ctx); machines.GPUs[0].NodeID != "a" {
		t.Fatalf("dry run changed the declaration: %+v", machines.GPUs)
	}
	if _, err = store.ApplyMachines(ctx, machine.Spec{Nodes: []machine.Node{{ID: "a"}, {ID: "b", GPUs: []string{"GPU-X"}}}}, false); err != nil {
		t.Fatal(err)
	}
	// Until b reports it, the GPU stays on a, unschedulable, without a's claims.
	if r := resourceByID(t, store, "gpu-GPU-X"); r.NodeID != "a" || r.DerivedState != "undeclared" {
		t.Fatalf("moved GPU before b reports it: %+v", r)
	}
	if machines, _ = store.ListMachines(ctx); machines.GPUs[0].State != "pending" {
		t.Fatalf("declared GPU before b reports it: %+v", machines.GPUs)
	}
	if claims, _ := store.ListClaims(ctx); len(claims) != 0 {
		t.Fatalf("claims of the old host were kept: %+v", claims)
	}
	if err = gpuPoll(t, store, "b-gpu", "b", "GPU-X", false); err != nil {
		t.Fatal(err)
	}
	if r := resourceByID(t, store, "gpu-GPU-X"); r.NodeID != "b" || r.ProviderID != "b-gpu" || r.DerivedState != "available" {
		t.Fatalf("moved GPU after b reports it: %+v", r)
	}
	if machines, _ = store.ListMachines(ctx); len(machines.Undeclared) != 0 || machines.GPUs[0].State != "observed" {
		t.Fatalf("machines after the move: %+v", machines)
	}
	// The old node can no longer report it.
	if err = gpuPoll(t, store, "a-gpu", "a", "GPU-X", false); err != nil {
		t.Fatal(err)
	}
	if r := resourceByID(t, store, "gpu-GPU-X"); r.NodeID != "b" {
		t.Fatalf("the old node took the GPU back: %+v", r)
	}
	events, err := store.ListEvents(ctx, EventFilter{})
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, e := range events {
		seen[e.EventType] = true
	}
	if !seen["gpu_declaration_changed"] || !seen["gpu_ownership_transferred"] {
		t.Fatalf("events: %+v", seen)
	}
}

func TestMachineApplyRefusesToMoveALeasedGPU(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	ctx := context.Background()
	if err := store.UpsertNode(ctx, Node{ID: "other", Name: "other", OS: "linux", Architecture: "amd64", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.SubmitExecution(ctx, schedulerExecutionSpec("leased-gpu")); err != nil {
		t.Fatal(err)
	}
	if reservation, err := store.ReserveNext(ctx, "executor"); err != nil || reservation == nil {
		t.Fatalf("reserve: %+v %v", reservation, err)
	}
	for _, spec := range []machine.Spec{
		{Nodes: []machine.Node{{ID: "node"}, {ID: "other", GPUs: []string{"GPU-0"}}}},
		{Nodes: []machine.Node{{ID: "node"}}},
	} {
		if _, err := store.ApplyMachines(ctx, spec, false); !errors.Is(err, ErrMachineConflict) {
			t.Fatalf("moved or dropped a leased GPU: %+v %v", spec, err)
		}
	}
	if _, err := store.ApplyMachines(ctx, machine.Spec{Nodes: []machine.Node{{ID: "node", GPUs: []string{"GPU-0"}}}}, false); err != nil {
		t.Fatalf("an unchanged declaration of a leased GPU: %v", err)
	}
	if _, err := store.ApplyMachines(ctx, machine.Spec{Nodes: []machine.Node{{ID: "ghost"}}}, false); err == nil {
		t.Fatal("declared an unknown node")
	}
}

func TestSchedulerSkipsAnUndeclaredGPU(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	ctx := context.Background()
	if _, err := store.ApplyMachines(ctx, machine.Spec{Nodes: []machine.Node{{ID: "node"}}}, false); err != nil {
		t.Fatal(err)
	}
	if err := gpuPoll(t, store, "gpu-provider", "node", "GPU-0", false); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.SubmitExecution(ctx, schedulerExecutionSpec("undeclared-gpu")); err != nil {
		t.Fatal(err)
	}
	if reservation, err := store.ReserveNext(ctx, "executor"); err != nil || reservation != nil {
		t.Fatalf("reserved an undeclared GPU: %+v %v", reservation, err)
	}
	if _, err := store.ApplyMachines(ctx, machine.Spec{Nodes: []machine.Node{{ID: "node", GPUs: []string{"GPU-0"}}}}, false); err != nil {
		t.Fatal(err)
	}
	refreshSchedulerGPU(t, store)
	if reservation, err := store.ReserveNext(ctx, "executor"); err != nil || reservation == nil {
		t.Fatalf("declared GPU was not reserved: %+v %v", reservation, err)
	}
}
