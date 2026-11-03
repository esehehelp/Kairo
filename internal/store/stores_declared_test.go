package store

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"kairo/internal/machine"
)

// addTestDisk registers a disk resource (filesystem identity) on a node, as
// its filesystem provider would.
func addTestDisk(t *testing.T, store *Store, nodeID, filesystem string) {
	t.Helper()
	ctx := context.Background()
	if err := store.UpsertProvider(ctx, nodeID+"-fs", nodeID, "filesystem", nil); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertResourceInstance(ctx, ResourceInstance{ID: nodeID + "-disk-" + filesystem, NodeID: nodeID, ProviderID: nodeID + "-fs", Kind: "disk", StableIdentity: filesystem, Binding: json.RawMessage(`{}`), AdminState: "enabled"}); err != nil {
		t.Fatal(err)
	}
}

func storesSpec(stores ...machine.Store) machine.Spec {
	return machine.Spec{Nodes: []machine.Node{{ID: "node", GPUs: []string{"GPU-0"}}}, Stores: stores}
}

func TestApplyMachinesDeclaresStores(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	addTestDisk(t, store, "node", "D:")
	ctx := context.Background()
	scratch := machine.Store{ID: "d-scratch", Class: machine.StoreVolatile, Node: "node", Root: "D:/kairo", Filesystem: "D:"}

	// the filesystem must be a disk of the node, the executor one of its own
	wrong := scratch
	wrong.Filesystem = "E:"
	if _, err := store.ApplyMachines(ctx, storesSpec(wrong), false); err == nil || !strings.Contains(err.Error(), "filesystem E: is not a disk of node node (its disks: D:)") {
		t.Fatalf("unknown filesystem: %v", err)
	}
	wrong = scratch
	wrong.Executor = "elsewhere"
	if _, err := store.ApplyMachines(ctx, storesSpec(wrong), false); err == nil || !strings.Contains(err.Error(), "executor elsewhere is not an executor of node node") {
		t.Fatalf("unknown executor: %v", err)
	}
	wrong = scratch
	wrong.Node = "nowhere"
	if _, err := store.ApplyMachines(ctx, storesSpec(wrong), false); err == nil || !strings.Contains(err.Error(), "unknown node nowhere") {
		t.Fatalf("unknown node: %v", err)
	}

	dry, err := store.ApplyMachines(ctx, storesSpec(scratch), true)
	if err != nil || len(dry.StoreChanges) != 1 || dry.StoreChanges[0].To == nil || dry.StoreChanges[0].To.Backend != machine.StoreBackendLocalFS {
		t.Fatalf("dry run: %+v %v", dry, err)
	}
	if stores, _ := store.NodeStores(ctx, "node"); len(stores) != 0 {
		t.Fatalf("a dry run declared %+v", stores)
	}
	if _, err = store.ApplyMachines(ctx, storesSpec(scratch), false); err != nil {
		t.Fatal(err)
	}
	stores, err := store.NodeStores(ctx, "node")
	if err != nil || len(stores) != 1 || stores[0].ID != "d-scratch" || stores[0].Root != "D:/kairo" || stores[0].Backend != machine.StoreBackendLocalFS {
		t.Fatalf("node stores: %+v %v", stores, err)
	}
	if other, _ := store.NodeStores(ctx, "other"); len(other) != 0 {
		t.Fatalf("another node's stores: %+v", other)
	}
	machines, err := store.ListMachines(ctx)
	if err != nil || len(machines.Stores) != 1 {
		t.Fatalf("machines: %+v %v", machines, err)
	}
	again, err := store.ApplyMachines(ctx, storesSpec(scratch), false)
	if err != nil || len(again.StoreChanges) != 0 || again.UnchangedStores != 1 {
		t.Fatalf("unchanged: %+v %v", again, err)
	}
	moved := scratch
	moved.Root = "D:/kairo2"
	changed, err := store.ApplyMachines(ctx, storesSpec(moved), false)
	if err != nil || len(changed.StoreChanges) != 1 || changed.StoreChanges[0].From == nil || changed.StoreChanges[0].From.Root != "D:/kairo" || changed.StoreChanges[0].To.Root != "D:/kairo2" {
		t.Fatalf("changed: %+v %v", changed, err)
	}
	dropped, err := store.ApplyMachines(ctx, storesSpec(), false)
	if err != nil || len(dropped.StoreChanges) != 1 || dropped.StoreChanges[0].To != nil {
		t.Fatalf("dropped: %+v %v", dropped, err)
	}
	if stores, _ = store.NodeStores(ctx, "node"); len(stores) != 0 {
		t.Fatalf("dropped store still declared: %+v", stores)
	}
}
