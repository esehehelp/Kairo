package store

import (
	"context"
	"strings"
	"testing"

	"kairo/internal/machine"
)

func packOrders(t *testing.T, store *Store) map[string]PackOrder {
	t.Helper()
	orders, err := store.PacksToMake(context.Background(), "executor")
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]PackOrder{}
	for _, o := range orders {
		out[o.Kind+":"+o.SourceID] = o
	}
	if len(out) != len(orders) {
		t.Fatalf("an object ordered twice: %+v", orders)
	}
	return out
}

// A run's output directory is ordered packed once its run has ended and its
// last attempt has quiesced; a failed attempt's scratch once it is kept and
// the attempt has quiesced; each once, as one object, until its pack is
// recorded, which must be <root>/packs/<object id>.tar.zst with its list.
func TestPacksAreOrderedOnceNothingWritesThere(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	addTestDisk(t, store, "node", "D:")
	ctx := context.Background()
	declareTestStore(t, store, machine.Store{ID: "d-scratch", Class: machine.StoreVolatile, Node: "node", Root: "D:/kairo", Filesystem: "D:"})
	launch := authorizeScratchTest(t, store, "packs")
	if err := store.RecordAttemptScratch(ctx, scratchOf(launch, "d-scratch")); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordRunOutput(ctx, outputOf(launch, "d-scratch")); err != nil {
		t.Fatal(err)
	}
	epoch := launch.Lease.CoordinationEpoch
	if err := store.ActivateLaunch(ctx, launch.Attempt.ID, launch.Lease.ID, epoch, launch.AuthorizationToken, 100, "launcher:100"); err != nil {
		t.Fatal(err)
	}
	if orders := packOrders(t, store); len(orders) != 0 {
		t.Fatalf("ordered while running: %+v", orders)
	}
	if err := store.RecordTerminal(ctx, launch.Attempt.ID, launch.Lease.ID, epoch, 1, ""); err != nil {
		t.Fatal(err)
	}
	if orders := packOrders(t, store); len(orders) != 0 {
		t.Fatalf("ordered before the attempt quiesced: %+v", orders)
	}
	refreshSchedulerGPU(t, store)
	if err := store.FinalizeQuiescence(ctx, launch.Attempt.ID, launch.Lease.ID, epoch); err != nil {
		t.Fatal(err)
	}
	orders := packOrders(t, store)
	out, ok := orders["output:"+launch.RunID]
	if len(orders) != 1 || !ok || out.StoreID != "d-scratch" || out.Root != "D:/kairo" || out.Path != "D:/kairo/outputs/"+launch.RunID {
		t.Fatalf("after quiescence (the scratch not kept yet): %+v", orders)
	}
	settleExpect(t, store, map[string]ScratchSettlement{launch.Attempt.ID: {Action: ScratchKeep, Reason: "exit 1"}})
	if err := store.SettleScratch(ctx, ScratchSettled{AttemptID: launch.Attempt.ID, State: "kept"}); err != nil {
		t.Fatal(err)
	}
	orders = packOrders(t, store)
	scr, ok := orders["scratch:"+launch.Attempt.ID]
	if len(orders) != 2 || !ok || orders["output:"+launch.RunID].ObjectID != out.ObjectID || scr.Path != "D:/kairo/scratch/"+launch.Attempt.ID {
		t.Fatalf("once the scratch is kept: %+v", orders)
	}

	sha := strings.Repeat("ab", 32)
	made := PackMade{ObjectID: out.ObjectID, ExecutorID: "executor", PackPath: "D:/kairo/packs/" + out.ObjectID + ".tar.zst",
		ManifestPath: "D:/kairo/packs/" + out.ObjectID + ".tar.zst.files.tsv", PackBytes: 100, ContentBytes: 900, Files: 3, PackSHA256: sha, ManifestSHA256: sha}
	refused := func(what string, change func(*PackMade), want string) {
		t.Helper()
		bad := made
		change(&bad)
		if err := store.RecordPack(ctx, bad); err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("%s: %v, want %q", what, err, want)
		}
	}
	refused("by another executor", func(m *PackMade) { m.ExecutorID = "another" }, "executor executor's to pack")
	refused("elsewhere", func(m *PackMade) { m.PackPath = "D:/kairo/outputs/x.tar.zst" }, "is not <root>/packs/<object id>.tar.zst")
	refused("another object's", func(m *PackMade) { m.PackPath = "D:/kairo/packs/" + scr.ObjectID + ".tar.zst" }, "is not <root>/packs")
	refused("its list elsewhere", func(m *PackMade) { m.ManifestPath = "D:/kairo/packs/list.tsv" }, "is not beside its pack")
	refused("a bad sha256", func(m *PackMade) { m.PackSHA256 = "xyz" }, "not 64 hex digits")
	refused("empty", func(m *PackMade) { m.PackBytes = 0 }, "impossible sizes")
	if err := store.RecordPack(ctx, made); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordPack(ctx, made); err != nil {
		t.Fatalf("recorded again: %v", err)
	}
	other := made
	other.PackSHA256 = strings.Repeat("cd", 32)
	if err := store.RecordPack(ctx, other); err == nil {
		t.Fatal("another pack recorded over a packed object")
	}
	orders = packOrders(t, store)
	if len(orders) != 1 || orders["scratch:"+launch.Attempt.ID].ObjectID != scr.ObjectID {
		t.Fatalf("after the output was packed: %+v", orders)
	}
	var state string
	var packBytes int64
	if err := store.db.QueryRow(`SELECT state,pack_bytes FROM data_objects WHERE id=$1`, out.ObjectID).Scan(&state, &packBytes); err != nil || state != "packed" || packBytes != 100 {
		t.Fatalf("object: %s %d %v", state, packBytes, err)
	}
	stores, err := store.ListStoreStatus(ctx)
	if err != nil || len(stores) != 1 || stores[0].Packs != 1 || stores[0].PackBytes != 100 {
		t.Fatalf("store status: %+v %v", stores, err)
	}

	// a directory the declaration no longer makes Kairo's is not ordered
	declareTestStore(t, store, machine.Store{ID: "d-scratch", Class: machine.StoreVolatile, Node: "node", Root: "D:/moved", Filesystem: "D:"})
	launch2 := authorizeScratchTest(t, store, "moved")
	if err := store.RecordRunOutput(ctx, RunOutput{AttemptID: launch2.Attempt.ID, LeaseID: launch2.Lease.ID, Epoch: launch2.Lease.CoordinationEpoch, RunID: launch2.RunID,
		StoreID: "d-scratch", ExecutorID: "executor", Root: "D:/moved", Path: "D:/moved/outputs/" + launch2.RunID}); err != nil {
		t.Fatal(err)
	}
	if err := store.ActivateLaunch(ctx, launch2.Attempt.ID, launch2.Lease.ID, launch2.Lease.CoordinationEpoch, launch2.AuthorizationToken, 100, "launcher:100"); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordTerminal(ctx, launch2.Attempt.ID, launch2.Lease.ID, launch2.Lease.CoordinationEpoch, 0, ""); err != nil {
		t.Fatal(err)
	}
	refreshSchedulerGPU(t, store)
	if err := store.FinalizeQuiescence(ctx, launch2.Attempt.ID, launch2.Lease.ID, launch2.Lease.CoordinationEpoch); err != nil {
		t.Fatal(err)
	}
	declareTestStore(t, store, machine.Store{ID: "d-scratch", Class: machine.StoreVolatile, Node: "node", Root: "D:/elsewhere", Filesystem: "D:"})
	// neither the new directory nor the scratch ordered before (its store has
	// moved since) is handed out
	if orders = packOrders(t, store); len(orders) != 0 {
		t.Fatalf("a directory no longer declared was ordered: %+v", orders)
	}
}
