package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"kairo/internal/machine"
)

// retireFixture: a direct execution's run whose output directory on
// d-scratch (served by "executor", the test's WSL executor, so the stores
// name it) was packed as obj_out and copied to e-durable, not yet verified.
func retireFixture(t *testing.T) (*Store, *Launch, string) {
	t.Helper()
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	addTestDisk(t, store, "node", "D:")
	ctx := context.Background()
	// "executor" (a WSL one in the test inventory) packs on d-scratch;
	// durable-exec serves both stores, which name no executor
	if err := store.UpsertExecutor(ctx, Executor{ID: "durable-exec", NodeID: "node", Kind: "windows", Attributes: json.RawMessage(`{"environment":"windows"}`), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	declareTestStore(t, store,
		machine.Store{ID: "d-scratch", Class: machine.StoreVolatile, Node: "node", Root: "D:/kairo", Filesystem: "D:"},
		machine.Store{ID: "e-durable", Class: machine.StoreDurable, Node: "node", Root: "E:/kairo", Filesystem: "D:"},
	)
	launch := authorizeScratchTest(t, store, "retire")
	if err := store.RecordRunOutput(ctx, outputOf(launch, "d-scratch")); err != nil {
		t.Fatal(err)
	}
	epoch := launch.Lease.CoordinationEpoch
	if err := store.ActivateLaunch(ctx, launch.Attempt.ID, launch.Lease.ID, epoch, launch.AuthorizationToken, 100, "launcher:100"); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordTerminal(ctx, launch.Attempt.ID, launch.Lease.ID, epoch, 0, ""); err != nil {
		t.Fatal(err)
	}
	refreshSchedulerGPU(t, store)
	if err := store.FinalizeQuiescence(ctx, launch.Attempt.ID, launch.Lease.ID, epoch); err != nil {
		t.Fatal(err)
	}
	orders := packOrders(t, store)
	out := orders["output:"+launch.RunID]
	sha := strings.Repeat("ab", 32)
	if err := store.RecordPack(ctx, PackMade{ObjectID: out.ObjectID, ExecutorID: "executor", PackPath: "D:/kairo/packs/" + out.ObjectID + ".tar.zst",
		ManifestPath: "D:/kairo/packs/" + out.ObjectID + ".tar.zst.files.tsv", PackBytes: 100, ContentBytes: 900, Files: 3, PackSHA256: sha, ManifestSHA256: sha}); err != nil {
		t.Fatal(err)
	}
	return store, launch, out.ObjectID
}

func verifyCopy(t *testing.T, store *Store, objectID string) CopyOrder {
	t.Helper()
	ctx := context.Background()
	copies, err := store.CopiesToMake(ctx, "durable-exec")
	if err != nil || len(copies) != 1 || copies[0].ObjectID != objectID || !copies[0].Source.Local {
		t.Fatalf("copies %+v %v", copies, err)
	}
	c := copies[0]
	path := "E:/kairo/objects/" + c.Month + "/" + objectID + ".tar.zst"
	if err := store.RecordCopy(ctx, CopyMade{ObjectID: objectID, StoreID: "e-durable", ExecutorID: "durable-exec", Path: path, ManifestPath: path + ".files.tsv",
		Bytes: c.PackBytes, SHA256: c.PackSHA256, ManifestSHA256: c.ManifestSHA256}); err != nil {
		t.Fatal(err)
	}
	return c
}

// A source is deleted only once its copy is verified: claimed first (and
// the declaration cannot then change under it), then deleted, which marks
// the output directory deleted and asks for the node to be trimmed; a
// deletion refused for safety keeps it.
func TestASourceIsDeletedOnlyOnceItsCopyIsVerified(t *testing.T) {
	store, launch, objectID := retireFixture(t)
	ctx := context.Background()
	if orders, err := store.DeletionsToMake(ctx, "executor"); err != nil || len(orders) != 0 {
		t.Fatalf("ordered before a verified copy: %+v %v", orders, err)
	}
	settle := func(state, reason string) error {
		return store.SettleDeletion(ctx, ObjectDeletion{ObjectID: objectID, ExecutorID: "executor", State: state, Reason: reason})
	}
	if err := settle(SourceDeleting, ""); !errors.Is(err, ErrStaleEpoch) {
		t.Fatalf("claimed before a verified copy: %v", err)
	}
	verifyCopy(t, store, objectID)
	orders, err := store.DeletionsToMake(ctx, "executor")
	if err != nil || len(orders) != 1 || orders[0].ObjectID != objectID || orders[0].Kind != ObjectOutput || orders[0].SourceID != launch.RunID || orders[0].Root != "D:/kairo" {
		t.Fatalf("orders %+v %v", orders, err)
	}
	if err := settle(SourceDeleted, ""); err == nil {
		t.Fatal("deleted without a claim")
	}
	if err := store.SettleDeletion(ctx, ObjectDeletion{ObjectID: objectID, ExecutorID: "another", State: SourceDeleting}); err == nil {
		t.Fatal("claimed by another executor")
	}
	if err := settle(SourceDeleting, ""); err != nil {
		t.Fatal(err)
	}
	if err := settle(SourceDeleting, ""); err != nil {
		t.Fatalf("claimed again: %v", err)
	}
	// while it is being deleted, a declaration under which the directory is
	// no longer Kairo's is refused
	moved := []machine.Store{
		{ID: "d-scratch", Class: machine.StoreVolatile, Node: "node", Root: "D:/elsewhere", Filesystem: "D:"},
		{ID: "e-durable", Class: machine.StoreDurable, Node: "node", Root: "E:/kairo", Filesystem: "D:"},
	}
	if _, err := store.ApplyMachines(ctx, storesSpec(moved...), false); !errors.Is(err, ErrMachineConflict) || !strings.Contains(err.Error(), "object "+objectID) {
		t.Fatalf("a declaration moving it while deleted: %v", err)
	}
	if orders, err := store.DeletionsToMake(ctx, "executor"); err != nil || len(orders) != 1 {
		t.Fatalf("a claimed deletion is ordered again: %+v %v", orders, err)
	}
	if err := settle(SourceDeleted, ""); err != nil {
		t.Fatal(err)
	}
	var outputState string
	if err := store.db.QueryRow(`SELECT state FROM run_outputs WHERE run_id=$1`, launch.RunID).Scan(&outputState); err != nil || outputState != "deleted" {
		t.Fatalf("output directory %s %v", outputState, err)
	}
	var requested int64
	if err := store.db.QueryRow(`SELECT requested_bytes FROM node_trims WHERE node_id='node'`).Scan(&requested); err != nil || requested != 1000 {
		t.Fatalf("trim requested for %d bytes %v", requested, err)
	}
	if orders, err := store.DeletionsToMake(ctx, "executor"); err != nil || len(orders) != 0 {
		t.Fatalf("ordered once deleted: %+v %v", orders, err)
	}
	stores, err := store.ListStoreStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range stores {
		if s.ID == "d-scratch" && (s.Packs != 0 || s.Outputs != 0) || s.ID == "e-durable" && (s.Copies != 1 || s.CopyBytes != 100) {
			t.Fatalf("status %s: packs %d outputs %d copies %d (%d)", s.ID, s.Packs, s.Outputs, s.Copies, s.CopyBytes)
		}
	}
	if _, err := store.ApplyMachines(ctx, storesSpec(moved...), false); err != nil {
		t.Fatalf("the declaration once deleted: %v", err)
	}
}

func TestADeletionRefusedForSafetyKeepsTheSource(t *testing.T) {
	store, _, objectID := retireFixture(t)
	ctx := context.Background()
	verifyCopy(t, store, objectID)
	for _, state := range []string{SourceDeleting, SourceKept} {
		if err := store.SettleDeletion(ctx, ObjectDeletion{ObjectID: objectID, ExecutorID: "executor", State: state, Reason: "not deleted: a link"}); err != nil {
			t.Fatal(err)
		}
	}
	var state, reason string
	if err := store.db.QueryRow(`SELECT source_state,source_reason FROM data_objects WHERE id=$1`, objectID).Scan(&state, &reason); err != nil || state != SourceKept || reason != "not deleted: a link" {
		t.Fatalf("source %s (%s) %v", state, reason, err)
	}
	if orders, err := store.DeletionsToMake(ctx, "executor"); err != nil || len(orders) != 0 {
		t.Fatalf("a kept source is ordered: %+v %v", orders, err)
	}
	var trims int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM node_trims`).Scan(&trims); err != nil || trims != 0 {
		t.Fatalf("a trim asked for nothing deleted: %d %v", trims, err)
	}
}

// Verified copies are read back once ScrubInterval has passed since they
// were last read (or verified), oldest first, within a store's bytes a day;
// one that does not read back as its pack is damaged and listed.
func TestCopiesAreReadBackWithinTheDailyBudget(t *testing.T) {
	store, _, objectID := retireFixture(t)
	ctx := context.Background()
	order := verifyCopy(t, store, objectID)
	store.SetDataPolicy(DataPolicy{ScrubInterval: time.Hour, ScrubBytesPerDay: 150})
	if scrubs, err := store.ScrubsToMake(ctx, "durable-exec"); err != nil || len(scrubs) != 0 {
		t.Fatalf("read back before its interval: %+v %v", scrubs, err)
	}
	if _, err := store.testExec(`UPDATE object_copies SET verified_at='2026-01-01T00:00:00Z'`); err != nil {
		t.Fatal(err)
	}
	insertPacked(t, store, "obj_two", "executor", "d-scratch", "D:/kairo", order.PackSHA256)
	if _, err := store.testExec(`INSERT INTO object_copies(object_id,store_id,executor_id,root,month,state,path,created_at,updated_at,verified_at)
		VALUES('obj_two','e-durable','durable-exec','E:/kairo','202611','verified','E:/kairo/objects/202611/obj_two.tar.zst','2026-01-02T00:00:00Z','2026-01-02T00:00:00Z','2026-01-02T00:00:00Z')`); err != nil {
		t.Fatal(err)
	}
	scrubs, err := store.ScrubsToMake(ctx, "durable-exec")
	if err != nil || len(scrubs) != 1 || scrubs[0].ObjectID != objectID || scrubs[0].Month != order.Month || scrubs[0].PackSHA256 != order.PackSHA256 {
		t.Fatalf("within 150 bytes a day (100 each): %+v %v", scrubs, err)
	}
	ok := ScrubResult{ObjectID: objectID, StoreID: "e-durable", ExecutorID: "durable-exec", Bytes: order.PackBytes, SHA256: order.PackSHA256, ManifestSHA256: order.ManifestSHA256}
	if err := store.RecordScrub(ctx, ok); err != nil {
		t.Fatal(err)
	}
	if scrubs, err := store.ScrubsToMake(ctx, "durable-exec"); err != nil || len(scrubs) != 0 {
		t.Fatalf("over the day's budget: %+v %v", scrubs, err)
	}
	store.SetDataPolicy(DataPolicy{ScrubInterval: time.Hour, ScrubBytesPerDay: 1000})
	scrubs, err = store.ScrubsToMake(ctx, "durable-exec")
	if err != nil || len(scrubs) != 1 || scrubs[0].ObjectID != "obj_two" {
		t.Fatalf("the next one: %+v %v", scrubs, err)
	}
	bad := ScrubResult{ObjectID: "obj_two", StoreID: "e-durable", ExecutorID: "durable-exec", Bytes: 100, SHA256: strings.Repeat("00", 32), ManifestSHA256: order.ManifestSHA256}
	if err := store.RecordScrub(ctx, bad); err != nil {
		t.Fatal(err)
	}
	stores, err := store.ListStoreStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range stores {
		if s.ID == "e-durable" && (len(s.Damaged) != 1 || s.Damaged[0].ObjectID != "obj_two" || !strings.Contains(s.Damaged[0].Problem, "read back as 100 bytes")) {
			t.Fatalf("damaged: %+v", s.Damaged)
		}
	}
	if err := store.RecordScrub(ctx, ScrubResult{ObjectID: "obj_two", StoreID: "e-durable", ExecutorID: "nobody"}); err == nil {
		t.Fatal("a read back recorded by an unknown executor")
	}
}

// A host's controller trims the containers of its host that had something
// deleted since their last trim, at most hourly, and reports how it went.
func TestDeletionsAreTrimmedByTheHostsController(t *testing.T) {
	store, _, objectID := retireFixture(t)
	ctx := context.Background()
	verifyCopy(t, store, objectID)
	spec := storesSpec(
		machine.Store{ID: "d-scratch", Class: machine.StoreVolatile, Node: "node", Root: "D:/kairo", Filesystem: "D:"},
		machine.Store{ID: "e-durable", Class: machine.StoreDurable, Node: "node", Root: "E:/kairo", Filesystem: "D:"},
	)
	if err := store.UpsertNode(ctx, Node{ID: "pve0-host", Name: "pve0-host", OS: "linux", Architecture: "amd64", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	spec.Hosts = []machine.Host{{ID: "pve0", Nodes: []string{"node"}, Controller: "pve0-host", Containers: map[string]int{"node": 101}}}
	if _, err := store.ApplyMachines(ctx, spec, false); err != nil {
		t.Fatal(err)
	}
	if trims, err := store.TrimsToMake(ctx, "pve0-host"); err != nil || len(trims) != 0 {
		t.Fatalf("a trim with nothing deleted: %+v %v", trims, err)
	}
	for _, state := range []string{SourceDeleting, SourceDeleted} {
		if err := store.SettleDeletion(ctx, ObjectDeletion{ObjectID: objectID, ExecutorID: "executor", State: state}); err != nil {
			t.Fatal(err)
		}
	}
	if trims, err := store.TrimsToMake(ctx, "another-host"); err != nil || len(trims) != 0 {
		t.Fatalf("another controller asked to trim: %+v %v", trims, err)
	}
	trims, err := store.TrimsToMake(ctx, "pve0-host")
	if err != nil || len(trims) != 1 || trims[0] != (TrimOrder{NodeID: "node", VMID: 101, RequestedBytes: 1000}) {
		t.Fatalf("trims %+v %v", trims, err)
	}
	if err := store.RecordTrim(ctx, "another-host", TrimDone{NodeID: "node"}); err == nil {
		t.Fatal("a trim recorded by another host's controller")
	}
	if err := store.RecordTrim(ctx, "pve0-host", TrimDone{NodeID: "node", Output: "/: 9.3 GiB trimmed"}); err != nil {
		t.Fatal(err)
	}
	if trims, err := store.TrimsToMake(ctx, "pve0-host"); err != nil || len(trims) != 0 {
		t.Fatalf("trimmed again at once: %+v %v", trims, err)
	}
	// deleted again within the hour: trimmed once the hour has passed
	if _, err := store.testExec(`UPDATE node_trims SET requested_at=$1`, time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	if trims, err := store.TrimsToMake(ctx, "pve0-host"); err != nil || len(trims) != 0 {
		t.Fatalf("trimmed again within the hour: %+v %v", trims, err)
	}
	if _, err := store.testExec(`UPDATE node_trims SET trimmed_at='2026-01-01T00:00:00Z'`); err != nil {
		t.Fatal(err)
	}
	if trims, err := store.TrimsToMake(ctx, "pve0-host"); err != nil || len(trims) != 1 {
		t.Fatalf("not trimmed once the hour passed: %+v %v", trims, err)
	}
}
