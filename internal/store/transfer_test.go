package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"kairo/internal/datainput"
	"kairo/internal/machine"
)

// Temporal data read on a node it is not on, still present where it was
// made, is brought from there, not made again: packed there for the move
// (its pack must list what its identity lists), fetched from that node's
// agent into the reader's node, not copied to a durable store, and the pack
// removed once nothing waits for it; the directory is not evicted meanwhile.
func TestTemporalDataIsBroughtFromWhereItIs(t *testing.T) {
	store := importFixture(t)
	ctx := context.Background()
	if err := store.UpsertNode(ctx, Node{ID: "node2", Name: "node2", OS: "linux", Architecture: "amd64", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertExecutor(ctx, Executor{ID: "exec2", NodeID: "node2", Kind: "linux", Attributes: json.RawMessage(`{"environment":"linux"}`), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertExecutor(ctx, Executor{ID: "win-exec", NodeID: "node", Kind: "windows", Attributes: json.RawMessage(`{"environment":"windows","pack_addr":"192.168.1.12:7475"}`), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	addTestDisk(t, store, "node2", "/")
	spec := storesSpec(
		machine.Store{ID: "d-scratch", Class: machine.StoreVolatile, Node: "node", Root: "D:/kairo", Filesystem: "D:"},
		machine.Store{ID: "e-durable", Class: machine.StoreDurable, Node: "node", Root: "E:/kairo", Filesystem: "D:"},
		machine.Store{ID: "n2-scratch", Class: machine.StoreVolatile, Node: "node2", Root: "/srv/kairo-store", Filesystem: "/"},
	)
	spec.Nodes = append(spec.Nodes, machine.Node{ID: "node2"})
	if _, err := store.ApplyMachines(ctx, spec, false); err != nil {
		t.Fatal(err)
	}
	// a dataset only a Windows task can build, imported as made by it
	if _, err := store.ApplyProject(ctx, projectSpec(t, `schema_version = 1
[project]
name = "imp"
[[tasks]]
name = "build"
queue = "q"
depends_on = []
policy = "RunToCompletion@v1"
[tasks.execution]
schema_version = 2
argv = ["build-dataset"]
cwd = "/work"
regenerable = true
[tasks.execution.executor]
labels = { environment = "windows" }
`)); err != nil {
		t.Fatal(err)
	}
	imported, err := store.ImportData(ctx, ImportRequest{Project: "imp", Node: "node", Paths: []string{"D:/data/shards"}, Name: "shards", RegenerableBy: "build"})
	if err != nil {
		t.Fatal(err)
	}
	object := imported[0].ObjectID
	list := "path\tbytes\tsha256\tmtime_ns\nshard-0.bin\t4\t" + strings.Repeat("ab", 32) + "\t1\n"
	sum := sha256.Sum256([]byte(list))
	listSHA := hex.EncodeToString(sum[:])
	if err = store.RecordPack(ctx, PackMade{ObjectID: object, ExecutorID: "win-exec", ManifestPath: "D:/kairo/packs/" + object + ".tar.zst.files.tsv",
		ContentBytes: 4, Files: 1, ManifestSHA256: listSHA, Identity: true, Manifest: list}); err != nil {
		t.Fatal(err)
	}
	reader := schedulerExecutionSpec("reader")
	reader.Scope.Project = "imp"
	reader.ExecutorSelector = json.RawMessage(`{"labels":{"environment":"linux"},"node":"node2"}`)
	reader.Exclusive, reader.Checkpointable, reader.Preemptible = []ExclusiveRequest{}, false, false
	reader.Inputs = []datainput.Input{{Name: "data", Data: "shards"}}
	execution, _, err := store.SubmitExecution(ctx, reader)
	if err != nil {
		t.Fatal(err)
	}
	declared, err := declaredStores(ctx, store.db)
	if err != nil {
		t.Fatal(err)
	}
	wait := func() string {
		t.Helper()
		why, err := inputsWaitTx(ctx, store.db, declared, execution.ProjectScopeID, string(reader.ExecutorSelector), `[{"name":"data","data":"shards"}]`)
		if err != nil {
			t.Fatal(err)
		}
		return why
	}
	reconcile := func() {
		t.Helper()
		if err := store.ReconcileProjectTasks(ctx); err != nil {
			t.Fatal(err)
		}
	}
	reconcile()
	if why := wait(); !strings.Contains(why, "being packed on node to be brought to node2") {
		t.Fatalf("wait: %q", why)
	}
	var ordered *int64
	if err = store.db.QueryRowContext(ctx, `SELECT regeneration_ordinal FROM data_objects WHERE id=$1`, object).Scan(&ordered); err != nil || ordered != nil {
		t.Fatalf("made again although it is there to bring: %v %v", ordered, err)
	}
	// packed for the move on its node; the directory not evicted meanwhile
	orders, err := store.PacksToMake(ctx, "win-exec")
	if err != nil || len(orders) != 1 || !orders[0].Transfer || orders[0].ManifestSHA256 != listSHA || orders[0].Identity {
		t.Fatalf("orders %+v %v", orders, err)
	}
	observe := func(free int64) {
		t.Helper()
		total := int64(1000)
		if err := store.RecordStoreObservation(ctx, StoreObservation{StoreID: "d-scratch", ExecutorID: "win-exec", Root: "D:/kairo", Usable: true, TotalBytes: &total, FreeBytes: &free}); err != nil {
			t.Fatal(err)
		}
	}
	observe(10) // short of room
	if evictions, err := store.EvictionsToMake(ctx, "win-exec"); err != nil || len(evictions) != 0 {
		t.Fatalf("evicted while it is moved: %+v %v", evictions, err)
	}
	observe(900)
	packSHA := strings.Repeat("cd", 32)
	made := PackMade{ObjectID: object, ExecutorID: "win-exec", PackPath: "D:/kairo/packs/" + object + ".tar.zst", ManifestPath: "D:/kairo/packs/" + object + ".tar.zst.files.tsv",
		PackBytes: 3, ContentBytes: 4, Files: 1, PackSHA256: packSHA, ManifestSHA256: strings.Repeat("ef", 32), Transfer: true}
	if err = store.RecordPack(ctx, made); err == nil || !strings.Contains(err.Error(), "changed since it was identified") {
		t.Fatalf("a pack listing other files: %v", err)
	}
	made.ManifestSHA256 = listSHA
	if err = store.RecordPack(ctx, made); err != nil {
		t.Fatal(err)
	}
	if copies, err := store.CopiesToMake(ctx, "win-exec"); err != nil || len(copies) != 0 {
		t.Fatalf("temporal data copied to a durable store: %+v %v", copies, err)
	}
	// brought to node2 from the agent of node
	reconcile()
	if why := wait(); !strings.Contains(why, "being brought to node2 from node") {
		t.Fatalf("wait: %q", why)
	}
	fetches, err := store.FetchesToMake(ctx, "exec2")
	if err != nil || len(fetches) != 1 || fetches[0].PackSHA256 != packSHA || fetches[0].Source.Addr != "192.168.1.12:7475" || fetches[0].Source.Token == "" || fetches[0].Source.Month != "" {
		t.Fatalf("fetches %+v %v", fetches, err)
	}
	if read, err := store.AuthorizePackRead(ctx, "node", object, fetches[0].Source.Token); err != nil || read.StoreID != "d-scratch" {
		t.Fatalf("pack read %+v %v", read, err)
	}
	if err = store.RecordFetch(ctx, FetchMade{ObjectID: object, StoreID: "n2-scratch", ExecutorID: "exec2", Path: "/srv/kairo-store/inputs/" + object, Bytes: 4}); err != nil {
		t.Fatal(err)
	}
	if why := wait(); why != "" {
		t.Fatalf("still waiting once brought: %q", why)
	}
	// nothing waits for it elsewhere: its pack removed, the identity kept
	reconcile()
	orders, err = store.PacksToMake(ctx, "win-exec")
	if err != nil || len(orders) != 1 || !orders[0].Drop {
		t.Fatalf("orders %+v %v", orders, err)
	}
	if err = store.RecordPack(ctx, PackMade{ObjectID: object, ExecutorID: "win-exec", Dropped: true}); err != nil {
		t.Fatal(err)
	}
	var transfer, path *string
	var manifest string
	if err = store.db.QueryRowContext(ctx, `SELECT transfer_state,pack_path,manifest_sha256 FROM data_objects WHERE id=$1`, object).Scan(&transfer, &path, &manifest); err != nil || transfer != nil || path != nil || manifest != listSHA {
		t.Fatalf("after the pack was removed: %v %v %s %v", transfer, path, manifest, err)
	}
	if orders, err = store.PacksToMake(ctx, "win-exec"); err != nil || len(orders) != 0 {
		t.Fatalf("orders %+v %v", orders, err)
	}
}
