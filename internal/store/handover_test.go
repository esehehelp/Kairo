package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"kairo/internal/datainput"
	"kairo/internal/machine"
)

// Temporal data an executor wrote, then retired for another on its node (a
// host executor for a container one), is packed to be brought by the one
// that serves its store now: its agent, registering without the old one,
// hands its data work over. Nothing waits for the retired executor.
func TestARetiredExecutorsDataWorkGoesToItsStoresServer(t *testing.T) {
	store := importFixture(t)
	ctx := context.Background()
	if err := store.UpsertNode(ctx, Node{ID: "node2", Name: "node2", OS: "linux", Architecture: "amd64", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertExecutor(ctx, Executor{ID: "exec2", NodeID: "node2", Kind: "linux", Attributes: json.RawMessage(`{"environment":"linux"}`), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertExecutor(ctx, Executor{ID: "win-exec", NodeID: "node", Kind: "windows", Attributes: json.RawMessage(`{"environment":"windows","pack_addr":"192.0.2.12:7475"}`), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	addTestDisk(t, store, "node2", "/")
	// stores that name no executor: served by the node's first with a pack server
	spec := storesSpec(
		machine.Store{ID: "d-scratch", Class: machine.StoreVolatile, Node: "node", Root: "D:/kairo", Filesystem: "D:"},
		machine.Store{ID: "e-durable", Class: machine.StoreDurable, Node: "node", Root: "E:/kairo", Filesystem: "D:"},
		machine.Store{ID: "n2-scratch", Class: machine.StoreVolatile, Node: "node2", Root: "/srv/kairo-store", Filesystem: "/"},
	)
	spec.Nodes = append(spec.Nodes, machine.Node{ID: "node2"})
	if _, err := store.ApplyMachines(ctx, spec, false); err != nil {
		t.Fatal(err)
	}
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
	var writer string
	if err := store.db.QueryRowContext(ctx, `SELECT executor_id FROM data_objects WHERE id=$1`, object).Scan(&writer); err != nil {
		t.Fatal(err)
	}
	list := "path\tbytes\tsha256\tmtime_ns\nshard-0.bin\t4\t" + strings.Repeat("ab", 32) + "\t1\n"
	sum := sha256.Sum256([]byte(list))
	if err = store.RecordPack(ctx, PackMade{ObjectID: object, ExecutorID: writer, ManifestPath: "D:/kairo/packs/" + object + ".tar.zst.files.tsv",
		ContentBytes: 4, Files: 1, ManifestSHA256: hex.EncodeToString(sum[:]), Identity: true, Manifest: list}); err != nil {
		t.Fatal(err)
	}
	reader := schedulerExecutionSpec("reader")
	reader.Scope.Project = "imp"
	reader.ExecutorSelector = json.RawMessage(`{"labels":{"environment":"linux"},"node":"node2"}`)
	reader.Exclusive, reader.Checkpointable, reader.Preemptible = []ExclusiveRequest{}, false, false
	reader.Inputs = []datainput.Input{{Name: "data", Data: "shards"}}
	if _, _, err := store.SubmitExecution(ctx, reader); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	// the writer retired for a container executor before it packed the move
	if err := store.UpsertExecutor(ctx, Executor{ID: "win-wslc", NodeID: "node", Kind: "wslc", Attributes: json.RawMessage(`{"environment":"linux","pack_addr":"192.0.2.12:7475"}`), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	gone, err := store.UndeclareExecutors(ctx, "node", []string{"win-wslc"})
	if err != nil || !slices.Contains(gone, writer) {
		t.Fatalf("undeclared %v %v", gone, err)
	}
	var owner string
	if err := store.db.QueryRowContext(ctx, `SELECT executor_id FROM data_objects WHERE id=$1`, object).Scan(&owner); err != nil || owner != "win-wslc" {
		t.Fatalf("the object's executor: %q %v", owner, err)
	}
	orders, err := store.PacksToMake(ctx, "win-wslc")
	if err != nil || len(orders) != 1 || !orders[0].Transfer || orders[0].ObjectID != object {
		t.Fatalf("packs for win-wslc: %+v %v", orders, err)
	}
	// once more: nothing left to hand over
	if _, err := store.UndeclareExecutors(ctx, "node", []string{"win-wslc"}); err != nil {
		t.Fatal(err)
	}
}
