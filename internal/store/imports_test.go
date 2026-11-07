package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"kairo/internal/machine"
)

// importFixture: node "node" with the test's WSL executor ("executor") and a
// Windows one ("win-exec"), a volatile store on D: and a durable one on E:
// (neither naming an executor, so win-exec serves both), and project "imp".
func importFixture(t *testing.T) *Store {
	t.Helper()
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	addTestDisk(t, store, "node", "D:")
	ctx := context.Background()
	if err := store.UpsertExecutor(ctx, Executor{ID: "win-exec", NodeID: "node", Kind: "windows", Attributes: json.RawMessage(`{"environment":"windows"}`), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	declareTestStore(t, store,
		machine.Store{ID: "d-scratch", Class: machine.StoreVolatile, Node: "node", Root: "D:/kairo", Filesystem: "D:"},
		machine.Store{ID: "e-durable", Class: machine.StoreDurable, Node: "node", Root: "E:/kairo", Filesystem: "D:"},
	)
	if _, err := store.ApplyProject(ctx, projectSpec(t, `schema_version = 1
[project]
name = "imp"
[[tasks]]
name = "noop"
queue = "q"
depends_on = []
policy = "RunToCompletion@v1"
[tasks.execution]
schema_version = 2
argv = ["true"]
cwd = "/work"
`)); err != nil {
		t.Fatal(err)
	}
	return store
}

// An import is refused unless each directory is an absolute path of the
// packing store's family, outside every store of the node and holding none,
// not a filesystem's root, not overlapping one being imported, and (to be
// deleted) on the packing store's drive.
func TestAnImportNamesDirectoriesOutsideEveryStore(t *testing.T) {
	store := importFixture(t)
	ctx := context.Background()
	refused := map[string]ImportRequest{
		"unknown project":   {Project: "nope", Node: "node", Paths: []string{"D:/data/a"}},
		"no store":          {Project: "imp", Node: "other", Paths: []string{"D:/data/a"}},
		"inside a store":    {Project: "imp", Node: "node", Paths: []string{"D:/kairo/outputs/x"}},
		"holds a store":     {Project: "imp", Node: "node", Paths: []string{"E:/"}},
		"around a store":    {Project: "imp", Node: "node", Paths: []string{"e:/"}},
		"a drive's root":    {Project: "imp", Node: "node", Paths: []string{"F:/"}},
		"relative":          {Project: "imp", Node: "node", Paths: []string{"data/a"}},
		"POSIX on Windows":  {Project: "imp", Node: "node", Paths: []string{"/srv/data"}},
		"dot dot":           {Project: "imp", Node: "node", Paths: []string{"D:/data/../kairo"}},
		"other drive":       {Project: "imp", Node: "node", Paths: []string{"G:/data"}, DeleteSource: true},
		"no paths":          {Project: "imp", Node: "node"},
		"name with newline": {Project: "imp", Node: "node", Paths: []string{"D:/data/a"}, Name: "a\nb"},
	}
	for why, req := range refused {
		if _, err := store.ImportData(ctx, req); !errors.Is(err, ErrImportRefused) {
			t.Fatalf("%s: %v", why, err)
		}
	}
	objects, err := store.ImportData(ctx, ImportRequest{Project: "imp", Node: "node", Paths: []string{"D:/data/a", "D:/data/b"}, Name: "set", DeleteSource: true})
	if err != nil || len(objects) != 2 {
		t.Fatalf("import: %+v %v", objects, err)
	}
	for i, o := range objects {
		if o.Name != "set/"+[]string{"a", "b"}[i] || o.StoreID != "d-scratch" || o.ExecutorID != "win-exec" {
			t.Fatalf("object %d: %+v", i, o)
		}
	}
	// F: is not deleted, so it may be imported (kept where it is)
	if _, err := store.ImportData(ctx, ImportRequest{Project: "imp", Node: "node", Paths: []string{"F:/data"}}); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"D:/data/a/sub", "d:/DATA/b", "D:/data"} {
		if _, err := store.ImportData(ctx, ImportRequest{Project: "imp", Node: "node", Paths: []string{p}}); !errors.Is(err, ErrImportRefused) || !strings.Contains(err.Error(), "being imported") {
			t.Fatalf("%s overlaps an import: %v", p, err)
		}
	}
	// a store declared over a directory being imported stops its packing
	orders, err := store.PacksToMake(ctx, "win-exec")
	if err != nil || len(orders) != 3 {
		t.Fatalf("pack orders: %+v %v", orders, err)
	}
	for _, o := range orders {
		if o.Kind != ObjectImport || o.SourceID != o.ObjectID || o.Root != "D:/kairo" || o.StoreID != "d-scratch" || o.Bytes != nil {
			t.Fatalf("pack order %+v", o)
		}
	}
	if others, err := store.PacksToMake(ctx, "executor"); err != nil || len(others) != 0 {
		t.Fatalf("the WSL executor packs nothing: %+v %v", others, err)
	}
	declareTestStore(t, store,
		machine.Store{ID: "d-scratch", Class: machine.StoreVolatile, Node: "node", Root: "D:/kairo", Filesystem: "D:"},
		machine.Store{ID: "e-durable", Class: machine.StoreDurable, Node: "node", Root: "E:/kairo", Filesystem: "D:"},
		machine.Store{ID: "f-data", Class: machine.StoreVolatile, Node: "node", Root: "F:/data", Filesystem: "D:"},
	)
	if orders, err = store.PacksToMake(ctx, "win-exec"); err != nil || len(orders) != 2 {
		t.Fatalf("an import inside a store declared since is not packed: %+v %v", orders, err)
	}
	listed, err := store.DataObjects(ctx, DataObjectFilter{Project: "imp", Kind: ObjectImport})
	if err != nil || len(listed) != 3 || listed[0].Name == nil || *listed[0].Name != "data" || listed[0].DeleteSource {
		t.Fatalf("listed: %+v %v", listed, err)
	}
}

// An imported directory is deleted once its copy is verified only when the
// import asked for it: claimed, deleted (and its node trimmed); without
// delete_source the claim is refused and it is kept.
func TestAnImportedDirectoryIsDeletedOnlyWhenAskedFor(t *testing.T) {
	store := importFixture(t)
	ctx := context.Background()
	deleted, err := store.ImportData(ctx, ImportRequest{Project: "imp", Node: "node", Paths: []string{"D:/data/a"}, DeleteSource: true})
	if err != nil {
		t.Fatal(err)
	}
	kept, err := store.ImportData(ctx, ImportRequest{Project: "imp", Node: "node", Paths: []string{"D:/data/b"}})
	if err != nil {
		t.Fatal(err)
	}
	sha := strings.Repeat("ab", 32)
	for _, o := range []ImportedObject{deleted[0], kept[0]} {
		if err := store.RecordPack(ctx, PackMade{ObjectID: o.ObjectID, ExecutorID: "win-exec", PackPath: "D:/kairo/packs/" + o.ObjectID + ".tar.zst",
			ManifestPath: "D:/kairo/packs/" + o.ObjectID + ".tar.zst.files.tsv", PackBytes: 100, ContentBytes: 900, Files: 3, PackSHA256: sha, ManifestSHA256: sha}); err != nil {
			t.Fatal(err)
		}
	}
	if orders, err := store.DeletionsToMake(ctx, "win-exec"); err != nil || len(orders) != 0 {
		t.Fatalf("nothing is deleted before a verified copy: %+v %v", orders, err)
	}
	copies, err := store.CopiesToMake(ctx, "win-exec")
	if err != nil || len(copies) != 2 {
		t.Fatalf("copies %+v %v", copies, err)
	}
	for _, c := range copies {
		path := "E:/kairo/objects/" + c.Month + "/" + c.ObjectID + ".tar.zst"
		if err := store.RecordCopy(ctx, CopyMade{ObjectID: c.ObjectID, StoreID: "e-durable", ExecutorID: "win-exec", Path: path, ManifestPath: path + ".files.tsv",
			Bytes: c.PackBytes, SHA256: c.PackSHA256, ManifestSHA256: c.ManifestSHA256}); err != nil {
			t.Fatal(err)
		}
	}
	orders, err := store.DeletionsToMake(ctx, "win-exec")
	if err != nil || len(orders) != 2 {
		t.Fatalf("deletions %+v %v", orders, err)
	}
	byID := map[string]DeletionOrder{}
	for _, o := range orders {
		byID[o.ObjectID] = o
	}
	if o := byID[deleted[0].ObjectID]; o.Kind != ObjectImport || o.Path != "D:/data/a" || !o.DeleteSource {
		t.Fatalf("deletion order %+v", o)
	}
	if o := byID[kept[0].ObjectID]; o.DeleteSource {
		t.Fatalf("an import without delete_source is not deleted: %+v", o)
	}
	settle := func(objectID, state, reason string) error {
		return store.SettleDeletion(ctx, ObjectDeletion{ObjectID: objectID, ExecutorID: "win-exec", State: state, Reason: reason})
	}
	if err := settle(kept[0].ObjectID, SourceDeleting, ""); !errors.Is(err, ErrStaleEpoch) {
		t.Fatalf("an import without delete_source is never claimed for deletion: %v", err)
	}
	if err := settle(kept[0].ObjectID, SourceKept, "imported without delete_source"); err != nil {
		t.Fatal(err)
	}
	// a store declared over the directory while its deletion is claimed is refused
	if err := settle(deleted[0].ObjectID, SourceDeleting, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ApplyMachines(ctx, storesSpec(
		machine.Store{ID: "d-scratch", Class: machine.StoreVolatile, Node: "node", Root: "D:/kairo", Filesystem: "D:"},
		machine.Store{ID: "e-durable", Class: machine.StoreDurable, Node: "node", Root: "E:/kairo", Filesystem: "D:"},
		machine.Store{ID: "data", Class: machine.StoreVolatile, Node: "node", Root: "D:/data", Filesystem: "D:"},
	), false); !errors.Is(err, ErrMachineConflict) || !strings.Contains(err.Error(), deleted[0].ObjectID) {
		t.Fatalf("declaration over a directory being deleted: %v", err)
	}
	if err := settle(deleted[0].ObjectID, SourceDeleted, ""); err != nil {
		t.Fatal(err)
	}
	var requested int64
	if err := store.db.QueryRowContext(ctx, `SELECT requested_bytes FROM node_trims WHERE node_id='node'`).Scan(&requested); err != nil || requested != 1000 {
		t.Fatalf("trim requested: %d %v", requested, err)
	}
	if orders, err := store.DeletionsToMake(ctx, "win-exec"); err != nil || len(orders) != 0 {
		t.Fatalf("settled: %+v %v", orders, err)
	}
	listed, err := store.DataObjects(ctx, DataObjectFilter{Project: "imp"})
	if err != nil || len(listed) != 2 {
		t.Fatalf("listed %+v %v", listed, err)
	}
	for _, o := range listed {
		want := map[string]string{deleted[0].ObjectID: SourceDeleted, kept[0].ObjectID: SourceKept}[o.ID]
		if o.SourceState != want || o.Copies != 1 || o.CopyPath == nil {
			t.Fatalf("object %+v", o)
		}
	}
	// once settled, the directory may be imported again
	again, err := store.ImportData(ctx, ImportRequest{Project: "imp", Node: "node", Paths: []string{"D:/data/a"}})
	if err != nil {
		t.Fatal(err)
	}
	// imported again with delete_source while not settled: the same import
	// now deletes it once copied
	upgraded, err := store.ImportData(ctx, ImportRequest{Project: "imp", Node: "node", Paths: []string{"D:/data/a"}, DeleteSource: true})
	if err != nil || len(upgraded) != 1 || upgraded[0].ObjectID != again[0].ObjectID {
		t.Fatalf("upgrade: %+v %v (was %+v)", upgraded, err, again)
	}
	var deleteSource bool
	if err := store.db.QueryRowContext(ctx, `SELECT delete_source FROM data_objects WHERE id=$1`, again[0].ObjectID).Scan(&deleteSource); err != nil || !deleteSource {
		t.Fatalf("delete_source %v %v", deleteSource, err)
	}
	if _, err := store.ImportData(ctx, ImportRequest{Project: "imp", Node: "node", Paths: []string{"D:/data/a/sub"}, DeleteSource: true}); err == nil {
		t.Fatal("a directory inside one being imported is imported")
	}
}

// A directory is packed on the volatile store of its own filesystem when
// the node has one, else on the first by id.
func TestAnImportIsPackedOnTheStoreOfItsFilesystem(t *testing.T) {
	store := importFixture(t)
	ctx := context.Background()
	addTestDisk(t, store, "node", "F:")
	declareTestStore(t, store,
		machine.Store{ID: "d-scratch", Class: machine.StoreVolatile, Node: "node", Root: "D:/kairo", Filesystem: "D:"},
		machine.Store{ID: "e-durable", Class: machine.StoreDurable, Node: "node", Root: "E:/kairo", Filesystem: "D:"},
		machine.Store{ID: "f-models", Class: machine.StoreVolatile, Node: "node", Root: "F:/kairo-store", Filesystem: "F:"},
	)
	objects, err := store.ImportData(ctx, ImportRequest{Project: "imp", Node: "node", Paths: []string{"F:/Strata-data", "D:/data/x", "G:/elsewhere"}})
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []string{"f-models", "d-scratch", "d-scratch"} {
		if objects[i].StoreID != want {
			t.Fatalf("%s packed on %s, want %s", objects[i].Path, objects[i].StoreID, want)
		}
	}
	if _, err := store.ImportData(ctx, ImportRequest{Project: "imp", Node: "node", Paths: []string{"F:/models"}, DeleteSource: true}); err != nil {
		t.Fatalf("an F: directory may be deleted once copied, packed on F:: %v", err)
	}
}

// An execution naming a volatile store is planned onto its node and its
// launch names the store; one naming a store that is not volatile is not
// pinned (its launch is refused by the executor).
func TestAnExecutionNamesTheStoreItsDirectoriesGoOn(t *testing.T) {
	store := importFixture(t)
	ctx := context.Background()
	spec := schedulerExecutionSpec("on-d")
	spec.Scope.Project = "imp"
	spec.ExecutorSelector = json.RawMessage(`{"labels":{"environment":"windows"}}`)
	spec.Exclusive, spec.Checkpointable, spec.Preemptible = []ExclusiveRequest{}, false, false
	spec.Store = "d-scratch"
	execution, _, err := store.SubmitExecution(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	var selector map[string]any
	if err := json.Unmarshal(execution.ExecutorSelector, &selector); err != nil || selector["node"] != "node" {
		t.Fatalf("selector %s %v", execution.ExecutorSelector, err)
	}
	reservation, err := store.ReserveNext(ctx, "win-exec")
	if err != nil || reservation == nil {
		t.Fatalf("reserve: %+v %v", reservation, err)
	}
	if err = store.MarkLeasePrepared(ctx, reservation.Lease.ID, reservation.Lease.CoordinationEpoch); err != nil {
		t.Fatal(err)
	}
	launch, err := store.AuthorizeLaunch(ctx, reservation)
	if err != nil || launch.Store != "d-scratch" {
		t.Fatalf("launch store %q %v", launch.Store, err)
	}
	durable := spec
	durable.ClientRequestID, durable.Scope.Task, durable.Store = "on-e", "other", "e-durable"
	got, _, err := store.SubmitExecution(ctx, durable)
	if err != nil {
		t.Fatal(err)
	}
	selector = nil
	if err := json.Unmarshal(got.ExecutorSelector, &selector); err != nil || selector["node"] != nil {
		t.Fatalf("pinned to a durable store's node: %s %v", got.ExecutorSelector, err)
	}
}
