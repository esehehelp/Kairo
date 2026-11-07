package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"kairo/internal/machine"
)

// A regenerable task's run output is temporal data: its executor is told to
// make only its identity (the file list), which is kept; it is never copied
// to a durable store. An import names it as made by a regenerable task.
func TestARegenerableTasksOutputIsTemporal(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	addTestDisk(t, store, "node", "/")
	ctx := context.Background()
	if err := store.UpsertExecutor(ctx, Executor{ID: "win-exec", NodeID: "node", Kind: "windows", Attributes: json.RawMessage(`{"environment":"windows"}`), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	declareTestStore(t, store,
		machine.Store{ID: "w-scratch", Class: machine.StoreVolatile, Node: "node", Executor: "executor", Root: "/var/kairo", Filesystem: "/"},
		machine.Store{ID: "d-scratch", Class: machine.StoreVolatile, Node: "node", Root: "D:/kairo", Filesystem: "/"},
		machine.Store{ID: "e-durable", Class: machine.StoreDurable, Node: "node", Root: "E:/kairo", Filesystem: "/"},
	)
	if _, err := store.ApplyProject(ctx, projectSpec(t, `schema_version = 1
[project]
name = "sumire"
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
labels = { environment = "wsl2" }
[[tasks]]
name = "notes"
queue = "q"
depends_on = []
policy = "RunToCompletion@v1"
[tasks.execution]
schema_version = 2
argv = ["write-notes"]
cwd = "/work"
[tasks.execution.executor]
labels = { environment = "windows" }
`)); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	build := authorizeNext(t, store)
	out := RunOutput{AttemptID: build.Attempt.ID, LeaseID: build.Lease.ID, Epoch: build.Lease.CoordinationEpoch, RunID: build.RunID,
		StoreID: "w-scratch", ExecutorID: "executor", Root: "/var/kairo", Path: "/var/kairo/outputs/" + build.RunID}
	if err := store.RecordRunOutput(ctx, out); err != nil {
		t.Fatal(err)
	}
	epoch := build.Lease.CoordinationEpoch
	if err := store.ActivateLaunch(ctx, build.Attempt.ID, build.Lease.ID, epoch, build.AuthorizationToken, 100, "launcher:100"); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordTerminal(ctx, build.Attempt.ID, build.Lease.ID, epoch, 0, ""); err != nil {
		t.Fatal(err)
	}
	refreshSchedulerGPU(t, store)
	if err := store.FinalizeQuiescence(ctx, build.Attempt.ID, build.Lease.ID, epoch); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	orders, err := store.PacksToMake(ctx, "executor")
	if err != nil || len(orders) != 1 || !orders[0].Identity || orders[0].SourceID != build.RunID {
		t.Fatalf("orders %+v %v", orders, err)
	}
	objectID := orders[0].ObjectID
	list := "path\tbytes\tsha256\tmtime_ns\nshard-0.bin\t4\t" + strings.Repeat("ab", 32) + "\t1\n"
	sum := sha256.Sum256([]byte(list))
	listPath := "/var/kairo/packs/" + objectID + ".tar.zst.files.tsv"
	identity := PackMade{ObjectID: objectID, ExecutorID: "executor", ManifestPath: listPath, ContentBytes: 4, Files: 1,
		ManifestSHA256: hex.EncodeToString(sum[:]), Identity: true, Manifest: list}
	for why, bad := range map[string]PackMade{
		"a pack": func() PackMade {
			m := identity
			m.PackPath, m.PackBytes = "/var/kairo/packs/"+objectID+".tar.zst", 10
			return m
		}(),
		"another list":   func() PackMade { m := identity; m.Manifest += "x\t1\t" + strings.Repeat("cd", 32) + "\t1\n"; return m }(),
		"not identity":   func() PackMade { m := identity; m.Identity = false; return m }(),
		"misplaced list": func() PackMade { m := identity; m.ManifestPath = "/var/kairo/elsewhere.tsv"; return m }(),
	} {
		if err := store.RecordPack(ctx, bad); err == nil {
			t.Fatalf("%s recorded", why)
		}
	}
	if err := store.RecordPack(ctx, identity); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordPack(ctx, identity); err != nil {
		t.Fatalf("recorded again: %v", err)
	}
	if copies, err := store.CopiesToMake(ctx, "win-exec"); err != nil || len(copies) != 0 {
		t.Fatalf("temporal data copied to a durable store: %+v %v", copies, err)
	}
	listed, err := store.DataObjects(ctx, DataObjectFilter{Project: "sumire"})
	if err != nil || len(listed) != 1 || listed[0].Lifecycle != LifecycleTemporal || listed[0].State != "packed" {
		t.Fatalf("listed %+v %v", listed, err)
	}
	var manifest string
	if err := store.db.QueryRowContext(ctx, `SELECT manifest FROM data_objects WHERE id=$1`, objectID).Scan(&manifest); err != nil || manifest != list {
		t.Fatalf("identity kept: %q %v", manifest, err)
	}
	// imports named as made by a task: a regenerable one only, never deleted
	if _, err := store.ImportData(ctx, ImportRequest{Project: "sumire", Node: "node", Paths: []string{"D:/datasets/v8"}, RegenerableBy: "notes"}); !errors.Is(err, ErrImportRefused) {
		t.Fatalf("made by a task not regenerable: %v", err)
	}
	if _, err := store.ImportData(ctx, ImportRequest{Project: "sumire", Node: "node", Paths: []string{"D:/datasets/v8"}, RegenerableBy: "build", DeleteSource: true}); !errors.Is(err, ErrImportRefused) {
		t.Fatalf("temporal and deleted: %v", err)
	}
	imported, err := store.ImportData(ctx, ImportRequest{Project: "sumire", Node: "node", Paths: []string{"D:/datasets/v8"}, Name: "v8", RegenerableBy: "build"})
	if err != nil {
		t.Fatal(err)
	}
	orders, err = store.PacksToMake(ctx, "win-exec")
	if err != nil || len(orders) != 1 || orders[0].ObjectID != imported[0].ObjectID || !orders[0].Identity {
		t.Fatalf("import orders %+v %v", orders, err)
	}
}
