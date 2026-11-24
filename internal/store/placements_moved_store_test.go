package store

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"kairo/internal/datainput"
	"kairo/internal/machine"
)

// A durable copy is read through whoever serves its store now: after the
// store moved to another node (its disk mounted there), a fetch reads it
// through that node's pack server, not through the agent that wrote it.
func TestACopyIsReadThroughItsStoresServerNow(t *testing.T) {
	store := importFixture(t)
	ctx := context.Background()
	for _, n := range []struct{ node, executor, attrs string }{
		{"node2", "exec2", `{"environment":"linux"}`},
		{"node3", "host3", `{"pack_addr":"192.0.2.30:7475"}`},
	} {
		if err := store.UpsertNode(ctx, Node{ID: n.node, Name: n.node, OS: "linux", Architecture: "amd64", Enabled: true}); err != nil {
			t.Fatal(err)
		}
		if err := store.UpsertExecutor(ctx, Executor{ID: n.executor, NodeID: n.node, Kind: "linux", Attributes: json.RawMessage(n.attrs), Enabled: true}); err != nil {
			t.Fatal(err)
		}
		addTestDisk(t, store, n.node, "/")
	}
	if err := store.UpsertExecutor(ctx, Executor{ID: "win-exec", NodeID: "node", Kind: "windows", Attributes: json.RawMessage(`{"environment":"windows","pack_addr":"192.0.2.12:7475"}`), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	stores := func(durable machine.Store) machine.Spec {
		spec := storesSpec(
			machine.Store{ID: "d-scratch", Class: machine.StoreVolatile, Node: "node", Executor: "win-exec", Root: "D:/kairo", Filesystem: "D:"},
			durable,
			machine.Store{ID: "n2-scratch", Class: machine.StoreVolatile, Node: "node2", Root: "/srv/kairo-store", Filesystem: "/"},
		)
		spec.Nodes = append(spec.Nodes, machine.Node{ID: "node2"}, machine.Node{ID: "node3"})
		return spec
	}
	if _, err := store.ApplyMachines(ctx, stores(machine.Store{ID: "z-durable", Class: machine.StoreDurable, Node: "node", Executor: "win-exec", Root: "/srv/durable", Filesystem: "D:"}), false); err != nil {
		t.Fatal(err)
	}
	imported, err := store.ImportData(ctx, ImportRequest{Project: "imp", Node: "node", Paths: []string{"D:/data/sumire"}, Name: "sumire", DeleteSource: true})
	if err != nil {
		t.Fatal(err)
	}
	objectID := imported[0].ObjectID
	sha := strings.Repeat("ab", 32)
	if err := store.RecordPack(ctx, PackMade{ObjectID: objectID, ExecutorID: "win-exec", PackPath: "D:/kairo/packs/" + objectID + ".tar.zst",
		ManifestPath: "D:/kairo/packs/" + objectID + ".tar.zst.files.tsv", PackBytes: 100, ContentBytes: 900, Files: 3, PackSHA256: sha, ManifestSHA256: sha}); err != nil {
		t.Fatal(err)
	}
	copies, err := store.CopiesToMake(ctx, "win-exec")
	if err != nil || len(copies) != 1 {
		t.Fatalf("copies %+v %v", copies, err)
	}
	c := copies[0]
	copyPath := "/srv/durable/objects/" + c.Month + "/" + objectID + ".tar.zst"
	if err := store.RecordCopy(ctx, CopyMade{ObjectID: objectID, StoreID: "z-durable", ExecutorID: "win-exec", Path: copyPath, ManifestPath: copyPath + ".files.tsv",
		Bytes: c.PackBytes, SHA256: c.PackSHA256, ManifestSHA256: c.ManifestSHA256}); err != nil {
		t.Fatal(err)
	}
	// the durable store's disk now hangs on node3, and the pack's volatile
	// store is no longer declared: the copy on node3 is the only source
	spec := stores(machine.Store{ID: "z-durable", Class: machine.StoreDurable, Node: "node3", Root: "/srv/durable", Filesystem: "/"})
	spec.Stores = storesWithout(spec.Stores, "d-scratch")
	if _, err := store.ApplyMachines(ctx, spec, false); err != nil {
		t.Fatal(err)
	}
	reader := schedulerExecutionSpec("reader")
	reader.Scope.Project = "imp"
	reader.ExecutorSelector = json.RawMessage(`{"labels":{"environment":"linux"},"node":"node2"}`)
	reader.Exclusive, reader.Checkpointable, reader.Preemptible = []ExclusiveRequest{}, false, false
	reader.Inputs = []datainput.Input{{Name: "data", Data: "sumire"}}
	if _, _, err := store.SubmitExecution(ctx, reader); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	fetches, err := store.FetchesToMake(ctx, "exec2")
	if err != nil || len(fetches) != 1 {
		t.Fatalf("fetches %+v %v", fetches, err)
	}
	if f := fetches[0]; f.Source.Local || f.Source.StoreID != "z-durable" || f.Source.Addr != "192.0.2.30:7475" || f.Source.Token == "" {
		t.Fatalf("fetch order %+v", f)
	}
}

// storesWithout is stores without the one of id.
func storesWithout(stores []machine.Store, id string) []machine.Store {
	out := make([]machine.Store, 0, len(stores))
	for _, s := range stores {
		if s.ID != id {
			out = append(out, s)
		}
	}
	return out
}
