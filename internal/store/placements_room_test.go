package store

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"kairo/internal/machine"
)

// An object is brought to the first volatile store of the node with room for
// what it unpacks; a fetch long waiting on a store without room moves to one
// with room (event fetch_moved), and is ordered there.
func TestAFetchGoesWhereThereIsRoom(t *testing.T) {
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
	addTestDisk(t, store, "node2", "/big")
	spec := storesSpec(
		machine.Store{ID: "d-scratch", Class: machine.StoreVolatile, Node: "node", Root: "D:/kairo", Filesystem: "D:"},
		machine.Store{ID: "e-durable", Class: machine.StoreDurable, Node: "node", Root: "E:/kairo", Filesystem: "D:"},
		machine.Store{ID: "a-small", Class: machine.StoreVolatile, Node: "node2", Root: "/srv/kairo-store", Filesystem: "/", ReserveFreeBytes: 1000},
		machine.Store{ID: "b-big", Class: machine.StoreVolatile, Node: "node2", Root: "/big/kairo-store", Filesystem: "/big"},
	)
	spec.Nodes = append(spec.Nodes, machine.Node{ID: "node2"})
	if _, err := store.ApplyMachines(ctx, spec, false); err != nil {
		t.Fatal(err)
	}
	sha := strings.Repeat("ab", 32)
	packed := func(path, name string) string {
		t.Helper()
		imported, err := store.ImportData(ctx, ImportRequest{Project: "imp", Node: "node", Paths: []string{path}, Name: name})
		if err != nil {
			t.Fatal(err)
		}
		id := imported[0].ObjectID
		if err = store.RecordPack(ctx, PackMade{ObjectID: id, ExecutorID: "win-exec", PackPath: "D:/kairo/packs/" + id + ".tar.zst",
			ManifestPath: "D:/kairo/packs/" + id + ".tar.zst.files.tsv", PackBytes: 100, ContentBytes: 900, Files: 3, PackSHA256: sha, ManifestSHA256: sha}); err != nil {
			t.Fatal(err)
		}
		copies, err := store.CopiesToMake(ctx, "win-exec")
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range copiesOnly(copies) {
			p := "E:/kairo/objects/" + c.Month + "/" + c.ObjectID + ".tar.zst"
			if err = store.RecordCopy(ctx, CopyMade{ObjectID: c.ObjectID, StoreID: "e-durable", ExecutorID: "win-exec", Path: p, ManifestPath: p + ".files.tsv",
				Bytes: c.PackBytes, SHA256: c.PackSHA256, ManifestSHA256: c.ManifestSHA256}); err != nil {
				t.Fatal(err)
			}
		}
		return id
	}
	first := packed("D:/data/first", "first")
	second := packed("D:/data/second", "second")
	storeOf := func(objectID string) string {
		t.Helper()
		var storeID string
		if err := store.db.QueryRowContext(ctx, `SELECT store_id FROM object_placements WHERE object_id=$1`, objectID).Scan(&storeID); err != nil {
			t.Fatal(err)
		}
		return storeID
	}
	// never reported: taken as having room; the first by id
	if _, err := store.FetchData(ctx, FetchRequest{Project: "imp", Node: "node2", Objects: []string{first}}); err != nil {
		t.Fatal(err)
	}
	if got := storeOf(first); got != "a-small" {
		t.Fatalf("first fetch on %s", got)
	}
	// a-small has room for 500 bytes past its reserve: 900 do not fit
	free, total := int64(1500), int64(100000)
	if err := store.RecordStoreObservation(ctx, StoreObservation{StoreID: "a-small", ExecutorID: "exec2", Root: "/srv/kairo-store", Usable: true, FreeBytes: &free, TotalBytes: &total}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.FetchData(ctx, FetchRequest{Project: "imp", Node: "node2", Objects: []string{second}}); err != nil {
		t.Fatal(err)
	}
	if got := storeOf(second); got != "b-big" {
		t.Fatalf("a fetch ordered onto %s, which has no room for it", got)
	}
	// the first one, ordered on a-small, is moved only once it has waited
	orders, err := store.FetchesToMake(ctx, "exec2")
	if err != nil {
		t.Fatal(err)
	}
	if got := storeOf(first); got != "a-small" {
		t.Fatalf("a fetch just ordered was moved to %s", got)
	}
	if _, err = store.testExec(`UPDATE object_placements SET created_at='2026-01-01T00:00:00Z' WHERE object_id=$1`, first); err != nil {
		t.Fatal(err)
	}
	if orders, err = store.FetchesToMake(ctx, "exec2"); err != nil {
		t.Fatal(err)
	}
	if got := storeOf(first); got != "b-big" {
		t.Fatalf("a fetch long waiting for room stayed on %s", got)
	}
	for _, o := range orders {
		if o.ObjectID == first && (o.StoreID != "b-big" || o.Root != "/big/kairo-store") {
			t.Fatalf("the moved fetch is ordered %+v", o)
		}
	}
	var events int
	if err = store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM coordination_events WHERE event_type='fetch_moved' AND aggregate_id=$1`, first).Scan(&events); err != nil || events != 1 {
		t.Fatalf("fetch_moved events %d %v", events, err)
	}
}

// A durable store's room counts the copies verified there since its free
// bytes were last reported: a report minutes old does not let copies
// overrun its reserve.
func TestCopiesVerifiedSinceTheLastReportCountAgainstTheRoom(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	addTestDisk(t, store, "node", "D:")
	ctx := context.Background()
	if err := store.UpsertExecutor(ctx, Executor{ID: "durable-exec", NodeID: "node", Kind: "windows", Attributes: json.RawMessage(`{"environment":"windows"}`), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	declareTestStore(t, store,
		machine.Store{ID: "c-durable", Class: machine.StoreDurable, Node: "node", Root: "C:/_kairo-durable", Filesystem: "D:", ReserveFreeBytes: 1000},
		machine.Store{ID: "e-durable", Class: machine.StoreDurable, Node: "node", Root: "E:/kairo", Filesystem: "D:"},
		machine.Store{ID: "d-scratch", Class: machine.StoreVolatile, Node: "node", Root: "D:/kairo", Filesystem: "D:"},
	)
	// room for one pack of 100 bytes on c-durable
	free, total := int64(1150), int64(10000)
	if err := store.RecordStoreObservation(ctx, StoreObservation{StoreID: "c-durable", ExecutorID: "durable-exec", Root: "C:/_kairo-durable", Usable: true, FreeBytes: &free, TotalBytes: &total}); err != nil {
		t.Fatal(err)
	}
	sha := strings.Repeat("aa", 32)
	insertPacked(t, store, "obj_a", "executor", "d-scratch", "D:/kairo", sha)
	orders, err := store.CopiesToMake(ctx, "durable-exec")
	if orders = copiesOnly(orders); err != nil || len(orders) != 1 || orders[0].StoreID != "c-durable" {
		t.Fatalf("first %+v %v", orders, err)
	}
	if err = store.RecordCopy(ctx, CopyMade{ObjectID: "obj_a", StoreID: "c-durable", ExecutorID: "durable-exec", Path: "C:/_kairo-durable/objects/202611/obj_a.tar.zst",
		ManifestPath: "C:/_kairo-durable/objects/202611/obj_a.tar.zst.files.tsv", Bytes: 100, SHA256: sha, ManifestSHA256: sha}); err != nil {
		t.Fatal(err)
	}
	// no new report: the copy just verified still takes its room
	insertPacked(t, store, "obj_b", "executor", "d-scratch", "D:/kairo", sha)
	orders, err = store.CopiesToMake(ctx, "durable-exec")
	for _, o := range copiesOnly(orders) {
		if o.ObjectID == "obj_b" && o.StoreID != "e-durable" {
			t.Fatalf("obj_b went to %s past its reserve: %+v", o.StoreID, orders)
		}
	}
	if err != nil {
		t.Fatal(err)
	}
}
