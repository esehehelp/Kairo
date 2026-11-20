package store

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"kairo/internal/machine"
)

// A durable store's listing is compared with the records: a copy the records
// lack there is adopted from its description (the object recorded as
// described, packed, its source kept; the copy verified); a recorded copy not
// listed is marked damaged; a listing that would make most of the store
// missing changes nothing; a copy that is not the object recorded is refused.
func TestADurableStoresDirectoryIsAdoptedAndChecked(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	addTestDisk(t, store, "node", "D:")
	ctx := context.Background()
	if err := store.UpsertExecutor(ctx, Executor{ID: "durable-exec", NodeID: "node", Kind: "windows", Attributes: json.RawMessage(`{"environment":"windows"}`), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	declareTestStore(t, store,
		machine.Store{ID: "e-durable", Class: machine.StoreDurable, Node: "node", Root: "E:/kairo", Filesystem: "D:"},
		machine.Store{ID: "d-scratch", Class: machine.StoreVolatile, Node: "node", Root: "D:/kairo", Filesystem: "D:"},
	)
	sha := strings.Repeat("ab", 32)
	for _, id := range []string{"obj_a", "obj_k"} {
		insertPacked(t, store, id, "executor", "d-scratch", "D:/kairo", sha)
		if _, err := store.CopiesToMake(ctx, "durable-exec"); err != nil {
			t.Fatal(err)
		}
		if err := store.RecordCopy(ctx, CopyMade{ObjectID: id, StoreID: "e-durable", ExecutorID: "durable-exec", Path: "E:/kairo/objects/202611/" + id + ".tar.zst",
			ManifestPath: "E:/kairo/objects/202611/" + id + ".tar.zst.files.tsv", Bytes: 100, SHA256: sha, ManifestSHA256: sha}); err != nil {
			t.Fatal(err)
		}
	}
	listedAt := time.Now().UTC().Add(time.Second).Format(time.RFC3339Nano)
	listing := DurableListing{ExecutorID: "durable-exec", StoreID: "e-durable", Root: "E:/kairo", ListedAt: listedAt,
		Copies: map[string]string{"obj_a": "202611", "obj_k": "202611", "obj_lost": "202611"}}
	if _, err := store.InventoryDurable(ctx, DurableListing{ExecutorID: "executor", StoreID: "e-durable", Root: "E:/kairo", ListedAt: listedAt, Copies: listing.Copies}); err == nil {
		t.Fatal("a listing by an executor not serving the store was taken")
	}
	inv, err := store.InventoryDurable(ctx, listing)
	if err != nil || len(inv.Unknown) != 1 || inv.Unknown[0] != "obj_lost" || inv.Missing != 0 {
		t.Fatalf("inventory %+v %v", inv, err)
	}
	project, name, files, content := "lost-project", "datasets/x", int64(3), int64(300)
	described := ObjectDescription{Format: DescriptionFormat, ObjectID: "obj_lost", Kind: ObjectImport, Lifecycle: LifecycleDurable, Project: &project, Name: &name,
		SourceID: "obj_lost", SourcePath: "D:/data/x", Node: "node", PackedOn: "d-scratch", Files: &files, ContentBytes: &content,
		PackBytes: 120, PackSHA256: sha, ManifestSHA256: sha, CreatedAt: "2026-11-15T10:00:00Z"}
	bad := described
	bad.Lifecycle = LifecycleTemporal
	if _, err = store.AdoptCopies(ctx, "durable-exec", "e-durable", "E:/kairo", []AdoptedCopy{{Month: "202611", Description: bad}}); err == nil {
		t.Fatal("a temporal object was adopted from a durable store")
	}
	if n, err := store.AdoptCopies(ctx, "durable-exec", "e-durable", "E:/kairo", []AdoptedCopy{{Month: "202611", Description: described}}); err != nil || n != 1 {
		t.Fatalf("adopted %d %v", n, err)
	}
	if n, err := store.AdoptCopies(ctx, "durable-exec", "e-durable", "E:/kairo", []AdoptedCopy{{Month: "202611", Description: described}}); err != nil || n != 0 {
		t.Fatalf("adopted again %d %v", n, err)
	}
	objects, err := store.DataObjects(ctx, DataObjectFilter{Project: "lost-project"})
	if err != nil || len(objects) != 1 {
		t.Fatalf("adopted objects %+v %v", objects, err)
	}
	o := objects[0]
	if o.ID != "obj_lost" || o.State != "packed" || o.SourceState != "kept" || o.Copies != 1 || o.Name == nil || *o.Name != name || o.PackBytes == nil || *o.PackBytes != 120 {
		t.Fatalf("adopted object %+v", o)
	}
	// a copy that is not the object recorded
	other := described
	other.ObjectID, other.SourceID, other.PackSHA256 = "obj_a", "obj_a", strings.Repeat("ff", 32)
	if _, err = store.AdoptCopies(ctx, "durable-exec", "e-durable", "E:/kairo", []AdoptedCopy{{Month: "202611", Description: other}}); err == nil {
		t.Fatal("a copy differing from the recorded object was adopted")
	}

	// obj_k is gone from the directory: marked damaged
	listing.Copies = map[string]string{"obj_a": "202611", "obj_lost": "202611"}
	listing.ListedAt = "2026-11-19T12:00:00.5Z"
	if _, err = store.testExec(`UPDATE object_copies SET updated_at='2026-11-19T12:00:00Z' WHERE object_id='obj_k'`); err != nil {
		t.Fatal(err)
	}
	if inv, err = store.InventoryDurable(ctx, listing); err != nil || inv.Missing != 1 {
		t.Fatalf("a recorded copy gone: %+v %v", inv, err)
	}
	var problem *string
	if err = store.db.QueryRowContext(ctx, `SELECT check_problem FROM object_copies WHERE object_id='obj_k' AND store_id='e-durable'`).Scan(&problem); err != nil || problem == nil {
		t.Fatalf("obj_k not marked: %v %v", problem, err)
	}
	// an empty directory (a disk not mounted) is not believed
	listing.Copies = map[string]string{}
	if _, err = store.InventoryDurable(ctx, listing); err == nil {
		t.Fatal("an empty listing of a store with copies was believed")
	}
}
