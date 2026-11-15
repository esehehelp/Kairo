package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"kairo/internal/machine"
)

// copiesOnly are orders but the describe-only ones (descriptions.go).
func copiesOnly(orders []CopyOrder) []CopyOrder {
	var out []CopyOrder
	for _, o := range orders {
		if !o.DescribeOnly {
			out = append(out, o)
		}
	}
	return out
}

// insertPacked makes a packed object of executorID on storeID at root.
func insertPacked(t *testing.T, store *Store, objectID, executorID, storeID, root, sha string) {
	t.Helper()
	if _, err := store.testExec(`INSERT INTO data_objects(id,kind,source_id,store_id,executor_id,root,source_path,state,pack_path,manifest_path,content_bytes,files,skipped,pack_bytes,pack_sha256,manifest_sha256,created_at,updated_at,packed_at)
		VALUES($1,'output',$1,$2,$3,$4,$4,'packed',$4,$4,10,1,0,100,$5,$5,'2026-11-06T01:02:03Z','2026-11-06T01:02:03Z','2026-11-06T01:02:03Z')`,
		objectID, storeID, executorID, root, sha); err != nil {
		t.Fatal(err)
	}
}

// A packed object goes to the first durable store by id, taken by one
// executor serving it: read locally when its pack is on a store that
// executor serves, else from the pack's agent with a token that only that
// agent's node can use, until the next order replaces it; the copy is
// verified only as <root>/objects/<month>/<object id>.tar.zst matching the
// pack.
func TestCopiesGoToTheDurableStoreLocallyOrWithAToken(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	addTestDisk(t, store, "node", "D:")
	ctx := context.Background()
	if err := store.UpsertNode(ctx, Node{ID: "remote", Name: "remote", OS: "linux", Architecture: "amd64", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertExecutor(ctx, Executor{ID: "remote-exec", NodeID: "remote", Kind: "linux", Attributes: json.RawMessage(`{"pack_addr":"10.0.0.2:7475"}`), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	addTestDisk(t, store, "remote", "/")
	if err := store.UpsertNode(ctx, Node{ID: "third", Name: "third", OS: "linux", Architecture: "amd64", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	// the test inventory's "executor" is a WSL one: the node's stores naming no
	// executor are served by durable-exec
	if err := store.UpsertExecutor(ctx, Executor{ID: "durable-exec", NodeID: "node", Kind: "windows", Attributes: json.RawMessage(`{"environment":"windows"}`), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	declareTestStore(t, store,
		machine.Store{ID: "e-durable", Class: machine.StoreDurable, Node: "node", Root: "E:/kairo", Filesystem: "D:"},
		machine.Store{ID: "z-durable", Class: machine.StoreDurable, Node: "third", Root: "/srv/durable", Filesystem: "/"},
		machine.Store{ID: "d-scratch", Class: machine.StoreVolatile, Node: "node", Root: "D:/kairo", Filesystem: "D:"},
		machine.Store{ID: "r-pool", Class: machine.StoreVolatile, Node: "remote", Root: "/srv/kairo", Filesystem: "/"},
	)
	shaLocal, shaRemote := strings.Repeat("aa", 32), strings.Repeat("bb", 32)
	insertPacked(t, store, "obj_local", "executor", "d-scratch", "D:/kairo", shaLocal)
	insertPacked(t, store, "obj_remote", "remote-exec", "r-pool", "/srv/kairo", shaRemote)

	// only an executor serving the first durable store (e-durable) takes them:
	// not the remote one, nor the WSL one of the node
	if orders, err := store.CopiesToMake(ctx, "executor"); err != nil || len(orders) != 0 {
		t.Fatalf("the WSL executor took %+v %v", orders, err)
	}
	if orders, err := store.CopiesToMake(ctx, "remote-exec"); err != nil || len(orders) != 0 {
		t.Fatalf("the remote executor took %+v %v", orders, err)
	}
	orders, err := store.CopiesToMake(ctx, "durable-exec")
	if err != nil || len(orders) != 2 {
		t.Fatalf("orders %+v %v", orders, err)
	}
	byObject := map[string]CopyOrder{}
	for _, o := range orders {
		byObject[o.ObjectID] = o
	}
	local, remote := byObject["obj_local"], byObject["obj_remote"]
	if local.StoreID != "e-durable" || local.Root != "E:/kairo" || local.Month != "202611" || !local.Source.Local || local.Source.Token != "" ||
		local.Source.StoreID != "d-scratch" || local.PackBytes != 100 || local.PackSHA256 != shaLocal {
		t.Fatalf("local order %+v", local)
	}
	if remote.Source.Local || remote.Source.Addr != "10.0.0.2:7475" || !strings.HasPrefix(remote.Source.Token, "kairo_pack_") || remote.Source.Root != "/srv/kairo" {
		t.Fatalf("remote order %+v", remote)
	}
	read, err := store.AuthorizePackRead(ctx, "remote", "obj_remote", remote.Source.Token)
	if err != nil || read.StoreID != "r-pool" || read.Root != "/srv/kairo" {
		t.Fatalf("read %+v %v", read, err)
	}
	for what, try := range map[string][3]string{
		"by another node":      {"node", "obj_remote", remote.Source.Token},
		"of another object":    {"remote", "obj_local", remote.Source.Token},
		"with a made-up token": {"remote", "obj_remote", "kairo_pack_guess"},
		"with no token at all": {"remote", "obj_remote", ""},
	} {
		if _, err := store.AuthorizePackRead(ctx, try[0], try[1], try[2]); !errors.Is(err, ErrNotFound) {
			t.Fatalf("a read %s: %v", what, err)
		}
	}
	// the next order's token replaces it
	again, err := store.CopiesToMake(ctx, "durable-exec")
	if err != nil || len(again) != 2 {
		t.Fatalf("orders again %+v %v", again, err)
	}
	if _, err := store.AuthorizePackRead(ctx, "remote", "obj_remote", remote.Source.Token); !errors.Is(err, ErrNotFound) {
		t.Fatalf("an old token still reads: %v", err)
	}
	// an expired token reads nothing
	var fresh string
	for _, o := range again {
		if o.ObjectID == "obj_remote" {
			fresh = o.Source.Token
		}
	}
	if _, err := store.testExec(`UPDATE object_copies SET token_expires_at='2026-01-01T00:00:00Z' WHERE object_id='obj_remote'`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AuthorizePackRead(ctx, "remote", "obj_remote", fresh); !errors.Is(err, ErrNotFound) {
		t.Fatalf("an expired token reads: %v", err)
	}

	made := CopyMade{ObjectID: "obj_local", StoreID: "e-durable", ExecutorID: "durable-exec", Path: "E:/kairo/objects/202611/obj_local.tar.zst",
		ManifestPath: "E:/kairo/objects/202611/obj_local.tar.zst.files.tsv", Bytes: 100, SHA256: shaLocal, ManifestSHA256: shaLocal}
	refused := func(what string, change func(*CopyMade), want error, text string) {
		t.Helper()
		bad := made
		change(&bad)
		err := store.RecordCopy(ctx, bad)
		if err == nil || want != nil && !errors.Is(err, want) || !strings.Contains(err.Error(), text) {
			t.Fatalf("%s: %v, want %q", what, err, text)
		}
	}
	refused("by another executor", func(m *CopyMade) { m.ExecutorID = "remote-exec" }, nil, "is executor durable-exec's")
	refused("in another month", func(m *CopyMade) { m.Path = "E:/kairo/objects/202610/obj_local.tar.zst" }, nil, "is not <root>/objects/202611")
	refused("outside the objects", func(m *CopyMade) { m.Path = "E:/kairo/obj_local.tar.zst" }, nil, "is not <root>/objects")
	refused("its list elsewhere", func(m *CopyMade) { m.ManifestPath = "E:/kairo/objects/202611/list.tsv" }, nil, "is not beside its copy")
	refused("short", func(m *CopyMade) { m.Bytes = 99 }, ErrCopyMismatch, "copied as 99 bytes")
	refused("another sha256", func(m *CopyMade) { m.SHA256 = shaRemote }, ErrCopyMismatch, "sha256")
	refused("another list", func(m *CopyMade) { m.ManifestSHA256 = shaRemote }, ErrCopyMismatch, "list")
	if err := store.RecordCopy(ctx, made); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCopy(ctx, made); err != nil {
		t.Fatalf("recorded again: %v", err)
	}
	orders, err = store.CopiesToMake(ctx, "durable-exec")
	if orders = copiesOnly(orders); err != nil || len(orders) != 1 || orders[0].ObjectID != "obj_remote" {
		t.Fatalf("after the local copy was verified: %+v %v", orders, err)
	}

	// a durable store on the pack's node takes a new object before one
	// elsewhere, read where it is; the copied one is not copied again
	declareTestStore(t, store,
		machine.Store{ID: "a-durable", Class: machine.StoreDurable, Node: "remote", Root: "/srv/first", Filesystem: "/"},
		machine.Store{ID: "e-durable", Class: machine.StoreDurable, Node: "node", Root: "E:/kairo", Filesystem: "D:"},
		machine.Store{ID: "d-scratch", Class: machine.StoreVolatile, Node: "node", Root: "D:/kairo", Filesystem: "D:"},
		machine.Store{ID: "r-pool", Class: machine.StoreVolatile, Node: "remote", Root: "/srv/kairo", Filesystem: "/"},
	)
	insertPacked(t, store, "obj_new", "remote-exec", "r-pool", "/srv/kairo", shaLocal)
	orders, err = store.CopiesToMake(ctx, "remote-exec")
	var newOrders []CopyOrder
	for _, o := range copiesOnly(orders) {
		if o.ObjectID == "obj_new" {
			newOrders = append(newOrders, o)
		}
	}
	if err != nil || len(newOrders) != 1 || newOrders[0].StoreID != "a-durable" || !newOrders[0].Source.Local {
		t.Fatalf("the durable store on the pack's node: %+v %v", orders, err)
	}
}

// A packed object goes to the first durable store by id with room for it:
// its free bytes as last reported, less its reserve and the packs of the
// copies being made there; a store never reported is taken as having room.
func TestCopiesGoToTheFirstDurableStoreWithRoom(t *testing.T) {
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
	sha := strings.Repeat("aa", 32)
	insertPacked(t, store, "obj_a", "executor", "d-scratch", "D:/kairo", sha)
	insertPacked(t, store, "obj_b", "executor", "d-scratch", "D:/kairo", sha)
	// room for one pack of 100 bytes on c-durable: 1150 free, 1000 kept
	free, total := int64(1150), int64(10000)
	if err := store.RecordStoreObservation(ctx, StoreObservation{StoreID: "c-durable", ExecutorID: "durable-exec", Root: "C:/_kairo-durable", Usable: true, FreeBytes: &free, TotalBytes: &total}); err != nil {
		t.Fatal(err)
	}
	orders, err := store.CopiesToMake(ctx, "durable-exec")
	if err != nil || len(orders) != 2 {
		t.Fatalf("orders %+v %v", orders, err)
	}
	to := map[string]string{}
	for _, o := range orders {
		to[o.ObjectID] = o.StoreID
	}
	if to["obj_a"] != "c-durable" || to["obj_b"] != "e-durable" {
		t.Fatalf("copies go to %v, want obj_a to c-durable (room for one) and obj_b to e-durable", to)
	}
}

// A copy is described as it is written (its order carries the description:
// whose it is and what it holds); one made without a description is
// described afterwards (a describe-only order), and is no longer once that
// is recorded, by an executor serving its store.
func TestDurableCopiesAreDescribed(t *testing.T) {
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
	insertPacked(t, store, "obj_a", "executor", "d-scratch", "D:/kairo", sha)
	insertPacked(t, store, "obj_b", "executor", "d-scratch", "D:/kairo", sha)
	orders, err := store.CopiesToMake(ctx, "durable-exec")
	if err != nil || len(orders) != 2 {
		t.Fatalf("orders %+v %v", orders, err)
	}
	for _, o := range orders {
		d := o.Description
		if o.DescribeOnly || d == nil || d.Format != DescriptionFormat || d.ObjectID != o.ObjectID || d.Kind != "output" || d.PackBytes != 100 ||
			d.PackSHA256 != sha || d.Node != "node" || d.PackedOn != "d-scratch" || d.SourcePath != "D:/kairo" {
			t.Fatalf("order %s describes %+v", o.ObjectID, d)
		}
	}
	made := func(id string, described bool) CopyMade {
		return CopyMade{ObjectID: id, StoreID: "e-durable", ExecutorID: "durable-exec", Path: "E:/kairo/objects/202611/" + id + ".tar.zst",
			ManifestPath: "E:/kairo/objects/202611/" + id + ".tar.zst.files.tsv", Bytes: 100, SHA256: sha, ManifestSHA256: sha, Described: described}
	}
	if err = store.RecordCopy(ctx, made("obj_a", true)); err != nil {
		t.Fatal(err)
	}
	if err = store.RecordCopy(ctx, made("obj_b", false)); err != nil {
		t.Fatal(err)
	}
	orders, err = store.CopiesToMake(ctx, "durable-exec")
	if err != nil || len(orders) != 1 || orders[0].ObjectID != "obj_b" || !orders[0].DescribeOnly || orders[0].Description == nil || orders[0].Month != "202611" {
		t.Fatalf("describe-only orders %+v %v", orders, err)
	}
	if err = store.RecordDescribed(ctx, "executor", "obj_b", "e-durable"); err == nil {
		t.Fatal("recorded by an executor not serving the store")
	}
	if err = store.RecordDescribed(ctx, "durable-exec", "obj_b", "e-durable"); err != nil {
		t.Fatal(err)
	}
	if orders, err = store.CopiesToMake(ctx, "durable-exec"); err != nil || len(orders) != 0 {
		t.Fatalf("orders once described %+v %v", orders, err)
	}
}

// An object with a verified copy is copied to each other durable store the
// executor serves too, read from that copy (its pack may be gone), after any
// object nowhere durable yet; a store without room for it takes none.
func TestADurableObjectIsReplicatedToEveryDurableStore(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	addTestDisk(t, store, "node", "D:")
	ctx := context.Background()
	if err := store.UpsertExecutor(ctx, Executor{ID: "durable-exec", NodeID: "node", Kind: "windows", Attributes: json.RawMessage(`{"environment":"windows"}`), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	declareTestStore(t, store,
		machine.Store{ID: "c-durable", Class: machine.StoreDurable, Node: "node", Root: "C:/_kairo-durable", Filesystem: "D:"},
		machine.Store{ID: "e-durable", Class: machine.StoreDurable, Node: "node", Root: "E:/kairo", Filesystem: "D:"},
		machine.Store{ID: "d-scratch", Class: machine.StoreVolatile, Node: "node", Root: "D:/kairo", Filesystem: "D:"},
	)
	sha := strings.Repeat("cd", 32)
	insertPacked(t, store, "obj_a", "executor", "d-scratch", "D:/kairo", sha)
	orders, err := store.CopiesToMake(ctx, "durable-exec")
	if err != nil || len(orders) != 1 || orders[0].StoreID != "c-durable" || orders[0].Source.Month != "" {
		t.Fatalf("first copy %+v %v", orders, err)
	}
	if err = store.RecordCopy(ctx, CopyMade{ObjectID: "obj_a", StoreID: "c-durable", ExecutorID: "durable-exec", Path: "C:/_kairo-durable/objects/202611/obj_a.tar.zst",
		ManifestPath: "C:/_kairo-durable/objects/202611/obj_a.tar.zst.files.tsv", Bytes: 100, SHA256: sha, ManifestSHA256: sha}); err != nil {
		t.Fatal(err)
	}
	// a new object, nowhere durable yet, goes before the replica
	insertPacked(t, store, "obj_b", "executor", "d-scratch", "D:/kairo", sha)
	orders, err = store.CopiesToMake(ctx, "durable-exec")
	if orders = copiesOnly(orders); err != nil || len(orders) != 2 {
		t.Fatalf("orders %+v %v", orders, err)
	}
	if orders[0].ObjectID != "obj_b" || orders[0].Source.Month != "" {
		t.Fatalf("the object nowhere durable is not first: %+v", orders)
	}
	r := orders[1]
	if r.ObjectID != "obj_a" || r.StoreID != "e-durable" || !r.Source.Local || r.Source.StoreID != "c-durable" || r.Source.Root != "C:/_kairo-durable" || r.Source.Month != "202611" {
		t.Fatalf("replica %+v", r)
	}
}

// A durable store on another node takes a replica of an object whose copies
// are all elsewhere, read from one of them through the pack server of the
// agent that made it, with a token only that agent's node can use.
func TestAReplicaIsReadFromADurableCopyOnAnotherNode(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	addTestDisk(t, store, "node", "D:")
	ctx := context.Background()
	if err := store.UpsertExecutor(ctx, Executor{ID: "durable-exec", NodeID: "node", Kind: "windows", Attributes: json.RawMessage(`{"environment":"windows","pack_addr":"10.0.0.1:7475"}`), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertNode(ctx, Node{ID: "remote", Name: "remote", OS: "linux", Architecture: "amd64", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertExecutor(ctx, Executor{ID: "remote-exec", NodeID: "remote", Kind: "linux", Attributes: json.RawMessage(`{"pack_addr":"10.0.0.2:7475"}`), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	addTestDisk(t, store, "remote", "/")
	declareTestStore(t, store,
		machine.Store{ID: "e-durable", Class: machine.StoreDurable, Node: "node", Root: "E:/kairo", Filesystem: "D:"},
		machine.Store{ID: "d-scratch", Class: machine.StoreVolatile, Node: "node", Root: "D:/kairo", Filesystem: "D:"},
		machine.Store{ID: "p-durable", Class: machine.StoreDurable, Node: "remote", Root: "/srv/_kairo-durable", Filesystem: "/"},
	)
	sha := strings.Repeat("ef", 32)
	insertPacked(t, store, "obj_x", "executor", "d-scratch", "D:/kairo", sha)
	// its first copy on its own node's store, though p-durable sorts first
	orders, err := store.CopiesToMake(ctx, "durable-exec")
	if orders = copiesOnly(orders); err != nil || len(orders) != 1 || orders[0].StoreID != "e-durable" || !orders[0].Source.Local {
		t.Fatalf("first copy %+v %v", orders, err)
	}
	if orders, err = store.CopiesToMake(ctx, "remote-exec"); err != nil || len(copiesOnly(orders)) != 0 {
		t.Fatalf("a replica before any verified copy: %+v %v", orders, err)
	}
	if err = store.RecordCopy(ctx, CopyMade{ObjectID: "obj_x", StoreID: "e-durable", ExecutorID: "durable-exec", Path: "E:/kairo/objects/202611/obj_x.tar.zst",
		ManifestPath: "E:/kairo/objects/202611/obj_x.tar.zst.files.tsv", Bytes: 100, SHA256: sha, ManifestSHA256: sha}); err != nil {
		t.Fatal(err)
	}
	orders, err = store.CopiesToMake(ctx, "remote-exec")
	if orders = copiesOnly(orders); err != nil || len(orders) != 1 {
		t.Fatalf("replica %+v %v", orders, err)
	}
	o := orders[0]
	if o.StoreID != "p-durable" || o.Source.Local || o.Source.StoreID != "e-durable" || o.Source.Root != "E:/kairo" || o.Source.Month != "202611" ||
		o.Source.Addr != "10.0.0.1:7475" || !strings.HasPrefix(o.Source.Token, "kairo_pack_") {
		t.Fatalf("replica source %+v", o)
	}
	if _, err = store.AuthorizePackRead(ctx, "remote", "obj_x", o.Source.Token); !errors.Is(err, ErrNotFound) {
		t.Fatalf("read on the replica's own node: %v", err)
	}
	read, err := store.AuthorizePackRead(ctx, "node", "obj_x", o.Source.Token)
	if err != nil || read.StoreID != "e-durable" || read.Root != "E:/kairo" || read.Month != "202611" {
		t.Fatalf("read of the durable copy: %+v %v", read, err)
	}
}
