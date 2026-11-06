package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"kairo/internal/machine"
)

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
	declareTestStore(t, store,
		machine.Store{ID: "e-durable", Class: machine.StoreDurable, Node: "node", Executor: "executor", Root: "E:/kairo", Filesystem: "D:"},
		machine.Store{ID: "z-durable", Class: machine.StoreDurable, Node: "remote", Root: "/srv/durable", Filesystem: "/"},
		machine.Store{ID: "d-scratch", Class: machine.StoreVolatile, Node: "node", Executor: "executor", Root: "D:/kairo", Filesystem: "D:"},
		machine.Store{ID: "r-pool", Class: machine.StoreVolatile, Node: "remote", Root: "/srv/kairo", Filesystem: "/"},
	)
	shaLocal, shaRemote := strings.Repeat("aa", 32), strings.Repeat("bb", 32)
	insertPacked(t, store, "obj_local", "executor", "d-scratch", "D:/kairo", shaLocal)
	insertPacked(t, store, "obj_remote", "remote-exec", "r-pool", "/srv/kairo", shaRemote)

	// only an executor serving the first durable store (e-durable) takes them
	// (the test executor is a WSL one: it serves the stores naming it)
	if orders, err := store.CopiesToMake(ctx, "remote-exec"); err != nil || len(orders) != 0 {
		t.Fatalf("the remote executor took %+v %v", orders, err)
	}
	orders, err := store.CopiesToMake(ctx, "executor")
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
	again, err := store.CopiesToMake(ctx, "executor")
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

	made := CopyMade{ObjectID: "obj_local", StoreID: "e-durable", ExecutorID: "executor", Path: "E:/kairo/objects/202611/obj_local.tar.zst",
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
	refused("by another executor", func(m *CopyMade) { m.ExecutorID = "remote-exec" }, nil, "is executor executor's")
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
	orders, err = store.CopiesToMake(ctx, "executor")
	if err != nil || len(orders) != 1 || orders[0].ObjectID != "obj_remote" {
		t.Fatalf("after the local copy was verified: %+v %v", orders, err)
	}

	// a durable store declared ahead of it takes new objects; the copied one
	// is not copied again
	declareTestStore(t, store,
		machine.Store{ID: "a-durable", Class: machine.StoreDurable, Node: "remote", Root: "/srv/first", Filesystem: "/"},
		machine.Store{ID: "e-durable", Class: machine.StoreDurable, Node: "node", Executor: "executor", Root: "E:/kairo", Filesystem: "D:"},
		machine.Store{ID: "d-scratch", Class: machine.StoreVolatile, Node: "node", Executor: "executor", Root: "D:/kairo", Filesystem: "D:"},
		machine.Store{ID: "r-pool", Class: machine.StoreVolatile, Node: "remote", Root: "/srv/kairo", Filesystem: "/"},
	)
	insertPacked(t, store, "obj_new", "executor", "d-scratch", "D:/kairo", shaLocal)
	orders, err = store.CopiesToMake(ctx, "remote-exec")
	if err != nil || len(orders) != 1 || orders[0].ObjectID != "obj_new" || orders[0].StoreID != "a-durable" || orders[0].Source.Local {
		t.Fatalf("the new first durable store: %+v %v", orders, err)
	}
}
