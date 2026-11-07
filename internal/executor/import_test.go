package executor

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"kairo/internal/machine"
	"kairo/internal/store"
)

// importedTree makes a directory outside the store with a file and a
// subdirectory, as data an import names.
func importedTree(t *testing.T, dir string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(dir, "shards"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "meta.json"), []byte(`{"rows": 3}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "shards", "0.bin"), bytes.Repeat([]byte{7}, 4096), 0o644); err != nil {
		t.Fatal(err)
	}
}

// A round packs an imported directory onto the store it was ordered packed
// on (outside the store, only read); a directory missing fails and is tried
// again later.
func TestAnImportedDirectoryIsPacked(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	root, data := filepath.Join(base, "volatile"), filepath.Join(base, "data", "a")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	importedTree(t, data)
	fake := &fakePacks{fakeOutputs: &fakeOutputs{fakeScratch: &fakeScratch{fakeStores: &fakeStores{}}}}
	var logs bytes.Buffer
	e := storesExecutor(t, fake, &logs)
	e.stores = []servedStore{{decl: machine.Store{ID: "c-scratch", Class: machine.StoreVolatile, Root: root}, fs: localStoreFS{}, usable: true}}
	fake.orders = []store.PackOrder{
		{ObjectID: "obj_a", Kind: store.ObjectImport, SourceID: "obj_a", StoreID: "c-scratch", Root: root, Path: data},
		{ObjectID: "obj_gone", Kind: store.ObjectImport, SourceID: "obj_gone", StoreID: "c-scratch", Root: root, Path: filepath.Join(base, "data", "gone")},
	}
	e.packDue(ctx)
	if len(fake.made) != 1 || fake.made[0].ObjectID != "obj_a" || fake.made[0].Files != 2 || fake.made[0].ContentBytes != 4096+11 {
		t.Fatalf("made: %+v\n%s", fake.made, logs.String())
	}
	if entries := unpack(t, fake.made[0].PackPath); len(entries) != 3 || entries["meta.json"] != `{"rows": 3}` || len(entries["shards/0.bin"]) != 4096 {
		t.Fatalf("packed: %v", entries)
	}
	if !exists(filepath.Join(data, "meta.json")) {
		t.Fatal("the imported directory was changed")
	}
	if !strings.Contains(logs.String(), "imported directory not packed") || !strings.Contains(logs.String(), "obj_gone") {
		t.Fatalf("missing directory not warned:\n%s", logs.String())
	}
}

// Once copied, an imported directory is deleted only when the import asked
// for it and it holds just what was packed; without delete_source it stays
// and only its pack goes; one changed since is kept with why.
func TestAnImportedDirectoryIsDeletedOnlyWhenAskedFor(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	root := filepath.Join(base, "volatile")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	dirs := map[string]string{}
	packs := map[string]packResult{}
	for _, name := range []string{"a", "b", "c"} {
		dirs[name] = filepath.Join(base, "data", name)
		importedTree(t, dirs[name])
		res, err := (localStoreFS{}).PackDir(ctx, root, dirs[name], "obj_"+name)
		if err != nil {
			t.Fatal(err)
		}
		packs[name] = res
	}
	if err := os.WriteFile(filepath.Join(dirs["c"], "shards", "1.bin"), []byte("late"), 0o644); err != nil {
		t.Fatal(err)
	}
	fake := &fakeRetire{fakeCopies: newFakeCopies()}
	var logs bytes.Buffer
	e := storesExecutor(t, fake, &logs)
	e.stores = []servedStore{{decl: machine.Store{ID: "c-scratch", Class: machine.StoreVolatile, Root: root}, fs: localStoreFS{}, usable: true}}
	fake.deletions = []store.DeletionOrder{
		{ObjectID: "obj_a", Kind: store.ObjectImport, SourceID: "obj_a", StoreID: "c-scratch", Root: root, Path: dirs["a"], DeleteSource: true},
		{ObjectID: "obj_b", Kind: store.ObjectImport, SourceID: "obj_b", StoreID: "c-scratch", Root: root, Path: dirs["b"]},
		{ObjectID: "obj_c", Kind: store.ObjectImport, SourceID: "obj_c", StoreID: "c-scratch", Root: root, Path: dirs["c"], DeleteSource: true},
	}
	e.deleteDue(ctx)
	var got []string
	for _, s := range fake.settled {
		got = append(got, s.ObjectID+":"+s.State)
	}
	if strings.Join(got, ",") != "obj_a:deleting,obj_a:deleted,obj_b:kept,obj_c:deleting,obj_c:kept" {
		t.Fatalf("settled %v\n%s", got, logs.String())
	}
	if !strings.Contains(fake.settled[2].Reason, "without delete_source") || !strings.Contains(fake.settled[4].Reason, "shards/1.bin is not in its pack") {
		t.Fatalf("reasons: %+v", fake.settled)
	}
	if exists(dirs["a"]) || !exists(filepath.Join(dirs["b"], "meta.json")) || !exists(filepath.Join(dirs["c"], "meta.json")) {
		t.Fatalf("a deleted %v, b kept %v, c kept %v", !exists(dirs["a"]), exists(dirs["b"]), exists(dirs["c"]))
	}
	if !exists(filepath.Join(base, "data")) {
		t.Fatal("the imported directory's parent was deleted")
	}
	for name, res := range packs {
		if exists(res.packPath) || exists(res.manifestPath) {
			t.Fatalf("pack of %s left", name)
		}
	}
}

// An imported directory is deleted through its parent's handle only as a
// plain directory: a link in its place is refused, never followed.
func TestRemoveImportedDirRefusesALink(t *testing.T) {
	base := t.TempDir()
	target := filepath.Join(base, "target")
	importedTree(t, target)
	link := filepath.Join(base, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("no symlink here: %v", err)
	}
	if err := removeImportedDir(link); err == nil {
		t.Fatal("a link was taken as the directory")
	}
	if !exists(filepath.Join(target, "meta.json")) {
		t.Fatal("removed through a link")
	}
	if err := removeImportedDir(filepath.Join(base, "missing")); err != nil {
		t.Fatalf("a directory gone: %v", err)
	}
	if err := onSameFilesystem(target, base); err != nil {
		t.Fatalf("same filesystem: %v", err)
	}
}

// A temporal object's identity is the file list a pack of it would have,
// made without writing the pack.
func TestAnIdentityIsThePacksFileListWithoutThePack(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	root, data := filepath.Join(base, "volatile"), filepath.Join(base, "data")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	importedTree(t, data)
	id, err := (localStoreFS{}).IdentifyDir(ctx, root, data, "obj_t")
	if err != nil {
		t.Fatal(err)
	}
	if id.packPath != "" || id.packBytes != 0 || exists(filepath.Join(root, packsDirName, "obj_t"+packSuffix)) {
		t.Fatalf("an identity wrote a pack: %+v", id)
	}
	if !exists(id.manifestPath) || id.files != 2 || id.contentBytes != 4096+11 || !strings.HasPrefix(id.listBody, manifestHeader+"\n") {
		t.Fatalf("identity %+v", id)
	}
	pack, err := (localStoreFS{}).PackDir(ctx, root, data, "obj_p")
	if err != nil {
		t.Fatal(err)
	}
	if pack.manifestHash != id.manifestHash {
		t.Fatalf("the identity's list differs from the pack's: %s, %s", id.manifestHash, pack.manifestHash)
	}
}
