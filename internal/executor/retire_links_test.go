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

// retireWith packs run exe_1's output directory on a volatile store, made
// by before (and changed by after, once packed), then has its deletion
// settled: what was settled, and the directory.
func retireWith(t *testing.T, before, after func(t *testing.T, dir string)) ([]store.ObjectDeletion, string) {
	t.Helper()
	ctx := context.Background()
	volatile := filepath.Join(t.TempDir(), "volatile")
	dir, err := (localStoreFS{}).CreateKind(ctx, volatile, kindOutput, "exe_1")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "weights.bin"), bytes.Repeat([]byte{1, 2, 3}, 1000), 0o644); err != nil {
		t.Fatal(err)
	}
	before(t, dir)
	if _, err := (localStoreFS{}).Pack(ctx, volatile, kindOutput, "exe_1", "obj_1"); err != nil {
		t.Fatal(err)
	}
	after(t, dir)
	fake := &fakeRetire{fakeCopies: newFakeCopies()}
	var logs bytes.Buffer
	e := storesExecutor(t, fake, &logs)
	e.storeWork().stores = []servedStore{{decl: machine.Store{ID: "c-scratch", Class: machine.StoreVolatile, Root: volatile}, fs: localStoreFS{}, usable: true}}
	fake.deletions = []store.DeletionOrder{{ObjectID: "obj_1", Kind: store.ObjectOutput, SourceID: "exe_1", StoreID: "c-scratch", Root: volatile}}
	e.storeWork().deleteDue(ctx)
	return fake.settled, dir
}

// A source an operator released (kept once: its pack and file list are gone)
// is deleted without being checked against its pack.
func TestAReleasedSourceIsDeletedWithoutItsPack(t *testing.T) {
	ctx := context.Background()
	volatile := filepath.Join(t.TempDir(), "volatile")
	res := packed(t, volatile)
	dir := filepath.Join(volatile, outputDirName, "exe_1")
	if err := removePack(localStoreFS{}, volatile, "obj_1"); err != nil || exists(res.packPath) || exists(res.manifestPath) {
		t.Fatalf("pack removed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "late.txt"), []byte("written after it was packed"), 0o644); err != nil {
		t.Fatal(err)
	}
	fake := &fakeRetire{fakeCopies: newFakeCopies()}
	var logs bytes.Buffer
	e := storesExecutor(t, fake, &logs)
	e.storeWork().stores = []servedStore{{decl: machine.Store{ID: "c-scratch", Class: machine.StoreVolatile, Root: volatile}, fs: localStoreFS{}, usable: true}}
	order := store.DeletionOrder{ObjectID: "obj_1", Kind: store.ObjectOutput, SourceID: "exe_1", StoreID: "c-scratch", Root: volatile}
	fake.deletions = []store.DeletionOrder{order}
	e.storeWork().deleteDue(ctx)
	if len(fake.settled) != 2 || fake.settled[1].State != store.SourceKept || !exists(dir) {
		t.Fatalf("not released: settled %+v, want kept (its file list is gone)", fake.settled)
	}
	order.Released = true
	fake.deletions, fake.settled = []store.DeletionOrder{order}, nil
	e.storeWork().deletions = round{}
	e.storeWork().deleteDue(ctx)
	if len(fake.settled) != 2 || fake.settled[1].State != store.SourceDeleted || exists(dir) {
		t.Fatalf("released: settled %+v (source there: %v)", fake.settled, exists(dir))
	}
}

// A symbolic link is packed as one, so a source holding the links it was
// packed with is deleted once copied; one whose link points elsewhere since
// is kept.
func TestASourceWithTheLinksItWasPackedWithIsDeleted(t *testing.T) {
	link := func(target string) func(t *testing.T, dir string) {
		return func(t *testing.T, dir string) {
			t.Helper()
			_ = os.Remove(filepath.Join(dir, "latest"))
			if err := os.Symlink(target, filepath.Join(dir, "latest")); err != nil {
				t.Skipf("no symlink here: %v", err)
			}
		}
	}
	nothing := func(*testing.T, string) {}
	settled, dir := retireWith(t, link("weights.bin"), nothing)
	if len(settled) != 2 || settled[1].State != store.SourceDeleted || exists(dir) {
		t.Fatalf("settled %+v (source there: %v)", settled, exists(dir))
	}
	settled, dir = retireWith(t, link("weights.bin"), link("other.bin"))
	if len(settled) != 2 || settled[1].State != store.SourceKept || !strings.Contains(settled[1].Reason, "latest is a link not in its pack as it is") || !exists(dir) {
		t.Fatalf("settled %+v, want kept for the link pointing elsewhere", settled)
	}
}
