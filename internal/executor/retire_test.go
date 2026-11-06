package executor

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"kairo/internal/machine"
	"kairo/internal/store"
)

// fakeRetire orders deletions and read backs and records what is settled.
type fakeRetire struct {
	*fakeCopies
	rmu       sync.Mutex
	deletions []store.DeletionOrder
	settled   []store.ObjectDeletion
	scrubs    []store.ScrubOrder
	scrubbed  []store.ScrubResult
}

func (f *fakeRetire) DeletionsToMake(context.Context, string) ([]store.DeletionOrder, error) {
	f.rmu.Lock()
	defer f.rmu.Unlock()
	return append([]store.DeletionOrder(nil), f.deletions...), nil
}

func (f *fakeRetire) SettleDeletion(_ context.Context, settled store.ObjectDeletion) error {
	f.rmu.Lock()
	defer f.rmu.Unlock()
	f.settled = append(f.settled, settled)
	return nil
}

func (f *fakeRetire) ScrubsToMake(context.Context, string) ([]store.ScrubOrder, error) {
	f.rmu.Lock()
	defer f.rmu.Unlock()
	return append([]store.ScrubOrder(nil), f.scrubs...), nil
}

func (f *fakeRetire) RecordScrub(_ context.Context, r store.ScrubResult) error {
	f.rmu.Lock()
	defer f.rmu.Unlock()
	f.scrubbed = append(f.scrubbed, r)
	return nil
}

// A copied source is deleted (claimed first): the output directory, its
// pack and file list; one whose marker names another is kept, with why.
func TestACopiedSourceIsDeleted(t *testing.T) {
	ctx := context.Background()
	volatile := filepath.Join(t.TempDir(), "volatile")
	res := packed(t, volatile)
	fake := &fakeRetire{fakeCopies: newFakeCopies()}
	var logs bytes.Buffer
	e := storesExecutor(t, fake, &logs)
	e.stores = []servedStore{{decl: machine.Store{ID: "c-scratch", Class: machine.StoreVolatile, Root: volatile}, fs: localStoreFS{}, usable: true}}
	// another run's output directory whose marker names something else
	other, err := (localStoreFS{}).CreateKind(ctx, volatile, kindOutput, "exe_2")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(other, outputMarkerName), []byte("exe_3\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	fake.deletions = []store.DeletionOrder{
		{ObjectID: "obj_1", Kind: store.ObjectOutput, SourceID: "exe_1", StoreID: "c-scratch", Root: volatile},
		{ObjectID: "obj_2", Kind: store.ObjectOutput, SourceID: "exe_2", StoreID: "c-scratch", Root: volatile},
		{ObjectID: "obj_3", Kind: store.ObjectOutput, SourceID: "exe_4", StoreID: "c-scratch", Root: volatile + "-moved"},
	}
	e.deleteDue(ctx)
	var got []string
	for _, s := range fake.settled {
		got = append(got, s.ObjectID+":"+s.State)
	}
	if strings.Join(got, ",") != "obj_1:deleting,obj_1:deleted,obj_2:deleting,obj_2:kept" {
		t.Fatalf("settled %v", got)
	}
	if !strings.Contains(fake.settled[3].Reason, "not deleted") {
		t.Fatalf("kept without why: %+v", fake.settled[3])
	}
	for _, p := range []string{filepath.Join(volatile, outputDirName, "exe_1"), res.packPath, res.manifestPath} {
		if exists(p) {
			t.Fatalf("%s left", p)
		}
	}
	if !exists(other) {
		t.Fatal("a directory not Kairo's for that run was deleted")
	}
}

// A durable copy is read back around the cache: one that matches reads as
// its pack, one changed since or gone is reported as such; a store whose
// objects directory is not there now reports nothing.
func TestDurableCopiesAreReadBack(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	volatile, durable := filepath.Join(base, "volatile"), filepath.Join(base, "durable")
	res := packed(t, volatile)
	if err := os.MkdirAll(durable, 0o755); err != nil {
		t.Fatal(err)
	}
	fake := &fakeRetire{fakeCopies: newFakeCopies()}
	var logs bytes.Buffer
	e := storesExecutor(t, fake, &logs)
	e.stores = []servedStore{
		{decl: machine.Store{ID: "c-scratch", Class: machine.StoreVolatile, Root: volatile}, fs: localStoreFS{}, usable: true},
		{decl: machine.Store{ID: "e-durable", Class: machine.StoreDurable, Root: durable}, fs: localStoreFS{}, usable: true},
	}
	order := store.ScrubOrder{ObjectID: "obj_1", StoreID: "e-durable", Root: durable, Month: "202611", PackBytes: res.packBytes, PackSHA256: res.packSHA256, ManifestSHA256: res.manifestHash}
	// nothing copied yet: the objects directory is not there
	fake.scrubs = []store.ScrubOrder{order}
	e.scrubDue(ctx)
	if len(fake.scrubbed) != 0 {
		t.Fatalf("read back with no objects directory: %+v", fake.scrubbed)
	}
	fake.copies = []store.CopyOrder{orderFor(res, durable, store.CopySource{Local: true, StoreID: "c-scratch", Root: volatile})}
	e.copyDue(ctx)
	e.scrubDue(ctx)
	if len(fake.scrubbed) != 1 || fake.scrubbed[0].Problem != "" || fake.scrubbed[0].SHA256 != res.packSHA256 || fake.scrubbed[0].Bytes != res.packBytes ||
		fake.scrubbed[0].ManifestSHA256 != res.manifestHash || fake.scrubbed[0].ExecutorID != "exec" {
		t.Fatalf("read back %+v", fake.scrubbed)
	}
	copyPath := filepath.Join(durable, objectsDirName, "202611", "obj_1.tar.zst")
	body, err := os.ReadFile(copyPath)
	if err != nil {
		t.Fatal(err)
	}
	body[len(body)/2] ^= 0xff
	if err := os.WriteFile(copyPath, body, 0o644); err != nil {
		t.Fatal(err)
	}
	e.scrubDue(ctx)
	if len(fake.scrubbed) != 2 || fake.scrubbed[1].SHA256 == res.packSHA256 || !strings.Contains(logs.String(), "copy damaged") {
		t.Fatalf("a changed copy: %+v", fake.scrubbed)
	}
	if err := os.Remove(copyPath); err != nil {
		t.Fatal(err)
	}
	e.scrubDue(ctx)
	if len(fake.scrubbed) != 3 || !strings.Contains(fake.scrubbed[2].Problem, "its pack") {
		t.Fatalf("a copy gone: %+v", fake.scrubbed)
	}
}
