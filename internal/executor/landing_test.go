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

// outputTree makes run's output directory under root (its marker, a
// checkpoint and a log) and returns its path.
func outputTree(t *testing.T, root, run string) string {
	t.Helper()
	dir, err := (localStoreFS{}).CreateKind(context.Background(), root, kindOutput, run)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "ckpt"), 0o755); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{"ckpt/step-100.pt": strings.Repeat("w", 4096), "train.log": "step 100\n"} {
		if err := os.WriteFile(filepath.Join(dir, filepath.FromSlash(name)), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// A run's output directory brought here (a move, or a rerun's checkpoint)
// is fetched and checked as an input is, then put in place as
// <root>/outputs/<run id> with its marker: what the run's next attempt here
// finds. Nothing is left in <root>/inputs; one already there for the run is
// taken as it is, and a directory of another run in its place is refused.
func TestARunsOutputDirectoryIsBroughtInPlace(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	from, to := filepath.Join(base, "from"), filepath.Join(base, "to")
	for _, d := range []string{from, to} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	outputTree(t, from, "exe_run")
	res, err := (localStoreFS{}).Pack(ctx, from, kindOutput, "exe_run", "obj_move")
	if err != nil {
		t.Fatal(err)
	}
	fake := &fakeFetches{}
	var logs bytes.Buffer
	e := storesExecutor(t, fake, &logs)
	e.storeWork().stores = []servedStore{
		{decl: machine.Store{ID: "from", Class: machine.StoreVolatile, Root: from}, fs: localStoreFS{}, usable: true},
		{decl: machine.Store{ID: "to", Class: machine.StoreVolatile, Root: to}, fs: localStoreFS{}, usable: true},
	}
	order := store.FetchOrder{ObjectID: "obj_move", StoreID: "to", Root: to, PackBytes: res.packBytes, ContentBytes: res.contentBytes,
		PackSHA256: res.packSHA256, ManifestSHA256: res.manifestHash, Source: store.CopySource{Local: true, StoreID: "from", Root: from}, RunID: "exe_run"}
	fake.orders = []store.FetchOrder{order}
	e.storeWork().fetchDue(ctx)
	landed := filepath.Join(to, outputDirName, "exe_run")
	if len(fake.made) != 1 || fake.made[0].RunID != "exe_run" || fake.made[0].Path != landed || fake.made[0].Bytes != res.contentBytes {
		t.Fatalf("made %+v\n%s", fake.made, logs.String())
	}
	if body, err := os.ReadFile(filepath.Join(landed, "ckpt", "step-100.pt")); err != nil || len(body) != 4096 {
		t.Fatalf("checkpoint %d bytes, %v", len(body), err)
	}
	r, err := os.OpenRoot(landed)
	if err != nil {
		t.Fatal(err)
	}
	named, err := readMarkerOf(r, kindOutput.marker)
	r.Close()
	if err != nil || named != "exe_run" {
		t.Fatalf("marker %q %v", named, err)
	}
	// the run's next attempt here finds it
	if dir, err := (localStoreFS{}).CreateKind(ctx, to, kindOutput, "exe_run"); err != nil || dir != landed {
		t.Fatalf("found %q %v", dir, err)
	}
	if entries, err := os.ReadDir(filepath.Join(to, inputsDirName)); err != nil || len(entries) != 0 {
		t.Fatalf("left in inputs: %v %v", entries, err)
	}
	// asked again (not recorded, say): taken as it is
	fake.made = nil
	e.storeWork().fetchDue(ctx)
	if len(fake.made) != 1 || fake.made[0].Path != landed {
		t.Fatalf("again: %+v\n%s", fake.made, logs.String())
	}
	// another run's directory in its place is refused
	other := order
	other.RunID = "exe_other"
	if _, err := (localStoreFS{}).CreateKind(ctx, to, kindOutput, "exe_mine"); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(to, outputDirName, "exe_mine"), filepath.Join(to, outputDirName, "exe_other")); err != nil {
		t.Fatal(err)
	}
	if _, err := e.storeWork().landOne(ctx, e.storeWork().stores[1], other, map[string]servedStore{"from": e.storeWork().stores[0]}); err == nil || !strings.Contains(err.Error(), "not the run's") {
		t.Fatalf("over another run's directory: %v", err)
	}
}

// fakeDrops records the packs removed.
type fakeDrops struct {
	Coordinator
	mu   sync.Mutex
	made []store.PackMade
}

func (f *fakeDrops) RecordPack(_ context.Context, made store.PackMade) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.made = append(f.made, made)
	return nil
}

// Once a moved directory has landed where it went, the pack made to move it
// and the directory it left are removed (DropSource); without DropSource
// (the move given up) the directory stays.
func TestAMovedDirectoryIsRemovedWithItsPack(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	dir := outputTree(t, root, "exe_run")
	if _, err := (localStoreFS{}).Pack(ctx, root, kindOutput, "exe_run", "obj_move"); err != nil {
		t.Fatal(err)
	}
	fake := &fakeDrops{}
	var logs bytes.Buffer
	e := storesExecutor(t, fake, &logs)
	s := servedStore{decl: machine.Store{ID: "from", Class: machine.StoreVolatile, Root: root}, fs: localStoreFS{}, usable: true}
	pack := filepath.Join(root, packsDirName, "obj_move"+packSuffix)

	e.storeWork().dropTransferPack(ctx, store.PackOrder{ObjectID: "obj_move", Kind: store.ObjectOutputMove, SourceID: "exe_run", StoreID: "from", Root: root, Drop: true}, s, true)
	if _, err := os.Stat(pack); !os.IsNotExist(err) {
		t.Fatalf("pack left: %v", err)
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("the directory went without DropSource: %v", err)
	}
	if len(fake.made) != 1 || !fake.made[0].Dropped || fake.made[0].SourceRemoved {
		t.Fatalf("made %+v", fake.made)
	}
	e.storeWork().dropTransferPack(ctx, store.PackOrder{ObjectID: "obj_move", Kind: store.ObjectOutputMove, SourceID: "exe_run", StoreID: "from", Root: root, Drop: true, DropSource: true}, s, true)
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("directory left: %v\n%s", err, logs.String())
	}
	if len(fake.made) != 2 || !fake.made[1].SourceRemoved {
		t.Fatalf("made %+v", fake.made)
	}
}
