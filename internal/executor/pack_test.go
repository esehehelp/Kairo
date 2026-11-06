package executor

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/klauspost/compress/zstd"

	"kairo/internal/machine"
	"kairo/internal/store"
)

// unpack reads a pack: its entries (a directory ending in "/") with the
// contents of its regular files.
func unpack(t *testing.T, packPath string) map[string]string {
	t.Helper()
	f, err := os.Open(packPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	zr, err := zstd.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer zr.Close()
	tr := tar.NewReader(zr)
	out := map[string]string{}
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(tr)
		if err != nil {
			t.Fatal(err)
		}
		out[h.Name] = string(body)
	}
}

func sha256File(t *testing.T, p string) string {
	t.Helper()
	body, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

// A run's output directory is packed whole into <root>/packs/<object
// id>.tar.zst with its file list: regular files and directories, the
// marker left out, links skipped and never followed; packing again makes the
// same pack; a directory whose marker does not name it is refused.
func TestPackPacksADirectoryWithItsFileList(t *testing.T) {
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "store")
	fs := localStoreFS{}
	dir, err := fs.CreateKind(ctx, root, kindOutput, "exe_1")
	if err != nil {
		t.Fatal(err)
	}
	files := map[string]string{"metrics.json": `{"loss": 0.5}`, "ckpt/step-5.pt": strings.Repeat("w", 100000), "ckpt/empty": ""}
	for name, body := range files {
		p := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	outside := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(outside, []byte("outside"), 0o644); err != nil {
		t.Fatal(err)
	}
	linked := false
	if err := os.Symlink(outside, filepath.Join(dir, "link")); err == nil {
		linked = true
	}
	res, err := fs.Pack(ctx, root, kindOutput, "exe_1", "obj_1")
	if err != nil {
		t.Fatal(err)
	}
	wantPack := filepath.Join(root, packsDirName, "obj_1.tar.zst")
	if res.packPath != wantPack || res.manifestPath != wantPack+manifestSuffix {
		t.Fatalf("paths %s %s", res.packPath, res.manifestPath)
	}
	if res.files != 3 || res.contentBytes != int64(len(files["metrics.json"])+100000) {
		t.Fatalf("files %d, bytes %d", res.files, res.contentBytes)
	}
	if want := int64(0); linked {
		want = 1
		if res.skipped != want {
			t.Fatalf("skipped %d, want the link", res.skipped)
		}
	} else if res.skipped != want {
		t.Fatalf("skipped %d", res.skipped)
	}
	if info, err := os.Stat(res.packPath); err != nil || info.Size() != res.packBytes {
		t.Fatalf("pack size: %v %v, recorded %d", info, err, res.packBytes)
	}
	if got := sha256File(t, res.packPath); got != res.packSHA256 {
		t.Fatalf("pack sha256 %s, recorded %s", got, res.packSHA256)
	}
	if got := sha256File(t, res.manifestPath); got != res.manifestHash {
		t.Fatalf("file list sha256 %s, recorded %s", got, res.manifestHash)
	}
	entries := unpack(t, res.packPath)
	var names []string
	for name := range entries {
		names = append(names, name)
	}
	sort.Strings(names)
	if strings.Join(names, ",") != "ckpt/,ckpt/empty,ckpt/step-5.pt,metrics.json" {
		t.Fatalf("entries %v (no marker, no link)", names)
	}
	for name, body := range files {
		if entries[name] != body {
			t.Fatalf("%s: %d bytes packed", name, len(entries[name]))
		}
	}
	list, err := os.ReadFile(res.manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(string(list), "\n"), "\n")
	if lines[0] != manifestHeader || len(lines) != 4 || !strings.HasPrefix(lines[3], "metrics.json\t13\t") ||
		!strings.HasPrefix(lines[2], "ckpt/step-5.pt\t100000\t"+sha256Hex(strings.Repeat("w", 100000))+"\t") {
		t.Fatalf("file list:\n%s", list)
	}
	if leftovers, _ := filepath.Glob(filepath.Join(root, packsDirName, "*"+partialSuffix)); len(leftovers) != 0 {
		t.Fatalf("partial files left: %v", leftovers)
	}
	if body, _ := os.ReadFile(outside); string(body) != "outside" {
		t.Fatal("the file a link points at was touched")
	}

	// packed again, the same files go in (the pack's bytes may differ: a
	// directory's mtime is packed too, and Windows may still be settling it)
	again, err := fs.Pack(ctx, root, kindOutput, "exe_1", "obj_1")
	if err != nil || again.manifestHash != res.manifestHash || again.files != res.files || again.packSHA256 != sha256File(t, again.packPath) {
		t.Fatalf("packed again: %+v %v", again, err)
	}
	// a stale partial from a round cut short is replaced
	if err := os.WriteFile(filepath.Join(root, packsDirName, "obj_1.tar.zst"+partialSuffix), []byte("junk"), 0o644); err != nil {
		t.Fatal(err)
	}
	if again, err = fs.Pack(ctx, root, kindOutput, "exe_1", "obj_1"); err != nil || again.manifestHash != res.manifestHash || again.packSHA256 != sha256File(t, again.packPath) {
		t.Fatalf("packed over a partial: %v", err)
	}

	// what is not Kairo's directory of that id is refused
	if err := os.WriteFile(filepath.Join(dir, outputMarkerName), []byte("exe_2\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := fs.Pack(ctx, root, kindOutput, "exe_1", "obj_2"); !errors.Is(err, errScratchMarker) {
		t.Fatalf("a directory whose marker names another: %v", err)
	}
	if _, err := fs.Pack(ctx, root, kindScratch, "exe_1", "obj_3"); err == nil {
		t.Fatal("an output directory packed as a scratch")
	}
	if _, err := fs.Pack(ctx, root, kindOutput, "../x", "obj_4"); !errors.Is(err, errOutsideRoot) {
		t.Fatalf("a path as id: %v", err)
	}
}

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func TestFileListEscapesWhatWouldBreakALine(t *testing.T) {
	if got := escapeListPath("a\tb\nc\\d\re"); got != `a\tb\nc\\d\re` {
		t.Fatalf("escaped %q", got)
	}
	if got := escapeListPath("plain/name.txt"); got != "plain/name.txt" {
		t.Fatalf("escaped %q", got)
	}
}

// fakePacks orders packs and records what is made.
type fakePacks struct {
	*fakeOutputs
	pmu    sync.Mutex
	orders []store.PackOrder
	made   []store.PackMade
}

func (f *fakePacks) PacksToMake(context.Context, string) ([]store.PackOrder, error) {
	f.pmu.Lock()
	defer f.pmu.Unlock()
	return append([]store.PackOrder(nil), f.orders...), nil
}

func (f *fakePacks) RecordPack(_ context.Context, made store.PackMade) error {
	f.pmu.Lock()
	defer f.pmu.Unlock()
	f.made = append(f.made, made)
	return nil
}

// A round packs what is ordered on a usable store this executor serves at
// the ordered root, and records each pack; what is not served here as
// ordered, or lies inside a WSL distro, waits.
func TestPackRoundPacksWhatIsOrdered(t *testing.T) {
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "volatile")
	fake := &fakePacks{fakeOutputs: &fakeOutputs{fakeScratch: &fakeScratch{fakeStores: &fakeStores{}}}}
	var logs bytes.Buffer
	e := storesExecutor(t, fake, &logs)
	e.stores = []servedStore{
		{decl: machine.Store{ID: "c-scratch", Class: machine.StoreVolatile, Root: root}, fs: localStoreFS{}, usable: true},
		{decl: machine.Store{ID: "w-scratch", Class: machine.StoreVolatile, Root: "/var/tmp/kairo"}, fs: wslStoreFS{}, usable: true},
	}
	if _, err := (localStoreFS{}).CreateKind(ctx, root, kindOutput, "exe_1"); err != nil {
		t.Fatal(err)
	}
	scratch, err := (localStoreFS{}).Create(ctx, root, "att_1")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(scratch, "core"), []byte("dump"), 0o644); err != nil {
		t.Fatal(err)
	}
	fake.orders = []store.PackOrder{
		{ObjectID: "obj_out", Kind: store.ObjectOutput, SourceID: "exe_1", StoreID: "c-scratch", Root: root},
		{ObjectID: "obj_scr", Kind: store.ObjectScratch, SourceID: "att_1", StoreID: "c-scratch", Root: root},
		{ObjectID: "obj_moved", Kind: store.ObjectOutput, SourceID: "exe_1", StoreID: "c-scratch", Root: root + "-elsewhere"},
		{ObjectID: "obj_wsl", Kind: store.ObjectOutput, SourceID: "exe_2", StoreID: "w-scratch", Root: "/var/tmp/kairo"},
	}
	e.packDue(ctx)
	if len(fake.made) != 2 || fake.made[0].ObjectID != "obj_out" || fake.made[1].ObjectID != "obj_scr" {
		t.Fatalf("made: %+v", fake.made)
	}
	scr := fake.made[1]
	if scr.ExecutorID != "exec" || scr.Files != 1 || scr.ContentBytes != 4 || scr.PackPath != filepath.Join(root, packsDirName, "obj_scr.tar.zst") ||
		scr.PackSHA256 != sha256File(t, scr.PackPath) || scr.ManifestPath != scr.PackPath+manifestSuffix {
		t.Fatalf("scratch pack: %+v", scr)
	}
	if entries := unpack(t, scr.PackPath); len(entries) != 1 || entries["core"] != "dump" {
		t.Fatalf("scratch packed: %v", entries)
	}
	if !strings.Contains(logs.String(), "directories of a store inside a WSL distro are not packed yet") {
		t.Fatalf("WSL object not warned:\n%s", logs.String())
	}
	// warned once per object
	e.packDue(ctx)
	if n := strings.Count(logs.String(), "not packed yet"); n != 1 {
		t.Fatalf("warned %d times", n)
	}
}
