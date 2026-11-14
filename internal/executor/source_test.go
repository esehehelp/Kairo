package executor

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"kairo/internal/machine"
	"kairo/internal/store"
)

// fakeSources serves the snapshots it holds as the daemon does.
type fakeSources struct {
	*fakeStores
	files   map[string]string // "<id>/<pack|list>" -> path
	fetched int
}

func (f *fakeSources) FetchSource(_ context.Context, id, file string, w io.Writer) error {
	path, ok := f.files[id+"/"+file]
	if !ok {
		return store.ErrNotFound
	}
	f.fetched++
	body, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	_, err = w.Write(body)
	return err
}

// A snapshot holds the regular files of the declared paths but what is
// excluded; it is brought to the node once, checked, and unpacked under
// <root>/sources/<id>, where an attempt then runs; a pack that is not what
// the launch says is not unpacked.
func TestACodeSnapshotIsPackedAndBroughtWhereItRuns(t *testing.T) {
	ctx := context.Background()
	repo := t.TempDir()
	for name, body := range map[string]string{
		"src/pkg/a.py":              "print('a')\n",
		"src/pkg/__pycache__/a.pyc": "compiled",
		"configs/run.toml":          "lr = 1e-4\n",
		"pyproject.toml":            "[project]\n",
		"notes.txt":                 "not declared",
	} {
		p := filepath.Join(repo, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	sp, err := PackSource(ctx, repo, []string{"src", "configs", "pyproject.toml"}, []string{"__pycache__"}, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if sp.Files != 3 || !strings.Contains(sp.List, "src/pkg/a.py\t") || strings.Contains(sp.List, "__pycache__") || strings.Contains(sp.List, "notes.txt") {
		t.Fatalf("snapshot %+v\n%s", sp, sp.List)
	}
	again, err := PackSource(ctx, repo, []string{"pyproject.toml", "configs", "src"}, []string{"__pycache__"}, t.TempDir())
	if err != nil || again.ID != sp.ID {
		t.Fatalf("the same files make another snapshot: %s %s %v", again.ID, sp.ID, err)
	}

	root := filepath.Join(t.TempDir(), "volatile")
	fake := &fakeSources{fakeStores: &fakeStores{}, files: map[string]string{sp.ID + "/pack": sp.PackPath, sp.ID + "/list": sp.ListPath}}
	var logs bytes.Buffer
	e := storesExecutor(t, fake, &logs)
	e.stores = []servedStore{{decl: machine.Store{ID: "c-scratch", Class: machine.StoreVolatile, Root: root}, fs: localStoreFS{}, usable: true}}
	src := &store.LaunchSource{ID: sp.ID, PackBytes: sp.PackBytes, PackSHA256: sp.PackSHA256, ListSHA256: sp.ListSHA256}
	dir, err := e.ensureSource(ctx, src)
	if err != nil {
		t.Fatal(err)
	}
	if dir != filepath.Join(root, sourcesDirName, sp.ID) {
		t.Fatalf("unpacked at %s", dir)
	}
	if body, err := os.ReadFile(filepath.Join(dir, "configs", "run.toml")); err != nil || string(body) != "lr = 1e-4\n" {
		t.Fatalf("unpacked file: %q %v", body, err)
	}
	if exists(filepath.Join(dir, "notes.txt")) || exists(filepath.Join(dir, "src", "pkg", "__pycache__")) {
		t.Fatal("unpacked what was not declared")
	}
	if _, err = e.ensureSource(ctx, src); err != nil || fake.fetched != 2 {
		t.Fatalf("brought again: %d fetches, %v", fake.fetched, err)
	}
	// a launch naming another pack: not unpacked
	bad := &store.LaunchSource{ID: "src_" + strings.Repeat("1", 32), PackBytes: sp.PackBytes, PackSHA256: strings.Repeat("ab", 32), ListSHA256: sp.ListSHA256}
	fake.files[bad.ID+"/pack"], fake.files[bad.ID+"/list"] = sp.PackPath, sp.ListPath
	if _, err = e.ensureSource(ctx, bad); err == nil || exists(filepath.Join(root, sourcesDirName, bad.ID)) {
		t.Fatalf("a pack that is not the snapshot's was unpacked: %v", err)
	}
	if entries, _ := os.ReadDir(filepath.Join(root, sourcesDirName)); len(entries) != 1 {
		t.Fatalf("left behind: %v", fmt.Sprint(entries))
	}
}
