package executor

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
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
	t.Cleanup(func() { makeTreeWritable(root) })
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
	if entries, _ := os.ReadDir(filepath.Join(root, sourcesDirName)); len(entries) != 2 {
		t.Fatalf("left behind: %v", fmt.Sprint(entries))
	}
}

// makeTreeWritable gives back the write bits an unpacked snapshot lost, so
// that the test's directory can be removed.
func makeTreeWritable(root string) {
	_ = filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err == nil && d.Type()&os.ModeSymlink == 0 {
			if info, err := d.Info(); err == nil {
				_ = os.Chmod(p, info.Mode().Perm()|0o700)
			}
		}
		return nil
	})
}

// snapshotFixture packs files (path -> body; a body starting with "#!" is a
// script) and returns the launch of it and an executor serving a volatile
// store at root that fetches it from fake.
func snapshotFixture(t *testing.T, files map[string]string) (*store.LaunchSource, *fakeSources, string, func(*bytes.Buffer) *Local) {
	t.Helper()
	repo := t.TempDir()
	for name, body := range files {
		p := filepath.Join(repo, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	paths := make([]string, 0, len(files))
	for name := range files {
		paths = append(paths, name)
	}
	sp, err := PackSource(context.Background(), repo, paths, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(t.TempDir(), "volatile")
	t.Cleanup(func() { makeTreeWritable(root) })
	fake := &fakeSources{fakeStores: &fakeStores{}, files: map[string]string{sp.ID + "/pack": sp.PackPath, sp.ID + "/list": sp.ListPath}}
	executor := func(logs *bytes.Buffer) *Local {
		e := storesExecutor(t, fake, logs)
		e.stores = []servedStore{{decl: machine.Store{ID: "c-scratch", Class: machine.StoreVolatile, Root: root}, fs: localStoreFS{}, usable: true}}
		return e
	}
	return &store.LaunchSource{ID: sp.ID, PackBytes: sp.PackBytes, PackSHA256: sp.PackSHA256, ListSHA256: sp.ListSHA256}, fake, root, executor
}

// A snapshot is unpacked read-only (its directories too, but on Windows),
// a script executable: "./scripts/x.sh" runs, packed on Windows or not.
func TestASnapshotIsUnpackedReadOnlyWithItsScriptsExecutable(t *testing.T) {
	src, _, _, executor := snapshotFixture(t, map[string]string{
		"scripts/x.sh": "#!/bin/sh\necho x\n",
		"src/a.py":     "print('a')\n",
	})
	var logs bytes.Buffer
	dir, err := executor(&logs).ensureSource(context.Background(), src)
	if err != nil {
		t.Fatal(err)
	}
	script, err := os.Stat(filepath.Join(dir, "scripts", "x.sh"))
	if err != nil {
		t.Fatal(err)
	}
	module, err := os.Stat(filepath.Join(dir, "src", "a.py"))
	if err != nil {
		t.Fatal(err)
	}
	if script.Mode().Perm()&0o222 != 0 || module.Mode().Perm()&0o222 != 0 {
		t.Fatalf("writable: %v %v", script.Mode(), module.Mode())
	}
	if runtime.GOOS == "windows" {
		return
	}
	if script.Mode().Perm() != 0o555 || module.Mode().Perm() != 0o444 {
		t.Fatalf("modes %v %v, want 0555 and 0444", script.Mode().Perm(), module.Mode().Perm())
	}
	if d, err := os.Stat(filepath.Join(dir, "src")); err != nil || d.Mode().Perm() != 0o555 {
		t.Fatalf("directory %v %v, want 0555", d.Mode(), err)
	}
	if out, err := exec.Command(filepath.Join(dir, "scripts", "x.sh")).Output(); err != nil || string(out) != "x\n" {
		t.Fatalf("script: %q %v", out, err)
	}
}

// What an attempt changed in the unpacked snapshot (a file added, one
// written) is found at the next launch: the tree is set aside, an attempt
// still running in it keeps it, and the snapshot is unpacked again.
func TestASnapshotChangedInPlaceIsSetAsideAndUnpackedAgain(t *testing.T) {
	src, fake, root, executor := snapshotFixture(t, map[string]string{
		"src/pkg/a.py":   "print('a')\n",
		"configs/r.toml": "x = 1\n",
	})
	var logs bytes.Buffer
	e := executor(&logs)
	ctx := context.Background()
	for _, change := range []struct {
		name  string
		apply func(dir string) error
	}{
		{"added", func(dir string) error {
			return os.WriteFile(filepath.Join(dir, "src", "pkg", "b.py"), []byte("x"), 0o644)
		}},
		{"changed", func(dir string) error {
			return os.WriteFile(filepath.Join(dir, "configs", "r.toml"), []byte("x = 22\n"), 0o644)
		}},
		{"removed", func(dir string) error {
			return os.Remove(filepath.Join(dir, "src", "pkg", "a.py"))
		}},
	} {
		dir, err := e.ensureSource(ctx, src)
		if err != nil {
			t.Fatal(err)
		}
		fetched := fake.fetched
		if _, err = e.ensureSource(ctx, src); err != nil || fake.fetched != fetched {
			t.Fatalf("%s: an untouched snapshot brought again (%d fetches) %v", change.name, fake.fetched-fetched, err)
		}
		makeTreeWritable(dir)
		if err := change.apply(dir); err != nil {
			t.Fatal(err)
		}
		again, err := e.ensureSource(ctx, src)
		if err != nil || again != dir || fake.fetched != fetched+2 {
			t.Fatalf("%s: %s %v, %d fetches", change.name, again, err, fake.fetched-fetched)
		}
		if body, err := os.ReadFile(filepath.Join(dir, "configs", "r.toml")); err != nil || string(body) != "x = 1\n" ||
			exists(filepath.Join(dir, "src", "pkg", "b.py")) || !exists(filepath.Join(dir, "src", "pkg", "a.py")) {
			t.Fatalf("%s: not unpacked again: %q %v", change.name, body, err)
		}
		if !strings.Contains(logs.String(), change.name+" ") {
			t.Fatalf("%s: change not named:\n%s", change.name, logs.String())
		}
	}
	aside, _ := filepath.Glob(filepath.Join(root, sourcesDirName, src.ID+changedInfix+"*"))
	if len(aside) != 3 || !exists(filepath.Join(aside[0], "src", "pkg", "b.py")) {
		t.Fatalf("set aside: %v", aside)
	}
}

// Executors of one process unpacking the same snapshot at once: one brings
// it, the others take it; nothing is left beside it.
func TestASnapshotUnpackedAtOnceByTwoExecutorsIsBroughtOnce(t *testing.T) {
	src, fake, root, executor := snapshotFixture(t, map[string]string{"src/a.py": "print('a')\n"})
	var logs bytes.Buffer
	executors := []*Local{executor(&logs), executor(&logs), executor(&logs)}
	errs := make(chan error, len(executors))
	for _, e := range executors {
		go func() {
			_, err := e.ensureSource(context.Background(), src)
			errs <- err
		}()
	}
	for range executors {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if fake.fetched != 2 {
		t.Fatalf("%d fetches, want the pack and the list once", fake.fetched)
	}
	entries, _ := os.ReadDir(filepath.Join(root, sourcesDirName))
	if len(entries) != 2 {
		t.Fatalf("beside the snapshot: %v", entries)
	}
}
