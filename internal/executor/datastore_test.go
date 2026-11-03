package executor

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"

	"kairo/internal/machine"
	"kairo/internal/store"
)

// testFilesystem is the filesystem identity dir is on, as a filesystem
// provider names it: its drive on Windows, a path of its filesystem
// elsewhere.
func testFilesystem(dir string) string {
	if runtime.GOOS == "windows" {
		return filepath.VolumeName(dir)
	}
	return dir
}

// otherFilesystem is a filesystem identity dir is not on.
func otherFilesystem() string {
	if runtime.GOOS == "windows" {
		return "Q:"
	}
	return "/proc"
}

// fakeStores serves the node's stores and records what is reported of them.
type fakeStores struct {
	Coordinator
	mu       sync.Mutex
	stores   []machine.Store
	err      error
	observed []store.StoreObservation
}

func (f *fakeStores) NodeStores(context.Context, string) ([]machine.Store, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]machine.Store(nil), f.stores...), f.err
}

func (f *fakeStores) RecordStoreObservation(_ context.Context, obs store.StoreObservation) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.observed = append(f.observed, obs)
	return nil
}

func storesExecutor(t *testing.T, st Coordinator, logs *bytes.Buffer) *Local {
	t.Helper()
	e := NewLocal("exec", st, t.TempDir(), slog.New(slog.NewTextHandler(logs, nil)))
	e.NodeID = "node"
	return e
}

// A daemon older than stores (404) leaves the executor without stores,
// warned once, and it goes on.
func TestStoresUnavailableAreWarnedOnce(t *testing.T) {
	var logs bytes.Buffer
	e := storesExecutor(t, &fakeStores{err: errors.New("agent API 404: 404 page not found")}, &logs)
	for i := 0; i < 3; i++ {
		e.storesAt = e.storesAt.AddDate(-1, 0, 0) // due again
		e.refreshStores(context.Background())
	}
	if n := strings.Count(logs.String(), "cannot read this node's stores"); n != 1 {
		t.Fatalf("warned %d times:\n%s", n, logs.String())
	}
	if len(e.servedStores()) != 0 {
		t.Fatalf("stores without a daemon that has them: %+v", e.servedStores())
	}
}

// An executor serves its node's stores in its namespace: one naming it, and
// (unless it is WSL) one naming no executor. It makes a missing root and
// checks the root's filesystem; a store on another one is unusable.
func TestStoresServedMadeAndChecked(t *testing.T) {
	dir := t.TempDir()
	fake := &fakeStores{stores: []machine.Store{
		{ID: "local", Class: machine.StoreVolatile, Node: "node", Root: filepath.Join(dir, "local-root"), Filesystem: testFilesystem(dir)},
		{ID: "wrong", Class: machine.StoreDurable, Node: "node", Root: filepath.Join(dir, "wrong-root"), Filesystem: otherFilesystem()},
		{ID: "wsl-only", Class: machine.StoreVolatile, Node: "node", Executor: "ubuntu-wsl", Root: "/var/tmp/kairo", Filesystem: "D:"},
	}}
	var logs bytes.Buffer
	e := storesExecutor(t, fake, &logs)
	e.refreshStores(context.Background())
	served := e.servedStores()
	if len(served) != 2 || served[0].decl.ID != "local" || !served[0].usable || served[1].decl.ID != "wrong" || served[1].usable || !strings.Contains(served[1].problem, "is not on filesystem") {
		t.Fatalf("served: %+v", served)
	}
	if fi, err := os.Stat(filepath.Join(dir, "local-root")); err != nil || !fi.IsDir() {
		t.Fatalf("root not made: %v", err)
	}
	obs := e.observeStore(context.Background(), served[0])
	if !obs.Usable || obs.TotalBytes == nil || obs.FreeBytes == nil || *obs.TotalBytes <= 0 || obs.Problem != "" || len(obs.Scratch) != 0 {
		t.Fatalf("observation: %+v", obs)
	}
	if obs = e.observeStore(context.Background(), served[1]); obs.Usable || obs.TotalBytes != nil || obs.Problem == "" {
		t.Fatalf("unusable observation: %+v", obs)
	}

	// A WSL executor serves only the store naming it.
	wsl := storesExecutor(t, fake, &logs)
	wsl.ID, wsl.Kind = "ubuntu-wsl", "wsl2"
	wsl.storeFSOverride = localStoreFS{}
	if got := []bool{wsl.serves(fake.stores[0]), wsl.serves(fake.stores[2])}; got[0] || !got[1] {
		t.Fatalf("WSL executor serves %v", got)
	}
}

// Scan counts only the directories Kairo made under <root>/scratch (their
// marker names them), never through a link.
func TestScanCountsOnlyKairosScratch(t *testing.T) {
	root := t.TempDir()
	mk := func(name, marker string, size int) string {
		d := filepath.Join(root, scratchDirName, name)
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
		if marker != "" {
			if err := os.WriteFile(filepath.Join(d, scratchMarkerName), []byte(marker+"\n"), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		if err := os.WriteFile(filepath.Join(d, "data"), make([]byte, size), 0o644); err != nil {
			t.Fatal(err)
		}
		return d
	}
	mk("att_1", "att_1", 10)
	mk("att_2", "another", 20)
	mk("att_3", "", 30)
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, scratchMarkerName), []byte("att_4\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(outside, "big"), make([]byte, 1000), 0o644); err != nil {
		t.Fatal(err)
	}
	_ = os.Symlink(outside, filepath.Join(root, scratchDirName, "att_4")) // (may need a privilege on Windows)
	sizes, err := localStoreFS{}.Scan(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	if len(sizes) != 1 || sizes["att_1"] != 10+int64(len("att_1\n")) {
		t.Fatalf("sizes: %v", sizes)
	}
	if empty, err := (localStoreFS{}).Scan(context.Background(), t.TempDir()); err != nil || len(empty) != 0 {
		t.Fatalf("a root without scratch: %v %v", empty, err)
	}
}
