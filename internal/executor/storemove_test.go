package executor

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"kairo/internal/machine"
)

// A store being moved is not used while the daemon says something still
// uses it where it is; then its executor renames it to its declared root,
// with the move's marker in it, and says so in its observations until the
// move is recorded, after which the marker goes. A root already there that
// Kairo did not move there is never taken for the store.
func TestAStoreBeingMovedIsRenamedOnceNothingUsesIt(t *testing.T) {
	dir := t.TempDir()
	from, to := filepath.Join(dir, "kairo-store"), filepath.Join(dir, "_kairo-volatile")
	if err := os.MkdirAll(filepath.Join(from, "scratch", "att_1"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(from, "scratch", "att_1", "kept.txt"), []byte("k"), 0o644); err != nil {
		t.Fatal(err)
	}
	decl := machine.Store{ID: "d", Class: machine.StoreVolatile, Node: "node", Root: to, Filesystem: testFilesystem(dir), MovedFrom: from}
	fake := &fakeStores{}
	var logs bytes.Buffer
	e := storesExecutor(t, fake, &logs)
	refresh := func(d machine.Store) servedStore {
		t.Helper()
		fake.mu.Lock()
		fake.stores = []machine.Store{d}
		fake.mu.Unlock()
		e.storeWork().storesAt = time.Time{}
		e.storeWork().refreshStores(context.Background())
		served := e.storeWork().servedStores()
		if len(served) != 1 {
			t.Fatalf("served %+v", served)
		}
		return served[0]
	}

	// Waiting: not used, not moved, and the new root is not made.
	waiting := decl
	waiting.MoveWait = "1 execution(s) using it hold a lease"
	s := refresh(waiting)
	if s.usable || !strings.Contains(s.problem, "once nothing uses it there") || s.movedFrom != "" {
		t.Fatalf("waiting: %+v", s)
	}
	if _, err := os.Stat(to); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the new root was made while waiting: %v", err)
	}
	if obs := e.storeWork().observeStore(context.Background(), s); obs.Usable || obs.MovedFrom != "" {
		t.Fatalf("waiting observation: %+v", obs)
	}

	// Free: renamed, usable there, and the observation says it was moved.
	s = refresh(decl)
	if !s.usable || s.movedFrom != from {
		t.Fatalf("moved: %+v\n%s", s, logs.String())
	}
	if _, err := os.Stat(from); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the old root is still there: %v", err)
	}
	if body, err := os.ReadFile(filepath.Join(to, "scratch", "att_1", "kept.txt")); err != nil || string(body) != "k" {
		t.Fatalf("what was in the store: %q %v", body, err)
	}
	if obs := e.storeWork().observeStore(context.Background(), s); !obs.Usable || obs.MovedFrom != from || obs.Root != to {
		t.Fatalf("moved observation: %+v", obs)
	}
	// Asked again before the daemon has recorded it: there, moved.
	if s = refresh(decl); !s.usable || s.movedFrom != from {
		t.Fatalf("moved, asked again: %+v", s)
	}

	// Recorded: the declaration no longer says moved_from; the marker goes.
	recorded := decl
	recorded.MovedFrom = ""
	if s = refresh(recorded); !s.usable || s.movedFrom != "" {
		t.Fatalf("recorded: %+v", s)
	}
	if _, err := os.Stat(filepath.Join(to, storeMoveMarker)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the marker is kept once the move is recorded: %v", err)
	}

	// A root there already, not moved there by Kairo: not the store.
	other := filepath.Join(dir, "other")
	if err := os.Mkdir(other, 0o755); err != nil {
		t.Fatal(err)
	}
	stray := machine.Store{ID: "d", Class: machine.StoreVolatile, Node: "node", Root: other, Filesystem: testFilesystem(dir), MovedFrom: to}
	if s = refresh(stray); s.usable || !strings.Contains(s.problem, "was not moved there by Kairo") {
		t.Fatalf("a root there already: %+v", s)
	}
	if _, err := os.Stat(to); err != nil {
		t.Fatalf("the store was moved onto a root there already: %v", err)
	}

	// Neither root there: not usable, nothing made.
	gone := machine.Store{ID: "d", Class: machine.StoreVolatile, Node: "node", Root: filepath.Join(dir, "new"), Filesystem: testFilesystem(dir), MovedFrom: filepath.Join(dir, "absent")}
	if s = refresh(gone); s.usable || s.problem == "" {
		t.Fatalf("neither root there: %+v", s)
	}
	if _, err := os.Stat(filepath.Join(dir, "new")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a root was made for a store with nothing to move: %v", err)
	}
}
