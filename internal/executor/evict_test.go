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

// fakeEvictions orders evictions and records what is settled.
type fakeEvictions struct {
	*fakeCopies
	emu       sync.Mutex
	evictions []store.EvictionOrder
	settled   []store.EvictionSettled
}

func (f *fakeEvictions) EvictionsToMake(context.Context, string) ([]store.EvictionOrder, error) {
	f.emu.Lock()
	defer f.emu.Unlock()
	return append([]store.EvictionOrder(nil), f.evictions...), nil
}

func (f *fakeEvictions) SettleEviction(_ context.Context, settled store.EvictionSettled) error {
	f.emu.Lock()
	defer f.emu.Unlock()
	f.settled = append(f.settled, settled)
	return nil
}

// What a store short of room is ordered to evict goes: an object brought
// there, a temporal run output directory and an imported directory with
// their identities' file lists; a temporal directory that holds what its
// identity does not, or that an attempt still runs in, is kept with why; an
// order for a root not served is left.
func TestEvictionsDeleteWhatCanBeHadAgain(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	volatile := filepath.Join(base, "volatile")
	output, err := (localStoreFS{}).CreateKind(ctx, volatile, kindOutput, "exe_1")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(output, "shard.bin"), bytes.Repeat([]byte{7}, 4096), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := (localStoreFS{}).Identify(ctx, volatile, kindOutput, "exe_1", "obj_1"); err != nil {
		t.Fatal(err)
	}
	imported := filepath.Join(base, "datasets", "v8")
	if err := os.MkdirAll(filepath.Join(imported, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(imported, "sub", "rows.tsv"), []byte("a\tb\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := (localStoreFS{}).IdentifyDir(ctx, volatile, imported, "obj_2"); err != nil {
		t.Fatal(err)
	}
	brought := filepath.Join(volatile, inputsDirName, "obj_3")
	if err := os.MkdirAll(filepath.Join(brought, "d"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(brought, "d", "x"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	fake := &fakeEvictions{fakeCopies: newFakeCopies()}
	var logs bytes.Buffer
	e := storesExecutor(t, fake, &logs)
	e.stores = []servedStore{{decl: machine.Store{ID: "c-scratch", Class: machine.StoreVolatile, Root: volatile}, fs: localStoreFS{}, usable: true}}
	order := func(objectID, kind, objectKind, sourceID, path string) store.EvictionOrder {
		return store.EvictionOrder{ObjectID: objectID, Kind: kind, ObjectKind: objectKind, SourceID: sourceID, StoreID: "c-scratch", Root: volatile, Path: path}
	}
	settled := func() string {
		fake.emu.Lock()
		defer fake.emu.Unlock()
		var got []string
		for _, s := range fake.settled {
			got = append(got, s.ObjectID+":"+map[bool]string{true: "evicted", false: "kept"}[s.Evicted])
		}
		fake.settled = nil
		return strings.Join(got, ",")
	}

	// changed since identified, and in use: kept
	if err := os.WriteFile(filepath.Join(imported, "added.txt"), []byte("new"), 0o644); err != nil {
		t.Fatal(err)
	}
	e.outputRuns = map[string]string{"att_9": "exe_1"}
	fake.evictions = []store.EvictionOrder{
		order("obj_1", store.EvictTemporal, store.ObjectOutput, "exe_1", ""),
		order("obj_2", store.EvictTemporal, store.ObjectImport, "obj_2", imported),
	}
	e.evictDue(ctx)
	if got := settled(); got != "obj_1:kept,obj_2:kept" {
		t.Fatalf("settled %s", got)
	}
	if !exists(filepath.Join(output, "shard.bin")) || !exists(filepath.Join(imported, "added.txt")) || !exists(filepath.Join(volatile, packsDirName, "obj_2"+packSuffix+manifestSuffix)) {
		t.Fatal("a kept directory, or its identity, went")
	}
	if !strings.Contains(logs.String(), "kept instead of evicted") {
		t.Fatalf("not warned: %s", logs.String())
	}

	// as identified, not in use: evicted
	if err := os.Remove(filepath.Join(imported, "added.txt")); err != nil {
		t.Fatal(err)
	}
	e.outputRuns = nil
	fake.evictions = append(fake.evictions,
		order("obj_3", store.EvictPlacement, "", "", ""),
		store.EvictionOrder{ObjectID: "obj_4", Kind: store.EvictPlacement, StoreID: "c-scratch", Root: volatile + "-moved"},
	)
	e.evictDue(ctx)
	if got := settled(); got != "obj_1:evicted,obj_2:evicted,obj_3:evicted" {
		t.Fatalf("settled %s", got)
	}
	for _, p := range []string{output, imported, brought, filepath.Join(volatile, packsDirName, "obj_1"+packSuffix+manifestSuffix), filepath.Join(volatile, packsDirName, "obj_2"+packSuffix+manifestSuffix)} {
		if exists(p) {
			t.Fatalf("%s left", p)
		}
	}
	if !exists(filepath.Join(base, "datasets")) {
		t.Fatal("the imported directory's parent went")
	}
	// gone already: evicted all the same
	e.evictDue(ctx)
	if got := settled(); got != "obj_1:evicted,obj_2:evicted,obj_3:evicted" {
		t.Fatalf("again: %s", got)
	}
}
