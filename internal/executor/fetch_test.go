package executor

import (
	"bytes"
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"kairo/internal/machine"
	"kairo/internal/store"
)

// fakeFetches orders objects brought here and records what is.
type fakeFetches struct {
	Coordinator
	fmu    sync.Mutex
	orders []store.FetchOrder
	made   []store.FetchMade
}

func (f *fakeFetches) FetchesToMake(context.Context, string) ([]store.FetchOrder, error) {
	f.fmu.Lock()
	defer f.fmu.Unlock()
	return append([]store.FetchOrder(nil), f.orders...), nil
}

func (f *fakeFetches) RecordFetch(_ context.Context, made store.FetchMade) error {
	f.fmu.Lock()
	defer f.fmu.Unlock()
	f.made = append(f.made, made)
	return nil
}

// A durable copy of an object (its pack and file list under
// <root>/objects/<month>) is brought to a volatile store: unpacked under
// <root>/inputs/<object id> with each file as packed (content and mtime),
// the pack itself not left behind; a copy that does not match its order is
// refused and nothing is left.
func TestAnObjectIsBroughtFromItsDurableCopy(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	volatile, durable, data := filepath.Join(base, "volatile"), filepath.Join(base, "durable"), filepath.Join(base, "data")
	for _, d := range []string{volatile, durable} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	importedTree(t, data)
	old := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	if err := os.Chtimes(filepath.Join(data, "meta.json"), old, old); err != nil {
		t.Fatal(err)
	}
	res, err := (localStoreFS{}).PackDir(ctx, volatile, data, "obj_d")
	if err != nil {
		t.Fatal(err)
	}
	month := filepath.Join(durable, objectsDirName, "202611")
	if err := os.MkdirAll(month, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{res.packPath, res.manifestPath} {
		body, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(month, filepath.Base(p)), body, 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(p); err != nil {
			t.Fatal(err)
		}
	}
	fake := &fakeFetches{}
	var logs bytes.Buffer
	e := storesExecutor(t, fake, &logs)
	e.stores = []servedStore{
		{decl: machine.Store{ID: "c-scratch", Class: machine.StoreVolatile, Root: volatile}, fs: localStoreFS{}, usable: true},
		{decl: machine.Store{ID: "e-durable", Class: machine.StoreDurable, Root: durable}, fs: localStoreFS{}, usable: true},
	}
	order := store.FetchOrder{ObjectID: "obj_d", StoreID: "c-scratch", Root: volatile, PackBytes: res.packBytes, ContentBytes: res.contentBytes,
		PackSHA256: res.packSHA256, ManifestSHA256: res.manifestHash, Source: store.CopySource{Local: true, StoreID: "e-durable", Root: durable, Month: "202611"}}
	bad := order
	bad.ObjectID, bad.PackSHA256 = "obj_bad", strings.Repeat("0", 64)
	for _, name := range []string{"obj_bad" + packSuffix, "obj_bad" + packSuffix + manifestSuffix} {
		src := filepath.Join(month, strings.Replace(name, "obj_bad", "obj_d", 1))
		body, _ := os.ReadFile(src)
		if err := os.WriteFile(filepath.Join(month, name), body, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	fake.orders = []store.FetchOrder{order, bad}
	e.fetchDue(ctx)
	if len(fake.made) != 1 || fake.made[0].ObjectID != "obj_d" || fake.made[0].Bytes != res.contentBytes || fake.made[0].Path != filepath.Join(volatile, inputsDirName, "obj_d") {
		t.Fatalf("made %+v\n%s", fake.made, logs.String())
	}
	got := filepath.Join(volatile, inputsDirName, "obj_d")
	body, err := os.ReadFile(filepath.Join(got, "meta.json"))
	if err != nil || string(body) != `{"rows": 3}` {
		t.Fatalf("meta.json: %q %v", body, err)
	}
	if fi, err := os.Stat(filepath.Join(got, "meta.json")); err != nil || !fi.ModTime().Equal(old) {
		t.Fatalf("mtime %v %v, want %v", fi.ModTime(), err, old)
	}
	if fi, err := os.Stat(filepath.Join(got, "shards", "0.bin")); err != nil || fi.Size() != 4096 {
		t.Fatalf("shards/0.bin: %v", err)
	}
	for _, p := range []string{got + partialSuffix, filepath.Join(volatile, inputsDirName, "obj_d"+packSuffix+partialSuffix), filepath.Join(volatile, inputsDirName, "obj_bad")} {
		if exists(p) {
			t.Fatalf("%s left", p)
		}
	}
	if !strings.Contains(logs.String(), "obj_bad") {
		t.Fatalf("the bad copy was not warned of:\n%s", logs.String())
	}
	// asked again (not recorded, say): taken as it is
	fake.orders, fake.made = []store.FetchOrder{order}, nil
	e.fetches = round{}
	e.fetchDue(ctx)
	if len(fake.made) != 1 || fake.made[0].Bytes != res.contentBytes {
		t.Fatalf("again: %+v", fake.made)
	}
}

// The agent holding a durable copy hands it over to a fetch whose token the
// daemon authorizes for it (store.PackRead with its month), and another
// node's executor brings it from there (obj_1: the one the fake authorizes).
func TestAnObjectIsBroughtFromAnotherNodesDurableCopy(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	volatile, durable, data := filepath.Join(base, "volatile"), filepath.Join(base, "durable"), filepath.Join(base, "data")
	for _, d := range []string{volatile, durable} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	importedTree(t, data)
	res, err := (localStoreFS{}).PackDir(ctx, volatile, data, "obj_1")
	if err != nil {
		t.Fatal(err)
	}
	month := filepath.Join(durable, objectsDirName, "202611")
	if err := os.MkdirAll(month, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{res.packPath, res.manifestPath} {
		if err := os.Rename(p, filepath.Join(month, filepath.Base(p))); err != nil {
			t.Fatal(err)
		}
	}
	copies := newFakeCopies()
	copies.token, copies.readFor = "kairo_fetch_secret", store.PackRead{StoreID: "e-durable", Root: durable, Month: "202611"}
	var serverLogs, logs bytes.Buffer
	holder := storesExecutor(t, copies, &serverLogs)
	holder.stores = []servedStore{{decl: machine.Store{ID: "e-durable", Class: machine.StoreDurable, Root: durable}, fs: localStoreFS{}, usable: true}}
	server := httptest.NewTLSServer(&PackServer{Store: copies, NodeID: "node", Executors: []*Local{holder}, Logger: holder.Logger})
	defer server.Close()

	fake := &fakeFetches{}
	e := storesExecutor(t, fake, &logs)
	e.PackClient = server.Client()
	e.stores = []servedStore{{decl: machine.Store{ID: "c-scratch", Class: machine.StoreVolatile, Root: volatile}, fs: localStoreFS{}, usable: true}}
	fake.orders = []store.FetchOrder{{ObjectID: "obj_1", StoreID: "c-scratch", Root: volatile, PackBytes: res.packBytes, ContentBytes: res.contentBytes,
		PackSHA256: res.packSHA256, ManifestSHA256: res.manifestHash,
		Source: store.CopySource{StoreID: "e-durable", Root: durable, Month: "202611", Addr: strings.TrimPrefix(server.URL, "https://"), Token: "kairo_fetch_secret"}}}
	e.fetchDue(ctx)
	if len(fake.made) != 1 || fake.made[0].Bytes != res.contentBytes {
		t.Fatalf("made %+v\n%s\n%s", fake.made, logs.String(), serverLogs.String())
	}
	if body, err := os.ReadFile(filepath.Join(volatile, inputsDirName, "obj_1", "meta.json")); err != nil || string(body) != `{"rows": 3}` {
		t.Fatalf("meta.json: %q %v", body, err)
	}
}
