package executor

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"kairo/internal/machine"
	"kairo/internal/store"
)

// fakeCopies orders copies, authorizes pack reads for one token, and records
// what is copied.
type fakeCopies struct {
	*fakePacks
	cmu     sync.Mutex
	copies  []store.CopyOrder
	copied  []store.CopyMade
	token   string
	readFor store.PackRead
	reads   int
}

func (f *fakeCopies) CopiesToMake(context.Context, string) ([]store.CopyOrder, error) {
	f.cmu.Lock()
	defer f.cmu.Unlock()
	return append([]store.CopyOrder(nil), f.copies...), nil
}

func (f *fakeCopies) RecordCopy(_ context.Context, made store.CopyMade) error {
	f.cmu.Lock()
	defer f.cmu.Unlock()
	f.copied = append(f.copied, made)
	return nil
}

func (f *fakeCopies) AuthorizePackRead(_ context.Context, nodeID, objectID, token string) (store.PackRead, error) {
	f.cmu.Lock()
	defer f.cmu.Unlock()
	f.reads++
	if nodeID != "node" || objectID != "obj_1" || token == "" || token != f.token {
		return store.PackRead{}, store.ErrNotFound
	}
	return f.readFor, nil
}

func newFakeCopies() *fakeCopies {
	return &fakeCopies{fakePacks: &fakePacks{fakeOutputs: &fakeOutputs{fakeScratch: &fakeScratch{fakeStores: &fakeStores{}}}}}
}

// packed makes a run's output directory with files and packs it as obj_1 on
// the volatile store at root.
func packed(t *testing.T, root string) packResult {
	t.Helper()
	ctx := context.Background()
	dir, err := (localStoreFS{}).CreateKind(ctx, root, kindOutput, "exe_1")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "weights.bin"), bytes.Repeat([]byte{1, 2, 3, 4, 5}, 400000), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "log.txt"), []byte(strings.Repeat("step ok\n", 5000)), 0o644); err != nil {
		t.Fatal(err)
	}
	res, err := (localStoreFS{}).Pack(ctx, root, kindOutput, "exe_1", "obj_1")
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func orderFor(res packResult, durable string, source store.CopySource) store.CopyOrder {
	return store.CopyOrder{ObjectID: "obj_1", StoreID: "e-durable", Root: durable, Month: "202611", PackBytes: res.packBytes,
		PackSHA256: res.packSHA256, ManifestSHA256: res.manifestHash, Source: source}
}

// A pack on a store this executor serves is copied to the durable store
// locally, read back and recorded; a copy that does not match is not
// renamed or recorded and its partial files go.
func TestCopyToTheDurableStoreLocally(t *testing.T) {
	base := t.TempDir()
	volatile, durable := filepath.Join(base, "volatile"), filepath.Join(base, "durable")
	res := packed(t, volatile)
	fake := newFakeCopies()
	var logs bytes.Buffer
	e := storesExecutor(t, fake, &logs)
	e.stores = []servedStore{
		{decl: machine.Store{ID: "c-scratch", Class: machine.StoreVolatile, Root: volatile}, fs: localStoreFS{}, usable: true},
		{decl: machine.Store{ID: "e-durable", Class: machine.StoreDurable, Root: durable}, fs: localStoreFS{}, usable: true},
	}
	if err := os.MkdirAll(durable, 0o755); err != nil {
		t.Fatal(err)
	}
	// a copy that does not match: nothing renamed, recorded, or left
	bad := orderFor(res, durable, store.CopySource{Local: true, StoreID: "c-scratch", Root: volatile})
	bad.PackSHA256 = strings.Repeat("0", 64)
	fake.copies = []store.CopyOrder{bad}
	e.copyDue(context.Background())
	dir := filepath.Join(durable, objectsDirName, "202611")
	if len(fake.copied) != 0 || !strings.Contains(logs.String(), "the copy read back does not match the pack") {
		t.Fatalf("a mismatching copy was recorded: %+v\n%s", fake.copied, logs.String())
	}
	if left, _ := filepath.Glob(filepath.Join(dir, "*")); len(left) != 0 {
		t.Fatalf("left on the durable store: %v", left)
	}

	fake.copies = []store.CopyOrder{orderFor(res, durable, store.CopySource{Local: true, StoreID: "c-scratch", Root: volatile})}
	e.copyDue(context.Background())
	want := store.CopyMade{ObjectID: "obj_1", StoreID: "e-durable", ExecutorID: "exec", Path: filepath.Join(dir, "obj_1.tar.zst"),
		ManifestPath: filepath.Join(dir, "obj_1.tar.zst.files.tsv"), Bytes: res.packBytes, SHA256: res.packSHA256, ManifestSHA256: res.manifestHash}
	if len(fake.copied) != 1 || fake.copied[0] != want {
		t.Fatalf("copied %+v, want %+v\n%s", fake.copied, want, logs.String())
	}
	if got := sha256File(t, want.Path); got != res.packSHA256 {
		t.Fatalf("the copy's sha256 %s", got)
	}
	if left, _ := filepath.Glob(filepath.Join(dir, "*"+partialSuffix)); len(left) != 0 {
		t.Fatalf("partial files left: %v", left)
	}
	// ordered again (recorded after all, or not): read back as it is
	e.copyDue(context.Background())
	if len(fake.copied) != 2 || fake.copied[1] != want {
		t.Fatalf("copied again %+v", fake.copied)
	}
}

// A pack on another node is fetched from its agent's pack server with the
// copy's token, an interrupted fetch resumed with a range request; a wrong
// token, or a name that is not a pack, gets nothing.
func TestCopyFetchesFromThePacksAgent(t *testing.T) {
	base := t.TempDir()
	volatile, durable := filepath.Join(base, "volatile"), filepath.Join(base, "durable")
	res := packed(t, volatile)
	if err := os.MkdirAll(durable, 0o755); err != nil {
		t.Fatal(err)
	}
	fake := newFakeCopies()
	fake.token, fake.readFor = "kairo_pack_secret", store.PackRead{StoreID: "c-scratch", Root: volatile}
	var serverLogs, logs bytes.Buffer
	source := storesExecutor(t, fake, &serverLogs)
	source.stores = []servedStore{{decl: machine.Store{ID: "c-scratch", Class: machine.StoreVolatile, Root: volatile}, fs: localStoreFS{}, usable: true}}
	server := httptest.NewTLSServer(&PackServer{Store: fake, NodeID: "node", Executors: []*Local{source}, Logger: source.Logger})
	defer server.Close()
	addr := strings.TrimPrefix(server.URL, "https://")

	e := storesExecutor(t, fake, &logs)
	e.PackClient = server.Client()
	e.stores = []servedStore{{decl: machine.Store{ID: "e-durable", Class: machine.StoreDurable, Root: durable}, fs: localStoreFS{}, usable: true}}
	order := orderFor(res, durable, store.CopySource{StoreID: "c-scratch", Root: volatile, Addr: addr, Token: "kairo_pack_wrong"})

	fake.copies = []store.CopyOrder{order}
	e.copyDue(context.Background())
	if len(fake.copied) != 0 || !strings.Contains(logs.String(), "403 Forbidden") {
		t.Fatalf("copied with a wrong token: %+v\n%s", fake.copied, logs.String())
	}

	// half the pack is there from a fetch cut short: the rest is asked for
	dir := filepath.Join(durable, objectsDirName, "202611")
	half, err := os.ReadFile(res.packPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "obj_1.tar.zst"+partialSuffix), half[:len(half)/2], 0o644); err != nil {
		t.Fatal(err)
	}
	var ranges []string
	server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ranges = append(ranges, r.URL.Path+" "+r.Header.Get("Range"))
		(&PackServer{Store: fake, NodeID: "node", Executors: []*Local{source}, Logger: source.Logger}).ServeHTTP(w, r)
	})
	order.Source.Token = "kairo_pack_secret"
	fake.copies = []store.CopyOrder{order}
	e.copyDue(context.Background())
	if len(fake.copied) != 1 || fake.copied[0].SHA256 != res.packSHA256 || fake.copied[0].Bytes != res.packBytes {
		t.Fatalf("not copied: %+v\n%s", fake.copied, logs.String())
	}
	if strings.Join(ranges, ";") != "/packs/obj_1.tar.zst.files.tsv ;/packs/obj_1.tar.zst bytes="+strconv.Itoa(len(half)/2)+"-" {
		t.Fatalf("requests %q", ranges)
	}

	// what is not a pack is never served
	for _, p := range []string{"/packs/obj_1.tar.zst/../x", "/packs/..%2Fobj_1.tar.zst", "/packs/obj_1", "/outputs/exe_1"} {
		req, _ := http.NewRequest(http.MethodGet, server.URL+p, nil)
		req.Header.Set("Authorization", "Bearer kairo_pack_secret")
		resp, err := server.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("%s: %s", p, resp.Status)
		}
	}
}

// What is read around the cache is what was written.
func TestHashUncachedReadsWhatWasWritten(t *testing.T) {
	dir := t.TempDir()
	body := bytes.Repeat([]byte("kairo"), 300001) // not a multiple of any sector
	if err := os.WriteFile(filepath.Join(dir, "f"), body, 0o644); err != nil {
		t.Fatal(err)
	}
	r, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	sum, n, err := hashUncached(r, "f", filepath.Join(dir, "f"))
	if err != nil || n != int64(len(body)) || sum != sha256Hex(string(body)) {
		t.Fatalf("hash %s %d %v", sum, n, err)
	}
	if err := os.Mkdir(filepath.Join(dir, "d"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, _, err := hashUncached(r, "d", filepath.Join(dir, "d")); err == nil || errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a directory hashed: %v", err)
	}
}
