package executor

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"kairo/internal/loader"
	"kairo/internal/machine"
	"kairo/internal/store"
)

// fakeRaws is the daemon's side of the raw cache: identities (the first of
// a URL stands), placements and counts.
type fakeRaws struct {
	Coordinator
	mu         sync.Mutex
	identities map[string]loader.File
	placed     []string // store id (empty: identity only) and sha256
	counts     map[string]int64
	refuse     error
	// refusePlacement refuses placements on a store (identities pass)
	refusePlacement error
}

func (f *fakeRaws) RawIdentity(_ context.Context, _, url string) (loader.File, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id, ok := f.identities[url]
	return id, ok, nil
}

func (f *fakeRaws) RawPlaced(_ context.Context, _, storeID, _ string, file loader.File) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.refuse != nil {
		return f.refuse
	}
	if storeID != "" && f.refusePlacement != nil {
		return f.refusePlacement
	}
	if kept, ok := f.identities[file.URL]; ok && kept.SHA256 != file.SHA256 {
		return fmt.Errorf("%w: another identity", store.ErrRawChanged)
	}
	f.identities[file.URL] = file
	f.placed = append(f.placed, storeID+"/"+file.SHA256)
	return nil
}

func (f *fakeRaws) RawRecords(_ context.Context, _ string, c store.RecordsCount) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.counts[c.SHA256+"|"+c.Format] = c.Records
	return nil
}

func rawLaunch(id string, files ...loader.File) *store.Launch {
	return &store.Launch{Attempt: store.Attempt{ID: id}, Streams: []store.LaunchStream{{Name: "s", Spec: loader.Spec{Sources: []loader.SourceSpec{{Name: "raw", Format: "text", Files: files}}}}}}
}

// A raw file an attempt asks for is fetched once into the store's raw
// cache, identified from the bytes written, recorded and only then linked
// into the attempt's own view and handed over; its records counted here;
// another attempt is handed it from the cache; a file of no stream of the
// attempt is refused; one not what was published, or not what the daemon
// has, is a change and nothing of it stays; one larger than the store's room
// is identified only and read from upstream; a file handed over stays
// readable once evicted; an agent restarted serves the views again.
func TestTheRawCacheHandsAttemptsOnlyCheckedFilesOfTheirOwn(t *testing.T) {
	content := bytes.Repeat([]byte("line of raw text\n"), 20000)
	sum := sha256.Sum256(content)
	sha := hex.EncodeToString(sum[:])
	var gets atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			gets.Add(1)
		}
		http.ServeContent(w, r, "raw.txt", time.Time{}, bytes.NewReader(content))
	}))
	defer srv.Close()
	root, err := os.MkdirTemp("", "kr")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	fake := &fakeRaws{identities: map[string]loader.File{}, counts: map[string]int64{}}
	var logs bytes.Buffer
	e := storesExecutor(t, fake, &logs)
	served := servedStore{decl: machine.Store{ID: "v", Class: machine.StoreVolatile, Root: root}, fs: localStoreFS{}, usable: true}
	e.storeWork().stores = []servedStore{served}
	cache, err := e.storeWork().rawCacheFor(served)
	if err != nil {
		t.Fatal(err)
	}
	defer cache.close()
	cache.floor = func() int64 { return 0 } // the test's disk may be below a tenth free
	ctx := context.Background()
	file := loader.File{Name: "raw.txt", URL: srv.URL + "/raw.txt"}
	other := loader.File{Name: "raw2.txt", URL: srv.URL + "/raw2.txt", SHA256: strings.Repeat("0", 64)}
	const a1, a2 = "att_00000000000000000000000000000001", "att_00000000000000000000000000000002"
	view1, err := cache.attach(rawLaunch(a1, file, other))
	if err != nil {
		t.Fatal(err)
	}
	agent := loader.NewAgent(view1, loader.HTTPOrigin{})

	got, origin, err := agent.Resolve(ctx, file)
	if err != nil {
		t.Fatal(err)
	}
	if got.SHA256 != sha || got.Size != int64(len(content)) || !got.Identified() || origin == (loader.HTTPOrigin{}) {
		t.Fatalf("resolved %+v via %v", got, origin)
	}
	if linked, err := os.ReadFile(filepath.Join(view1, sha)); err != nil || !bytes.Equal(linked, content) {
		t.Fatalf("in the attempt's view: %v", err)
	}
	if strings.Join(fake.placed, ",") != "/"+sha+",v/"+sha || gets.Load() != 1 { // its identity, then the file
		t.Fatalf("placed %v after %d fetches", fake.placed, gets.Load())
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		fake.mu.Lock()
		n := fake.counts[sha+"|text"]
		fake.mu.Unlock()
		if n == 20000 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("records counted %d, want 20000", n)
		}
		time.Sleep(20 * time.Millisecond)
	}
	// read through the view, every chunk checked
	r, err := (&loader.Fetcher{Resolve: agent.Resolve}).Open(ctx, file, 0)
	if err != nil {
		t.Fatal(err)
	}
	read, err := io.ReadAll(r)
	r.Close()
	if err != nil || !bytes.Equal(read, content) {
		t.Fatalf("read through the view: %v", err)
	}

	// another attempt: from the cache
	view2, err := cache.attach(rawLaunch(a2, got))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := loader.NewAgent(view2, loader.HTTPOrigin{}).Resolve(ctx, got); err != nil || gets.Load() != 1 {
		t.Fatalf("another attempt: %v, %d fetches", err, gets.Load())
	}
	// a file of no stream of the attempt
	if _, _, err := loader.NewAgent(view2, loader.HTTPOrigin{}).Resolve(ctx, other); !errors.Is(err, loader.ErrRefused) {
		t.Fatalf("a file not the attempt's: %v", err)
	}
	// not what was published: a change, nothing kept
	if _, _, err := agent.Resolve(ctx, other); !errors.Is(err, loader.ErrChanged) {
		t.Fatalf("published otherwise: %v", err)
	}
	entries, _ := os.ReadDir(filepath.Join(cache.dir, ".partial"))
	if len(entries) != 0 {
		t.Fatalf("left behind %v", entries)
	}
	// refused by the daemon: a change
	third := loader.File{Name: "raw3.txt", URL: srv.URL + "/raw3.txt"}
	const a3 = "att_00000000000000000000000000000003"
	view3, err := cache.attach(rawLaunch(a3, third))
	if err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	fake.refuse = fmt.Errorf("%w: test", store.ErrRawChanged)
	fake.mu.Unlock()
	if _, _, err := loader.NewAgent(view3, loader.HTTPOrigin{}).Resolve(ctx, third); !errors.Is(err, loader.ErrChanged) {
		t.Fatalf("refused: %v", err)
	}
	fake.mu.Lock()
	fake.refuse = nil
	fake.mu.Unlock()

	// no room above the floor: identified, read from upstream
	cache.floor = func() int64 { return 1 << 62 }
	if got3, origin, err := loader.NewAgent(view3, loader.HTTPOrigin{}).Resolve(ctx, third); err != nil || got3.SHA256 != sha || origin != (loader.HTTPOrigin{}) {
		t.Fatalf("not kept %+v via %v: %v", got3, origin, err)
	}

	// a file views hold is not evicted (its name gone would free nothing);
	// one no view holds is
	held := filepath.Join(root, rawDirName, sha)
	if err := removeRaw(root, sha); !errors.Is(err, errEvictInUse) {
		t.Fatalf("evicting a file held (just handed, linked in views): %v", err)
	}
	unheld := strings.Repeat("e", 64)
	if err := os.WriteFile(filepath.Join(root, rawDirName, unheld), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := removeRaw(root, unheld); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, rawDirName, unheld)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("an unheld file not evicted: %v", err)
	}

	// upstream gone: another URL published as the same bytes is handed the
	// file kept here
	srv.Close()
	fourth := loader.File{Name: "raw4.txt", URL: srv.URL + "/raw4.txt", PublishedSHA256: sha}
	const a4 = "att_00000000000000000000000000000004"
	view4, err := cache.attach(rawLaunch(a4, fourth))
	if err != nil {
		t.Fatal(err)
	}
	ctx4, cancel4 := context.WithTimeout(ctx, 10*time.Second) // upstream gone: never there
	defer cancel4()
	if got4, origin, err := loader.NewAgent(view4, loader.HTTPOrigin{}).Resolve(ctx4, fourth); err != nil || got4.SHA256 != sha || origin == (loader.HTTPOrigin{}) {
		t.Fatalf("kept under another URL, upstream gone: %+v via %v: %v", got4, origin, err)
	}

	// its name gone anyway (an eviction that came first): what attempts
	// were handed stays readable in their views, and is handed again from
	// there, upstream gone
	if err := os.Remove(held); err != nil {
		t.Fatal(err)
	}
	if kept, err := os.ReadFile(filepath.Join(view1, sha)); err != nil || !bytes.Equal(kept, content) {
		t.Fatalf("handed over, then evicted: %v", err)
	}
	if again, origin, err := agent.Resolve(ctx, file); err != nil || again.SHA256 != sha || origin == (loader.HTTPOrigin{}) {
		t.Fatalf("held, upstream gone: %+v via %v: %v", again, origin, err)
	}

	// an agent restarted serves the views again
	cache.close()
	e.storeWork().raws = nil
	again, err := e.storeWork().rawCacheFor(served)
	if err != nil {
		t.Fatal(err)
	}
	defer again.close()
	if _, ok := again.views[a1]; !ok {
		t.Fatalf("views after a restart: %v", again.views)
	}
	again.floor = func() int64 { return 0 }
	if _, _, err := loader.NewAgent(view2, loader.HTTPOrigin{}).Resolve(ctx, got); err != nil {
		t.Fatalf("after a restart: %v", err)
	}
}

// A file whose size upstream does not say is not kept: it is identified
// once and read from upstream.
func TestARawFileOfUnknownSizeIsNotKept(t *testing.T) {
	content := bytes.Repeat([]byte("x\n"), 1000)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			return
		}
		w.(http.Flusher).Flush() // chunked: no length
		w.Write(content)
	}))
	defer srv.Close()
	root, err := os.MkdirTemp("", "kr")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	fake := &fakeRaws{identities: map[string]loader.File{}, counts: map[string]int64{}}
	var logs bytes.Buffer
	e := storesExecutor(t, fake, &logs)
	served := servedStore{decl: machine.Store{ID: "v", Class: machine.StoreVolatile, Root: root}, fs: localStoreFS{}, usable: true}
	cache, err := e.storeWork().rawCacheFor(served)
	if err != nil {
		t.Fatal(err)
	}
	defer cache.close()
	cache.floor = func() int64 { return 0 }
	file := loader.File{Name: "u.txt", URL: srv.URL + "/u.txt"}
	view, err := cache.attach(rawLaunch("att_00000000000000000000000000000009", file))
	if err != nil {
		t.Fatal(err)
	}
	got, origin, err := loader.NewAgent(view, loader.HTTPOrigin{}).Resolve(context.Background(), file)
	if err != nil || origin != (loader.HTTPOrigin{}) || got.Size != int64(len(content)) {
		t.Fatalf("unknown size: %+v via %v: %v", got, origin, err)
	}
	if strings.Join(fake.placed, ",") != "/"+got.SHA256 {
		t.Fatalf("placed %v", fake.placed)
	}
}

// A file installed whose placement was not recorded (the agent went in
// between) is found again by its URL, upstream gone.
func TestAnInstalledRawFileIsFoundAgainByItsURL(t *testing.T) {
	content := bytes.Repeat([]byte("a line\n"), 1000)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "f.txt", time.Time{}, bytes.NewReader(content))
	}))
	root, err := os.MkdirTemp("", "kr")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	fake := &fakeRaws{identities: map[string]loader.File{}, counts: map[string]int64{}}
	var logs bytes.Buffer
	e := storesExecutor(t, fake, &logs)
	served := servedStore{decl: machine.Store{ID: "v", Class: machine.StoreVolatile, Root: root}, fs: localStoreFS{}, usable: true}
	cache, err := e.storeWork().rawCacheFor(served)
	if err != nil {
		t.Fatal(err)
	}
	cache.floor = func() int64 { return 0 }
	file := loader.File{Name: "f.txt", URL: srv.URL + "/f.txt"}
	view, err := cache.attach(rawLaunch("att_0000000000000000000000000000000a", file))
	if err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	fake.refusePlacement = fmt.Errorf("%w: test", store.ErrStreamReportRefused)
	fake.mu.Unlock()
	if _, _, err := loader.NewAgent(view, loader.HTTPOrigin{}).Resolve(context.Background(), file); err == nil {
		t.Fatal("handed though not placed")
	}
	// the agent went; upstream gone
	cache.close()
	e.storeWork().raws = nil
	srv.Close()
	fake.mu.Lock()
	fake.refusePlacement = nil
	fake.mu.Unlock()
	again, err := e.storeWork().rawCacheFor(served)
	if err != nil {
		t.Fatal(err)
	}
	defer again.close()
	again.floor = func() int64 { return 0 }
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	got, origin, err := loader.NewAgent(view, loader.HTTPOrigin{}).Resolve(ctx, file)
	if err != nil || origin == (loader.HTTPOrigin{}) || got.Size != int64(len(content)) {
		t.Fatalf("found again: %+v via %v: %v", got, origin, err)
	}
}
