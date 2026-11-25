package executor

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
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

// fakeRaws records the raw files placed, refusing when told to.
type fakeRaws struct {
	Coordinator
	mu     sync.Mutex
	placed []string // store id (empty: identity only) and sha256
	refuse error
}

func (f *fakeRaws) RawPlaced(_ context.Context, _, storeID, _ string, file loader.File) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.refuse != nil {
		return f.refuse
	}
	f.placed = append(f.placed, storeID+"/"+file.SHA256)
	return nil
}

// A raw file a loader asks for is fetched once into the store's raw cache,
// identified as it comes, recorded and only then handed over; asked again it
// is handed from the cache; one not what was published, or refused by the
// daemon, is a change and nothing of it stays; one larger than the store's
// room is identified only and read from upstream; a loader reads every
// chunk checked.
func TestTheRawCacheHandsOverOnlyCheckedFiles(t *testing.T) {
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
	fake := &fakeRaws{}
	var logs bytes.Buffer
	e := storesExecutor(t, fake, &logs)
	served := servedStore{decl: machine.Store{ID: "v", Class: machine.StoreVolatile, Root: root}, fs: localStoreFS{}, usable: true}
	e.storeWork().stores = []servedStore{served}
	cache, err := e.storeWork().rawCacheFor(served)
	if err != nil {
		t.Fatal(err)
	}
	defer cache.ln.Close()
	agent := loader.NewAgent(cache.dir, loader.HTTPOrigin{})
	ctx := context.Background()
	file := loader.File{Name: "raw.txt", URL: srv.URL + "/raw.txt"}

	got, origin, err := agent.Resolve(ctx, file)
	if err != nil {
		t.Fatal(err)
	}
	if got.SHA256 != sha || got.Size != int64(len(content)) || !got.Identified() || got.URL != file.URL || origin == (loader.HTTPOrigin{}) {
		t.Fatalf("resolved %+v via %v", got, origin)
	}
	if kept, err := os.ReadFile(filepath.Join(cache.dir, sha)); err != nil || !bytes.Equal(kept, content) {
		t.Fatalf("kept: %v", err)
	}
	if strings.Join(fake.placed, ",") != "v/"+sha || gets.Load() != 1 {
		t.Fatalf("placed %v after %d fetches", fake.placed, gets.Load())
	}
	// asked again, as identified: from the cache
	if _, _, err := agent.Resolve(ctx, got); err != nil || gets.Load() != 1 {
		t.Fatalf("again: %v, %d fetches", err, gets.Load())
	}
	// read through the cache, every chunk checked
	r, err := (&loader.Fetcher{Resolve: agent.Resolve}).Open(ctx, file, 0)
	if err != nil {
		t.Fatal(err)
	}
	read, err := io.ReadAll(r)
	r.Close()
	if err != nil || !bytes.Equal(read, content) {
		t.Fatalf("read through the cache: %v", err)
	}

	// not what was published: a change, nothing kept
	other := loader.File{Name: "raw2.txt", URL: srv.URL + "/raw2.txt", SHA256: strings.Repeat("0", 64)}
	if _, _, err := agent.Resolve(ctx, other); !errors.Is(err, loader.ErrChanged) {
		t.Fatalf("published otherwise: %v", err)
	}
	// refused by the daemon: a change, nothing kept
	fake.refuse = store.ErrRawChanged
	if _, _, err := agent.Resolve(ctx, loader.File{Name: "raw3.txt", URL: srv.URL + "/raw3.txt"}); !errors.Is(err, loader.ErrChanged) {
		t.Fatalf("refused: %v", err)
	}
	fake.refuse = nil
	entries, _ := os.ReadDir(filepath.Join(cache.dir, ".partial"))
	if len(entries) != 0 {
		t.Fatalf("left behind %v", entries)
	}

	// larger than the room above the reserve: identified, read from upstream
	cache.reserve = 1 << 62
	big := loader.File{Name: "raw4.txt", URL: srv.URL + "/raw4.txt"}
	got, origin, err = agent.Resolve(ctx, big)
	if err != nil || got.SHA256 != sha || origin != (loader.HTTPOrigin{}) {
		t.Fatalf("fallback %+v via %v: %v", got, origin, err)
	}
	if fake.placed[len(fake.placed)-1] != "/"+sha {
		t.Fatalf("placed %v", fake.placed)
	}
	if err := removeRaw(root, sha); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(cache.dir, sha)); !os.IsNotExist(err) {
		t.Fatalf("evicted file there: %v", err)
	}
}
