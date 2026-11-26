package executor

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"kairo/internal/loader"
	"kairo/internal/machine"
	"kairo/internal/store"
)

func (f *fakeRaws) RawSources(_ context.Context, _, _ string) ([]store.RawSource, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sources, nil
}

func (f *fakeRaws) AuthorizeRawRead(_ context.Context, _, sha256, token string) (store.PackRead, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	read, ok := f.reads[sha256+"/"+token]
	if !ok {
		return store.PackRead{}, store.ErrNotFound
	}
	return read, nil
}

// keepingNode is another node's agent keeping body as raw file sha256 in
// its raw cache, serving it with token (its transfer endpoint).
func keepingNode(t *testing.T, sha256, token string, body []byte) *httptest.Server {
	t.Helper()
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, rawDirName), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, rawDirName, sha256), body, 0o644); err != nil {
		t.Fatal(err)
	}
	fake := &fakeRaws{identities: map[string]loader.File{}, counts: map[string]int64{}, reads: map[string]store.PackRead{sha256 + "/" + token: {StoreID: "far-v", Root: root}}}
	var logs bytes.Buffer
	e := storesExecutor(t, fake, &logs)
	e.storeWork().stores = []servedStore{{decl: machine.Store{ID: "far-v", Class: machine.StoreVolatile, Root: root}, fs: localStoreFS{}, usable: true}}
	srv := httptest.NewTLSServer(&PackServer{Store: fake, NodeID: "far", Executors: []*Local{e}, Logger: slog.New(slog.NewTextHandler(&logs, nil))})
	t.Cleanup(srv.Close)
	return srv
}

// A raw file identified before and kept on another node is read from that
// node's agent (its transfer endpoint, with the token the daemon issued),
// not from upstream; a node refusing the token, or giving other bytes, is
// passed over, as is one giving more than it; with none giving it right,
// upstream is read.
func TestARawFileComesFromAnotherNodeKeepingIt(t *testing.T) {
	content := bytes.Repeat([]byte("kept on another node\n"), 20000)
	identity, err := loader.Identify(bytes.NewReader(content), "raw.txt", loader.DefaultChunkBytes)
	if err != nil {
		t.Fatal(err)
	}
	var upstreamGets atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			upstreamGets.Add(1)
		}
		http.ServeContent(w, r, "raw.txt", time.Time{}, bytes.NewReader(content))
	}))
	defer upstream.Close()
	identity.URL = upstream.URL + "/raw.txt"

	good := keepingNode(t, identity.SHA256, "good", content)
	other := bytes.Clone(content)
	other[100] = 'X'
	wrong := keepingNode(t, identity.SHA256, "wrong", other)
	long := keepingNode(t, identity.SHA256, "long", append(bytes.Clone(content), "more"...))
	addr := func(s *httptest.Server) string { return strings.TrimPrefix(s.URL, "https://") }
	// the two nodes' agents share the test servers' CA: one client trusts both
	pool := good.Client().Transport.(*http.Transport).TLSClientConfig.RootCAs
	pool.AddCert(wrong.Certificate())
	pool.AddCert(long.Certificate())

	fake := &fakeRaws{identities: map[string]loader.File{identity.URL: identity}, counts: map[string]int64{}, sources: []store.RawSource{
		{NodeID: "refusing", Addr: addr(good), Token: "not-issued"},
		{NodeID: "wrong", Addr: addr(wrong), Token: "wrong"},
		{NodeID: "long", Addr: addr(long), Token: "long"},
		{NodeID: "far", Addr: addr(good), Token: "good"},
	}}
	var logs bytes.Buffer
	e := storesExecutor(t, fake, &logs)
	e.PackClient = good.Client()
	root := t.TempDir()
	served := servedStore{decl: machine.Store{ID: "v", Class: machine.StoreVolatile, Root: root}, fs: localStoreFS{}, usable: true}
	e.storeWork().stores = []servedStore{served}
	cache, err := e.storeWork().rawCacheFor(served)
	if err != nil {
		t.Fatal(err)
	}
	defer cache.close()
	cache.floor = func() int64 { return 0 }
	ctx := context.Background()
	got, kept, path, err := cache.make(ctx, loader.File{Name: "raw.txt", URL: identity.URL})
	if err != nil {
		t.Fatalf("%v\n%s", err, logs.String())
	}
	unpinRaw(path)
	body, readErr := os.ReadFile(path)
	if !kept || got.SHA256 != identity.SHA256 || readErr != nil || !bytes.Equal(body, content) || upstreamGets.Load() != 0 {
		t.Fatalf("kept %v %+v, upstream read %d times: %v\n%s", kept, got, upstreamGets.Load(), readErr, logs.String())
	}
	if !strings.Contains(logs.String(), "raw file read from another node") || !strings.Contains(logs.String(), "node_id=wrong") || !strings.Contains(logs.String(), "node_id=long") {
		t.Fatalf("logs:\n%s", logs.String())
	}
	entries, _ := os.ReadDir(filepath.Join(root, rawDirName, ".partial"))
	if len(entries) != 0 {
		t.Fatalf("partials left: %d", len(entries))
	}

	// no node gives it right: upstream
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	fake.sources = fake.sources[:3]
	fake.mu.Unlock()
	got, kept, path, err = cache.make(ctx, loader.File{Name: "raw.txt", URL: identity.URL})
	if err != nil {
		t.Fatal(err)
	}
	unpinRaw(path)
	if !kept || got.SHA256 != identity.SHA256 || upstreamGets.Load() != 1 {
		t.Fatalf("kept %v %+v, upstream read %d times\n%s", kept, got, upstreamGets.Load(), logs.String())
	}
}
