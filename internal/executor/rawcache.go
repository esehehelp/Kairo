package executor

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	"kairo/internal/loader"
	"kairo/internal/store"
)

// The raw cache (issue #43; loader/agent.go): a volatile store this executor
// serves keeps raw files for the streams of attempts at <root>/raw/<sha256>,
// handed read-only to their containers (/kairo/raw) with this agent's socket
// in it. A loader asks for each file before reading it (POST /ensure): a
// file kept is handed as it is (the loader checks every chunk as it reads);
// another is fetched from upstream into <root>/raw/.partial once, checked
// against its identity as it comes (or, never read before, identified as it
// comes and checked at its end against what was published of it), recorded
// with the daemon (RawPlaced: its identity, and the file kept there) and
// only then renamed into place and handed over. A file larger than the
// store's room above its reserve is not kept: it is read once only to be
// identified (and recorded), and the loader reads it from upstream, checked.
// Kept files are evicted least recently used first when the store runs
// short (store/evictions.go): a loader reading one reads on (an open file
// outlives its name).

// rawDirName is the directory of a volatile store raw files are kept in.
const rawDirName = "raw"

// rawCache is the raw cache of one volatile store.
type rawCache struct {
	w       *storeWorker
	storeID string
	root    string // the store's root
	dir     string // <root>/raw
	reserve int64
	fetch   *loader.Fetcher

	mu       sync.Mutex
	inflight map[string]*rawFlight // by URL
	ln       net.Listener
}

// rawFlight is a file being made ready, for every loader asking for it.
type rawFlight struct {
	done   chan struct{}
	answer loader.EnsureAnswer
	status int
}

var sha256Name = regexp.MustCompile(`^[0-9a-f]{64}$`)

// rawCacheFor is the raw cache of volatile store s, its socket listening.
func (e *storeWorker) rawCacheFor(s servedStore) (*rawCache, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if c, ok := e.raws[s.decl.ID]; ok && c.root == s.decl.Root {
		return c, nil
	}
	dir := filepath.Join(s.decl.Root, rawDirName)
	if err := os.MkdirAll(filepath.Join(dir, ".partial"), 0o755); err != nil {
		return nil, err
	}
	if err := os.Chmod(dir, 0o755); err != nil {
		return nil, err
	}
	// what a fetch cut short left behind
	if entries, err := os.ReadDir(filepath.Join(dir, ".partial")); err == nil {
		for _, entry := range entries {
			_ = os.Remove(filepath.Join(dir, ".partial", entry.Name()))
		}
	}
	sock := filepath.Join(dir, loader.AgentSocket)
	_ = os.Remove(sock)
	ln, err := net.Listen("unix", sock)
	if err != nil {
		return nil, err
	}
	// the attempts' containers map their root to another user
	if err := os.Chmod(sock, 0o666); err != nil {
		ln.Close()
		return nil, err
	}
	reserve := s.decl.ReserveFreeBytes
	if reserve <= 0 {
		reserve = 1 << 30
	}
	c := &rawCache{w: e, storeID: s.decl.ID, root: s.decl.Root, dir: dir, reserve: reserve, inflight: map[string]*rawFlight{}, ln: ln,
		fetch: &loader.Fetcher{Origin: loader.HTTPOrigin{Client: &http.Client{}}, Problem: func(f loader.File, err error) {
			if err != nil {
				e.Logger.Warn("raw file not fetched yet", "url", f.URL, "error", err)
			}
		}}}
	if e.raws == nil {
		e.raws = map[string]*rawCache{}
	}
	e.raws[s.decl.ID] = c
	go func() {
		srv := &http.Server{Handler: http.HandlerFunc(c.serve), ReadHeaderTimeout: time.Minute}
		_ = srv.Serve(ln)
	}()
	return c, nil
}

func (c *rawCache) serve(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || r.URL.Path != "/ensure" {
		http.NotFound(w, r)
		return
	}
	var f loader.File
	if err := json.NewDecoder(io.LimitReader(r.Body, 256<<20)).Decode(&f); err != nil || f.URL == "" || f.Name == "" {
		writeEnsure(w, http.StatusBadRequest, loader.EnsureAnswer{Error: "a file with a URL and a name"})
		return
	}
	c.mu.Lock()
	flight, ok := c.inflight[f.URL]
	if !ok {
		flight = &rawFlight{done: make(chan struct{})}
		c.inflight[f.URL] = flight
		go func() {
			// not the request's context: other loaders may wait on it
			flight.status, flight.answer = c.ensure(context.Background(), f)
			c.mu.Lock()
			delete(c.inflight, f.URL)
			c.mu.Unlock()
			close(flight.done)
		}()
	}
	c.mu.Unlock()
	select {
	case <-flight.done:
		writeEnsure(w, flight.status, flight.answer)
	case <-r.Context().Done():
	}
}

func writeEnsure(w http.ResponseWriter, status int, answer loader.EnsureAnswer) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(answer)
}

// ensure makes f ready: kept and recorded, or identified and recorded to be
// read from upstream.
func (c *rawCache) ensure(ctx context.Context, f loader.File) (int, loader.EnsureAnswer) {
	fail := func(err error) (int, loader.EnsureAnswer) {
		answer := loader.EnsureAnswer{Error: err.Error(), Changed: errors.Is(err, loader.ErrChanged) || errors.Is(err, store.ErrRawChanged)}
		if answer.Changed {
			return http.StatusConflict, answer
		}
		return http.StatusServiceUnavailable, answer
	}
	// kept already
	if f.Identified() && sha256Name.MatchString(f.SHA256) {
		if info, err := os.Stat(filepath.Join(c.dir, f.SHA256)); err == nil && info.Mode().IsRegular() && info.Size() == f.Size {
			if err := c.w.Store.RawPlaced(ctx, c.w.ID, c.storeID, c.root, f); err != nil {
				return fail(err)
			}
			return http.StatusOK, loader.EnsureAnswer{File: f, Name: f.SHA256}
		}
	}
	// room for it, as far as is known
	_, free, err := diskUsage(c.root)
	if err != nil {
		return fail(err)
	}
	size := f.Size
	if size <= 0 {
		size = c.contentLength(ctx, f)
	}
	if size > 0 && size > free-c.reserve {
		identity, err := c.identify(ctx, f, io.Discard)
		if err != nil {
			return fail(err)
		}
		if err := c.w.Store.RawPlaced(ctx, c.w.ID, "", "", identity); err != nil {
			return fail(err)
		}
		return http.StatusOK, loader.EnsureAnswer{File: identity, Fallback: true}
	}
	// fetched into place
	var suffix [8]byte
	_, _ = rand.Read(suffix[:])
	partial := filepath.Join(c.dir, ".partial", hex.EncodeToString(suffix[:]))
	out, err := os.OpenFile(partial, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return fail(err)
	}
	identity, err := c.identify(ctx, f, out)
	if err == nil {
		err = out.Sync()
	}
	if cerr := out.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(partial)
		return fail(err)
	}
	if err := c.w.Store.RawPlaced(ctx, c.w.ID, c.storeID, c.root, identity); err != nil {
		os.Remove(partial)
		return fail(err)
	}
	if err := os.Rename(partial, filepath.Join(c.dir, identity.SHA256)); err != nil {
		os.Remove(partial)
		return fail(err)
	}
	c.w.Logger.Info("raw file kept", "url", identity.URL, "sha256", identity.SHA256, "bytes", identity.Size, "store_id", c.storeID)
	return http.StatusOK, loader.EnsureAnswer{File: identity, Name: identity.SHA256}
}

// identify reads f from upstream into out: checked against its identity,
// or identified as it is read and checked at its end against what was
// published of it.
func (c *rawCache) identify(ctx context.Context, f loader.File, out io.Writer) (loader.File, error) {
	r, err := c.fetch.Open(ctx, f, 0)
	if err != nil {
		return loader.File{}, err
	}
	defer r.Close()
	if _, err := io.Copy(out, r); err != nil {
		return loader.File{}, err
	}
	id, ok := r.(interface{ Identity() (loader.File, bool) })
	if !ok {
		return loader.File{}, errors.New("a reader without the identity it checked")
	}
	identity, done := id.Identity()
	if !done {
		return loader.File{}, fmt.Errorf("%s read without its identity", f.URL)
	}
	identity.URL, identity.Name = f.URL, f.Name
	return identity, nil
}

// contentLength is what upstream says f's size is (0: not said).
func (c *rawCache) contentLength(ctx context.Context, f loader.File) int64 {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, f.URL, nil)
	if err != nil {
		return 0
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.ContentLength < 0 {
		return 0
	}
	return resp.ContentLength
}

// removeRaw evicts the raw file sha of the store at root.
func removeRaw(root, sha string) error {
	if !sha256Name.MatchString(sha) {
		return fmt.Errorf("%w: raw file %q", errOutsideRoot, sha)
	}
	err := os.Remove(filepath.Join(root, rawDirName, sha))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}
