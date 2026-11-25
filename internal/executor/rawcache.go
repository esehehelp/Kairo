package executor

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"syscall"
	"time"

	"kairo/internal/loader"
	"kairo/internal/machine"
	"kairo/internal/store"
)

// The raw cache (issue #43; loader/agent.go). A volatile store this
// executor serves keeps raw files for streams at <root>/raw/<sha256>; what is
// being fetched goes to <root>/raw/.partial, which no attempt sees.
//
// Each attempt reading streams has a view of its own, <root>/raw/views/
// <attempt id>, mounted read-only in its container (/kairo/raw): the agent's
// socket for it (.sock), and a hard link to each file it was handed.
// A request on that socket is the attempt's: it may ask only for the files
// of its own streams (as its launch, from the daemon, listed them), and a
// file it was handed stays readable until the attempt ends, whatever is
// evicted meanwhile. The view's list is kept beside the views
// (<root>/raw/.attempts/<id>.json), so an agent restarted while the attempt
// runs serves it again; a view whose attempt has not run here for a while
// goes.
//
// A file is made ready once for every attempt asking (one fetch per URL):
// the identity the daemon has of its URL is asked first (the first one
// found stands); a file of that identity kept here is handed as it is.
// Otherwise the file is fetched from upstream into .partial while its whole
// sha256 and the sha256 of each chunk are computed from the bytes written,
// then compared with the identity the daemon has, if any, and with what was
// published of it; it is recorded with the daemon (RawPlaced, retried while
// the daemon is away, the file kept meanwhile) and only then renamed into
// place. Each asking attempt then has the result checked against its own
// declaration of the file before it is linked into its view. A file the
// store has no room for (above its eviction floor, less what other fetches
// hold) is not kept: read once to be identified when it never was, it is read
// by the loader from upstream, checked. The records each kept file holds, as
// each source of the attempt reads it, are counted here and recorded
// (RawRecords): a later start mid-way passes whole files over by them.

// rawDirName is the directory of a volatile store raw files are kept in.
const rawDirName = "raw"

// maxSocketPath is the longest socket path a Unix socket binds to.
const maxSocketPath = 107

// rawViewGrace is how long a view stays after its attempt is no longer
// seen running here (an agent restarted adopts its attempts meanwhile).
const rawViewGrace = 10 * time.Minute

var sha256Name = regexp.MustCompile(`^[0-9a-f]{64}$`)
var attemptIDName = regexp.MustCompile(`^att_[0-9a-f]{32}$`)

// rawCache is the raw cache of one volatile store.
type rawCache struct {
	w       *storeWorker
	storeID string
	root    string
	dir     string // <root>/raw
	floor   func() int64
	fetch   *loader.Fetcher

	mu       sync.Mutex
	flights  map[string]*rawFlight // by URL
	reserved int64                 // bytes the fetches under way may write
	views    map[string]*rawView   // by attempt
	counted  map[string]bool       // sha256|count key, counted or being
	swept    time.Time
}

// rawFlight is a file being made ready, for every attempt asking for it.
type rawFlight struct {
	done     chan struct{}
	declared loader.File // the declaration it was made under
	identity loader.File
	kept     bool
	err      error
}

// rawWant is a file an attempt's streams read: as its launch had it (the
// identity the daemon had of it then, else what was published) and how its
// sources read it.
type rawWant struct {
	File     loader.File      `json:"file"`
	Readings []rawWantReading `json:"readings"`
}

type rawWantReading struct {
	Format string `json:"format"`
	Field  string `json:"field,omitempty"`
}

// rawView is an attempt's view of the raw cache.
type rawView struct {
	attempt string
	dir     string
	mu      sync.Mutex
	wants   map[string]rawWant // by URL
	ln      net.Listener
	gone    time.Time // first seen not running here (zero: running)
}

// handed records the identity of a file handed to the view's attempt that
// had none at its launch: handed again from the view, whatever is evicted
// or restarted meanwhile.
func (c *rawCache) handed(v *rawView, url string, identity loader.File) {
	v.mu.Lock()
	defer v.mu.Unlock()
	want := v.wants[url]
	if want.File.Identified() {
		return
	}
	identity.PublishedSHA256, identity.PublishedSize = want.File.PublishedSHA256, want.File.PublishedSize
	want.File = identity
	v.wants[url] = want
	body, err := json.Marshal(v.wants)
	if err == nil {
		err = writeFileAtomic(filepath.Join(c.dir, ".attempts", v.attempt+".json"), body)
	}
	if err != nil {
		c.w.Logger.Warn("raw view not kept", "attempt_id", v.attempt, "error", err)
	}
}

// keepRawCaches starts the raw cache of each usable volatile store this
// executor serves (serving again the views of the attempts that had one)
// and, now and then, takes away the views of attempts gone.
func (e *storeWorker) keepRawCaches() {
	for _, s := range e.servedStores() {
		if !s.usable || s.decl.Class != machine.StoreVolatile {
			continue
		}
		if _, isLocal := s.fs.(localStoreFS); !isLocal {
			continue
		}
		c, err := e.rawCacheFor(s)
		if err != nil {
			e.Logger.Warn("raw cache not started", "store_id", s.decl.ID, "error", err)
			continue
		}
		c.sweep()
	}
}

// rawCacheFor is the raw cache of volatile store s.
func (e *storeWorker) rawCacheFor(s servedStore) (*rawCache, error) {
	e.mu.Lock()
	if c, ok := e.raws[s.decl.ID]; ok && c.root == s.decl.Root {
		e.mu.Unlock()
		return c, nil
	}
	e.mu.Unlock()
	dir := filepath.Join(s.decl.Root, rawDirName)
	for _, d := range []string{dir, filepath.Join(dir, ".partial"), filepath.Join(dir, ".attempts"), filepath.Join(dir, "views")} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, err
		}
	}
	for _, d := range []string{filepath.Join(dir, ".partial"), filepath.Join(dir, ".attempts")} {
		if err := os.Chmod(d, 0o700); err != nil {
			return nil, err
		}
	}
	// what a fetch cut short left behind
	if entries, err := os.ReadDir(filepath.Join(dir, ".partial")); err == nil {
		for _, entry := range entries {
			_ = os.Remove(filepath.Join(dir, ".partial", entry.Name()))
		}
	}
	decl := s.decl
	c := &rawCache{w: e, storeID: decl.ID, root: decl.Root, dir: dir, flights: map[string]*rawFlight{}, views: map[string]*rawView{}, counted: map[string]bool{},
		floor: func() int64 {
			if decl.ReserveFreeBytes > 0 {
				return decl.ReserveFreeBytes
			}
			total, _, err := diskUsage(decl.Root)
			if err != nil {
				return 1 << 30
			}
			return total / 10
		},
		fetch: &loader.Fetcher{Origin: loader.HTTPOrigin{Client: &http.Client{}}, Problem: func(f loader.File, err error) {
			if err != nil {
				e.Logger.Warn("raw file not fetched yet", "url", f.URL, "error", err)
			}
		}}}
	c.restoreViews()
	e.mu.Lock()
	defer e.mu.Unlock()
	if old, ok := e.raws[decl.ID]; ok && old.root == decl.Root {
		c.close()
		return old, nil
	}
	if e.raws == nil {
		e.raws = map[string]*rawCache{}
	}
	e.raws[decl.ID] = c
	return c, nil
}

// restoreViews serves again the views of attempts that had one (an agent
// restarted; one that failed before is tried again).
func (c *rawCache) restoreViews() {
	entries, err := os.ReadDir(filepath.Join(c.dir, ".attempts"))
	if err != nil {
		return
	}
	for _, entry := range entries {
		id, ok := cutSuffix(entry.Name(), ".json")
		if !ok || !attemptIDName.MatchString(id) {
			continue
		}
		c.mu.Lock()
		_, served := c.views[id]
		c.mu.Unlock()
		if served {
			continue
		}
		body, err := os.ReadFile(filepath.Join(c.dir, ".attempts", entry.Name()))
		var wants map[string]rawWant
		if err == nil {
			err = json.Unmarshal(body, &wants)
		}
		if err == nil {
			err = c.openView(id, wants)
		}
		if err != nil {
			c.w.Logger.Warn("raw view not served again", "attempt_id", id, "error", err)
		}
	}
}

func cutSuffix(s, suffix string) (string, bool) {
	if len(s) < len(suffix) || s[len(s)-len(suffix):] != suffix {
		return "", false
	}
	return s[:len(s)-len(suffix)], true
}

func (c *rawCache) close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, v := range c.views {
		v.ln.Close()
	}
}

// attach makes the view of a launch's attempt (its streams' files), to be
// mounted at /kairo/raw in its container; the directory it is.
func (c *rawCache) attach(launch *store.Launch) (string, error) {
	wants := map[string]rawWant{}
	for _, s := range launch.Streams {
		for _, src := range s.Spec.Sources {
			for _, f := range src.Files {
				w := wants[f.URL]
				w.File = f
				w.Readings = append(w.Readings, rawWantReading{Format: src.Format, Field: src.Field})
				wants[f.URL] = w
			}
		}
	}
	body, err := json.Marshal(wants)
	if err != nil {
		return "", err
	}
	id := launch.Attempt.ID
	if !attemptIDName.MatchString(id) {
		return "", fmt.Errorf("attempt id %q", id)
	}
	if err := writeFileAtomic(filepath.Join(c.dir, ".attempts", id+".json"), body); err != nil {
		return "", err
	}
	if err := c.openView(id, wants); err != nil {
		return "", err
	}
	return filepath.Join(c.dir, "views", id), nil
}

// openView serves an attempt's view on its socket.
func (c *rawCache) openView(id string, wants map[string]rawWant) error {
	c.mu.Lock()
	if _, ok := c.views[id]; ok {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()
	dir := filepath.Join(c.dir, "views", id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	sock := filepath.Join(dir, loader.AgentSocket)
	if len(sock) > maxSocketPath {
		return fmt.Errorf("the raw view's socket %s is longer than %d bytes: a store root of at most %d characters", sock, maxSocketPath, maxSocketPath-(len(sock)-len(c.root)))
	}
	_ = os.Remove(sock)
	ln, err := net.Listen("unix", sock)
	if err != nil {
		return err
	}
	// the attempt's container maps its root to another user
	if err := os.Chmod(sock, 0o666); err != nil {
		ln.Close()
		return err
	}
	v := &rawView{attempt: id, dir: dir, wants: wants, ln: ln}
	c.mu.Lock()
	c.views[id] = v
	c.mu.Unlock()
	go func() {
		srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { c.serve(v, w, r) }), ReadHeaderTimeout: time.Minute}
		_ = srv.Serve(ln)
	}()
	return nil
}

// sweep takes away, now and then, the views of attempts that have not run
// here for rawViewGrace.
func (c *rawCache) sweep() {
	c.mu.Lock()
	if time.Since(c.swept) < 30*time.Second {
		c.mu.Unlock()
		return
	}
	c.swept = time.Now()
	c.mu.Unlock()
	c.restoreViews()
	c.mu.Lock()
	views := make([]*rawView, 0, len(c.views))
	for _, v := range c.views {
		views = append(views, v)
	}
	c.mu.Unlock()
	for _, v := range views {
		if c.w.attemptRunsHere(v.attempt) {
			v.gone = time.Time{}
			continue
		}
		if v.gone.IsZero() {
			v.gone = time.Now()
			continue
		}
		if time.Since(v.gone) < rawViewGrace {
			continue
		}
		v.ln.Close()
		c.mu.Lock()
		delete(c.views, v.attempt)
		c.mu.Unlock()
		if err := os.RemoveAll(v.dir); err != nil {
			c.w.Logger.Warn("raw view not removed", "attempt_id", v.attempt, "error", err)
			continue
		}
		_ = os.Remove(filepath.Join(c.dir, ".attempts", v.attempt+".json"))
	}
}

// ensureRequest is what a loader asks for: a file by its URL.
type ensureRequest struct {
	URL string `json:"url"`
}

// serve answers an attempt's loader (POST /ensure).
func (c *rawCache) serve(v *rawView, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || r.URL.Path != "/ensure" {
		http.NotFound(w, r)
		return
	}
	var req ensureRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil || req.URL == "" {
		writeEnsure(w, http.StatusBadRequest, loader.EnsureAnswer{Error: "a request names a file by its URL"})
		return
	}
	v.mu.Lock()
	want, ok := v.wants[req.URL]
	v.mu.Unlock()
	if !ok {
		writeEnsure(w, http.StatusForbidden, loader.EnsureAnswer{Error: req.URL + " is no file of this attempt's streams"})
		return
	}
	status, answer := c.handOver(r.Context(), v, want)
	writeEnsure(w, status, answer)
}

func writeEnsure(w http.ResponseWriter, status int, answer loader.EnsureAnswer) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(answer)
}

// handOver makes want ready and hands it to the attempt of view v: checked
// against the attempt's own declaration of it, linked into its view.
func (c *rawCache) handOver(ctx context.Context, v *rawView, want rawWant) (int, loader.EnsureAnswer) {
	fail := func(err error) (int, loader.EnsureAnswer) {
		answer := loader.EnsureAnswer{Error: err.Error(), Changed: errors.Is(err, loader.ErrChanged) || errors.Is(err, store.ErrRawChanged)}
		switch {
		case answer.Changed:
			return http.StatusConflict, answer
		case errors.Is(err, store.ErrStreamReportRefused):
			return http.StatusForbidden, answer
		}
		return http.StatusServiceUnavailable, answer
	}
	// held already in the view (handed before; evicted since or not): as it is
	if want.File.Identified() && sha256Name.MatchString(want.File.SHA256) {
		if info, err := os.Stat(filepath.Join(v.dir, want.File.SHA256)); err == nil && info.Mode().IsRegular() && info.Size() == want.File.Size {
			if err := sameAsDeclared(want.File, want.File); err != nil {
				return fail(err)
			}
			for _, reading := range want.Readings {
				c.count(v, want.File, reading)
			}
			return http.StatusOK, loader.EnsureAnswer{File: want.File, Name: want.File.SHA256}
		}
	}
	for again := 0; ; again++ {
		identity, kept, err := c.ready(ctx, want.File)
		if err != nil {
			return fail(err)
		}
		if err := sameAsDeclared(identity, want.File); err != nil {
			return fail(err)
		}
		if !kept {
			return http.StatusOK, loader.EnsureAnswer{File: identity, Fallback: true}
		}
		link := filepath.Join(v.dir, identity.SHA256)
		rawLinks.Lock()
		err = os.Link(filepath.Join(c.dir, identity.SHA256), link)
		rawLinks.Unlock()
		if errors.Is(err, os.ErrNotExist) && again == 0 {
			continue // evicted since: made ready again
		}
		if err != nil && !errors.Is(err, os.ErrExist) {
			return fail(err)
		}
		c.handed(v, want.File.URL, identity)
		for _, reading := range want.Readings {
			c.count(v, identity, reading)
		}
		return http.StatusOK, loader.EnsureAnswer{File: identity, Name: identity.SHA256}
	}
}

// sameAsDeclared: identity is the file a launch declared: its identity then
// (when it had one) and what was published of it.
func sameAsDeclared(identity, declared loader.File) error {
	if declared.Identified() && (declared.SHA256 != identity.SHA256 || declared.Size != identity.Size || declared.ChunkBytes != identity.ChunkBytes || !equalStrings(declared.Chunks, identity.Chunks)) {
		return fmt.Errorf("%w: %s is %s (%d bytes), identified as %s (%d bytes)", loader.ErrChanged, identity.URL, identity.SHA256, identity.Size, declared.SHA256, declared.Size)
	}
	pinned := identity
	pinned.PublishedSHA256, pinned.PublishedSize = declared.PublishedSHA256, declared.PublishedSize
	if !declared.Identified() && declared.SHA256 != "" && pinned.PublishedSHA256 == "" {
		pinned.PublishedSHA256 = declared.SHA256
	}
	if why := pinned.AsPublished(); why != "" {
		return fmt.Errorf("%w: %s", loader.ErrChanged, why)
	}
	return nil
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// ready makes the file of declared's URL ready once for every asker: its
// identity, and whether it is kept here.
func (c *rawCache) ready(ctx context.Context, declared loader.File) (loader.File, bool, error) {
	c.mu.Lock()
	f, ok := c.flights[declared.URL]
	if !ok {
		f = &rawFlight{done: make(chan struct{}), declared: declared}
		c.flights[declared.URL] = f
		go func() {
			// not an asker's context: others may wait on it
			f.identity, f.kept, f.err = c.make(context.Background(), declared)
			c.mu.Lock()
			delete(c.flights, declared.URL)
			c.mu.Unlock()
			close(f.done)
		}()
	}
	c.mu.Unlock()
	select {
	case <-f.done:
		// a flight failed on another declaration's pins: made again under
		// this one's
		if errors.Is(f.err, loader.ErrChanged) && !samePins(f.declared, declared) {
			return c.ready(ctx, declared)
		}
		return f.identity, f.kept, f.err
	case <-ctx.Done():
		return loader.File{}, false, ctx.Err()
	}
}

// samePins: a and b were declared with the same identity and published
// values.
func samePins(a, b loader.File) bool {
	sizeA, sizeB := int64(-1), int64(-1)
	if a.PublishedSize != nil {
		sizeA = *a.PublishedSize
	}
	if b.PublishedSize != nil {
		sizeB = *b.PublishedSize
	}
	return a.PublishedSHA256 == b.PublishedSHA256 && sizeA == sizeB && a.SHA256 == b.SHA256 && a.Size == b.Size && equalStrings(a.Chunks, b.Chunks)
}

// make makes a file ready: the identity the daemon has of it (or makes it),
// the file kept here when there is room.
func (c *rawCache) make(ctx context.Context, declared loader.File) (loader.File, bool, error) {
	canonical, found, err := c.identityOf(ctx, declared.URL)
	if err != nil {
		return loader.File{}, false, err
	}
	if found {
		canonical.Name = declared.Name
		if info, err := os.Stat(filepath.Join(c.dir, canonical.SHA256)); err == nil && info.Mode().IsRegular() && info.Size() == canonical.Size {
			if err := c.placed(ctx, canonical); err != nil && !errors.Is(err, store.ErrRawEvicting) {
				return loader.File{}, false, err
			}
			return canonical, true, nil
		}
	} else if identity, ok := c.keptAsPublished(declared); ok {
		// kept already under another URL: its bytes are what was published
		if err := c.placed(ctx, identity); err != nil && !errors.Is(err, store.ErrRawEvicting) {
			return loader.File{}, false, err
		}
		return identity, true, nil
	}
	size := declared.Size
	if found {
		size = canonical.Size
	}
	if size <= 0 {
		size = c.contentLength(ctx, declared)
	}
	// a size not known is not kept: room is held for what is known only
	if size <= 0 || !c.reserve(size) {
		return c.notKept(ctx, declared, canonical, found)
	}
	defer c.release(size)
	identity, partial, err := c.fetchInto(ctx, declared, canonical, found, size)
	if errors.Is(err, syscall.ENOSPC) || errors.Is(err, errPastReserved) {
		return c.notKept(ctx, declared, canonical, found)
	}
	if err != nil {
		return loader.File{}, false, err
	}
	// installed, then placed: a crash between leaves a file the daemon does
	// not know (made again over it), never a placement without its file;
	// one there already (fetched for another URL, checked then) stays
	installed := filepath.Join(c.dir, identity.SHA256)
	err = os.Link(partial, installed)
	os.Remove(partial)
	fresh := err == nil
	if err != nil && !errors.Is(err, os.ErrExist) {
		return loader.File{}, false, err
	}
	if err := c.placed(ctx, identity); err != nil {
		if fresh {
			removeUnheld(installed)
		}
		if !errors.Is(err, store.ErrRawEvicting) {
			return loader.File{}, false, err
		}
		// being evicted as it was made: read from upstream, its identity kept
		if err := c.retryDaemon(ctx, func() error { return c.w.Store.RawPlaced(ctx, c.w.ID, "", "", identity) }); err != nil {
			return loader.File{}, false, err
		}
		return identity, false, nil
	}
	c.w.Logger.Info("raw file kept", "url", identity.URL, "sha256", identity.SHA256, "bytes", identity.Size, "store_id", c.storeID)
	return identity, true, nil
}

// removeUnheld removes a raw file no view holds.
func removeUnheld(path string) {
	rawLinks.Lock()
	defer rawLinks.Unlock()
	if links, err := linkCount(path); err == nil && links == 1 {
		os.Remove(path)
	}
}

// keptAsPublished is the identity of a file kept here under the sha256 the
// declaration published (fetched for another URL), checked by reading it
// whole; ok false when there is none, or it is not that.
func (c *rawCache) keptAsPublished(declared loader.File) (loader.File, bool) {
	if !sha256Name.MatchString(declared.PublishedSHA256) {
		return loader.File{}, false
	}
	f, err := os.Open(filepath.Join(c.dir, declared.PublishedSHA256))
	if err != nil {
		return loader.File{}, false
	}
	defer f.Close()
	id := newIdentifier(loader.DefaultChunkBytes)
	if _, err := io.Copy(id, f); err != nil {
		return loader.File{}, false
	}
	identity := id.file(declared.Name, declared.URL)
	if identity.SHA256 != declared.PublishedSHA256 || sameAsDeclared(identity, declared) != nil {
		return loader.File{}, false
	}
	return identity, true
}

// notKept is a file not kept for want of room: identified (once) when it
// never was, to be read from upstream.
func (c *rawCache) notKept(ctx context.Context, declared, canonical loader.File, found bool) (loader.File, bool, error) {
	if found {
		return canonical, false, nil
	}
	identity, err := c.identifyInto(ctx, declared, loader.File{}, false, io.Discard)
	if err != nil {
		return loader.File{}, false, err
	}
	if err := c.retryDaemon(ctx, func() error { return c.w.Store.RawPlaced(ctx, c.w.ID, "", "", identity) }); err != nil {
		return loader.File{}, false, err
	}
	return identity, false, nil
}

// fetchInto fetches a file into .partial: the identity of the bytes
// written, and where they are.
func (c *rawCache) fetchInto(ctx context.Context, declared, canonical loader.File, found bool, reserved int64) (loader.File, string, error) {
	var suffix [8]byte
	_, _ = rand.Read(suffix[:])
	partial := filepath.Join(c.dir, ".partial", hex.EncodeToString(suffix[:]))
	out, err := os.OpenFile(partial, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return loader.File{}, "", err
	}
	identity, err := c.identifyInto(ctx, declared, canonical, found, &cappedWriter{w: out, room: reserved})
	if err == nil {
		err = out.Sync()
	}
	if cerr := out.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(partial)
		return loader.File{}, "", err
	}
	return identity, partial, nil
}

// identifyInto reads a file from upstream into out (checked against the
// identity the daemon has, when found), computing the identity of the bytes
// it writes: their whole sha256 and each chunk's, compared at the end with
// the identity found and with what was published of it.
func (c *rawCache) identifyInto(ctx context.Context, declared, canonical loader.File, found bool, out io.Writer) (loader.File, error) {
	read := declared
	if found {
		read = canonical
	} else {
		read.ChunkBytes, read.Chunks = 0, nil // made as it is read
	}
	r, err := c.fetch.Open(ctx, read, 0)
	if err != nil {
		return loader.File{}, err
	}
	defer r.Close()
	id := newIdentifier(loader.DefaultChunkBytes)
	if found {
		id = newIdentifier(canonical.ChunkBytes)
	}
	if _, err := io.Copy(io.MultiWriter(out, id), r); err != nil {
		return loader.File{}, err
	}
	identity := id.file(declared.Name, declared.URL)
	if found {
		if err := sameAsDeclared(identity, canonical); err != nil {
			return loader.File{}, err
		}
	} else if err := sameAsDeclared(identity, declared); err != nil {
		return loader.File{}, err
	}
	return identity, nil
}

// errPastReserved: a body longer than the room held for it.
var errPastReserved = errors.New("longer than the room held for it")

// cappedWriter writes up to room bytes.
type cappedWriter struct {
	w    io.Writer
	room int64
}

func (c *cappedWriter) Write(b []byte) (int, error) {
	if int64(len(b)) > c.room {
		return 0, errPastReserved
	}
	n, err := c.w.Write(b)
	c.room -= int64(n)
	return n, err
}

// identifier computes a file's identity from the bytes written to it.
type identifier struct {
	chunk  int64
	whole  hash.Hash
	part   hash.Hash
	inPart int64
	size   int64
	chunks []string
}

func newIdentifier(chunk int64) *identifier {
	return &identifier{chunk: chunk, whole: sha256.New(), part: sha256.New()}
}

func (id *identifier) Write(b []byte) (int, error) {
	n := len(b)
	id.whole.Write(b)
	id.size += int64(n)
	for len(b) > 0 {
		take := min(int64(len(b)), id.chunk-id.inPart)
		id.part.Write(b[:take])
		id.inPart += take
		b = b[take:]
		if id.inPart == id.chunk {
			id.chunks = append(id.chunks, hex.EncodeToString(id.part.Sum(nil)))
			id.part.Reset()
			id.inPart = 0
		}
	}
	return n, nil
}

func (id *identifier) file(name, url string) loader.File {
	chunks := id.chunks
	if id.inPart > 0 {
		chunks = append(chunks, hex.EncodeToString(id.part.Sum(nil)))
	}
	if chunks == nil {
		chunks = []string{}
	}
	return loader.File{Name: name, URL: url, Size: id.size, SHA256: hex.EncodeToString(id.whole.Sum(nil)), ChunkBytes: id.chunk, Chunks: chunks}
}

// identityOf is the identity the daemon has of url (asked again while the
// daemon is away).
func (c *rawCache) identityOf(ctx context.Context, url string) (loader.File, bool, error) {
	var f loader.File
	var found bool
	err := c.retryDaemon(ctx, func() error {
		var err error
		f, found, err = c.w.Store.RawIdentity(ctx, c.w.ID, url)
		return err
	})
	return f, found, err
}

// placed records a kept file with the daemon (again while it is away).
func (c *rawCache) placed(ctx context.Context, identity loader.File) error {
	return c.retryDaemon(ctx, func() error { return c.w.Store.RawPlaced(ctx, c.w.ID, c.storeID, c.root, identity) })
}

// retryDaemon calls the daemon until it answers (up to an hour): what it
// refuses is the answer.
func (c *rawCache) retryDaemon(ctx context.Context, call func() error) error {
	pause := time.Second
	deadline := time.Now().Add(time.Hour)
	for {
		err := call()
		if err == nil || errors.Is(err, store.ErrRawChanged) || errors.Is(err, store.ErrStreamReportRefused) || errors.Is(err, store.ErrRawEvicting) || time.Now().After(deadline) {
			return err
		}
		c.w.Logger.Warn("raw cache: the daemon did not answer", "error", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pause):
		}
		pause = min(pause*2, time.Minute)
	}
}

// reserve holds size bytes of the store for a fetch when there is room
// above its floor (less what other fetches hold); a size not known (0) is
// taken as fitting.
func (c *rawCache) reserve(size int64) bool {
	_, free, err := diskUsage(c.root)
	if err != nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if size > 0 && size > free-c.floor()-c.reserved {
		return false
	}
	c.reserved += size
	return true
}

func (c *rawCache) release(size int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reserved -= size
}

// count counts, once, the records a file the attempt of view v was handed
// holds read as reading says, from its link in the view (it stays while the
// attempt runs), checking its bytes against its identity as it reads them;
// only a count of bytes so checked is recorded with the daemon.
func (c *rawCache) count(v *rawView, identity loader.File, reading rawWantReading) {
	key, err := loader.CountKey(reading.Format, reading.Field, identity.Name)
	if err != nil {
		return
	}
	marker := identity.SHA256 + "|" + key
	c.mu.Lock()
	if c.counted[marker] {
		c.mu.Unlock()
		return
	}
	c.counted[marker] = true
	c.mu.Unlock()
	go func() {
		err := c.countChecked(v, identity, reading)
		if err != nil {
			c.w.Logger.Warn("raw file's records not counted", "url", identity.URL, "error", err)
			c.mu.Lock()
			delete(c.counted, marker)
			c.mu.Unlock()
		}
	}()
}

func (c *rawCache) countChecked(v *rawView, identity loader.File, reading rawWantReading) error {
	f, err := os.Open(filepath.Join(v.dir, identity.SHA256))
	if err != nil {
		return err
	}
	defer f.Close()
	id := newIdentifier(identity.ChunkBytes)
	n, err := loader.CountRecords(io.TeeReader(f, id), identity.Name, reading.Format, reading.Field)
	if err != nil {
		return err
	}
	// what the decoder did not read (a framing reading to the end reads all)
	if _, err := io.Copy(id, f); err != nil {
		return err
	}
	if got := id.file(identity.Name, identity.URL); got.SHA256 != identity.SHA256 || got.Size != identity.Size || !equalStrings(got.Chunks, identity.Chunks) {
		return fmt.Errorf("%w: the kept %s is not its identity: not counted", loader.ErrChanged, identity.URL)
	}
	return c.retryDaemon(context.Background(), func() error {
		return c.w.Store.RawRecords(context.Background(), c.w.ID, store.RecordsCount{URL: identity.URL, SHA256: identity.SHA256, Format: reading.Format, Field: reading.Field, Records: n})
	})
}

// contentLength is what upstream says a file's size is (0: not said).
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

// rawLinks orders handing raw files to views (a link) and evicting them:
// a file is evicted only while no view holds it.
var rawLinks sync.Mutex

// writeFileAtomic writes path whole or not at all (a crash leaves the old).
func writeFileAtomic(path string, body []byte) error {
	tmp := path + ".new"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, err = f.Write(body)
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp, path)
	}
	if err != nil {
		os.Remove(tmp)
	}
	return err
}

// removeRaw evicts the raw file sha of the store at root (what attempts
// were handed of it stays in their views until they end).
func removeRaw(root, sha string) error {
	if !sha256Name.MatchString(sha) {
		return fmt.Errorf("%w: raw file %q", errOutsideRoot, sha)
	}
	rawLinks.Lock()
	defer rawLinks.Unlock()
	// held by attempts' views: removing its name frees nothing
	if links, err := linkCount(filepath.Join(root, rawDirName, sha)); err == nil && links > 1 {
		return fmt.Errorf("%w: raw file %s is held by %d attempts", errEvictInUse, sha, links-1)
	}
	err := os.Remove(filepath.Join(root, rawDirName, sha))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}
