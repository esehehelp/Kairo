package executor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"kairo/internal/machine"
	"kairo/internal/store"
)

// A packed object is copied to the durable store by an executor serving it
// (store.CopiesToMake): read on this node when the pack is on a store this
// executor serves, else fetched from the pack's agent (PackServer) over
// HTTPS with the token the daemon issued, an interrupted fetch resumed with
// a range request. The pack and its file list go to
// <root>/objects/<YYYYMM>/<object id>.tar.zst(.files.tsv), each written as
// .partial; once both are whole they are synced and read back from disk
// around the cache (hashUncached), and only if the size and sha256 match
// the pack's are they renamed, the directory synced (on Windows, each file
// flushed) and the copy recorded verified, once the files under their final
// names are the ones read back. A copy that does not match has its .partial
// files removed and is fetched again later (roundProblem); so has one renamed
// but not recorded that does not match. These, and only these, are what
// Kairo ever deletes on a durable store: its own writes, never recorded,
// never something kept there. A pack whose .partial is whole already is not
// fetched again; a source that yields more than the pack's size, or a fetch
// that receives nothing for stallTimeout, fails.

const (
	objectsDirName = "objects"
	// copyInterval: how often what is to be copied is asked for.
	copyInterval = time.Minute
	// copyRoundTimeout bounds one copying round.
	copyRoundTimeout = 12 * time.Hour
	// maxListBytes bounds a fetched file list.
	maxListBytes = 1 << 30
)

// stallTimeout: a fetch that receives nothing for this long is given up (a
// variable for tests).
var stallTimeout = 5 * time.Minute

var (
	// errCopyNotHere: the durable store is one this executor does not write
	// (inside a WSL distro).
	errCopyNotHere = errors.New("copies to a store inside a WSL distro are not made yet")
	// errCopyMismatch: a copy read back does not match the pack.
	errCopyMismatch = errors.New("the copy read back does not match the pack")
)

// makeCopies copies, in the background, what the daemon says is to be copied
// now (at most every copyInterval), once the daemon has answered for stores.
func (e *Local) makeCopies() {
	e.startRound(&e.copies, copyInterval, copyRoundTimeout, e.copyDue)
}

// copyDue makes every copy the daemon says is due on a usable durable store
// this executor serves at the ordered root, one at a time, and records each.
// A failure is warned once per object (again when it changes) and tried
// again later (roundProblem).
func (e *Local) copyDue(ctx context.Context) {
	orders, err := e.Store.CopiesToMake(ctx, e.ID)
	if err != nil {
		e.copyProblem("", "copies to make not read", err)
		return
	}
	e.copyProblem("", "", nil)
	served := map[string]servedStore{}
	for _, s := range e.servedStores() {
		served[s.decl.ID] = s
	}
	for _, o := range orders {
		s, ok := served[o.StoreID]
		if !ok || !s.usable || s.decl.Class != machine.StoreDurable || s.decl.Root != o.Root || !e.due(&e.copies, o.ObjectID) {
			continue
		}
		started := time.Now()
		made, err := e.copyOne(ctx, s, o, served)
		if err == nil {
			err = e.Store.RecordCopy(ctx, made)
		}
		if err != nil {
			e.copyProblem(o.ObjectID, "not copied", err)
			continue
		}
		e.copyProblem(o.ObjectID, "", nil)
		e.Logger.Info("copy verified", "object_id", o.ObjectID, "store_id", o.StoreID, "path", made.Path, "bytes", made.Bytes,
			"local", o.Source.Local, "seconds", time.Since(started).Seconds())
	}
}

// copyOne writes and reads back one copy (see above) and returns what to
// record. A copy already renamed (recorded after all, or not yet) is read
// back as it is.
func (e *Local) copyOne(ctx context.Context, s servedStore, o store.CopyOrder, served map[string]servedStore) (store.CopyMade, error) {
	made := store.CopyMade{ObjectID: o.ObjectID, StoreID: o.StoreID, ExecutorID: e.ID}
	if _, ok := s.fs.(localStoreFS); !ok {
		return made, errCopyNotHere
	}
	if !validScratchID(o.ObjectID) || !machine.ValidMonth(o.Month) {
		return made, fmt.Errorf("%w: object %q, month %q", errOutsideRoot, o.ObjectID, o.Month)
	}
	root := filepath.Clean(s.decl.Root)
	r, err := openDirPath(root, false)
	if err != nil {
		return made, err
	}
	defer r.Close()
	dir, err := openMade(r, objectsDirName, o.Month)
	if err != nil {
		return made, err
	}
	defer dir.Close()
	packName := o.ObjectID + packSuffix
	listName := packName + manifestSuffix
	dirPath := filepath.Join(root, objectsDirName, o.Month)
	made.Path, made.ManifestPath = filepath.Join(dirPath, packName), filepath.Join(dirPath, listName)
	if fi, err := dir.Lstat(packName); err == nil && fi.Mode().IsRegular() {
		if fi2, err := dir.Lstat(listName); err == nil && fi2.Mode().IsRegular() {
			return verifyCopy(dir, dirPath, packName, listName, o, made, false)
		}
	}
	if err = e.fetch(ctx, o, served, dir, packName, listName); err != nil {
		return made, err
	}
	return verifyCopy(dir, dirPath, packName+partialSuffix, listName+partialSuffix, o, made, true)
}

// verifyCopy reads the pack and list named back around the cache and checks
// them against the order; partial ones that match are renamed (the list
// first) and the renames synced; ones that do not match, partial or renamed
// but not recorded (an order is only for a copy not recorded), are removed.
// What is under the final names must then be the files read back.
func verifyCopy(dir *os.Root, dirPath, packName, listName string, o store.CopyOrder, made store.CopyMade, partial bool) (store.CopyMade, error) {
	var err error
	var listRead, packRead os.FileInfo
	made.ManifestSHA256, _, listRead, err = hashUncachedFile(dir, listName, filepath.Join(dirPath, listName))
	if err == nil {
		made.SHA256, made.Bytes, packRead, err = hashUncachedFile(dir, packName, filepath.Join(dirPath, packName))
	}
	if err != nil {
		return made, err
	}
	if made.Bytes != o.PackBytes || made.SHA256 != o.PackSHA256 || made.ManifestSHA256 != o.ManifestSHA256 {
		_ = dir.Remove(packName)
		_ = dir.Remove(listName)
		return made, fmt.Errorf("%w: %d bytes, sha256 %s (list %s); the pack has %d bytes, sha256 %s (list %s)",
			errCopyMismatch, made.Bytes, made.SHA256, made.ManifestSHA256, o.PackBytes, o.PackSHA256, o.ManifestSHA256)
	}
	finalPack := strings.TrimSuffix(packName, partialSuffix)
	finalList := finalPack + manifestSuffix
	if partial {
		if err = dir.Rename(listName, finalList); err != nil {
			return made, err
		}
		if err = dir.Rename(packName, finalPack); err != nil {
			return made, err
		}
	}
	if err = syncRenamed(dir, finalList, finalPack); err != nil {
		return made, err
	}
	want := map[string]string{finalList: o.ManifestSHA256, finalPack: o.PackSHA256}
	for name, read := range map[string]os.FileInfo{finalList: listRead, finalPack: packRead} {
		f, err := dir.OpenFile(name, os.O_RDONLY|markerOpenFlags, 0)
		if err != nil {
			return made, err
		}
		there, err := f.Stat()
		f.Close()
		if err != nil {
			return made, err
		}
		if os.SameFile(there, read) && there.Size() == read.Size() && there.ModTime().Equal(read.ModTime()) {
			continue
		}
		// Not the file read back: written over meanwhile, or on a filesystem
		// whose file ids do not survive a rename (exFAT, where an id is where
		// the file's directory entry is). Read it back again under its final
		// name: what is there must be what the order says.
		sum, n, _, err := hashUncachedFile(dir, name, filepath.Join(dirPath, name))
		if err != nil {
			return made, err
		}
		if sum != want[name] || name == finalPack && n != o.PackBytes {
			return made, fmt.Errorf("%w: %s is not the file read back", errRaced, filepath.Join(dirPath, name))
		}
	}
	return made, nil
}

// openMade opens (making them) the directories names under r, one at a
// time, each a plain directory.
func openMade(r *os.Root, names ...string) (*os.Root, error) {
	cur := r
	for i, name := range names {
		if err := cur.Mkdir(name, 0o755); err != nil && !errors.Is(err, fs.ErrExist) {
			if i > 0 {
				cur.Close()
			}
			return nil, err
		}
		next, err := openPlainDir(cur, name)
		if i > 0 {
			cur.Close()
		}
		if err != nil {
			return nil, err
		}
		cur = next
	}
	return cur, nil
}

// fetch writes the list (whole, again) and the pack (resumed when part of it
// is there) as .partial files in dir, synced.
func (e *Local) fetch(ctx context.Context, o store.CopyOrder, served map[string]servedStore, dir *os.Root, packName, listName string) error {
	var open func(name string, offset int64) (io.ReadCloser, int64, error)
	if o.Source.Local {
		src, ok := served[o.Source.StoreID]
		if !ok || !src.usable || src.decl.Root != o.Source.Root {
			return fmt.Errorf("the pack's store %s is not served here at %s", o.Source.StoreID, o.Source.Root)
		}
		if _, isLocal := src.fs.(localStoreFS); !isLocal {
			return errPackNotHere
		}
		open = func(name string, offset int64) (io.ReadCloser, int64, error) {
			if o.Source.Month != "" {
				return openLocalObject(o.Source.Root, o.Source.Month, name, offset)
			}
			return openLocalPack(o.Source.Root, name, offset)
		}
	} else {
		if o.Source.Addr == "" {
			return fmt.Errorf("the agent holding the pack (store %s) advertises no pack_addr", o.Source.StoreID)
		}
		if e.PackClient == nil {
			return errors.New("this agent has no client to fetch packs with")
		}
		open = func(name string, offset int64) (io.ReadCloser, int64, error) {
			return e.fetchRemote(ctx, o, name, offset)
		}
	}
	if err := writeFrom(ctx, dir, listName+partialSuffix, open, listName, false, -1); err != nil {
		return err
	}
	return writeFrom(ctx, dir, packName+partialSuffix, open, packName, true, o.PackBytes)
}

// writeFrom writes the source file name into partial in dir: from where an
// earlier write stopped when resume is set (the partial is extended), else
// from the start. The source says where it starts (offset); one that starts
// anywhere else than asked makes the write start over from there. size, when
// known (not negative), is the source's: a partial that long is whole
// already, one longer starts over, and the source may yield no more.
func writeFrom(ctx context.Context, dir *os.Root, partial string, open func(string, int64) (io.ReadCloser, int64, error), name string, resume bool, size int64) error {
	var have int64
	if fi, err := dir.Lstat(partial); err == nil {
		if !fi.Mode().IsRegular() {
			if err := dir.RemoveAll(partial); err != nil {
				return err
			}
		} else if resume {
			have = fi.Size()
		}
	}
	if size >= 0 && have == size && have > 0 {
		return nil // whole already: read back as it is
	}
	if size >= 0 && have > size {
		have = 0
	}
	src, from, err := open(name, have)
	if err != nil {
		return err
	}
	defer src.Close()
	flags := os.O_WRONLY | os.O_CREATE
	if from == 0 {
		flags |= os.O_TRUNC
	}
	f, err := dir.OpenFile(partial, flags, 0o644)
	if err != nil {
		return err
	}
	if from > 0 {
		if _, err = f.Seek(from, io.SeekStart); err != nil {
			f.Close()
			return err
		}
	}
	limit := int64(maxListBytes)
	if size >= 0 {
		limit = size - from
	}
	n, err := io.Copy(f, io.LimitReader(ctxReader{ctx, src}, limit+1))
	if err == nil && n > limit {
		err = fmt.Errorf("%s yields more than its %d bytes", name, from+limit)
	}
	if err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// ctxReader stops a copy when its context ends.
type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (c ctxReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(p)
}

// openLocalPack opens <root>/packs/name of a store this executor serves, a
// regular file reached through handles, at offset.
func openLocalPack(root, name string, offset int64) (io.ReadCloser, int64, error) {
	f, err := openPackFile(root, name)
	if err != nil {
		return nil, 0, err
	}
	if offset > 0 {
		if _, err = f.Seek(offset, io.SeekStart); err != nil {
			f.Close()
			return nil, 0, err
		}
	}
	return f, offset, nil
}

// openLocalObject opens a durable copy, <root>/objects/<month>/name of a
// store this executor serves, at offset.
func openLocalObject(root, month, name string, offset int64) (io.ReadCloser, int64, error) {
	f, err := openObjectFile(root, month, name)
	if err != nil {
		return nil, 0, err
	}
	if offset > 0 {
		if _, err = f.Seek(offset, io.SeekStart); err != nil {
			f.Close()
			return nil, 0, err
		}
	}
	return f, offset, nil
}

// openPackFile opens <root>/packs/name: the root reached one directory at a
// time, the file a regular file (not a link) and the one checked.
func openPackFile(root, name string) (*os.File, error) {
	if !strings.HasSuffix(name, packSuffix) && !strings.HasSuffix(name, packSuffix+manifestSuffix) ||
		!validScratchID(strings.TrimSuffix(strings.TrimSuffix(name, manifestSuffix), packSuffix)) {
		return nil, fmt.Errorf("%w: pack %q", errOutsideRoot, name)
	}
	r, err := openDirPath(filepath.Clean(root), false)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	pr, err := openPlainDir(r, packsDirName)
	if err != nil {
		return nil, err
	}
	defer pr.Close()
	fi, err := pr.Lstat(name)
	if err != nil {
		return nil, err
	}
	if isLink(fi) || !fi.Mode().IsRegular() {
		return nil, fmt.Errorf("%w: %s is not a regular file", errScratchLink, name)
	}
	f, err := pr.OpenFile(name, os.O_RDONLY|markerOpenFlags, 0)
	if err != nil {
		return nil, err
	}
	opened, err := f.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(fi, opened) {
		f.Close()
		return nil, fmt.Errorf("%w: %s", errRaced, name)
	}
	return f, nil
}

// fetchRemote asks the pack's agent for name from offset (a range request
// when offset > 0) and returns the body with where it starts. The request is
// given up once nothing has arrived for stallTimeout.
func (e *Local) fetchRemote(ctx context.Context, o store.CopyOrder, name string, offset int64) (io.ReadCloser, int64, error) {
	rctx, cancel := context.WithCancel(ctx)
	stall := time.AfterFunc(stallTimeout, cancel)
	giveUp := func() {
		stall.Stop()
		cancel()
	}
	req, err := http.NewRequestWithContext(rctx, http.MethodGet, "https://"+o.Source.Addr+"/packs/"+name, nil)
	if err != nil {
		giveUp()
		return nil, 0, err
	}
	req.Header.Set("Authorization", "Bearer "+o.Source.Token)
	if offset > 0 {
		req.Header.Set("Range", "bytes="+strconv.FormatInt(offset, 10)+"-")
	}
	resp, err := e.PackClient.Do(req)
	if err != nil {
		giveUp()
		return nil, 0, err
	}
	body := &watchedBody{ReadCloser: resp.Body, stall: stall, cancel: cancel}
	switch {
	case resp.StatusCode == http.StatusOK:
		return body, 0, nil
	case resp.StatusCode == http.StatusPartialContent && offset > 0:
		var start int64
		if _, scanErr := fmt.Sscanf(resp.Header.Get("Content-Range"), "bytes %d-", &start); scanErr != nil || start != offset {
			body.Close()
			return nil, 0, fmt.Errorf("fetch %s: range %q, asked from %d", name, resp.Header.Get("Content-Range"), offset)
		}
		return body, offset, nil
	case resp.StatusCode == http.StatusRequestedRangeNotSatisfiable:
		// The partial is longer than the pack: start over.
		body.Close()
		return e.fetchRemote(ctx, o, name, 0)
	}
	text, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	body.Close()
	return nil, 0, fmt.Errorf("fetch %s from %s: %s: %s", name, o.Source.Addr, resp.Status, strings.TrimSpace(string(text)))
}

// watchedBody is a fetched body whose request is given up once nothing has
// arrived for stallTimeout.
type watchedBody struct {
	io.ReadCloser
	stall  *time.Timer
	cancel context.CancelFunc
}

func (w *watchedBody) Read(p []byte) (int, error) {
	n, err := w.ReadCloser.Read(p)
	if n > 0 {
		w.stall.Reset(stallTimeout)
	}
	return n, err
}

func (w *watchedBody) Close() error {
	w.stall.Stop()
	err := w.ReadCloser.Close()
	w.cancel()
	return err
}

// copyProblem is roundProblem for copies.
func (e *Local) copyProblem(objectID, message string, err error) {
	e.roundProblem(&e.copies, objectID, message, err)
}

// PackServer hands this node's packs to the agents copying them to a durable
// store: GET /packs/<object id>.tar.zst and /packs/<object id>.tar.zst.files.tsv
// with "Authorization: Bearer <token>", each request asked of the daemon
// (store.AuthorizePackRead), which says which store holds the pack; the pack
// is read from that store as this agent's executors serve it, through
// handles, with range requests answered. Anything else is 404, and a request
// the daemon does not authorize 403, saying nothing more.
type PackServer struct {
	Store     Coordinator
	NodeID    string
	Executors []*Local
	Logger    *slog.Logger

	mu     sync.Mutex
	warned map[string]time.Time
}

// warn logs a refusal at most once a minute per object.
func (p *PackServer) warn(objectID, message string, args ...any) {
	p.mu.Lock()
	if p.warned == nil {
		p.warned = map[string]time.Time{}
	}
	if at, ok := p.warned[objectID]; ok && time.Since(at) < time.Minute {
		p.mu.Unlock()
		return
	}
	for id, at := range p.warned {
		if time.Since(at) >= time.Minute {
			delete(p.warned, id)
		}
	}
	p.warned[objectID] = time.Now()
	p.mu.Unlock()
	p.Logger.Warn(message, append([]any{"object_id", objectID}, args...)...)
}

func (p *PackServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	name, ok := strings.CutPrefix(r.URL.Path, "/packs/")
	objectID := strings.TrimSuffix(strings.TrimSuffix(name, manifestSuffix), packSuffix)
	if !ok || strings.Contains(name, "/") || !validScratchID(objectID) || name != objectID+packSuffix && name != objectID+packSuffix+manifestSuffix {
		http.NotFound(w, r)
		return
	}
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok || token == "" {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	read, err := p.Store.AuthorizePackRead(r.Context(), p.NodeID, objectID, token)
	if err != nil {
		p.warn(objectID, "pack read refused", "remote", r.RemoteAddr, "error", err)
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	var root string
	for _, e := range p.Executors {
		for _, s := range e.servedStores() {
			if _, isLocal := s.fs.(localStoreFS); isLocal && s.usable && s.decl.ID == read.StoreID && s.decl.Root == read.Root {
				root = s.decl.Root
			}
		}
	}
	if root == "" {
		http.Error(w, "the pack's store is not served here", http.StatusNotFound)
		return
	}
	open := func() (*os.File, error) { return openPackFile(root, name) }
	if read.Month != "" {
		// a durable copy, for a fetch (store/placements.go)
		open = func() (*os.File, error) { return openObjectFile(root, read.Month, name) }
	}
	f, err := open()
	if err != nil {
		p.warn(objectID, "pack not opened", "error", err)
		http.NotFound(w, r)
		return
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		http.Error(w, "pack not read", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	http.ServeContent(w, r, name, fi.ModTime(), f)
}

// hashOf is the sha256 (hex) and size of what r yields.
func hashOf(r io.Reader) (string, int64, error) {
	h := sha256.New()
	n, err := io.Copy(h, r)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}
