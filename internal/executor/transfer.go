package executor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"kairo/internal/agentops"
	"kairo/internal/bench"
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
	// errCopyMismatch: a copy read back does not match the pack.
	errCopyMismatch = errors.New("the copy read back does not match the pack")
)

// makeCopies copies, in the background, what the daemon says is to be copied
// now (at most every copyInterval), once the daemon has answered for stores.
func (e *storeWorker) makeCopies() {
	e.startRound(&e.copies, copyInterval, copyRoundTimeout, e.copyDue)
}

// copyDue makes every copy the daemon says is due on a usable durable store
// this executor serves at the ordered root, one at a time, and records each.
// A failure is warned once per object (again when it changes) and tried
// again later (roundProblem).
func (e *storeWorker) copyDue(ctx context.Context) {
	// Asked again after each copy: what came due meanwhile (an object
	// nowhere durable yet, which goes before any replica) is not left
	// behind a long copy. A copy or description tried once in this round is
	// not tried again in it.
	tried := map[string]bool{}
	for ctx.Err() == nil {
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
		var next *store.CopyOrder
		for i, o := range orders {
			key := o.ObjectID + "/" + o.StoreID
			s, ok := served[o.StoreID]
			if tried[key] || !ok || !s.usable || s.decl.Class != machine.StoreDurable || s.decl.Root != o.Root || !e.due(&e.copies, o.ObjectID) {
				continue
			}
			if o.Shed {
				tried[key] = true
				e.shedCopy(ctx, s, o)
				continue
			}
			if !o.DescribeOnly {
				if next == nil {
					next = &orders[i]
				}
				continue
			}
			tried[key] = true
			err := describeCopy(s, o)
			if err == nil {
				err = e.Store.RecordDescribed(ctx, e.ID, o.ObjectID, o.StoreID)
			}
			if err != nil {
				e.copyProblem(o.ObjectID, "not described", err)
				continue
			}
			e.copyProblem(o.ObjectID, "", nil)
		}
		if next == nil {
			return
		}
		o := *next
		tried[o.ObjectID+"/"+o.StoreID] = true
		s := served[o.StoreID]
		started := time.Now()
		made, err := e.copyOne(ctx, s, o, served)
		if err == nil && o.Description != nil {
			if err = describeCopy(s, o); err == nil {
				made.Described = true
			}
		}
		if err == nil {
			err = e.Store.RecordCopy(ctx, made)
		}
		if err != nil {
			e.copyProblem(o.ObjectID, "not copied", err)
			continue
		}
		e.copyProblem(o.ObjectID, "", nil)
		e.Logger.Info("copy verified", "object_id", o.ObjectID, "store_id", o.StoreID, "path", made.Path, "bytes", made.Bytes,
			"local", o.Source.Local, "replica_of", o.Source.Month != "", "seconds", time.Since(started).Seconds())
	}
}

// descriptionSuffix names a durable copy's description beside it
// (store/descriptions.go).
const descriptionSuffix = ".object.json"

// shedCopy removes a copy of a durable store below its reserve
// (store/shed.go), or of a dropped object (store/data_drop.go: the file list
// beside it sent first when the daemon has not kept it yet): claimed first,
// so a copy the daemon no longer lets go is left alone; its files then
// removed through the store's handles. Files a failed removal leaves are
// adopted again by the next inventory.
func (e *storeWorker) shedCopy(ctx context.Context, s servedStore, o store.CopyOrder) {
	if _, isLocal := s.fs.(localStoreFS); !isLocal {
		return
	}
	if o.SendList {
		list, err := readCopyList(s.decl.Root, o.Month, o.ObjectID)
		if err == nil {
			err = e.Store.KeepIdentity(ctx, e.ID, o.ObjectID, o.StoreID, list)
		}
		if err != nil {
			e.copyProblem(o.ObjectID, "file list of a dropped object not kept", err)
			return
		}
	}
	if err := e.Store.ClaimShed(ctx, e.ID, o.ObjectID, o.StoreID); err != nil {
		e.copyProblem(o.ObjectID, "copy not shed", err)
		return
	}
	if err := removeObjectCopy(s.decl.Root, o.Month, o.ObjectID); err != nil {
		e.copyProblem(o.ObjectID, "shed copy's files not removed", err)
		return
	}
	e.copyProblem(o.ObjectID, "", nil)
	if o.Drop {
		e.Logger.Info("copy removed: its object is dropped (only its file list is kept)", "object_id", o.ObjectID, "store_id", o.StoreID, "pack_bytes", o.PackBytes)
		return
	}
	e.Logger.Info("copy shed: its store is below its reserve", "object_id", o.ObjectID, "store_id", o.StoreID, "pack_bytes", o.PackBytes)
}

// readCopyList reads the file list beside the copy of id made in month on
// the durable store at root (<id>.tar.zst.files.tsv), of at most
// agentops.MaxIdentityList bytes.
func readCopyList(root, month, id string) (string, error) {
	if !machine.ValidMonth(month) || !machine.ScratchIDValid(id) {
		return "", fmt.Errorf("%w: object %q, month %q", errOutsideRoot, id, month)
	}
	r, err := openDirPath(filepath.Join(root, "objects", month), false)
	if err != nil {
		return "", err
	}
	defer r.Close()
	f, err := r.Open(id + packSuffix + manifestSuffix)
	if err != nil {
		return "", err
	}
	defer f.Close()
	body, err := io.ReadAll(io.LimitReader(f, agentops.MaxIdentityList+1))
	if err != nil {
		return "", err
	}
	if len(body) > agentops.MaxIdentityList {
		return "", fmt.Errorf("the file list of %s is more than %d bytes", id, agentops.MaxIdentityList)
	}
	return string(body), nil
}

// removeObjectCopy removes the copy of id made in month on the durable store
// at root: <id>.tar.zst, its file list and its description.
func removeObjectCopy(root, month, id string) error {
	if !machine.ValidMonth(month) || !machine.ScratchIDValid(id) {
		return fmt.Errorf("%w: object %q, month %q", errOutsideRoot, id, month)
	}
	r, err := openDirPath(filepath.Join(root, "objects", month), false)
	if err != nil {
		return err
	}
	defer r.Close()
	for _, name := range []string{id + packSuffix + descriptionSuffix, id + packSuffix + manifestSuffix, id + packSuffix} {
		if err := r.Remove(name); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}
	return nil
}

// describeCopy writes o's description beside its copy on s
// (<root>/objects/<month>/<object id>.tar.zst.object.json), synced: what the
// copy is, readable without the daemon. The copy must be there.
func describeCopy(s servedStore, o store.CopyOrder) error {
	if o.Description == nil {
		return errors.New("no description to write")
	}
	if !validScratchID(o.ObjectID) || !machine.ValidMonth(o.Month) {
		return fmt.Errorf("%w: object %q, month %q", errOutsideRoot, o.ObjectID, o.Month)
	}
	r, err := openDirPath(filepath.Clean(s.decl.Root), false)
	if err != nil {
		return err
	}
	defer r.Close()
	dir, err := openMade(r, objectsDirName, o.Month)
	if err != nil {
		return err
	}
	defer dir.Close()
	packName := o.ObjectID + packSuffix
	if fi, err := dir.Lstat(packName); err != nil || !fi.Mode().IsRegular() {
		return fmt.Errorf("the copy %s is not there to describe", packName)
	}
	body, err := json.MarshalIndent(o.Description, "", "  ")
	if err != nil {
		return err
	}
	name := packName + descriptionSuffix
	if err = writeRootFile(dir, name+partialSuffix, append(body, '\n')); err != nil {
		return err
	}
	if err = dir.Rename(name+partialSuffix, name); err != nil {
		return err
	}
	return syncRenamed(dir, name)
}

// copyOne writes and reads back one copy (see above) and returns what to
// record. A copy already renamed (recorded after all, or not yet) is read
// back as it is.
func (e *storeWorker) copyOne(ctx context.Context, s servedStore, o store.CopyOrder, served map[string]servedStore) (store.CopyMade, error) {
	made := store.CopyMade{ObjectID: o.ObjectID, StoreID: o.StoreID, ExecutorID: e.ID}
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
func (e *storeWorker) fetch(ctx context.Context, o store.CopyOrder, served map[string]servedStore, dir *os.Root, packName, listName string) error {
	return e.fetchAs(ctx, o, served, dir, packName, listName, packName, listName)
}

// fetchAs is fetch writing the pack and list read as packName and listName
// under names of its own (localPack, localList).
func (e *storeWorker) fetchAs(ctx context.Context, o store.CopyOrder, served map[string]servedStore, dir *os.Root, packName, listName, localPack, localList string) error {
	var open func(name string, offset int64) (io.ReadCloser, int64, error)
	if o.Source.Local {
		src, ok := served[o.Source.StoreID]
		if !ok || !src.usable || src.decl.Root != o.Source.Root {
			return fmt.Errorf("the pack's store %s is not served here at %s", o.Source.StoreID, o.Source.Root)
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
	if err := writeFrom(ctx, dir, localList+partialSuffix, open, listName, false, -1); err != nil {
		return err
	}
	return writeFrom(ctx, dir, localPack+partialSuffix, open, packName, true, o.PackBytes)
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
func (e *storeWorker) fetchRemote(ctx context.Context, o store.CopyOrder, name string, offset int64) (io.ReadCloser, int64, error) {
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
	body := &watchedBody{ReadCloser: resp.Body, stall: stall, cancel: cancel, addr: o.Source.Addr, start: time.Now()}
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

// TransferObserved hears what a fetch from another agent's transfer
// endpoint (addr) took: its bytes and its time (nil: nobody listens). The
// fabric measures its links by it (issue #47).
var TransferObserved func(addr string, bytes int64, took time.Duration)

// watchedBody is a fetched body whose request is given up once nothing has
// arrived for stallTimeout; what it read, and in what time, is told to
// TransferObserved when it is closed.
type watchedBody struct {
	io.ReadCloser
	stall  *time.Timer
	cancel context.CancelFunc
	addr   string
	start  time.Time
	read   int64
}

func (w *watchedBody) Read(p []byte) (int, error) {
	n, err := w.ReadCloser.Read(p)
	if n > 0 {
		w.stall.Reset(stallTimeout)
		w.read += int64(n)
	}
	return n, err
}

func (w *watchedBody) Close() error {
	w.stall.Stop()
	err := w.ReadCloser.Close()
	w.cancel()
	if observe := TransferObserved; observe != nil && w.read > 0 {
		observe(w.addr, w.read, time.Since(w.start))
	}
	return err
}

// copyProblem is roundProblem for copies.
func (e *storeWorker) copyProblem(objectID, message string, err error) {
	e.roundProblem(&e.copies, objectID, message, err)
}

// PackServer hands this node's packs to the agents copying them to a durable
// store: GET /packs/<object id>.tar.zst and /packs/<object id>.tar.zst.files.tsv
// with "Authorization: Bearer <token>", each request asked of the daemon
// (store.AuthorizePackRead), which says which store holds the pack; the pack
// is read from that store as this agent's executors serve it, through
// handles, with range requests answered. It hands the raw files its raw
// caches keep the same way (GET /raw/<sha256>, rawpeers.go). Anything else
// is 404, and a request the daemon does not authorize 403, saying nothing
// more.
type PackServer struct {
	Store     Coordinator
	NodeID    string
	Executors []*Local
	Logger    *slog.Logger
	// NoBench refuses network benches (an observe-only agent runs no work
	// and serves none).
	NoBench bool
	// FabricPeer reports whether a request's address is on the fabric
	// (nil: none is): only such a peer is served a sample (GET
	// /fabric/sample), which times the link from this node to it.
	FabricPeer func(host string) bool
	samples    int

	mu         sync.Mutex
	warned     map[string]time.Time
	benchConns int
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

// benchNetConnLimit bounds one network bench connection (a kind's
// measurement is minutes).
const benchNetConnLimit = 30 * time.Minute

// benchNetMaxConns bounds the network bench connections served at once.
const benchNetMaxConns = 16

// serveBenchNet hands a network bench's connection to the bench's server
// (bench.ServeConn), once the daemon says its token is that of a live
// network bench measuring this node (store.AuthorizeBenchNet): the bench
// measures the path transfers take, TLS included.
func (p *PackServer) serveBenchNet(w http.ResponseWriter, r *http.Request, runID string) {
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok || token == "" || runID == "" || strings.Contains(runID, "/") || !strings.EqualFold(r.Header.Get("Upgrade"), bench.PackUpgrade) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	if p.NoBench {
		http.Error(w, "this node serves no network bench (observe-only)", http.StatusServiceUnavailable)
		return
	}
	if err := p.Store.AuthorizeBenchNet(r.Context(), p.NodeID, runID, token); err != nil {
		p.warn(runID, "network bench refused", "remote", r.RemoteAddr, "error", err)
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	// as many connections as kairo bench serve takes
	p.mu.Lock()
	full := p.benchConns >= benchNetMaxConns
	if !full {
		p.benchConns++
	}
	p.mu.Unlock()
	if full {
		http.Error(w, "too many network bench connections", http.StatusServiceUnavailable)
		return
	}
	defer func() {
		p.mu.Lock()
		p.benchConns--
		p.mu.Unlock()
	}()
	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "the connection cannot be handed over", http.StatusHTTPVersionNotSupported)
		return
	}
	conn, buf, err := hj.Hijack()
	if err != nil {
		return
	}
	defer conn.Close()
	if _, err = buf.WriteString("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: " + bench.PackUpgrade + "\r\n\r\n"); err == nil {
		err = buf.Flush()
	}
	if err != nil {
		return
	}
	if err := bench.ServeConn(&bufferedConn{Conn: conn, r: buf.Reader}, benchNetConnLimit); err != nil {
		p.Logger.Info("network bench connection ended", "run", runID, "remote", r.RemoteAddr, "error", err)
	}
}

// bufferedConn reads what the HTTP server had buffered before the rest.
type bufferedConn struct {
	net.Conn
	r io.Reader
}

func (c *bufferedConn) Read(b []byte) (int, error) { return c.r.Read(b) }

func (p *PackServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// a sample a peer fetches, or sends (fabricsample.go)
	if r.URL.Path == FabricSamplePath {
		p.serveFabricSample(w, r)
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if runID, ok := strings.CutPrefix(r.URL.Path, bench.PackPathPrefix); ok {
		p.serveBenchNet(w, r, runID)
		return
	}
	if sha256, ok := strings.CutPrefix(r.URL.Path, rawPeerPrefix); ok {
		p.serveRaw(w, r, sha256)
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
		for _, s := range e.storeWork().servedStores() {
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
