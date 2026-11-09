package executor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"kairo/internal/machine"
	"kairo/internal/store"
)

// Once an object's copy on a durable store is verified, the executor that
// packed it deletes what it was made from (store.DeletionsToMake): the run's
// output directory or the scratch, by the store's rules (removeKindDir: the
// directory derived from the root, its kind and its id, its marker naming
// it, nothing reached through a link), then the pack and its file list. The
// deletion is claimed first (store.SettleDeletion deleting); before anything
// goes, the source is checked against the pack's file list (checkPacked):
// deleting it must lose nothing the pack does not hold. It is settled
// deleted once done; one refused for safety (the source holds what the pack
// does not, or is not Kairo's as the store's rules say) is settled kept, with
// why, its pack removed all the same (the copy holds it); one that failed
// otherwise (unreadable now, a race) stays claimed and is tried again. A
// source an attempt this executor launched still runs in is left for later.
//
// An executor serving a durable store reads its verified copies back now and
// then (store.ScrubsToMake), around the cache, and reports what it read; a
// copy missing or unreadable is reported damaged. Nothing is deleted or
// rewritten on a durable store for it.

const (
	deleteInterval     = time.Minute
	deleteRoundTimeout = 6 * time.Hour
	scrubEvery         = 10 * time.Minute
	scrubRoundTimeout  = 12 * time.Hour
)

// round is a background round's state: when it last started, whether one
// runs, the problems already warned of, by object, and when an object that
// failed is to be tried again.
type round struct {
	at       time.Time
	running  bool
	interval time.Duration
	problems map[string]string
	failures map[string]int
	retryAt  map[string]time.Time
}

// maxRetryDelay bounds how long an object that keeps failing waits between
// tries.
const maxRetryDelay = time.Hour

// startRound runs fn in the background, at most every interval, once the
// daemon has answered for stores, one round at a time.
func (e *Local) startRound(r *round, interval, timeout time.Duration, fn func(context.Context)) {
	e.mu.Lock()
	due := e.storesKnown && !r.running && time.Since(r.at) >= interval
	if due {
		r.running, r.at, r.interval = true, time.Now(), interval
	}
	e.mu.Unlock()
	if !due {
		return
	}
	go func() {
		defer func() {
			e.mu.Lock()
			r.running = false
			e.mu.Unlock()
		}()
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		fn(ctx)
	}()
}

// roundProblem warns of a round's problem once per object (again when it
// changes); an empty message clears it. objectID "" is the round itself. An
// object that failed is tried again at the next round, then, after each
// further failure in a row, after twice as long as before (from the round's
// interval), at most maxRetryDelay: one that cannot be done does not fetch or
// read again every round.
func (e *Local) roundProblem(r *round, objectID, message string, err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if message == "" {
		delete(r.problems, objectID)
		delete(r.failures, objectID)
		delete(r.retryAt, objectID)
		return
	}
	if objectID != "" {
		if r.failures == nil {
			r.failures, r.retryAt = map[string]int{}, map[string]time.Time{}
		}
		r.failures[objectID]++
		if n := r.failures[objectID]; n > 1 {
			delay := max(r.interval, time.Second)
			for i := 2; i < n && delay < maxRetryDelay; i++ {
				delay *= 2
			}
			r.retryAt[objectID] = time.Now().Add(min(delay, maxRetryDelay))
		}
	}
	text := message + ": " + err.Error()
	if r.problems[objectID] == text {
		return
	}
	if r.problems == nil {
		r.problems = map[string]string{}
	}
	r.problems[objectID] = text
	e.Logger.Warn(message, "executor_id", e.ID, "object_id", objectID, "error", err)
}

// due reports whether objectID is to be tried in this round: it has not
// failed, or its wait since is over.
func (e *Local) due(r *round, objectID string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	at, failed := r.retryAt[objectID]
	return !failed || !time.Now().Before(at)
}

func (e *Local) makeDeletions() {
	e.startRound(&e.deletions, deleteInterval, deleteRoundTimeout, e.deleteDue)
}

func (e *Local) makeScrubs() {
	e.startRound(&e.scrubs, scrubEvery, scrubRoundTimeout, e.scrubDue)
}

// deleteDue deletes every source the daemon says is due on a usable store
// this executor serves at the ordered root.
func (e *Local) deleteDue(ctx context.Context) {
	orders, err := e.Store.DeletionsToMake(ctx, e.ID)
	if err != nil {
		e.roundProblem(&e.deletions, "", "deletions to make not read", err)
		return
	}
	e.roundProblem(&e.deletions, "", "", nil)
	served := map[string]servedStore{}
	for _, s := range e.servedStores() {
		served[s.decl.ID] = s
	}
	for _, o := range orders {
		s, ok := served[o.StoreID]
		if o.Kind == store.ObjectImport {
			if ok && s.usable && s.decl.Root == o.Root && e.due(&e.deletions, o.ObjectID) {
				e.retireImport(ctx, o, s)
			}
			continue
		}
		kind, known := kindOf(o.Kind)
		if !known || !ok || !s.usable || s.decl.Root != o.Root {
			continue
		}
		if _, isLocal := s.fs.(localStoreFS); !isLocal || e.inUse(kind, o.SourceID) || !e.due(&e.deletions, o.ObjectID) {
			continue
		}
		settle := func(state, reason string) error {
			return e.Store.SettleDeletion(ctx, store.ObjectDeletion{ObjectID: o.ObjectID, ExecutorID: e.ID, State: state, Reason: reason})
		}
		if err := settle(store.SourceDeleting, ""); err != nil {
			e.roundProblem(&e.deletions, o.ObjectID, "source not claimed for deletion", err)
			continue
		}
		err := checkPacked(ctx, s.decl.Root, kind, o.SourceID, o.ObjectID)
		if err == nil {
			err = s.fs.RemoveKind(ctx, s.decl.Root, kind, kindPath(s.fs, s.decl.Root, kind, o.SourceID), o.SourceID, false)
		}
		if err == nil {
			err = removePack(s.fs, s.decl.Root, o.ObjectID)
		}
		state := store.SourceDeleted
		switch {
		case err == nil:
			err = settle(state, "")
		case errors.Is(err, errOutsideRoot), errors.Is(err, errScratchLink), errors.Is(err, errScratchMarker), errors.Is(err, errSourceChanged):
			reason := "not deleted: " + err.Error()
			// the pack goes all the same: its copy holds it
			if err = removePack(s.fs, s.decl.Root, o.ObjectID); err == nil {
				state = store.SourceKept
				err = settle(state, reason)
			}
		}
		if err != nil {
			e.roundProblem(&e.deletions, o.ObjectID, "source not deleted", err)
			continue
		}
		e.roundProblem(&e.deletions, o.ObjectID, "", nil)
		if state == store.SourceKept {
			e.Logger.Warn("source kept once copied: not deleted for safety", "object_id", o.ObjectID, "kind", o.Kind, "source_id", o.SourceID, "store_id", o.StoreID)
			continue
		}
		e.Logger.Info("source deleted once copied", "object_id", o.ObjectID, "kind", o.Kind, "source_id", o.SourceID, "store_id", o.StoreID)
	}
}

// retireImport settles an imported directory once its copy is verified.
// Imported without delete_source, only its pack goes and the directory is
// kept. With it, the deletion is claimed, the directory checked against the
// pack's file list (checkPackedDir) and to be on the filesystem of the store
// the pack was made on, then removed through its parent's handle, then its
// pack; a directory refused for safety is kept, with why, and its pack
// removed all the same.
func (e *Local) retireImport(ctx context.Context, o store.DeletionOrder, s servedStore) {
	if _, isLocal := s.fs.(localStoreFS); !isLocal {
		return
	}
	settle := func(state, reason string) error {
		return e.Store.SettleDeletion(ctx, store.ObjectDeletion{ObjectID: o.ObjectID, ExecutorID: e.ID, State: state, Reason: reason})
	}
	var err error
	state, reason := store.SourceDeleted, ""
	if !o.DeleteSource {
		state, reason = store.SourceKept, "imported without delete_source: left where it is"
		if err = removePack(s.fs, s.decl.Root, o.ObjectID); err == nil {
			err = settle(state, reason)
		}
	} else if err = settle(store.SourceDeleting, ""); err == nil {
		err = checkPackedDir(ctx, s.decl.Root, o.Path, o.ObjectID)
		if err == nil {
			err = onSameFilesystem(o.Path, s.decl.Root)
		}
		if err == nil {
			err = removeImportedDir(o.Path)
		}
		if err == nil {
			err = removePack(s.fs, s.decl.Root, o.ObjectID)
		}
		switch {
		case err == nil:
			err = settle(state, "")
		case errors.Is(err, errOutsideRoot), errors.Is(err, errScratchLink), errors.Is(err, errSourceChanged):
			state, reason = store.SourceKept, "not deleted: "+err.Error()
			if err = removePack(s.fs, s.decl.Root, o.ObjectID); err == nil {
				err = settle(state, reason)
			}
		}
	}
	if err != nil {
		e.roundProblem(&e.deletions, o.ObjectID, "imported directory not settled", err)
		return
	}
	e.roundProblem(&e.deletions, o.ObjectID, "", nil)
	if state == store.SourceKept {
		e.Logger.Info("imported directory kept once copied", "object_id", o.ObjectID, "path", o.Path, "reason", reason)
		return
	}
	e.Logger.Info("imported directory deleted once copied", "object_id", o.ObjectID, "path", o.Path)
}

// checkPackedDir is checkPacked for an imported directory dir: deleting it
// loses nothing objectID's pack does not hold. A directory gone is no
// matter.
func checkPackedDir(ctx context.Context, root, dir, objectID string) error {
	if !validScratchID(objectID) || !filepath.IsAbs(dir) {
		return fmt.Errorf("%w: directory %q, object %q", errOutsideRoot, dir, objectID)
	}
	root, dir = filepath.Clean(root), filepath.Clean(dir)
	sr, err := openDirPath(dir, false)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer sr.Close()
	listed, err := readPackList(root, objectID+packSuffix+manifestSuffix)
	if errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("%w: its pack's file list is gone", errSourceChanged)
	}
	if err != nil {
		return err
	}
	return checkAgainstList(ctx, sr, dir, "", listed)
}

// errOtherFilesystem: an imported directory not on the filesystem of the
// store its pack was made on (another disk, a mount, a durable store's).
var errOtherFilesystem = errors.New("not on the filesystem of the store it was packed on")

// onSameFilesystem checks that dir is on the filesystem root is on: the
// same drive (Windows; checkResolvesTo makes the drive in dir's path the
// volume it is on) or device (elsewhere). A directory gone is no matter (a
// removal cut short removed it).
func onSameFilesystem(dir, root string) error {
	if _, err := os.Lstat(dir); errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err := checkResolvesTo(dir); err != nil {
		return err
	}
	if runtime.GOOS == "windows" {
		if !strings.EqualFold(filepath.VolumeName(dir), filepath.VolumeName(root)) {
			return fmt.Errorf("%w: %s, %s", errSourceChanged, dir, errOtherFilesystem)
		}
		return nil
	}
	a, err := os.Stat(dir)
	if err != nil {
		return err
	}
	b, err := os.Stat(root)
	if err != nil {
		return err
	}
	if deviceOf(a) != deviceOf(b) {
		return fmt.Errorf("%w: %s, %s", errSourceChanged, dir, errOtherFilesystem)
	}
	return nil
}

// removeImportedDir removes dir through its parent's handle, opened one
// directory at a time from the volume's root (no link on the path): dir
// itself must be a plain directory, and os.Root.RemoveAll removes a link
// under it itself, never what it points at. A directory gone is no error.
func removeImportedDir(dir string) error {
	dir = filepath.Clean(dir)
	parent, base := filepath.Dir(dir), filepath.Base(dir)
	if parent == dir || base == "" || base == "." {
		return fmt.Errorf("%w: %s", errOutsideRoot, dir)
	}
	pr, err := openDirPath(parent, false)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer pr.Close()
	fi, err := pr.Lstat(base)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if isLink(fi) || !fi.IsDir() {
		return fmt.Errorf("%w: %s", errScratchLink, dir)
	}
	return pr.RemoveAll(base)
}

// inUse reports whether an attempt this executor launched still runs in the
// source of kind and id: an attempt of the run (its output directory), the
// attempt itself (its scratch).
func (e *Local) inUse(kind dirKind, id string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if kind.dir == kindScratch.dir {
		_, running := e.running[id]
		return running
	}
	for _, run := range e.outputRuns {
		if run == id {
			return true
		}
	}
	return false
}

// errSourceChanged: a source holds what its pack does not (written since it
// was packed, or left out of it: a link, a special file, a mount point).
var errSourceChanged = errors.New("the source holds what its pack does not")

// packedFile is a file as its pack's file list has it.
type packedFile struct{ size, mtime int64 }

// checkPacked checks that deleting the source of kind and id under root
// loses nothing objectID's pack (in <root>/packs) does not hold: each regular
// file there is in the pack's file list with the size and mtime it was
// packed with (stat'ed through its handle, as when it was packed), and
// nothing else is there but directories (links, special files and mount
// points were not packed). A listed file gone is no matter (a deletion cut
// short removed it), nor is a source gone.
func checkPacked(ctx context.Context, root string, kind dirKind, id, objectID string) error {
	if !validScratchID(id) || !validScratchID(objectID) {
		return fmt.Errorf("%w: id %q, object %q", errOutsideRoot, id, objectID)
	}
	root = filepath.Clean(root)
	r, err := openDirPath(root, false)
	if err != nil {
		return err
	}
	defer r.Close()
	kr, err := openPlainDir(r, kind.dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer kr.Close()
	sr, err := openPlainDir(kr, id)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer sr.Close()
	listed, err := readPackList(root, objectID+packSuffix+manifestSuffix)
	if errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("%w: its pack's file list is gone", errSourceChanged)
	}
	if err != nil {
		return err
	}
	return checkAgainstList(ctx, sr, filepath.Join(root, kind.dir, id), kind.marker, listed)
}

// checkAgainstList walks the source src (opened as sr) and checks that each
// regular file there is in listed as packed and nothing else is there but
// directories (the marker, if any, aside).
func checkAgainstList(ctx context.Context, sr *os.Root, src, marker string, listed map[string]packedFile) error {
	mounts, err := mountsUnderFn(src)
	if err != nil {
		return err
	}
	mounted := map[string]bool{}
	for _, m := range mounts {
		mounted[filepath.Clean(m)] = true
	}
	pending := []string{"."}
	for len(pending) != 0 {
		dir := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		var infos []fs.FileInfo
		if err := eachEntry(ctx, sr, filepath.FromSlash(dir), func(info fs.FileInfo) { infos = append(infos, info) }); err != nil {
			return err
		}
		for _, info := range infos {
			if err := ctx.Err(); err != nil {
				return err
			}
			rel := path.Join(dir, info.Name())
			switch {
			case marker != "" && dir == "." && info.Name() == marker:
			case isLink(info):
				return fmt.Errorf("%w: %s is a link (not packed)", errSourceChanged, rel)
			case info.IsDir():
				if mounted[filepath.Join(src, filepath.FromSlash(rel))] {
					return fmt.Errorf("%w: %s is a mount point (not packed)", errSourceChanged, rel)
				}
				pending = append(pending, rel)
			case info.Mode().IsRegular():
				want, ok := listed[rel]
				if !ok {
					return fmt.Errorf("%w: %s is not in its pack", errSourceChanged, rel)
				}
				fh, err := sr.OpenFile(filepath.FromSlash(rel), os.O_RDONLY|markerOpenFlags, 0)
				if errors.Is(err, fs.ErrNotExist) {
					continue
				}
				if err != nil {
					return err
				}
				opened, err := fh.Stat()
				fh.Close()
				if err != nil {
					return err
				}
				if !opened.Mode().IsRegular() || opened.Size() != want.size || opened.ModTime().UnixNano() != want.mtime {
					return fmt.Errorf("%w: %s has changed since it was packed", errSourceChanged, rel)
				}
			default:
				return fmt.Errorf("%w: %s is a special file (not packed)", errSourceChanged, rel)
			}
		}
	}
	return nil
}

// readPackList reads the file list name in <root>/packs: each file's size
// and mtime by its path.
func readPackList(root, name string) (map[string]packedFile, error) {
	f, err := openPackFile(root, name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	body, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(strings.TrimSuffix(string(body), "\n"), "\n")
	if len(lines) == 0 || lines[0] != manifestHeader {
		return nil, fmt.Errorf("%w: %s is not a file list Kairo wrote", errSourceChanged, name)
	}
	out := make(map[string]packedFile, len(lines)-1)
	for _, line := range lines[1:] {
		fields := strings.Split(line, "\t")
		if len(fields) != 4 {
			return nil, fmt.Errorf("%w: %s: line %q", errSourceChanged, name, line)
		}
		size, err1 := strconv.ParseInt(fields[1], 10, 64)
		mtime, err2 := strconv.ParseInt(fields[3], 10, 64)
		if err1 != nil || err2 != nil {
			return nil, fmt.Errorf("%w: %s: line %q", errSourceChanged, name, line)
		}
		out[unescapeListPath(fields[0])] = packedFile{size, mtime}
	}
	return out, nil
}

// unescapeListPath undoes escapeListPath.
func unescapeListPath(p string) string {
	if !strings.Contains(p, `\`) {
		return p
	}
	var b strings.Builder
	for i := 0; i < len(p); i++ {
		if p[i] != '\\' || i+1 == len(p) {
			b.WriteByte(p[i])
			continue
		}
		i++
		switch p[i] {
		case 't':
			b.WriteByte('\t')
		case 'r':
			b.WriteByte('\r')
		case 'n':
			b.WriteByte('\n')
		default:
			b.WriteByte(p[i])
		}
	}
	return b.String()
}

// removePack removes an object's pack and file list from <root>/packs
// through handles (a link there is removed itself); ones already gone are no
// error.
func removePack(sfs storeFS, root, objectID string) error {
	return removePackFiles(sfs, root, objectID, objectID+packSuffix+manifestSuffix, objectID+packSuffix)
}

// removePackOnly removes the pack of objectID under root but not its file
// list (a temporal object's identity).
func removePackOnly(sfs storeFS, root, objectID string) error {
	return removePackFiles(sfs, root, objectID, objectID+packSuffix)
}

func removePackFiles(sfs storeFS, root, objectID string, names ...string) error {
	if _, ok := sfs.(localStoreFS); !ok {
		return errPackNotHere
	}
	if !validScratchID(objectID) {
		return fmt.Errorf("%w: object %q", errOutsideRoot, objectID)
	}
	r, err := openDirPath(filepath.Clean(root), false)
	if err != nil {
		return err
	}
	defer r.Close()
	pr, err := openPlainDir(r, packsDirName)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer pr.Close()
	for _, name := range names {
		if err := pr.Remove(name); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}
	return nil
}

// scrubDue reads back every copy the daemon says is due on a usable durable
// store this executor serves at the ordered root, and reports each.
func (e *Local) scrubDue(ctx context.Context) {
	orders, err := e.Store.ScrubsToMake(ctx, e.ID)
	if err != nil {
		e.roundProblem(&e.scrubs, "", "copies to read back not read", err)
		return
	}
	e.roundProblem(&e.scrubs, "", "", nil)
	served := map[string]servedStore{}
	for _, s := range e.servedStores() {
		served[s.decl.ID] = s
	}
	for _, o := range orders {
		s, ok := served[o.StoreID]
		if !ok || !s.usable || s.decl.Class != machine.StoreDurable || s.decl.Root != o.Root {
			continue
		}
		if _, isLocal := s.fs.(localStoreFS); !isLocal {
			continue
		}
		result, reachable := readBack(s.decl.Root, o)
		if !reachable {
			// the store's root or objects directory is not there now: not
			// the copy's fault; asked again later
			continue
		}
		result.ExecutorID = e.ID
		if err := e.Store.RecordScrub(ctx, result); err != nil {
			e.roundProblem(&e.scrubs, o.ObjectID, "read back not recorded", err)
			continue
		}
		e.roundProblem(&e.scrubs, o.ObjectID, "", nil)
		if result.Problem != "" || result.SHA256 != o.PackSHA256 || result.Bytes != o.PackBytes || result.ManifestSHA256 != o.ManifestSHA256 {
			e.Logger.Warn("copy damaged", "object_id", o.ObjectID, "store_id", o.StoreID, "problem", result.Problem, "sha256", result.SHA256, "bytes", result.Bytes)
		}
	}
}

// readBack reads a copy's pack and list back around the cache. reachable is
// false when the store's objects directory cannot be opened at all (a drive
// away, say): nothing is known of the copy then.
func readBack(root string, o store.ScrubOrder) (store.ScrubResult, bool) {
	result := store.ScrubResult{ObjectID: o.ObjectID, StoreID: o.StoreID}
	if !validScratchID(o.ObjectID) || !machine.ValidMonth(o.Month) {
		result.Problem = "not a copy's name"
		return result, true
	}
	root = filepath.Clean(root)
	r, err := openDirPath(root, false)
	if err != nil {
		return result, false
	}
	defer r.Close()
	or, err := openPlainDir(r, objectsDirName)
	if err != nil {
		return result, false
	}
	defer or.Close()
	dir, err := openPlainDir(or, o.Month)
	if err != nil {
		result.Problem = "its month directory: " + err.Error()
		return result, true
	}
	defer dir.Close()
	dirPath := filepath.Join(root, objectsDirName, o.Month)
	packName := o.ObjectID + packSuffix
	if result.ManifestSHA256, _, err = hashUncached(dir, packName+manifestSuffix, filepath.Join(dirPath, packName+manifestSuffix)); err != nil {
		result.Problem = "its file list: " + err.Error()
		return result, true
	}
	if result.SHA256, result.Bytes, err = hashUncached(dir, packName, filepath.Join(dirPath, packName)); err != nil {
		result.Problem = "its pack: " + err.Error()
	}
	return result, true
}
