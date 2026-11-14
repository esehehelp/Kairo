package executor

import (
	"archive/tar"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/klauspost/compress/zstd"

	"kairo/internal/sourcesnap"
	"kairo/internal/store"
)

// Code snapshots (kairo source, store/sources.go): kairo project apply packs
// the files of a source's declared paths (PackSource) as a data object is
// packed, the daemon keeps the pack, and an attempt of a task bound to the
// snapshot runs in it: its executor brings it to the node once (fetched from
// the daemon, checked against its size, its sha256 and each file against
// the file list, as an input is: ensureSource) under <root>/sources/<id> of
// the first volatile store it serves, and starts the attempt there.

// sourcesDirName is the directory of a volatile store snapshots are
// unpacked in.
const sourcesDirName = "sources"

// SourcePack is a snapshot PackSource made: the pack and its file list
// written in a directory, and what they hold.
type SourcePack struct {
	ID           string
	PackPath     string
	ListPath     string
	List         string
	PackBytes    int64
	PackSHA256   string
	ListSHA256   string
	Files        int64
	ContentBytes int64
	// Skipped counts what was not packed: links and special files.
	Skipped int64
}

// PackSource packs the regular files of paths (relative to root; a file or
// a directory, taken whole) but those exclude names, into dir: <id>.tar.zst
// and its file list <id>.tar.zst.files.tsv. An exclude pattern without a
// slash is matched against every path element (".venv", "__pycache__",
// "*.pyc"); one with a slash against the whole relative path.
func PackSource(ctx context.Context, root string, paths, exclude []string, dir string) (SourcePack, error) {
	var sp SourcePack
	for _, pattern := range exclude {
		if _, err := path.Match(pattern, ""); err != nil {
			return sp, fmt.Errorf("exclude %q: %w", pattern, err)
		}
	}
	r, err := os.OpenRoot(root)
	if err != nil {
		return sp, err
	}
	defer r.Close()
	type file struct {
		rel  string
		info fs.FileInfo
	}
	var files []file
	seen := map[string]bool{}
	var add func(rel string) error
	add = func(rel string) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if sourceExcluded(rel, exclude) || seen[rel] {
			return nil
		}
		info, err := r.Lstat(filepath.FromSlash(rel))
		if err != nil {
			return err
		}
		switch {
		case isLink(info):
			sp.Skipped++
		case info.Mode().IsRegular():
			if !utf8.ValidString(rel) {
				return fmt.Errorf("%w: %q", errPackName, rel)
			}
			seen[rel] = true
			files = append(files, file{rel, info})
		case info.IsDir():
			var entries []fs.FileInfo
			if err := eachEntry(ctx, r, filepath.FromSlash(rel), func(info fs.FileInfo) { entries = append(entries, info) }); err != nil {
				return err
			}
			for _, e := range entries {
				if err := add(path.Join(rel, e.Name())); err != nil {
					return err
				}
			}
		default:
			sp.Skipped++
		}
		return nil
	}
	for _, p := range paths {
		rel := path.Clean(filepath.ToSlash(p))
		if rel == "." || rel == ".." || strings.HasPrefix(rel, "../") || path.IsAbs(rel) {
			return sp, fmt.Errorf("path %q is not under the source's root", p)
		}
		if err := add(rel); err != nil {
			return sp, fmt.Errorf("%s: %w", p, err)
		}
	}
	if len(files) == 0 {
		return sp, errors.New("nothing to snapshot: the paths hold no regular file")
	}
	sort.Slice(files, func(i, j int) bool { return files[i].rel < files[j].rel })
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return sp, err
	}
	tmp, err := os.CreateTemp(dir, "snapshot-*.tar.zst")
	if err != nil {
		return sp, err
	}
	defer func() {
		tmp.Close()
		_ = os.Remove(tmp.Name())
	}()
	packHash, counted := sha256.New(), &countWriter{}
	zw, err := zstd.NewWriter(io.MultiWriter(tmp, packHash, counted), zstd.WithEncoderLevel(zstd.SpeedBetterCompression))
	if err != nil {
		return sp, err
	}
	tw := tar.NewWriter(zw)
	var list strings.Builder
	list.WriteString(manifestHeader + "\n")
	for _, f := range files {
		line, size, err := packFile(r, tw, f.rel, f.info)
		if err != nil {
			return sp, err
		}
		list.WriteString(line + "\n")
		sp.Files++
		sp.ContentBytes += size
	}
	if err := tw.Close(); err != nil {
		return sp, err
	}
	if err := zw.Close(); err != nil {
		return sp, err
	}
	if err := tmp.Close(); err != nil {
		return sp, err
	}
	sp.List = list.String()
	if sp.ID, err = sourcesnap.ID(sp.List); err != nil {
		return sp, err
	}
	sp.PackBytes, sp.PackSHA256 = counted.n, hex.EncodeToString(packHash.Sum(nil))
	listHash := sha256.Sum256([]byte(sp.List))
	sp.ListSHA256 = hex.EncodeToString(listHash[:])
	sp.PackPath = filepath.Join(dir, sp.ID+packSuffix)
	sp.ListPath = sp.PackPath + manifestSuffix
	if err := os.WriteFile(sp.ListPath, []byte(sp.List), 0o644); err != nil {
		return sp, err
	}
	if err := os.Rename(tmp.Name(), sp.PackPath); err != nil {
		return sp, err
	}
	return sp, nil
}

// sourceExcluded reports whether an exclude pattern names rel (see
// PackSource).
func sourceExcluded(rel string, exclude []string) bool {
	for _, pattern := range exclude {
		if strings.Contains(pattern, "/") {
			if ok, _ := path.Match(pattern, rel); ok {
				return true
			}
			continue
		}
		for _, element := range strings.Split(rel, "/") {
			if ok, _ := path.Match(pattern, element); ok {
				return true
			}
		}
	}
	return false
}

// SourceFetcher brings a snapshot's pack and file list from the daemon.
type SourceFetcher interface {
	FetchSource(ctx context.Context, id, file string, w io.Writer) error
}

// ensureSource is where snapshot src is unpacked on this executor's node,
// bringing it there first if it is not: under <root>/sources/<id> of the
// first usable volatile store this executor serves. It is unpacked read-only
// (directories too, but on Windows), and what it was made is recorded
// beside it (<id>.state: each directory and file, with the size, mtime and
// mode of each file); before every launch in it the tree is checked against
// that record, since an attempt that is not confined can write there (root
// on Linux whatever the modes): one changed (a file added, removed or
// written) is set aside as <id>.changed-<time> (an attempt may still run in
// it) and the snapshot unpacked again. One snapshot at a time per directory
// in this process; a directory there is the snapshot (it is renamed into
// place only once every file was checked; an unpack another process renamed
// there first is taken).
func (e *Local) ensureSource(ctx context.Context, src *store.LaunchSource) (string, error) {
	if e.isWSL() {
		return "", errors.New("code snapshots are not run inside WSL yet")
	}
	fetch, ok := e.Store.(SourceFetcher)
	if !ok {
		return "", errors.New("this executor cannot fetch code snapshots")
	}
	if !sourcesnap.ValidID(src.ID) {
		return "", fmt.Errorf("%q is not a snapshot id", src.ID)
	}
	var s *servedStore
	for _, served := range e.servedStores() {
		if served.usable && served.decl.Class == "volatile" {
			s = &served
			break
		}
	}
	if s == nil {
		return "", errors.New("no usable volatile store to unpack the code snapshot on")
	}
	if _, ok := s.fs.(localStoreFS); !ok {
		return "", errors.New("code snapshots are unpacked on a local store only")
	}
	base := filepath.Join(s.decl.Root, sourcesDirName)
	final := filepath.Join(base, src.ID)
	defer lockSourceDir(final)()
	if err := os.MkdirAll(base, 0o755); err != nil {
		return "", err
	}
	dir, err := os.OpenRoot(base)
	if err != nil {
		return "", err
	}
	defer dir.Close()
	here, changes, err := checkUnpacked(dir, src.ID)
	if err != nil {
		return "", fmt.Errorf("check the unpacked snapshot: %w", err)
	}
	if here && len(changes) == 0 {
		markSourceUsed(dir, src.ID)
		return final, nil
	}
	if here {
		aside := fmt.Sprintf("%s%s%d", src.ID, changedInfix, time.Now().UnixNano())
		if err := dir.Rename(src.ID, aside); err != nil {
			return "", fmt.Errorf("the unpacked snapshot was changed in place (%s) and cannot be set aside: %w", strings.Join(changes, ", "), err)
		}
		_ = dir.Remove(src.ID + stateSuffix)
		e.Logger.Warn("code snapshot was changed in place: set aside, unpacked again", "source_id", src.ID, "set_aside", filepath.Join(base, aside), "changes", strings.Join(changes, ", "))
	}
	token := make([]byte, 6)
	if _, err := rand.Read(token); err != nil {
		return "", err
	}
	tmp := src.ID + "." + hex.EncodeToString(token)
	packName, listName, partial, statePartial := tmp+packSuffix+partialSuffix, tmp+packSuffix+manifestSuffix+partialSuffix, tmp+partialSuffix, tmp+stateSuffix+partialSuffix
	defer func() {
		_ = dir.Remove(packName)
		_ = dir.Remove(listName)
		_ = dir.Remove(statePartial)
		removeUnpacked(dir, partial)
	}()
	for _, f := range []struct {
		name, file, sha string
		bytes           int64
	}{{listName, "list", src.ListSHA256, -1}, {packName, "pack", src.PackSHA256, src.PackBytes}} {
		w, err := dir.OpenFile(f.name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
		if err != nil {
			return "", err
		}
		h, counted := sha256.New(), &countWriter{}
		err = fetch.FetchSource(ctx, src.ID, f.file, io.MultiWriter(w, h, counted))
		if err == nil {
			err = w.Sync()
		}
		if closeErr := w.Close(); err == nil {
			err = closeErr
		}
		if err != nil {
			return "", fmt.Errorf("fetch the snapshot's %s: %w", f.file, err)
		}
		if got := hex.EncodeToString(h.Sum(nil)); got != f.sha || f.bytes >= 0 && counted.n != f.bytes {
			return "", fmt.Errorf("%w: the snapshot's %s is %d bytes, sha256 %s; recorded %d bytes, sha256 %s", errInputMismatch, f.file, counted.n, got, f.bytes, f.sha)
		}
	}
	if err := dir.Mkdir(partial, 0o755); err != nil {
		return "", err
	}
	into, err := dir.OpenRoot(partial)
	if err != nil {
		return "", err
	}
	_, err = unpackChecked(ctx, dir, packName, listName, into, true)
	if err == nil {
		err = makeReadOnly(into)
	}
	var state string
	if err == nil {
		state, err = treeState(into)
	}
	into.Close()
	if err != nil {
		return "", fmt.Errorf("unpack the snapshot: %w", err)
	}
	// the record first: a tree there without one is taken as changed
	if err := writeRootFile(dir, statePartial, []byte(state)); err != nil {
		return "", err
	}
	if err := dir.Rename(statePartial, src.ID+stateSuffix); err != nil {
		return "", err
	}
	if err := dir.Rename(partial, src.ID); err != nil {
		// unpacked there meanwhile by another process (the same files,
		// modes and mtimes: the same record)?
		if here, changes, checkErr := checkUnpacked(dir, src.ID); checkErr == nil && here && len(changes) == 0 {
			markSourceUsed(dir, src.ID)
			return final, nil
		}
		return "", err
	}
	e.Logger.Info("code snapshot brought", "source_id", src.ID, "path", final)
	return final, nil
}

const (
	// stateSuffix: the record of an unpacked snapshot, beside it.
	stateSuffix = ".state"
	// changedInfix names an unpacked snapshot set aside as changed.
	changedInfix = ".changed-"
	stateHeader  = "kairo source state 1"
)

// sourceDirLocks: one lock per directory a snapshot is unpacked in, shared
// by every executor of this process (several may serve one store).
var sourceDirLocks sync.Map

func lockSourceDir(path string) (unlock func()) {
	m, _ := sourceDirLocks.LoadOrStore(path, &sync.Mutex{})
	mu := m.(*sync.Mutex)
	mu.Lock()
	return mu.Unlock
}

// markSourceUsed sets the mtime of a snapshot's record to now: when it was
// last launched in.
func markSourceUsed(dir *os.Root, id string) {
	now := time.Now()
	_ = dir.Chtimes(id+stateSuffix, now, now)
}

// makeReadOnly takes the write bits off every file under r and, but on
// Windows (where a directory's read-only attribute does not keep files from
// being made in it), every directory.
func makeReadOnly(r *os.Root) error {
	var dirs []string
	err := fs.WalkDir(r.FS(), ".", func(name string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		switch {
		case d.IsDir():
			dirs = append(dirs, name)
			return nil
		case info.Mode().IsRegular():
			return r.Chmod(filepath.FromSlash(name), info.Mode().Perm()&^0o222)
		default:
			return fmt.Errorf("%w: %s is neither a directory nor a regular file", errInputMismatch, name)
		}
	})
	if err != nil || runtime.GOOS == "windows" {
		return err
	}
	for i := len(dirs) - 1; i >= 0; i-- {
		if err := r.Chmod(filepath.FromSlash(dirs[i]), 0o555); err != nil {
			return err
		}
	}
	return nil
}

// removeUnpacked removes name (under dir) whole, read-only or not.
func removeUnpacked(dir *os.Root, name string) {
	if r, err := dir.OpenRoot(name); err == nil {
		_ = fs.WalkDir(r.FS(), ".", func(p string, d fs.DirEntry, err error) error {
			if err != nil || d.Type()&fs.ModeSymlink != 0 {
				return nil
			}
			if info, err := d.Info(); err == nil {
				_ = r.Chmod(filepath.FromSlash(p), info.Mode().Perm()|0o200)
			}
			return nil
		})
		r.Close()
	}
	_ = dir.RemoveAll(name)
}

// treeState is the record of the tree under r (see ensureSource): a line
// per directory ("d", path) and regular file ("f", path, size, mtime in ns,
// mode), in walk order; anything else is a "?" line.
func treeState(r *os.Root) (string, error) {
	var b strings.Builder
	b.WriteString(stateHeader + "\n")
	err := fs.WalkDir(r.FS(), ".", func(name string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if name == "." {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		switch {
		case d.IsDir():
			fmt.Fprintf(&b, "d\t%s\n", escapeListPath(name))
		case info.Mode().IsRegular():
			fmt.Fprintf(&b, "f\t%s\t%d\t%d\t%o\n", escapeListPath(name), info.Size(), info.ModTime().UnixNano(), info.Mode().Perm())
		default:
			fmt.Fprintf(&b, "?\t%s\t%s\n", escapeListPath(name), info.Mode().Type())
		}
		return nil
	})
	return b.String(), err
}

// checkUnpacked reports whether snapshot id is unpacked in dir and, if it
// is, how its tree differs from its record (none: as it was unpacked).
func checkUnpacked(dir *os.Root, id string) (here bool, changes []string, err error) {
	info, err := dir.Lstat(id)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil, nil
	}
	if err != nil {
		return false, nil, err
	}
	if !info.IsDir() || isLink(info) {
		return true, []string{"not a directory"}, nil
	}
	want, err := dir.ReadFile(id + stateSuffix)
	if errors.Is(err, fs.ErrNotExist) {
		return true, []string{"no record of how it was unpacked"}, nil
	}
	if err != nil {
		return true, nil, err
	}
	r, err := dir.OpenRoot(id)
	if err != nil {
		return true, nil, err
	}
	got, err := treeState(r)
	r.Close()
	if err != nil {
		return true, nil, err
	}
	if got == string(want) {
		return true, nil, nil
	}
	return true, stateChanges(string(want), got, 5), nil
}

// stateChanges names how record got differs from want: paths added,
// removed or changed, at most max of them and then how many more.
func stateChanges(want, got string, max int) []string {
	entries := func(state string) map[string]string {
		m := map[string]string{}
		lines := strings.Split(strings.TrimSuffix(state, "\n"), "\n")
		for _, line := range lines[min(1, len(lines)):] {
			kind, rest, _ := strings.Cut(line, "\t")
			name, attrs, _ := strings.Cut(rest, "\t")
			m[name] = kind + "\t" + attrs
		}
		return m
	}
	w, g := entries(want), entries(got)
	var all []string
	for name, attrs := range g {
		if old, ok := w[name]; !ok {
			all = append(all, "added "+name)
		} else if old != attrs {
			all = append(all, "changed "+name)
		}
	}
	for name := range w {
		if _, ok := g[name]; !ok {
			all = append(all, "removed "+name)
		}
	}
	if len(all) == 0 {
		all = append(all, "record differs")
	}
	sort.Strings(all)
	if len(all) > max {
		all = append(all[:max], fmt.Sprintf("%d more", len(all)-max))
	}
	return all
}

// writeRootFile writes name (under dir) whole, synced.
func writeRootFile(dir *os.Root, name string, body []byte) error {
	f, err := dir.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	_, err = f.Write(body)
	if err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	return err
}

// SourceKeeper says which snapshots are in use: those of executions not yet
// terminal.
type SourceKeeper interface {
	SourcesInUse(ctx context.Context, executorID string) ([]string, error)
}

const (
	sourceCleanInterval = time.Hour
	sourceCleanTimeout  = time.Hour
	// sourceKeep: how long an unpacked snapshot no execution uses any more
	// is kept after it was last launched in (a rerun soon after finds it).
	sourceKeep = 3 * 24 * time.Hour
	// sourceAsideKeep: how long a snapshot set aside as changed is kept
	// (once its snapshot is not in use).
	sourceAsideKeep = time.Hour
	// sourcePartialKeep: how long what an unpack left half done is kept.
	sourcePartialKeep = 24 * time.Hour
)

func (e *Local) makeSourceCleanups() {
	e.startRound(&e.sourceCleanups, sourceCleanInterval, sourceCleanTimeout, e.cleanSources)
}

// cleanSources removes from <root>/sources of each usable local volatile
// store this executor serves what is no longer used there (cleanSourcesIn),
// never without the daemon's word on which snapshots are in use.
func (e *Local) cleanSources(ctx context.Context) {
	keeper, ok := e.Store.(SourceKeeper)
	if !ok {
		return
	}
	ids, err := keeper.SourcesInUse(ctx, e.ID)
	if err != nil {
		e.roundProblem(&e.sourceCleanups, "", "code snapshots in use not read", err)
		return
	}
	e.roundProblem(&e.sourceCleanups, "", "", nil)
	inUse := make(map[string]bool, len(ids))
	for _, id := range ids {
		inUse[id] = true
	}
	for _, s := range e.servedStores() {
		if _, local := s.fs.(localStoreFS); !s.usable || s.decl.Class != "volatile" || !local {
			continue
		}
		e.cleanSourcesIn(filepath.Join(s.decl.Root, sourcesDirName), inUse, time.Now())
	}
}

// cleanSourcesIn removes from base, of what Kairo names there (src_<id>
// and what is beside it; nothing else is touched): an unpacked snapshot no
// execution uses whose record says it was last launched in sourceKeep ago
// or more (its record first, so a tree left half removed is taken as
// changed), with its record; one set aside as changed sourceAsideKeep ago
// whose snapshot no execution uses; a record without its tree; and what an
// unpack left half done sourcePartialKeep ago.
func (e *Local) cleanSourcesIn(base string, inUse map[string]bool, now time.Time) {
	dir, err := os.OpenRoot(base)
	if err != nil {
		return
	}
	defer dir.Close()
	entries, err := fs.ReadDir(dir.FS(), ".")
	if err != nil {
		e.roundProblem(&e.sourceCleanups, base, "code snapshots not listed", err)
		return
	}
	idLen := len("src_") + 32
	for _, entry := range entries {
		name := entry.Name()
		if len(name) < idLen || !sourcesnap.ValidID(name[:idLen]) || entry.Type()&fs.ModeSymlink != 0 {
			continue
		}
		id, rest := name[:idLen], name[idLen:]
		info, err := entry.Info()
		if err != nil {
			continue
		}
		removed := ""
		switch {
		case rest == "" && entry.IsDir():
			if inUse[id] {
				continue
			}
			unlock := lockSourceDir(filepath.Join(base, id))
			used := info.ModTime()
			if state, err := dir.Lstat(id + stateSuffix); err == nil {
				used = state.ModTime()
			}
			if now.Sub(used) >= sourceKeep {
				_ = dir.Remove(id + stateSuffix)
				removeUnpacked(dir, id)
				removed = "unpacked code snapshot removed: not used"
			}
			unlock()
		case rest == stateSuffix:
			if _, err := dir.Lstat(id); errors.Is(err, fs.ErrNotExist) && !inUse[id] && now.Sub(info.ModTime()) >= sourcePartialKeep {
				_ = dir.Remove(name)
			}
		case strings.HasPrefix(rest, changedInfix) && entry.IsDir():
			at, err := strconv.ParseInt(rest[len(changedInfix):], 10, 64)
			if err != nil || inUse[id] || now.Sub(time.Unix(0, at)) < sourceAsideKeep {
				continue
			}
			removeUnpacked(dir, name)
			removed = "code snapshot set aside as changed removed"
		case strings.HasSuffix(rest, partialSuffix):
			if now.Sub(info.ModTime()) < sourcePartialKeep {
				continue
			}
			if entry.IsDir() {
				removeUnpacked(dir, name)
			} else {
				_ = dir.Remove(name)
			}
		}
		if removed == "" {
			continue
		}
		if _, err := dir.Lstat(name); err == nil {
			e.roundProblem(&e.sourceCleanups, name, "code snapshot not removed", errors.New("still there"))
			continue
		}
		e.roundProblem(&e.sourceCleanups, name, "", nil)
		e.Logger.Info(removed, "source_id", id, "path", filepath.Join(base, name))
	}
}
