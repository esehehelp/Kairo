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
	"slices"
	"strings"
)

// A store's root belongs to Kairo. Under it an attempt's scratch is
// <root>/scratch/<attempt id>, holding the marker file .kairo-scratch that
// names the attempt, and a run's output directory is <root>/outputs/<run id>,
// holding the marker .kairo-output that names the run (a dirKind each). What
// Kairo counts there is only what it made (a directory whose marker names
// it); what it deletes is only a directory recorded for the id its marker
// names.
//
// Nothing is reached through a path: a root (or a scratch directory to
// size) is opened one directory at a time from its volume's root, each a
// plain directory (not a link) opened as an os.Root and checked to be the
// one looked at (the same file, identified through a handle on both sides),
// and a missing one is made through its parent's handle; below that every
// operation goes through os.Root handles (openat-style on Linux,
// handle-relative NtCreateFile on Windows), so a directory swapped for a link
// at any moment is refused, never followed out. Within an opened directory
// os.Root.RemoveAll removes a link itself (a symlink, a junction, any reparse
// point) and never what it points at, and no operation leaves the directory
// it started from. Files are deleted without changing their attributes, so a
// hard link to a file outside is unlinked without touching that file: on
// Linux a file's mode never matters, and on Windows the root must be on NTFS,
// where deletion ignores the read-only attribute (elsewhere Go clears it
// first, on the file a hard link shares), or on exFAT, which has no hard
// links. Only a directory a task made
// unwritable is made writable again, through a handle that can only be a
// directory (Linux). On Linux <root>/scratch must not be a mount point and a
// scratch with a mount point anywhere under it (a bind mount included, which
// os.Root would cross) is not deleted.

const (
	scratchDirName    = "scratch"
	scratchMarkerName = ".kairo-scratch"
	outputDirName     = "outputs"
	outputMarkerName  = ".kairo-output"
	// scratchListBatch is how many directory entries are read at a time.
	scratchListBatch = 1024
)

var (
	// errOutsideRoot: a scratch path that is not <root>/scratch/<attempt id>
	// of an absolute root.
	errOutsideRoot = errors.New("not a scratch directory of the store's root")
	// errScratchLink: a link, a non-directory or a mount point where a
	// plain directory must be (the root, a component of its path, the
	// scratch parent or a scratch directory).
	errScratchLink = errors.New("a link, mount point or non-directory where a plain directory must be")
	// errRaced: a directory or file replaced while it was being opened (a
	// race, so tried again, unlike a link found there).
	errRaced = errors.New("changed while it was opened")
	// errScratchMarker: a scratch directory whose marker is missing or names
	// another attempt.
	errScratchMarker = errors.New("scratch marker does not name the attempt")
	// errScratchNotEmpty: a scratch to be deleted only if empty holds more
	// than its marker.
	errScratchNotEmpty = errors.New("scratch directory holds more than its marker")
)

// dirKind is a kind of directory Kairo makes under a store's root: the
// directory it is made in (<root>/<dir>/<id>) and the marker naming its id.
type dirKind struct{ dir, marker string }

var (
	// kindScratch: an attempt's scratch, <root>/scratch/<attempt id>.
	kindScratch = dirKind{dir: scratchDirName, marker: scratchMarkerName}
	// kindOutput: a run's output directory, <root>/outputs/<run id>.
	kindOutput = dirKind{dir: outputDirName, marker: outputMarkerName}
)

// storeFS is how an executor reaches a store's root: in its own namespace
// (localStoreFS) or inside a WSL distro (wslStoreFS).
type storeFS interface {
	// Ensure makes the root if it is missing (only with create: never a
	// durable store's) and checks that it is a directory on the declared
	// filesystem whose path has no link in it.
	Ensure(ctx context.Context, root, filesystem string, create bool) error
	// Usage is the total and free bytes of the filesystem holding root.
	Usage(ctx context.Context, root string) (total, free int64, err error)
	// Place is where root really is ("" when it cannot be told): two roots
	// whose places overlap (placesOverlap) are one place on disk, or one
	// holds the other, whatever their paths.
	Place(ctx context.Context, root string) (string, error)
	// List is the Kairo scratch directories under root: ids, a directory
	// <root>/scratch/<id> whose marker names id, and unread, one named like a
	// scratch directory whose marker could not be read now (not known to be
	// Kairo's, nor known to be gone).
	List(ctx context.Context, root string) (ids, unread []string, err error)
	// Create makes <root>/scratch/<attemptID> with its marker and returns
	// its path (one already made for attemptID is taken as it is).
	Create(ctx context.Context, root, attemptID string) (string, error)
	// Remove deletes the scratch directory dir of attemptID under root
	// (the rules above); onlyEmpty deletes it only if it holds nothing but
	// its marker (errScratchNotEmpty otherwise). A directory already gone is
	// no error.
	Remove(ctx context.Context, root, dir, attemptID string, onlyEmpty bool) error
	// Size is the bytes of the regular files under dir (links not followed,
	// mount points not entered); 0 for a directory that is gone.
	Size(ctx context.Context, dir string) (int64, error)
	// ListKind, CreateKind and RemoveKind are List, Create and Remove for the
	// directories of kind (List, Create and Remove are kindScratch's).
	ListKind(ctx context.Context, root string, kind dirKind) (ids, unread []string, err error)
	CreateKind(ctx context.Context, root string, kind dirKind, id string) (string, error)
	RemoveKind(ctx context.Context, root string, kind dirKind, dir, id string, onlyEmpty bool) error
	// Pack packs the directory of id of kind into <root>/packs/<object
	// id>.tar.zst with its file list (pack.go).
	Pack(ctx context.Context, root string, kind dirKind, id, objectID string) (packResult, error)
}

// scanScratch sizes every Kairo scratch directory under root, by id.
func scanScratch(ctx context.Context, sfs storeFS, root string) (map[string]int64, error) {
	ids, _, err := sfs.List(ctx, root)
	if err != nil {
		return nil, err
	}
	out := map[string]int64{}
	for _, id := range ids {
		// One that cannot be sized now (deleted meanwhile, or unreadable)
		// is left out.
		if size, err := sfs.Size(ctx, scratchPath(sfs, root, id)); err == nil {
			out[id] = size
		}
	}
	return out, nil
}

// scratchPath is <root>/scratch/<id> as sfs spells paths (a Linux path for a
// WSL distro's root).
func scratchPath(sfs storeFS, root, id string) string { return kindPath(sfs, root, kindScratch, id) }

// kindPath is <root>/<kind's dir>/<id> as sfs spells paths.
func kindPath(sfs storeFS, root string, kind dirKind, id string) string {
	if _, ok := sfs.(wslStoreFS); ok {
		return path.Join(wslRootPath(root), kind.dir, id)
	}
	return filepath.Join(root, kind.dir, id)
}

// validScratchID reports whether id can name a scratch directory: a plain
// name (an attempt id), never a path or a dot name.
func validScratchID(id string) bool {
	if id == "" || len(id) > 128 {
		return false
	}
	for _, r := range id {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-') {
			return false
		}
	}
	return true
}

// localStoreFS reaches a root in the executor's own namespace.
type localStoreFS struct{}

func (localStoreFS) Ensure(_ context.Context, root, filesystem string, create bool) error {
	root = filepath.Clean(root)
	r, err := openDirPath(root, create)
	if !create && errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("root %s is not there (Kairo makes no durable store's root: one missing may be a disk not mounted): %w", root, err)
	}
	if err != nil {
		return err
	}
	r.Close()
	if err = checkResolvesTo(root); err != nil {
		return err
	}
	if err = checkFilesystemType(root); err != nil {
		return err
	}
	on, err := onFilesystem(root, filesystem)
	if err != nil {
		return fmt.Errorf("check that root %s is on %s: %w", root, filesystem, err)
	}
	if !on {
		return fmt.Errorf("root %s is not on filesystem %s", root, filesystem)
	}
	return nil
}

func (localStoreFS) Usage(_ context.Context, root string) (int64, int64, error) {
	return diskUsage(filepath.Clean(root))
}

func (localStoreFS) Place(_ context.Context, root string) (string, error) {
	return placeOf(filepath.Clean(root))
}

func (l localStoreFS) List(ctx context.Context, root string) ([]string, []string, error) {
	return l.ListKind(ctx, root, kindScratch)
}

func (localStoreFS) ListKind(ctx context.Context, root string, kind dirKind) ([]string, []string, error) {
	r, err := openDirPath(root, false)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	defer r.Close()
	sr, err := openPlainDir(r, kind.dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	defer sr.Close()
	names, err := readNames(ctx, sr)
	if err != nil {
		return nil, nil, err
	}
	var ids, unread []string
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		if !validScratchID(name) {
			continue
		}
		ar, err := openPlainDir(sr, name)
		if err != nil {
			// gone meanwhile, or a link: not a scratch directory
			if !errors.Is(err, fs.ErrNotExist) && !errors.Is(err, errScratchLink) {
				unread = append(unread, name)
			}
			continue
		}
		id, err := readMarkerOf(ar, kind.marker)
		ar.Close()
		switch {
		case err == nil:
			if id == name {
				ids = append(ids, name)
			}
		case !errors.Is(err, fs.ErrNotExist) && !errors.Is(err, errScratchMarker):
			unread = append(unread, name)
		}
	}
	return ids, unread, nil
}

// Scan is scanScratch on this filesystem.
func (l localStoreFS) Scan(ctx context.Context, root string) (map[string]int64, error) {
	return scanScratch(ctx, l, root)
}

func (l localStoreFS) Create(ctx context.Context, root, attemptID string) (string, error) {
	return l.CreateKind(ctx, root, kindScratch, attemptID)
}

func (localStoreFS) CreateKind(_ context.Context, root string, kind dirKind, attemptID string) (string, error) {
	if !validScratchID(attemptID) {
		return "", fmt.Errorf("%w: id %q", errOutsideRoot, attemptID)
	}
	root = filepath.Clean(root)
	// A root that is gone is made again (Ensure made it), through no link.
	r, err := openDirPath(root, true)
	if err != nil {
		return "", err
	}
	defer r.Close()
	if err = r.Mkdir(kind.dir, 0o755); err != nil && !errors.Is(err, fs.ErrExist) {
		return "", err
	}
	sr, err := openPlainDir(r, kind.dir)
	if err != nil {
		return "", err
	}
	defer sr.Close()
	if err = checkParentNotMounted(root, kind); err != nil {
		return "", err
	}
	dir := filepath.Join(root, kind.dir, attemptID)
	if err = sr.Mkdir(attemptID, 0o755); errors.Is(err, fs.ErrExist) {
		ar, openErr := openPlainDir(sr, attemptID)
		if openErr != nil {
			return "", openErr
		}
		id, markerErr := readMarkerOf(ar, kind.marker)
		ar.Close()
		if markerErr == nil && id == attemptID {
			return dir, nil
		}
		return "", err
	} else if err != nil {
		return "", err
	}
	ar, err := openPlainDir(sr, attemptID)
	if err != nil {
		_ = sr.Remove(attemptID)
		return "", err
	}
	// The marker is created, never opened: a file (or a link) put there
	// meanwhile is not written through.
	if err = writeMarkerOf(ar, kind.marker, attemptID); err != nil {
		_ = ar.Remove(kind.marker)
		ar.Close()
		_ = sr.Remove(attemptID)
		return "", err
	}
	ar.Close()
	return dir, nil
}

func (localStoreFS) Remove(ctx context.Context, root, dir, attemptID string, onlyEmpty bool) error {
	return removeScratch(ctx, root, dir, attemptID, onlyEmpty)
}

func (localStoreFS) RemoveKind(ctx context.Context, root string, kind dirKind, dir, id string, onlyEmpty bool) error {
	return removeKindDir(ctx, root, kind, dir, id, onlyEmpty)
}

func (localStoreFS) Size(ctx context.Context, dir string) (int64, error) {
	dir = filepath.Clean(dir)
	r, err := openDirPath(dir, false)
	if errors.Is(err, fs.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer r.Close()
	mounts, err := mountsUnderFn(dir)
	if err != nil {
		return 0, err
	}
	return treeSize(ctx, r, dir, mounts)
}

// openDirPath opens p, an absolute path, as an os.Root one directory at a
// time from its volume's root: each must be a plain directory, opened as the
// one checked (openPlainDir); with create, a missing one is made through its
// parent's handle. A link anywhere on the path, there all along or put there
// while it is opened, is refused, and nothing is made through one.
func openDirPath(p string, create bool) (*os.Root, error) {
	p = filepath.Clean(p)
	if !filepath.IsAbs(p) {
		return nil, fmt.Errorf("%w: %s is not an absolute path", errOutsideRoot, p)
	}
	volume := filepath.VolumeName(p)
	r, err := os.OpenRoot(volume + string(filepath.Separator))
	if err != nil {
		return nil, err
	}
	rest := strings.Trim(p[len(volume):], string(filepath.Separator))
	if rest == "" {
		return r, nil
	}
	for _, name := range strings.Split(rest, string(filepath.Separator)) {
		if create {
			if err = r.Mkdir(name, 0o755); err != nil && !errors.Is(err, fs.ErrExist) {
				r.Close()
				return nil, fmt.Errorf("make %s: %w", p, err)
			}
		}
		next, err := openPlainDir(r, name)
		r.Close()
		if err != nil {
			return nil, err
		}
		r = next
	}
	return r, nil
}

// openPlainDir opens name, a directory directly in parent, as an os.Root: it
// must be a plain directory (not a link), and the one opened must be the one
// checked (the same file, both identified through handles).
func openPlainDir(parent *os.Root, name string) (*os.Root, error) {
	fi, err := parent.Lstat(name)
	if err != nil {
		return nil, err
	}
	if isLink(fi) || !fi.IsDir() {
		return nil, fmt.Errorf("%w: %s", errScratchLink, filepath.Join(parent.Name(), name))
	}
	sub, err := parent.OpenRoot(name)
	if err != nil {
		return nil, err
	}
	opened, err := sub.Stat(".")
	if err != nil {
		sub.Close()
		return nil, err
	}
	if !os.SameFile(fi, opened) {
		sub.Close()
		return nil, fmt.Errorf("%w: %s", errRaced, filepath.Join(parent.Name(), name))
	}
	return sub, nil
}

// readMarker is the attempt id the marker in a scratch directory names.
func readMarker(dir *os.Root) (string, error) { return readMarkerOf(dir, scratchMarkerName) }

// readMarkerOf is the id the marker named marker in dir names. The marker
// must be a small regular file, not a link; it is opened without waiting (a
// FIFO put in its place does not block) and must be the file checked.
func readMarkerOf(dir *os.Root, marker string) (string, error) {
	fi, err := dir.Lstat(marker)
	if err != nil {
		return "", err
	}
	notMarker := fmt.Errorf("%w: %s is not a small regular file", errScratchMarker, filepath.Join(dir.Name(), marker))
	if isLink(fi) || !fi.Mode().IsRegular() || fi.Size() > 1024 {
		return "", notMarker
	}
	f, err := dir.OpenFile(marker, os.O_RDONLY|markerOpenFlags, 0)
	if err != nil {
		return "", err
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil {
		return "", err
	}
	if !opened.Mode().IsRegular() || !os.SameFile(fi, opened) {
		return "", notMarker
	}
	body, err := io.ReadAll(io.LimitReader(f, 1024))
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(body)), nil
}

// writeMarker creates the scratch marker naming attemptID.
func writeMarker(dir *os.Root, attemptID string) error {
	return writeMarkerOf(dir, scratchMarkerName, attemptID)
}

// writeMarkerOf creates the marker named marker naming id; it fails when
// anything is already there.
func writeMarkerOf(dir *os.Root, marker, attemptID string) error {
	f, err := dir.OpenFile(marker, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	if _, err = f.WriteString(attemptID + "\n"); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// readNames is the names of the entries of dir, read a batch at a time.
func readNames(ctx context.Context, dir *os.Root) ([]string, error) {
	f, err := dir.Open(".")
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var names []string
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		batch, err := f.Readdirnames(scratchListBatch)
		names = append(names, batch...)
		if errors.Is(err, io.EOF) || err == nil && len(batch) == 0 {
			// in the directory's own order on Linux: sorted, as os.ReadDir
			// gives them
			slices.Sort(names)
			return names, nil
		}
		if err != nil {
			return nil, err
		}
	}
}

// samePath compares two cleaned paths as the platform does (Windows: case
// insensitively).
func samePath(a, b string) bool {
	if runtime.GOOS == "windows" {
		return strings.EqualFold(a, b)
	}
	return a == b
}

// checkScratchParentNotMounted refuses a root whose <root>/scratch is a mount
// point (a bind mount of a place elsewhere, say).
func checkScratchParentNotMounted(root string) error { return checkParentNotMounted(root, kindScratch) }

// checkParentNotMounted refuses a root whose <root>/<kind's dir> is a mount
// point.
func checkParentNotMounted(root string, kind dirKind) error {
	parent := filepath.Join(root, kind.dir)
	mounts, err := mountsUnderFn(parent)
	if err != nil {
		return err
	}
	for _, m := range mounts {
		if samePath(m, parent) {
			return fmt.Errorf("%w: %s is a mount point", errScratchLink, parent)
		}
	}
	return nil
}

// removeScratch deletes dir, the scratch directory of attemptID under root,
// only if it is exactly <root>/scratch/<attemptID> of an absolute root, no
// directory on the way is a link, <root>/scratch is not a mount point and
// none is under dir, and its marker names attemptID. The marker goes last, so
// a deletion cut short is resumed; a directory without a marker is deleted
// only if it is empty (one whose deletion stopped between the marker and the
// directory). onlyEmpty deletes the directory only if it holds nothing but
// its marker. A directory already gone is no error.
func removeScratch(ctx context.Context, root, dir, attemptID string, onlyEmpty bool) error {
	return removeKindDir(ctx, root, kindScratch, dir, attemptID, onlyEmpty)
}

// removeKindDir is removeScratch for the directory of id of kind,
// <root>/<kind's dir>/<id>, with its marker.
func removeKindDir(ctx context.Context, root string, kind dirKind, dir, attemptID string, onlyEmpty bool) error {
	if !validScratchID(attemptID) {
		return fmt.Errorf("%w: id %q", errOutsideRoot, attemptID)
	}
	root = filepath.Clean(root)
	if !filepath.IsAbs(root) {
		return fmt.Errorf("%w: root %s is not absolute", errOutsideRoot, root)
	}
	want := filepath.Join(root, kind.dir, attemptID)
	if dir == "" || !samePath(filepath.Clean(dir), want) {
		return fmt.Errorf("%w: %s is not %s", errOutsideRoot, dir, want)
	}
	r, err := openDirPath(root, false)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer r.Close()
	sr, err := openPlainDir(r, kind.dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer sr.Close()
	mounts, err := mountsUnderFn(filepath.Join(root, kind.dir))
	if err != nil {
		return err
	}
	for _, m := range mounts {
		if samePath(m, filepath.Join(root, kind.dir)) || samePath(m, want) || strings.HasPrefix(m, want+string(filepath.Separator)) {
			return fmt.Errorf("%w: %s is a mount point", errScratchLink, m)
		}
	}
	// A scratch directory its task made unreadable or unwritable is made
	// the owner's again, so its marker can be read and its entries removed.
	if fi, err := sr.Lstat(attemptID); err == nil && !isLink(fi) && fi.IsDir() && fi.Mode().Perm()&0o700 != 0o700 {
		_ = chmodDir(sr, attemptID, fi.Mode().Perm()|0o700)
	}
	ar, err := openPlainDir(sr, attemptID)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	closed := false
	closeAttempt := func() {
		if !closed {
			ar.Close()
			closed = true
		}
	}
	defer closeAttempt()
	id, markerErr := readMarkerOf(ar, kind.marker)
	if markerErr != nil {
		if errors.Is(markerErr, fs.ErrNotExist) {
			names, err := readNames(ctx, ar)
			if err == nil && len(names) == 0 {
				closeAttempt()
				if err = sr.Remove(attemptID); err == nil || errors.Is(err, fs.ErrNotExist) {
					return nil // empty: a deletion stopped after its marker
				}
			}
		}
		if errors.Is(markerErr, fs.ErrNotExist) || errors.Is(markerErr, errScratchMarker) {
			// no marker in what holds more, or not a marker: not Kairo's
			return fmt.Errorf("%w: %s: %v", errScratchMarker, want, markerErr)
		}
		// unreadable now (denied, in use, an I/O error): tried again
		return fmt.Errorf("read the marker of %s: %w", want, markerErr)
	}
	if id != attemptID {
		return fmt.Errorf("%w: %s names %q, not %q", errScratchMarker, want, id, attemptID)
	}
	if onlyEmpty {
		names, err := readNames(ctx, ar)
		if err != nil {
			return err
		}
		for _, name := range names {
			if name != kind.marker {
				return fmt.Errorf("%w: %s", errScratchNotEmpty, want)
			}
		}
	} else if err = removeContents(ctx, ar, kind.marker); err != nil {
		return err
	}
	if err = ar.Remove(kind.marker); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	closeAttempt()
	if err = sr.Remove(attemptID); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

// removeContents removes what dir holds except the entry named keep, entry
// by entry through dir's Root (os.Root.RemoveAll: links removed themselves,
// never followed, nothing outside dir reached), reading the entries again
// after each batch (a directory read while entries go may skip some).
func removeContents(ctx context.Context, dir *os.Root, keep string) error {
	for {
		names, err := readBatch(dir)
		if err != nil {
			return err
		}
		left := 0
		for _, name := range names {
			if name == keep {
				continue
			}
			left++
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := dir.RemoveAll(name); err != nil {
				// A subdirectory a task made unwritable (Linux): writable
				// again, then once more.
				if !makeDirsWritable(ctx, dir, name) {
					return err
				}
				if err = dir.RemoveAll(name); err != nil {
					return err
				}
			}
		}
		if left == 0 {
			return nil
		}
	}
}

// readBatch is up to scratchListBatch names of dir's entries (dir opened
// again each time).
func readBatch(dir *os.Root) ([]string, error) {
	f, err := dir.Open(".")
	if err != nil {
		return nil, err
	}
	defer f.Close()
	names, err := f.Readdirnames(scratchListBatch)
	if errors.Is(err, io.EOF) {
		return nil, nil
	}
	return names, err
}

// eachEntry calls fn with each entry of name, a directory under r (as Lstat
// sees it: links not followed), read a batch at a time, the deadline checked
// between batches; a directory gone meanwhile has none.
func eachEntry(ctx context.Context, r *os.Root, name string, fn func(fs.FileInfo)) error {
	f, err := r.Open(name)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		infos, err := f.Readdir(scratchListBatch)
		for _, info := range infos {
			fn(info)
		}
		if errors.Is(err, io.EOF) || err == nil && len(infos) == 0 {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

// makeDirsWritable makes the directories at and under name in dir (links
// not followed) readable, writable and searchable by their owner, so what
// they hold can be removed, each through chmodDir (a directory only: a file's
// mode is never changed, as it may be shared with a file outside); it reports
// whether any was changed.
func makeDirsWritable(ctx context.Context, dir *os.Root, name string) bool {
	fi, err := dir.Lstat(name)
	if err != nil || isLink(fi) || !fi.IsDir() {
		return false
	}
	changed := false
	pending := []string{filepath.ToSlash(name)}
	if perm := fi.Mode().Perm(); perm&0o700 != 0o700 {
		if chmodDir(dir, name, perm|0o700) != nil {
			return false
		}
		changed = true
	}
	for len(pending) != 0 && ctx.Err() == nil {
		p := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		_ = eachEntry(ctx, dir, filepath.FromSlash(p), func(info fs.FileInfo) {
			if isLink(info) || !info.IsDir() {
				return
			}
			child := path.Join(p, info.Name())
			if perm := info.Mode().Perm(); perm&0o700 != 0o700 {
				if chmodDir(dir, filepath.FromSlash(child), perm|0o700) != nil {
					return
				}
				changed = true
			}
			pending = append(pending, child)
		})
	}
	return changed
}

// treeSize is the bytes of the regular files under r (opened at root): links
// are not followed, the mount points among mounts (paths under root) not
// entered, each directory read a batch at a time with the deadline checked.
func treeSize(ctx context.Context, r *os.Root, root string, mounts []string) (int64, error) {
	mounted := map[string]bool{}
	for _, m := range mounts {
		mounted[filepath.Clean(m)] = true
	}
	var total int64
	pending := []string{"."}
	for len(pending) != 0 {
		name := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		err := eachEntry(ctx, r, filepath.FromSlash(name), func(info fs.FileInfo) {
			child := path.Join(name, info.Name())
			switch {
			case isLink(info):
			case info.IsDir():
				if !mounted[filepath.Join(root, filepath.FromSlash(child))] {
					pending = append(pending, child)
				}
			case info.Mode().IsRegular():
				total += info.Size()
			}
		})
		if err != nil {
			return 0, err
		}
	}
	return total, nil
}

// placesOverlap reports whether two places (storeFS.Place, both known) are
// one place or one holds the other.
func placesOverlap(a, b string) bool {
	under := func(p, q string) bool { return p == q || strings.HasPrefix(p, strings.TrimSuffix(q, "/")+"/") }
	return a != "" && b != "" && (under(a, b) || under(b, a))
}

// mountsUnderFn is mountsUnder, replaced in tests.
var mountsUnderFn = mountsUnder
