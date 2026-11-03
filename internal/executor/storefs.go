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
	"strings"
)

// A store's root belongs to Kairo. Under it an attempt's scratch is
// <root>/scratch/<attempt id>, holding the marker file .kairo-scratch that
// names the attempt. What Kairo counts there is only what it made (a
// directory whose marker names it); what it deletes is only a scratch
// directory recorded for the attempt whose marker names that attempt.
//
// Nothing is reached through a path opened again after it was checked: the
// root's path has no link in it (every component checked, and on Windows the
// path the opened root resolves to must be the declared one), and below the
// root every operation goes through os.Root handles (openat-style on Linux,
// handle-relative NtCreateFile on Windows). A directory checked with Lstat is
// opened as a Root and must be the one checked (the same file), so a
// directory swapped for a link between the two is refused; within an opened
// directory os.Root.RemoveAll removes a link itself (a symlink, a junction,
// any reparse point) and never what it points at, and no operation leaves the
// directory it started from. Files are deleted without changing their
// attributes (Windows ignores the read-only attribute on deletion; on Linux a
// file's mode never matters), so a hard link to a file outside is unlinked
// without touching that file; only a scratch subdirectory a task made
// unwritable is made writable again, through its Root (directories cannot be
// hard linked). On Linux a scratch with a mount point anywhere under it (a
// bind mount included, which os.Root would cross) is not deleted.

const (
	scratchDirName    = "scratch"
	scratchMarkerName = ".kairo-scratch"
	// scratchListBatch is how many directory entries are read at a time.
	scratchListBatch = 1024
)

var (
	// errOutsideRoot: a scratch path that is not <root>/scratch/<attempt id>.
	errOutsideRoot = errors.New("not a scratch directory of the store's root")
	// errScratchLink: a link, a non-directory or a mount point where a
	// plain directory must be (the root, a component of its path, the
	// scratch parent or a scratch directory), or a directory changed while it
	// was being opened.
	errScratchLink = errors.New("a link, mount point or non-directory where a plain directory must be")
	// errScratchMarker: a scratch directory whose marker is missing or names
	// another attempt.
	errScratchMarker = errors.New("scratch marker does not name the attempt")
	// errScratchNotEmpty: a scratch to be deleted only if empty holds more
	// than its marker.
	errScratchNotEmpty = errors.New("scratch directory holds more than its marker")
)

// storeFS is how an executor reaches a store's root: in its own namespace
// (localStoreFS) or inside a WSL distro (wslStoreFS).
type storeFS interface {
	// Ensure makes the root if it is missing and checks that it is a
	// directory on the declared filesystem whose path has no link in it.
	Ensure(ctx context.Context, root, filesystem string) error
	// Usage is the total and free bytes of the filesystem holding root.
	Usage(ctx context.Context, root string) (total, free int64, err error)
	// List is the ids of the Kairo scratch directories under root (a
	// directory <root>/scratch/<id> whose marker names id).
	List(ctx context.Context, root string) ([]string, error)
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
}

// scanScratch sizes every Kairo scratch directory under root, by id.
func scanScratch(ctx context.Context, sfs storeFS, root string) (map[string]int64, error) {
	ids, err := sfs.List(ctx, root)
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
func scratchPath(sfs storeFS, root, id string) string {
	if _, ok := sfs.(wslStoreFS); ok {
		return path.Join(wslRootPath(root), scratchDirName, id)
	}
	return filepath.Join(root, scratchDirName, id)
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

func (localStoreFS) Ensure(_ context.Context, root, filesystem string) error {
	root = filepath.Clean(root)
	if !filepath.IsAbs(root) {
		return fmt.Errorf("root %s is not absolute", root)
	}
	if err := checkPathHasNoLinks(root); err != nil {
		return err
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return fmt.Errorf("make root %s: %w", root, err)
	}
	if err := checkPathHasNoLinks(root); err != nil {
		return err
	}
	fi, err := os.Lstat(root)
	if err != nil {
		return err
	}
	if !fi.IsDir() {
		return fmt.Errorf("root %s is not a directory", root)
	}
	if err = checkResolvesTo(root); err != nil {
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

func (localStoreFS) List(ctx context.Context, root string) ([]string, error) {
	r, err := openStoreRoot(root)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer r.Close()
	sr, err := openPlainDir(r, scratchDirName)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer sr.Close()
	names, err := readNames(ctx, sr)
	if err != nil {
		return nil, err
	}
	var ids []string
	for _, name := range names {
		if !validScratchID(name) {
			continue
		}
		ar, err := openPlainDir(sr, name)
		if err != nil {
			continue
		}
		id, err := readMarker(ar)
		ar.Close()
		if err == nil && id == name {
			ids = append(ids, name)
		}
	}
	return ids, nil
}

// Scan is scanScratch on this filesystem.
func (l localStoreFS) Scan(ctx context.Context, root string) (map[string]int64, error) {
	return scanScratch(ctx, l, root)
}

func (localStoreFS) Create(_ context.Context, root, attemptID string) (string, error) {
	if !validScratchID(attemptID) {
		return "", fmt.Errorf("%w: attempt id %q", errOutsideRoot, attemptID)
	}
	// A root that is gone is made again (Ensure made it), through no link.
	if err := checkPathHasNoLinks(filepath.Clean(root)); err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Clean(root), 0o755); err != nil {
		return "", err
	}
	r, err := openStoreRoot(root)
	if err != nil {
		return "", err
	}
	defer r.Close()
	if err = r.Mkdir(scratchDirName, 0o755); err != nil && !errors.Is(err, fs.ErrExist) {
		return "", err
	}
	sr, err := openPlainDir(r, scratchDirName)
	if err != nil {
		return "", err
	}
	defer sr.Close()
	dir := filepath.Join(filepath.Clean(root), scratchDirName, attemptID)
	if err = sr.Mkdir(attemptID, 0o755); errors.Is(err, fs.ErrExist) {
		ar, openErr := openPlainDir(sr, attemptID)
		if openErr != nil {
			return "", openErr
		}
		id, markerErr := readMarker(ar)
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
	if err = writeMarker(ar, attemptID); err != nil {
		_ = ar.Remove(scratchMarkerName)
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

func (localStoreFS) Size(ctx context.Context, dir string) (int64, error) {
	dir = filepath.Clean(dir)
	fi, err := os.Lstat(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if isLink(fi) || !fi.IsDir() {
		return 0, fmt.Errorf("%w: %s", errScratchLink, dir)
	}
	r, err := os.OpenRoot(dir)
	if err != nil {
		return 0, err
	}
	defer r.Close()
	if err = checkOpened(dir, fi, r); err != nil {
		return 0, err
	}
	mounts, err := mountsUnderFn(dir)
	if err != nil {
		return 0, err
	}
	return treeSize(ctx, r, dir, mounts)
}

// checkPathHasNoLinks checks that no existing component of path (an
// absolute, cleaned path) is a link of any kind; components that do not
// exist yet are fine.
func checkPathHasNoLinks(path string) error {
	volume := filepath.VolumeName(path)
	current := volume + string(filepath.Separator)
	rest := strings.TrimPrefix(path[len(volume):], string(filepath.Separator))
	if rest == "" {
		return nil
	}
	for _, part := range strings.Split(rest, string(filepath.Separator)) {
		current = filepath.Join(current, part)
		fi, err := os.Lstat(current)
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if isLink(fi) {
			return fmt.Errorf("%w: %s, on the path of %s, is a link", errScratchLink, current, path)
		}
		if !fi.IsDir() {
			return fmt.Errorf("%w: %s, on the path of %s, is not a directory", errScratchLink, current, path)
		}
	}
	return nil
}

// openStoreRoot opens a store's root as an os.Root, refusing a root whose
// path has a link in it or that changed while it was opened.
func openStoreRoot(root string) (*os.Root, error) {
	root = filepath.Clean(root)
	if err := checkPathHasNoLinks(root); err != nil {
		return nil, err
	}
	fi, err := os.Lstat(root)
	if err != nil {
		return nil, err
	}
	if isLink(fi) || !fi.IsDir() {
		return nil, fmt.Errorf("%w: %s", errScratchLink, root)
	}
	r, err := os.OpenRoot(root)
	if err != nil {
		return nil, err
	}
	if err = checkOpened(root, fi, r); err != nil {
		r.Close()
		return nil, err
	}
	return r, nil
}

// checkOpened checks that r, opened at path after Lstat gave fi, is that
// directory: the same file (where a FileInfo identifies one), and a path
// that still has no link in it.
func checkOpened(path string, fi os.FileInfo, r *os.Root) error {
	opened, err := r.Stat(".")
	if err != nil {
		return err
	}
	if !sameDirectory(fi, opened) {
		return fmt.Errorf("%w: %s changed while it was opened", errScratchLink, path)
	}
	return checkPathHasNoLinks(path)
}

// openPlainDir opens name, a directory directly in parent, as an os.Root: it
// must be a plain directory (not a link), and the one opened must be the one
// checked.
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
	if !sameDirectory(fi, opened) {
		sub.Close()
		return nil, fmt.Errorf("%w: %s changed while it was opened", errScratchLink, filepath.Join(parent.Name(), name))
	}
	return sub, nil
}

// readMarker is the attempt id the marker in a scratch directory names. The
// marker must be a small regular file, not a link.
func readMarker(dir *os.Root) (string, error) {
	fi, err := dir.Lstat(scratchMarkerName)
	if err != nil {
		return "", err
	}
	if isLink(fi) || !fi.Mode().IsRegular() || fi.Size() > 1024 {
		return "", fmt.Errorf("%w: %s is not a small regular file", errScratchMarker, filepath.Join(dir.Name(), scratchMarkerName))
	}
	f, err := dir.Open(scratchMarkerName)
	if err != nil {
		return "", err
	}
	defer f.Close()
	body, err := io.ReadAll(io.LimitReader(f, 1024))
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(body)), nil
}

// writeMarker creates the marker naming attemptID; it fails when anything
// is already there.
func writeMarker(dir *os.Root, attemptID string) error {
	f, err := dir.OpenFile(scratchMarkerName, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
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

// removeScratch deletes dir, the scratch directory of attemptID under root,
// only if it is exactly <root>/scratch/<attemptID>, the root's path has no
// link in it, neither <root>/scratch nor dir is a link, no mount point is
// under dir, and its marker names attemptID. The marker goes last, so a
// deletion cut short is resumed; a directory without a marker is deleted
// only if it is empty (one whose deletion stopped between the marker and the
// directory). onlyEmpty deletes the directory only if it holds nothing but
// its marker. A directory already gone is no error.
func removeScratch(ctx context.Context, root, dir, attemptID string, onlyEmpty bool) error {
	if !validScratchID(attemptID) {
		return fmt.Errorf("%w: attempt id %q", errOutsideRoot, attemptID)
	}
	root = filepath.Clean(root)
	want := filepath.Join(root, scratchDirName, attemptID)
	if dir == "" || !samePath(filepath.Clean(dir), want) {
		return fmt.Errorf("%w: %s is not %s", errOutsideRoot, dir, want)
	}
	r, err := openStoreRoot(root)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer r.Close()
	sr, err := openPlainDir(r, scratchDirName)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer sr.Close()
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
	mounts, err := mountsUnderFn(want)
	if err != nil {
		return err
	}
	if len(mounts) != 0 {
		return fmt.Errorf("%w: %s is mounted under %s", errScratchLink, mounts[0], want)
	}
	id, markerErr := readMarker(ar)
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
		return fmt.Errorf("%w: %s: %v", errScratchMarker, want, markerErr)
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
			if name != scratchMarkerName {
				return fmt.Errorf("%w: %s", errScratchNotEmpty, want)
			}
		}
	} else if err = removeContents(ctx, ar, scratchMarkerName); err != nil {
		return err
	}
	if err = ar.Remove(scratchMarkerName); err != nil && !errors.Is(err, fs.ErrNotExist) {
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
				// again through the Root, then once more.
				if !makeDirsWritable(dir, name) {
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

// treeSize is the bytes of the regular files under r (opened at path): links
// are not followed, the mount points among mounts (paths under path) not
// entered.
func treeSize(ctx context.Context, r *os.Root, path string, mounts []string) (int64, error) {
	mounted := map[string]bool{}
	for _, m := range mounts {
		mounted[filepath.Clean(m)] = true
	}
	var total int64
	err := fs.WalkDir(r.FS(), ".", func(name string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil // deleted meanwhile
			}
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		info, err := d.Info()
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		switch {
		case isLink(info):
			if d.IsDir() {
				return fs.SkipDir
			}
		case d.IsDir():
			if name != "." && mounted[filepath.Join(path, filepath.FromSlash(name))] {
				return fs.SkipDir
			}
		case info.Mode().IsRegular():
			total += info.Size()
		}
		return nil
	})
	return total, err
}

// sameDirectory reports whether a (from Lstat of a path) and b (from Stat
// of what was opened there) are the same file. On Windows a FileInfo from a
// path loads its file id from that path now, so a directory swapped for a
// link reads as another file.
func sameDirectory(a, b os.FileInfo) bool { return os.SameFile(a, b) }

// mountsUnderFn is mountsUnder, replaced in tests.
var mountsUnderFn = mountsUnder
