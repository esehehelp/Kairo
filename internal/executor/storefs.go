package executor

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// A store's root belongs to Kairo. Under it an attempt's scratch is
// <root>/scratch/<attempt id>, holding the marker file .kairo-scratch that
// names the attempt. What Kairo counts there is only what it made (a
// directory whose marker names it); what it deletes is only a scratch
// directory recorded for the attempt whose marker names that attempt, and it
// never goes through a link: a symlink, a Windows junction or any other
// reparse point is removed itself, never what it points at, and a directory
// on another filesystem (a mount point) is not entered.

const (
	scratchDirName    = "scratch"
	scratchMarkerName = ".kairo-scratch"
)

var (
	// errOutsideRoot: a scratch path that is not <root>/scratch/<attempt id>.
	errOutsideRoot = errors.New("not a scratch directory of the store's root")
	// errScratchLink: a link, a non-directory or a mount point where a
	// scratch directory (or the scratch parent) must be a plain directory.
	errScratchLink = errors.New("a link, mount point or non-directory where a plain directory must be")
	// errScratchMarker: a scratch directory whose marker is missing or names
	// another attempt.
	errScratchMarker = errors.New("scratch marker does not name the attempt")
)

// storeFS is how an executor reaches a store's root: in its own namespace
// (localStoreFS) or inside a WSL distro (wslStoreFS).
type storeFS interface {
	// Ensure makes the root if it is missing and checks that it is a
	// directory on the declared filesystem.
	Ensure(ctx context.Context, root, filesystem string) error
	// Usage is the total and free bytes of the filesystem holding root.
	Usage(ctx context.Context, root string) (total, free int64, err error)
	// Scan sizes the Kairo scratch directories under root (a directory
	// <root>/scratch/<id> whose marker names id), by id.
	Scan(ctx context.Context, root string) (map[string]int64, error)
	// Create makes <root>/scratch/<attemptID> with its marker and returns
	// its path.
	Create(ctx context.Context, root, attemptID string) (string, error)
	// Remove deletes the scratch directory dir of attemptID under root
	// (removeScratch's rules). A directory already gone is no error.
	Remove(ctx context.Context, root, dir, attemptID string) error
	// Size is the bytes of the files under dir (links not followed, mount
	// points not entered); 0 for a directory that is gone.
	Size(ctx context.Context, dir string) (int64, error)
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
	if err := os.MkdirAll(root, 0o755); err != nil {
		return fmt.Errorf("make root %s: %w", root, err)
	}
	fi, err := os.Stat(root)
	if err != nil {
		return err
	}
	if !fi.IsDir() {
		return fmt.Errorf("root %s is not a directory", root)
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

func (localStoreFS) Scan(_ context.Context, root string) (map[string]int64, error) {
	out := map[string]int64{}
	parent := filepath.Join(filepath.Clean(root), scratchDirName)
	pfi, err := os.Lstat(parent)
	if errors.Is(err, fs.ErrNotExist) {
		return out, nil
	}
	if err != nil {
		return nil, err
	}
	if isLink(pfi) || !pfi.IsDir() {
		return nil, fmt.Errorf("%w: %s", errScratchLink, parent)
	}
	entries, err := os.ReadDir(parent)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		name := entry.Name()
		if !validScratchID(name) {
			continue
		}
		dir := filepath.Join(parent, name)
		fi, err := os.Lstat(dir)
		if err != nil || isLink(fi) || !fi.IsDir() {
			continue
		}
		if id, err := readScratchMarker(dir); err != nil || id != name {
			continue // not Kairo's
		}
		size, err := treeSize(dir)
		if err != nil {
			return nil, err
		}
		out[name] = size
	}
	return out, nil
}

func (localStoreFS) Create(_ context.Context, root, attemptID string) (string, error) {
	if !validScratchID(attemptID) {
		return "", fmt.Errorf("%w: attempt id %q", errOutsideRoot, attemptID)
	}
	parent := filepath.Join(filepath.Clean(root), scratchDirName)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return "", err
	}
	pfi, err := os.Lstat(parent)
	if err != nil {
		return "", err
	}
	if isLink(pfi) || !pfi.IsDir() {
		return "", fmt.Errorf("%w: %s", errScratchLink, parent)
	}
	dir := filepath.Join(parent, attemptID)
	if err = os.Mkdir(dir, 0o755); err != nil {
		return "", err
	}
	if err = os.WriteFile(filepath.Join(dir, scratchMarkerName), []byte(attemptID+"\n"), 0o644); err != nil {
		_ = os.Remove(dir)
		return "", err
	}
	return dir, nil
}

func (localStoreFS) Remove(_ context.Context, root, dir, attemptID string) error {
	return removeScratch(root, dir, attemptID)
}

func (localStoreFS) Size(_ context.Context, dir string) (int64, error) {
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
	return treeSize(dir)
}

// readScratchMarker is the attempt id a scratch directory's marker names.
// The marker must be a small regular file, not a link.
func readScratchMarker(dir string) (string, error) {
	path := filepath.Join(dir, scratchMarkerName)
	fi, err := os.Lstat(path)
	if err != nil {
		return "", err
	}
	if isLink(fi) || !fi.Mode().IsRegular() || fi.Size() > 1024 {
		return "", fmt.Errorf("%w: %s is not a small regular file", errScratchMarker, path)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(body)), nil
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
// only if it is exactly <root>/scratch/<attemptID>, neither it nor
// <root>/scratch is a link, and its marker names attemptID. It walks the tree
// itself (Lstat by Lstat, not os.RemoveAll): a link (symlink, junction,
// reparse point) is removed itself and never entered, a directory on another
// filesystem is not entered (the deletion stops there), and the marker goes
// last, so a deletion cut short is resumed. A directory without a marker is
// deleted only if it is empty (one whose deletion stopped between the marker
// and the directory). A directory already gone is no error.
func removeScratch(root, dir, attemptID string) error {
	if !validScratchID(attemptID) {
		return fmt.Errorf("%w: attempt id %q", errOutsideRoot, attemptID)
	}
	root = filepath.Clean(root)
	parent := filepath.Join(root, scratchDirName)
	want := filepath.Join(parent, attemptID)
	if !samePath(filepath.Clean(dir), want) {
		return fmt.Errorf("%w: %s is not %s", errOutsideRoot, dir, want)
	}
	pfi, err := os.Lstat(parent)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if isLink(pfi) || !pfi.IsDir() {
		return fmt.Errorf("%w: %s", errScratchLink, parent)
	}
	fi, err := os.Lstat(want)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if isLink(fi) || !fi.IsDir() {
		return fmt.Errorf("%w: %s", errScratchLink, want)
	}
	id, markerErr := readScratchMarker(want)
	if markerErr != nil {
		if errors.Is(markerErr, fs.ErrNotExist) && os.Remove(want) == nil {
			return nil // empty: a deletion stopped after its marker
		}
		return fmt.Errorf("%w: %s: %v", errScratchMarker, want, markerErr)
	}
	if id != attemptID {
		return fmt.Errorf("%w: %s names %q, not %q", errScratchMarker, want, id, attemptID)
	}
	if err = removeContents(want, deviceOf(fi), scratchMarkerName); err != nil {
		return err
	}
	if err = removeEntry(filepath.Join(want, scratchMarkerName), false); err != nil {
		return err
	}
	return removeEntry(want, false)
}

// removeContents removes what dir holds except the entry named keep (""
// keeps nothing), never through a link and never into another filesystem.
func removeContents(dir string, device uint64, keep string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if keep != "" && entry.Name() == keep {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		fi, err := os.Lstat(path)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		switch {
		case isLink(fi):
			// The link itself goes, never what it points at.
			if err = removeEntry(path, true); err != nil {
				return err
			}
		case fi.IsDir():
			if deviceOf(fi) != device {
				return fmt.Errorf("%w: %s is on another filesystem", errScratchLink, path)
			}
			if err = removeContents(path, device, ""); err != nil {
				return err
			}
			if err = removeEntry(path, false); err != nil {
				return err
			}
		default:
			if err = removeEntry(path, false); err != nil {
				return err
			}
		}
	}
	return nil
}

// removeEntry removes one file, empty directory or link. A read-only file or
// directory (which Windows refuses to delete) is made writable first; a link
// never is (a mode change would reach what it points at).
func removeEntry(path string, link bool) error {
	err := os.Remove(path)
	if err == nil || errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if link || os.Chmod(path, 0o700) != nil {
		return err
	}
	if err = os.Remove(path); err == nil || errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}

// treeSize is the bytes of the regular files under dir: links are not
// followed, directories on another filesystem not entered.
func treeSize(dir string) (int64, error) {
	fi, err := os.Lstat(dir)
	if err != nil {
		return 0, err
	}
	return sizeUnder(dir, deviceOf(fi))
}

func sizeUnder(dir string, device uint64) (int64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, err
	}
	var total int64
	for _, entry := range entries {
		path := filepath.Join(dir, entry.Name())
		fi, err := os.Lstat(path)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return 0, err
		}
		switch {
		case isLink(fi):
		case fi.IsDir():
			if deviceOf(fi) != device {
				continue
			}
			size, err := sizeUnder(path, device)
			if err != nil {
				return 0, err
			}
			total += size
		case fi.Mode().IsRegular():
			total += fi.Size()
		}
	}
	return total, nil
}
