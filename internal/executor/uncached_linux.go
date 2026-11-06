//go:build linux

package executor

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

// hashUncachedFile is the sha256 (hex) and size of name in dir (a regular
// file), read after the cache's pages of it were dropped (it was synced, so
// they are clean): from the disk, not from what was just written; and the
// file read, as its handle stats it. Dropping is best effort: a filesystem
// with a cache of its own (ZFS's ARC) may still answer from memory.
func hashUncachedFile(dir *os.Root, name, fullPath string) (string, int64, os.FileInfo, error) {
	fi, err := dir.Lstat(name)
	if err != nil {
		return "", 0, nil, err
	}
	if !fi.Mode().IsRegular() {
		return "", 0, nil, errors.New(fullPath + " is not a regular file")
	}
	f, err := dir.OpenFile(name, os.O_RDONLY|unix.O_NOFOLLOW, 0)
	if err != nil {
		return "", 0, nil, err
	}
	defer f.Close()
	read, err := f.Stat()
	if err != nil {
		return "", 0, nil, err
	}
	if err = unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_DONTNEED); err != nil {
		return "", 0, nil, err
	}
	sum, n, err := hashOf(f)
	return sum, n, read, err
}
