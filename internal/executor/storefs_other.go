//go:build !windows

package executor

import (
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

// isLink reports whether fi (from Lstat) is a symbolic link.
func isLink(fi os.FileInfo) bool { return fi.Mode()&os.ModeSymlink != 0 }

// deviceOf is the device of the filesystem fi is on (a mount point has
// another than its parent).
func deviceOf(fi os.FileInfo) uint64 {
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		return uint64(st.Dev)
	}
	return 0
}

// onFilesystem reports whether root is on the filesystem mounted at
// filesystem (/, /srv): the same device.
func onFilesystem(root, filesystem string) (bool, error) {
	a, err := os.Stat(root)
	if err != nil {
		return false, err
	}
	b, err := os.Stat(filesystem)
	if err != nil {
		return false, err
	}
	return deviceOf(a) == deviceOf(b), nil
}

// diskUsage is the total and the free bytes (available to an unprivileged
// writer) of the filesystem holding dir.
func diskUsage(dir string) (int64, int64, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(dir, &st); err != nil {
		return 0, 0, err
	}
	return int64(uint64(st.Blocks) * uint64(st.Bsize)), int64(uint64(st.Bavail) * uint64(st.Bsize)), nil
}
