//go:build !windows

package executor

import "golang.org/x/sys/unix"

// diskFree is the space of the filesystem holding dir that an unprivileged
// writer may still use (statfs f_bavail).
func diskFree(dir string) (int64, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(dir, &st); err != nil {
		return 0, err
	}
	return int64(uint64(st.Bavail) * uint64(st.Bsize)), nil
}
