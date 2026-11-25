//go:build linux

package executor

import (
	"os"
	"syscall"
)

// linkCount is how many names a file has.
func linkCount(path string) (uint64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		return uint64(st.Nlink), nil
	}
	return 1, nil
}
