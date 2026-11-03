//go:build windows

package executor

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"golang.org/x/sys/windows"
)

// isLink reports whether fi (from Lstat) is a link of any kind: a symlink, a
// junction, or any other reparse point (Go reports junctions as irregular
// rather than as symlinks; the attribute says it either way).
func isLink(fi os.FileInfo) bool {
	if fi.Mode()&(os.ModeSymlink|os.ModeIrregular) != 0 {
		return true
	}
	if attributes, ok := fi.Sys().(*syscall.Win32FileAttributeData); ok && attributes.FileAttributes&syscall.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		return true
	}
	return false
}

// deviceOf: a volume mounted inside another is a reparse point (isLink), so
// every plain directory is on its parent's volume.
func deviceOf(os.FileInfo) uint64 { return 0 }

// onFilesystem reports whether root is on filesystem, a drive (E:).
func onFilesystem(root, filesystem string) (bool, error) {
	volume := strings.TrimRight(filepath.VolumeName(root), `\/`)
	return volume != "" && strings.EqualFold(volume, strings.TrimRight(filesystem, `\/`)), nil
}

// diskUsage is the total and the free bytes (available to this user) of the
// volume holding dir.
func diskUsage(dir string) (int64, int64, error) {
	path, err := windows.UTF16PtrFromString(dir)
	if err != nil {
		return 0, 0, err
	}
	var available, total, free uint64
	if err = windows.GetDiskFreeSpaceEx(path, &available, &total, &free); err != nil {
		return 0, 0, err
	}
	return int64(total), int64(available), nil
}
