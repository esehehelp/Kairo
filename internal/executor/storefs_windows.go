//go:build windows

package executor

import (
	"fmt"
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

// onFilesystem reports whether root is on filesystem, a drive (E:). With
// checkResolvesTo the drive in root's path is the volume it is on.
func onFilesystem(root, filesystem string) (bool, error) {
	volume := strings.TrimRight(filepath.VolumeName(root), `\/`)
	return volume != "" && strings.EqualFold(volume, strings.TrimRight(filesystem, `\/`)), nil
}

// checkResolvesTo checks that the directory opened at root is root itself:
// the final path of its handle (normalized, with a drive letter) is root, so
// no junction, mount point, substituted drive or short (8.3) name stands in
// its path and the drive in it is the volume it is on.
func checkResolvesTo(root string) error {
	path, err := windows.UTF16PtrFromString(root)
	if err != nil {
		return err
	}
	h, err := windows.CreateFile(path, 0, windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, nil,
		windows.OPEN_EXISTING, windows.FILE_FLAG_BACKUP_SEMANTICS, 0)
	if err != nil {
		return err
	}
	defer windows.CloseHandle(h)
	buf := make([]uint16, windows.MAX_LONG_PATH)
	// 0: FILE_NAME_NORMALIZED | VOLUME_NAME_DOS
	n, err := windows.GetFinalPathNameByHandle(h, &buf[0], uint32(len(buf)), 0)
	if err != nil {
		return err
	}
	if int(n) >= len(buf) {
		return fmt.Errorf("final path of %s too long", root)
	}
	final := windows.UTF16ToString(buf[:n])
	final = strings.TrimPrefix(final, `\\?\`)
	if !strings.EqualFold(filepath.Clean(final), filepath.Clean(root)) {
		return fmt.Errorf("%w: root %s resolves to %s", errScratchLink, root, final)
	}
	return nil
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

// mountsUnder: a volume mounted in a folder is a reparse point there, which
// nothing enters or deletes through; there is no mount to look for.
func mountsUnder(string) ([]string, error) { return nil, nil }

// makeDirsWritable: deletion ignores the read-only attribute on Windows, so
// nothing is ever made writable (an attribute changed on a hard link would
// change the file outside too).
func makeDirsWritable(*os.Root, string) bool { return false }
