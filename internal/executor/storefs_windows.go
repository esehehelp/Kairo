//go:build windows

package executor

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"unsafe"

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

// openTreeEntry reads through os.Root unless it refuses a reserved name.
// Native handle-relative opens bypass DOS device names without reopening
// an absolute path whose ancestors could have been replaced by links.
func openTreeEntry(r *os.Root, name string, flags int) (*os.File, error) {
	rel := filepath.ToSlash(name)
	if !reservedWindowsPath(rel) {
		return r.OpenFile(name, flags, 0)
	}
	f, err := r.Open(".")
	if err != nil {
		return nil, err
	}
	for part := range strings.SplitSeq(rel, "/") {
		next, err := openLiteralChild(f, part)
		f.Close()
		if err != nil {
			return nil, &os.PathError{Op: "openat", Path: name, Err: err}
		}
		f = next
	}
	return f, nil
}

// openLiteralChild opens one validated component, refusing every reparse
// point on the handle itself, before it can be read or used as a parent.
func openLiteralChild(parent *os.File, name string) (*os.File, error) {
	ntName, err := windows.NewNTUnicodeString(name)
	if err != nil {
		return nil, err
	}
	attrs := windows.OBJECT_ATTRIBUTES{RootDirectory: windows.Handle(parent.Fd()), ObjectName: ntName, Attributes: windows.OBJ_DONT_REPARSE}
	attrs.Length = uint32(unsafe.Sizeof(attrs))
	var h windows.Handle
	err = windows.NtCreateFile(&h, windows.FILE_GENERIC_READ, &attrs, &windows.IO_STATUS_BLOCK{}, nil, 0,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, windows.FILE_OPEN,
		windows.FILE_SYNCHRONOUS_IO_NONALERT|windows.FILE_OPEN_REPARSE_POINT, 0, 0)
	runtime.KeepAlive(parent)
	if err != nil {
		if err == windows.STATUS_REPARSE_POINT_ENCOUNTERED {
			return nil, errScratchLink
		}
		if status, ok := err.(windows.NTStatus); ok {
			err = status.Errno()
		}
		return nil, err
	}
	var info windows.ByHandleFileInformation
	if err = windows.GetFileInformationByHandle(h, &info); err == nil && info.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		err = errScratchLink
	}
	if err != nil {
		windows.CloseHandle(h)
		return nil, err
	}
	return os.NewFile(uintptr(h), filepath.Join(parent.Name(), name)), nil
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

// markerOpenFlags: nothing a marker's open could wait on can be put in its
// place here.
const markerOpenFlags = 0

// checkFilesystemType checks that root is on NTFS, where Go deletes a file
// ignoring its read-only attribute (FILE_DISPOSITION_IGNORE_READONLY_ATTRIBUTE),
// or on exFAT, which has no hard links (nor reparse points): elsewhere Go
// clears the attribute first, which a hard link shares with the file
// outside it points at, and on exFAT that file can only be the one deleted.
func checkFilesystemType(root string) error {
	fsName, err := filesystemName(root)
	if err != nil {
		return err
	}
	if fsName != "NTFS" && fsName != "exFAT" {
		return fmt.Errorf("root %s is on %s, not NTFS or exFAT (where a deletion never clears the read-only attribute of a file a hard link shares)", root, fsName)
	}
	return nil
}

// filesystemName is the name of the filesystem p is on (NTFS, exFAT, ...).
func filesystemName(p string) (string, error) {
	path, err := windows.UTF16PtrFromString(p)
	if err != nil {
		return "", err
	}
	volume := make([]uint16, windows.MAX_PATH+1)
	if err = windows.GetVolumePathName(path, &volume[0], uint32(len(volume))); err != nil {
		return "", fmt.Errorf("volume of %s: %w", p, err)
	}
	name := make([]uint16, windows.MAX_PATH+1)
	if err = windows.GetVolumeInformation(&volume[0], nil, 0, nil, nil, nil, &name[0], uint32(len(name))); err != nil {
		return "", fmt.Errorf("filesystem of %s: %w", p, err)
	}
	return windows.UTF16ToString(name), nil
}

// stableFileIDs reports whether the files under p keep one id that a
// directory listing and an opened handle agree on: not on exFAT or FAT,
// where an id is where a file's directory entry is and a listing need not
// carry it. Unknown: taken as stable (the stricter check).
func stableFileIDs(p string) bool {
	name, err := filesystemName(p)
	return err != nil || name != "exFAT" && name != "FAT32" && name != "FAT"
}

// placeOf: with checkResolvesTo a root's path is where it is, and roots are
// compared by path when they are declared; there is no other place to tell.
func placeOf(string) (string, error) { return "", nil }

// mountsUnder: a volume mounted in a folder is a reparse point there, which
// nothing enters or deletes through; there is no mount to look for.
func mountsUnder(string) ([]string, error) { return nil, nil }

// syncRenamed makes the renames of names in dir durable as far as Windows
// lets it: a directory cannot be flushed, so each file renamed is (with its
// directory entry on exFAT, its metadata and the journal on NTFS).
func syncRenamed(dir *os.Root, names ...string) error {
	for _, name := range names {
		f, err := dir.OpenFile(name, os.O_WRONLY, 0)
		if err != nil {
			return err
		}
		err = f.Sync()
		f.Close()
		if err != nil {
			return err
		}
	}
	return nil
}
