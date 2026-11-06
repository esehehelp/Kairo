//go:build windows

package executor

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

// uncachedBlock is the read size around the cache: a multiple of any sector
// size, read into a buffer aligned to 4 KiB, as FILE_FLAG_NO_BUFFERING
// requires.
const uncachedBlock = 1 << 20

// hashUncachedFile is the sha256 (hex) and size of the file at fullPath
// (name in dir, opened there first to check it is a regular file), read with
// FILE_FLAG_NO_BUFFERING: from the disk, not from the cache that still holds
// what was just written; and the file read, as its handle stats it. The file
// opened by its path must be the one opened in dir (nothing on the path
// changed meanwhile), and none may write it while it is read.
func hashUncachedFile(dir *os.Root, name, fullPath string) (string, int64, os.FileInfo, error) {
	fi, err := dir.Lstat(name)
	if err != nil {
		return "", 0, nil, err
	}
	if !fi.Mode().IsRegular() {
		return "", 0, nil, errors.New(fullPath + " is not a regular file")
	}
	inDir, err := dir.OpenFile(name, os.O_RDONLY, 0)
	if err != nil {
		return "", 0, nil, err
	}
	defer inDir.Close()
	inDirInfo, err := inDir.Stat()
	if err != nil {
		return "", 0, nil, err
	}
	p, err := windows.UTF16PtrFromString(fullPath)
	if err != nil {
		return "", 0, nil, err
	}
	h, err := windows.CreateFile(p, windows.GENERIC_READ, windows.FILE_SHARE_READ, nil, windows.OPEN_EXISTING,
		windows.FILE_FLAG_NO_BUFFERING|windows.FILE_FLAG_SEQUENTIAL_SCAN|windows.FILE_FLAG_OPEN_REPARSE_POINT, 0)
	if err != nil {
		return "", 0, nil, err
	}
	f := os.NewFile(uintptr(h), fullPath)
	defer f.Close()
	read, err := f.Stat()
	if err != nil {
		return "", 0, nil, err
	}
	if !os.SameFile(inDirInfo, read) {
		return "", 0, nil, fmt.Errorf("%w: %s", errRaced, fullPath)
	}
	var info windows.ByHandleFileInformation
	if err = windows.GetFileInformationByHandle(h, &info); err != nil {
		return "", 0, nil, err
	}
	if info.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 || info.FileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY != 0 {
		return "", 0, nil, errors.New(fullPath + " is not a regular file")
	}
	raw := make([]byte, uncachedBlock+4096)
	off := int((4096 - uintptr(unsafe.Pointer(&raw[0]))%4096) % 4096)
	buf := raw[off : off+uncachedBlock]
	sum := sha256.New()
	var total int64
	for {
		var n uint32
		err := windows.ReadFile(h, buf, &n, nil)
		if n > 0 {
			sum.Write(buf[:n])
			total += int64(n)
		}
		if err != nil {
			if errors.Is(err, windows.ERROR_HANDLE_EOF) {
				break
			}
			return "", 0, nil, err
		}
		if n == 0 {
			break
		}
	}
	return hex.EncodeToString(sum.Sum(nil)), total, read, nil
}
