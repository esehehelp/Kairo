//go:build !windows && !linux

package executor

import (
	"errors"
	"os"
)

// hashUncachedFile is the sha256 (hex) and size of name in dir (a regular
// file), and the file read, as its handle stats it. Elsewhere than Windows
// and Linux it is read as the system gives it.
func hashUncachedFile(dir *os.Root, name, fullPath string) (string, int64, os.FileInfo, error) {
	fi, err := dir.Lstat(name)
	if err != nil {
		return "", 0, nil, err
	}
	if !fi.Mode().IsRegular() {
		return "", 0, nil, errors.New(fullPath + " is not a regular file")
	}
	f, err := dir.Open(name)
	if err != nil {
		return "", 0, nil, err
	}
	defer f.Close()
	read, err := f.Stat()
	if err != nil {
		return "", 0, nil, err
	}
	sum, n, err := hashOf(f)
	return sum, n, read, err
}
