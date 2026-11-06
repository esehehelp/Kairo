//go:build !windows && !linux

package executor

import (
	"errors"
	"os"
)

// hashUncached is the sha256 (hex) and size of name in dir (a regular file).
// Elsewhere than Windows and Linux it is read as the system gives it.
func hashUncached(dir *os.Root, name, fullPath string) (string, int64, error) {
	fi, err := dir.Lstat(name)
	if err != nil {
		return "", 0, err
	}
	if !fi.Mode().IsRegular() {
		return "", 0, errors.New(fullPath + " is not a regular file")
	}
	f, err := dir.Open(name)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	return hashOf(f)
}
