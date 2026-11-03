//go:build !linux

package executor

import (
	"errors"
	"io/fs"
	"os"
)

// chmodDir: no handle here can only be a directory, so no mode is changed
// (on Windows deletion ignores the read-only attribute anyway).
func chmodDir(*os.Root, string, fs.FileMode) error { return errors.ErrUnsupported }
