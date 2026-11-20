//go:build windows

package executor

import (
	"errors"

	"golang.org/x/sys/windows"
)

// renameTransient: a rename refused while something holds a file of the
// tree open (renameSettled).
func renameTransient(err error) bool {
	return errors.Is(err, windows.ERROR_ACCESS_DENIED) || errors.Is(err, windows.ERROR_SHARING_VIOLATION)
}
