// Package secfile restricts files holding secrets (private keys, tokens, the
// coordination database) to the current user. Unix file modes do this on
// their own; on Windows a mode of 0600 is ignored, so the file's DACL is
// replaced by one that grants only the current user and SYSTEM and does not
// inherit the directory's entries (which commonly grant Authenticated Users).
package secfile

import (
	"errors"
	"io/fs"
)

// RestrictExisting restricts each path that exists and skips missing ones.
func RestrictExisting(paths ...string) error {
	for _, path := range paths {
		if err := Restrict(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}
	return nil
}
