//go:build !windows

package secfile

import "os"

// Restrict makes path readable and writable by its owner only.
func Restrict(path string) error {
	return os.Chmod(path, 0o600)
}
