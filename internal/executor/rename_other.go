//go:build !windows

package executor

// renameTransient: elsewhere an open file does not keep its directory from
// being renamed (renameSettled).
func renameTransient(error) bool { return false }
