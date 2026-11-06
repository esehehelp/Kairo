//go:build windows

package executor

// mountOf: on Windows a store's filesystem is its drive (testFilesystem).
func mountOf(string) string { return "" }
