//go:build !linux

package executor

// linkCount is how many names a file has (taken as one where it is not
// told: no container executor runs here).
func linkCount(string) (uint64, error) { return 1, nil }
