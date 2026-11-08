//go:build !windows

package executor

// openTree: an adopted attempt's tree is its launcher's process group, found
// through the attempt's pid (attemptTree).
func openTree(string) (*treeHandle, error) { return nil, nil }
