package executor

import "os"

// hashUncached is hashUncachedFile without the file read.
func hashUncached(dir *os.Root, name, fullPath string) (string, int64, error) {
	sum, n, _, err := hashUncachedFile(dir, name, fullPath)
	return sum, n, err
}
