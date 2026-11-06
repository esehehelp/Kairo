package executor

import (
	"os"
	"path/filepath"
)

// A temporary directory under an 8.3 short name (GitHub's runners have
// TEMP=C:\Users\RUNNER~1\...) is refused as a store root: it names another
// path than the one it resolves to. The tests' temporary directories go under
// the long name instead.
func init() {
	temp := os.TempDir()
	if long, err := filepath.EvalSymlinks(temp); err == nil && long != temp {
		os.Setenv("TMP", long)
		os.Setenv("TEMP", long)
	}
}
