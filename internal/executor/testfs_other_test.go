//go:build !windows

package executor

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
)

// mountOf is the mount point that covers dir (the longest holding it, the
// last listed of those), as a store would declare its filesystem; "" where
// there is no /proc.
func mountOf(dir string) string {
	f, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return ""
	}
	defer f.Close()
	dir = filepath.Clean(dir)
	best := ""
	lines := bufio.NewScanner(f)
	for lines.Scan() {
		fields := strings.Fields(lines.Text())
		if len(fields) < 5 {
			continue
		}
		point := filepath.Clean(unescapeMountField(fields[4]))
		if (point == "/" || point == dir || strings.HasPrefix(dir, point+"/")) && len(point) >= len(best) {
			best = point
		}
	}
	return best
}
