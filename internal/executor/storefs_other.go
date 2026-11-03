//go:build !windows

package executor

import (
	"bufio"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

// markerOpenFlags: a marker is opened without waiting, so a FIFO put in its
// place does not block the open.
const markerOpenFlags = syscall.O_NONBLOCK

// isLink reports whether fi (from Lstat) is a symbolic link.
func isLink(fi os.FileInfo) bool { return fi.Mode()&os.ModeSymlink != 0 }

// deviceOf is the device of the filesystem fi is on (a mount point has
// another than its parent).
func deviceOf(fi os.FileInfo) uint64 {
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		return uint64(st.Dev)
	}
	return 0
}

// onFilesystem reports whether root is on the filesystem mounted at
// filesystem (/, /srv): the same device.
func onFilesystem(root, filesystem string) (bool, error) {
	a, err := os.Stat(root)
	if err != nil {
		return false, err
	}
	b, err := os.Stat(filesystem)
	if err != nil {
		return false, err
	}
	return deviceOf(a) == deviceOf(b), nil
}

// checkResolvesTo checks that root resolves to itself (no link anywhere on
// its path).
func checkResolvesTo(root string) error {
	resolved, err := filepath.EvalSymlinks(root)
	if err != nil {
		return err
	}
	if resolved != root {
		return fmt.Errorf("%w: root %s resolves to %s", errScratchLink, root, resolved)
	}
	return nil
}

// checkFilesystemType: a file's mode does not matter to its deletion here, so
// any filesystem will do.
func checkFilesystemType(string) error { return nil }

// diskUsage is the total and the free bytes (available to an unprivileged
// writer) of the filesystem holding dir.
func diskUsage(dir string) (int64, int64, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(dir, &st); err != nil {
		return 0, 0, err
	}
	return int64(uint64(st.Blocks) * uint64(st.Bsize)), int64(uint64(st.Bavail) * uint64(st.Bsize)), nil
}

// mountsUnder is the mount points (bind mounts included) at or under dir,
// from /proc/self/mountinfo; none where there is no /proc (not Linux).
func mountsUnder(dir string) ([]string, error) {
	f, err := os.Open("/proc/self/mountinfo")
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return parseMountsUnder(bufio.NewScanner(f), filepath.Clean(dir))
}

// parseMountsUnder reads mountinfo lines (the mount point is the fifth
// field, with spaces and the like escaped as \ooo) and keeps the mount points
// at or under dir.
func parseMountsUnder(lines *bufio.Scanner, dir string) ([]string, error) {
	lines.Buffer(make([]byte, 64*1024), 1024*1024)
	var out []string
	for lines.Scan() {
		fields := strings.Fields(lines.Text())
		if len(fields) < 5 {
			continue
		}
		point := filepath.Clean(unescapeMountField(fields[4]))
		if point == dir || strings.HasPrefix(point, dir+"/") {
			out = append(out, point)
		}
	}
	return out, lines.Err()
}

// placeOf is where p (absolute, cleaned, without links) really is: the
// device of the filesystem mounted over it and its path within that
// filesystem, bind mounts undone ("8:1:/data/d" for /data/v bound from
// /data/d), from /proc/self/mountinfo; "" where there is no /proc.
func placeOf(p string) (string, error) {
	f, err := os.Open("/proc/self/mountinfo")
	if errors.Is(err, fs.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	defer f.Close()
	return parsePlace(bufio.NewScanner(f), p)
}

// parsePlace reads mountinfo lines (the device is the third field, the
// mounted directory of its filesystem the fourth, the mount point the fifth)
// and places p under the mount that covers it: the longest mount point
// holding it, the last of those (one mounted over another hides it).
func parsePlace(lines *bufio.Scanner, p string) (string, error) {
	lines.Buffer(make([]byte, 64*1024), 1024*1024)
	best, place := -1, ""
	for lines.Scan() {
		fields := strings.Fields(lines.Text())
		if len(fields) < 5 {
			continue
		}
		point := filepath.Clean(unescapeMountField(fields[4]))
		var rest string
		switch {
		case point == "/":
			rest = p
		case p == point:
			rest = "/"
		case strings.HasPrefix(p, point+"/"):
			rest = p[len(point):]
		default:
			continue
		}
		if len(point) >= best {
			best = len(point)
			place = fields[2] + ":" + path.Join(unescapeMountField(fields[3]), rest)
		}
	}
	return place, lines.Err()
}

// unescapeMountField undoes mountinfo's octal escapes (\040 for a space).
func unescapeMountField(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+4 <= len(s) {
			if v, err := strconv.ParseUint(s[i+1:i+4], 8, 8); err == nil {
				b.WriteByte(byte(v))
				i += 3
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}
