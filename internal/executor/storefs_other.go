//go:build !windows

package executor

import (
	"bufio"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

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

// makeDirsWritable makes the directories at and under name in dir (through
// dir's Root, links not followed) readable, writable and searchable by their
// owner, so what they hold can be removed; it reports whether any was
// changed. Only directories, which cannot be hard linked: a file's mode is
// never changed (it may be shared with a file outside).
func makeDirsWritable(dir *os.Root, name string) bool {
	changed := false
	_ = fs.WalkDir(dir.FS(), filepath.ToSlash(name), func(p string, d fs.DirEntry, err error) error {
		if err != nil || d == nil || !d.IsDir() || d.Type()&fs.ModeSymlink != 0 {
			return nil
		}
		info, err := d.Info()
		if err != nil || isLink(info) {
			return nil
		}
		if perm := info.Mode().Perm(); perm&0o700 != 0o700 {
			if dir.Chmod(filepath.FromSlash(p), perm|0o700) == nil {
				changed = true
			}
		}
		return nil
	})
	return changed
}
