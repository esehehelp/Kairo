//go:build linux

package executor

import (
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strconv"

	"golang.org/x/sys/unix"
)

// chmodDir sets the mode of name, a directory under r, through a handle that
// can only be that directory (O_PATH|O_NOFOLLOW|O_DIRECTORY, opened in its
// parent): a link or a file put in its place, a hard link to a file outside
// say, is refused, never changed.
func chmodDir(r *os.Root, name string, mode fs.FileMode) error {
	dir, base := path.Split(filepath.ToSlash(name))
	parent, err := r.Open(filepath.FromSlash(path.Clean("./" + dir)))
	if err != nil {
		return err
	}
	defer parent.Close()
	fd, err := unix.Openat(int(parent.Fd()), base, unix.O_PATH|unix.O_NOFOLLOW|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return &fs.PathError{Op: "openat", Path: name, Err: err}
	}
	defer unix.Close(fd)
	if err = unix.Chmod("/proc/self/fd/"+strconv.Itoa(fd), uint32(mode.Perm())); err != nil {
		return &fs.PathError{Op: "chmod", Path: name, Err: err}
	}
	return nil
}
