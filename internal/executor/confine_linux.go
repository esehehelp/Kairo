//go:build linux

package executor

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"

	"golang.org/x/sys/unix"
)

// On Linux a confined attempt's launcher is this binary run as
// confineHelperArg in a mount namespace of its own (CLONE_NEWNS; Go makes
// its mounts private, so nothing done there reaches the node): it binds a
// directory of the scratch over /tmp, binds each writable directory (and
// /tmp, /dev/shm and /proc) onto itself, makes every mount read-only
// (mount_setattr, recursively from /), makes those writable again, and
// execs the attempt's argv (looked up on its PATH) in its place, so the
// launcher's pid, process group and creation time are the attempt's. The
// agent must be able to make mounts (root, as in the containers it runs
// in); devices stay usable (a read-only mount does not stop writing to a
// device), and nothing is hidden: everything else is read as before.

const confineHelperArg = "__kairo-confine"

// confineCommand turns cmd into the confining launcher (see above).
func confineCommand(cmd *exec.Cmd, writable []string, scratch string) (func(), error) {
	self, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errConfineHere, err)
	}
	tmp := filepath.Join(scratch, "tmp")
	if err := os.Mkdir(tmp, 0o777); err != nil && !os.IsExist(err) {
		return nil, fmt.Errorf("%w: make %s: %v", errConfineHere, tmp, err)
	}
	if err := os.Chmod(tmp, 0o777|os.ModeSticky); err != nil {
		return nil, fmt.Errorf("%w: %v", errConfineHere, err)
	}
	args := []string{self, confineHelperArg, "--tmp", tmp}
	for _, w := range writable {
		args = append(args, "--write", w)
	}
	args = append(append(args, "--"), cmd.Args...)
	cmd.Path, cmd.Args = self, args
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Unshareflags |= syscall.CLONE_NEWNS
	return nil, nil
}

// runConfineHelper runs the confining launcher when args name it, and never
// returns then: it execs the attempt, or exits 126 (could not confine or
// exec) or 127 (argv[0] not found), saying why on stderr.
func runConfineHelper(args []string) {
	if len(args) < 2 || args[1] != confineHelperArg {
		return
	}
	var writable []string
	tmp := ""
	i := 2
	for ; i < len(args); i++ {
		switch {
		case args[i] == "--":
			i++
		case args[i] == "--write" && i+1 < len(args):
			writable = append(writable, args[i+1])
			i++
			continue
		case args[i] == "--tmp" && i+1 < len(args):
			tmp = args[i+1]
			i++
			continue
		default:
			fmt.Fprintf(os.Stderr, "kairo confine: unknown argument %q\n", args[i])
			os.Exit(126)
		}
		break
	}
	argv := args[i:]
	if len(argv) == 0 {
		fmt.Fprintln(os.Stderr, "kairo confine: no command")
		os.Exit(126)
	}
	if err := confineMounts(writable, tmp); err != nil {
		fmt.Fprintln(os.Stderr, "kairo confine:", err)
		os.Exit(126)
	}
	path, err := exec.LookPath(argv[0])
	if err != nil {
		fmt.Fprintln(os.Stderr, "kairo confine:", err)
		os.Exit(127)
	}
	err = syscall.Exec(path, argv, os.Environ())
	fmt.Fprintln(os.Stderr, "kairo confine: exec:", err)
	os.Exit(126)
}

// confineMounts makes this mount namespace read-only but for writable (see
// above). Every directory named must exist.
func confineMounts(writable []string, tmp string) error {
	for _, p := range []string{"/dev/shm", "/proc"} {
		if fi, err := os.Stat(p); err == nil && fi.IsDir() {
			writable = append(writable, p)
		}
	}
	// each its own mount, before /tmp is covered (a directory under /tmp is
	// reached by its path first)
	for _, w := range writable {
		if err := unix.Mount(w, w, "", unix.MS_BIND|unix.MS_REC, ""); err != nil {
			return fmt.Errorf("bind %s: %w", w, err)
		}
	}
	if tmp != "" {
		if fi, err := os.Stat("/tmp"); err == nil && fi.IsDir() {
			if err := unix.Mount(tmp, "/tmp", "", unix.MS_BIND|unix.MS_REC, ""); err != nil {
				return fmt.Errorf("bind %s over /tmp: %w", tmp, err)
			}
			writable = append(writable, "/tmp")
		}
	}
	if err := unix.MountSetattr(unix.AT_FDCWD, "/", unix.AT_RECURSIVE, &unix.MountAttr{Attr_set: unix.MOUNT_ATTR_RDONLY}); err != nil {
		return fmt.Errorf("make / read-only: %w", err)
	}
	for _, w := range writable {
		if err := unix.MountSetattr(unix.AT_FDCWD, w, unix.AT_RECURSIVE, &unix.MountAttr{Attr_clr: unix.MOUNT_ATTR_RDONLY}); err != nil {
			return fmt.Errorf("make %s writable: %w", w, err)
		}
	}
	return nil
}
