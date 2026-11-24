//go:build linux

package executor

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
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
	// the attempt's argv[0] is looked up by the launcher, on its own PATH
	cmd.Path, cmd.Args, cmd.Err = self, args, nil
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
	// /proc and /dev/shm stay writable. One that is its own mount already
	// is not bound onto itself: an unprivileged Proxmox container refuses
	// binding /proc (its profile), and needs no bind to change it alone.
	var system []string
	for _, p := range []string{"/dev/shm", "/proc"} {
		if fi, err := os.Stat(p); err == nil && fi.IsDir() {
			if mounted, known, err := isMountPoint(p); err == nil && known && mounted {
				system = append(system, p)
			} else {
				writable = append(writable, p)
			}
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
	if err := readOnlyBut(append(writable, system...)); err != nil {
		return err
	}
	return nil
}

// readOnlyBut makes every mount of this namespace read-only, one by one,
// but those at or under keep. A mount is not made read-only while a file on
// it is open for writing (EBUSY): a terminal's (devpts) or a device node
// mounted on its own (a container's /dev/console), which the process writes
// its console to, is left as it is; any other refusal fails.
func readOnlyBut(keep []string) error {
	body, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return err
	}
	for _, line := range strings.Split(string(body), "\n") {
		fields := strings.Fields(line)
		sep := slices.Index(fields, "-")
		if len(fields) < 5 || sep < 0 || sep+1 >= len(fields) {
			continue
		}
		point, fstype := filepath.Clean(unescapeMountField(fields[4])), fields[sep+1]
		if slices.ContainsFunc(keep, func(k string) bool { return point == k || strings.HasPrefix(point, strings.TrimSuffix(k, "/")+"/") }) {
			continue
		}
		err := unix.MountSetattr(unix.AT_FDCWD, point, 0, &unix.MountAttr{Attr_set: unix.MOUNT_ATTR_RDONLY})
		switch {
		case err == nil, errors.Is(err, unix.ENOENT):
		case errors.Is(err, unix.EBUSY) && (fstype == "devpts" || isCharDevice(point)):
		default:
			return fmt.Errorf("make %s read-only: %w", point, err)
		}
	}
	return nil
}

func isCharDevice(p string) bool {
	fi, err := os.Stat(p)
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}
