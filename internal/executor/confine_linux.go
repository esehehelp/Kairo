//go:build linux

package executor

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"golang.org/x/sys/unix"
)

// A container's init confines its argv (container_linux.go): in the
// container's mount namespace it binds a directory of the scratch over
// /tmp, binds each writable directory (and /tmp, /dev/shm and /proc) onto
// itself, makes every mount read-only (mount_setattr, recursively from /)
// and makes those writable again. Devices stay usable (a read-only mount
// does not stop writing to a device), and nothing is hidden: everything
// else is read as before.

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
