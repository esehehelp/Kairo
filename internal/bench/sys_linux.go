//go:build linux

package bench

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

func cpuModel() string {
	f, err := os.Open("/proc/cpuinfo")
	if err != nil {
		return ""
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	fallback := ""
	for s.Scan() {
		key, value, ok := strings.Cut(s.Text(), ":")
		if !ok {
			continue
		}
		switch strings.TrimSpace(key) {
		case "model name":
			return strings.TrimSpace(value)
		case "Model", "Hardware", "cpu model":
			if fallback == "" {
				fallback = strings.TrimSpace(value)
			}
		}
	}
	return fallback
}

// osVersion is the kernel release (uname -r).
func osVersion() string {
	var u unix.Utsname
	if unix.Uname(&u) != nil {
		return ""
	}
	return unix.ByteSliceToString(u.Release[:])
}

// memory is MemTotal and MemAvailable of /proc/meminfo.
func memory() (total, available int64, err error) {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	for s.Scan() {
		fields := strings.Fields(s.Text())
		if len(fields) < 2 {
			continue
		}
		v, _ := strconv.ParseInt(fields[1], 10, 64)
		switch fields[0] {
		case "MemTotal:":
			total = v * 1024
		case "MemAvailable:":
			available = v * 1024
		}
	}
	if total == 0 || available == 0 {
		return 0, 0, errors.New("/proc/meminfo has no MemTotal or MemAvailable")
	}
	return total, available, nil
}

// pinThread pins the calling OS thread to logical CPU cpu; the caller has
// locked its goroutine to the thread.
func pinThread(cpu int) error {
	var set unix.CPUSet
	set.Set(cpu)
	return unix.SchedSetaffinity(0, &set)
}

// diskFree is the space of the filesystem holding dir that an unprivileged
// writer may still use (statfs f_bavail).
func diskFree(dir string) (int64, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(dir, &st); err != nil {
		return 0, err
	}
	return int64(uint64(st.Bavail) * uint64(st.Bsize)), nil
}

// volumeInfo identifies the filesystem and the block device holding dir:
// the mount of /proc/self/mountinfo and the device's sysfs attributes.
func volumeInfo(dir string) map[string]any {
	info := map[string]any{}
	var st unix.Statfs_t
	if unix.Statfs(dir, &st) == nil {
		info["total_bytes"] = uint64(st.Blocks) * uint64(st.Bsize)
		info["free_bytes"] = uint64(st.Bavail) * uint64(st.Bsize)
		info["filesystem_magic"] = fmt.Sprintf("0x%x", uint64(st.Type))
		info["fsid"] = fmt.Sprintf("%08x%08x", uint32(st.Fsid.Val[0]), uint32(st.Fsid.Val[1]))
	}
	mountPoint, fsType, source, majorMinor := findMount(dir)
	if mountPoint == "" {
		return info
	}
	info["volume_path"], info["filesystem"], info["mount_source"] = mountPoint, fsType, source
	sys := "/sys/dev/block/" + majorMinor
	if _, err := os.Stat(sys); err != nil {
		return info
	}
	info["block_device"] = majorMinor
	// A partition's attributes are on its disk, the parent directory.
	disk := sys
	if _, err := os.Stat(filepath.Join(sys, "partition")); err == nil {
		disk = filepath.Join(sys, "..")
	}
	read := func(rel string) string {
		b, _ := os.ReadFile(filepath.Join(disk, rel))
		return strings.TrimSpace(string(b))
	}
	if model := strings.TrimSpace(read("device/vendor") + " " + read("device/model")); model != "" {
		info["disk_model"] = model
	}
	if serial := read("device/serial"); serial != "" {
		info["disk_serial"] = serial
	}
	if rev := read("device/rev"); rev != "" {
		info["disk_revision"] = rev
	} else if fw := read("device/firmware_rev"); fw != "" {
		info["disk_revision"] = fw
	}
	if name, err := filepath.EvalSymlinks(disk); err == nil {
		info["disk"] = filepath.Base(name)
	}
	for key, rel := range map[string]string{"logical_sector_bytes": "queue/logical_block_size", "physical_sector_bytes": "queue/physical_block_size", "rotational": "queue/rotational"} {
		if v, err := strconv.ParseInt(read(rel), 10, 64); err == nil {
			info[key] = v
		}
	}
	return info
}

// findMount is the mount holding dir: the longest mount point of
// /proc/self/mountinfo that contains it.
func findMount(dir string) (mountPoint, fsType, source, majorMinor string) {
	abs, err := filepath.EvalSymlinks(dir)
	if err != nil {
		abs = dir
	}
	f, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	for s.Scan() {
		// id parent major:minor root mountpoint options [optional...] - fstype source superoptions
		pre, post, ok := strings.Cut(s.Text(), " - ")
		fields, tail := strings.Fields(pre), strings.Fields(post)
		if !ok || len(fields) < 5 || len(tail) < 2 {
			continue
		}
		mp := unescapeMount(fields[4])
		if !within(abs, mp) || len(mp) < len(mountPoint) {
			continue
		}
		mountPoint, fsType, source, majorMinor = mp, tail[0], tail[1], fields[2]
	}
	return
}

func within(path, dir string) bool {
	return dir == "/" || path == dir || strings.HasPrefix(path, dir+"/")
}

// unescapeMount decodes the octal escapes (\040 for a space) of mountinfo.
func unescapeMount(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+3 < len(s) {
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

// testFile is a descriptor of the storage test file.
type testFile struct{ fd int }

// openTestFile opens path with O_DIRECT (the page cache bypassed; requests
// aligned to the logical block size). create makes the file, which must
// not exist. A filesystem refusing O_DIRECT (tmpfs) gets a buffered file
// and direct is false.
func openTestFile(path string, create, wantDirect bool) (f *testFile, direct bool, err error) {
	flags := unix.O_RDWR | unix.O_CLOEXEC
	if create {
		flags |= unix.O_CREAT | unix.O_EXCL
	}
	if wantDirect {
		fd, err := unix.Open(path, flags|unix.O_DIRECT, 0o600)
		if err == nil {
			return &testFile{fd}, true, nil
		}
		if !errors.Is(err, unix.EINVAL) && !errors.Is(err, unix.EOPNOTSUPP) {
			return nil, false, err
		}
	}
	fd, err := unix.Open(path, flags, 0o600)
	if err != nil {
		return nil, false, err
	}
	return &testFile{fd}, false, nil
}

func (f *testFile) ReadAt(p []byte, off int64) (int, error) { return unix.Pread(f.fd, p, off) }

func (f *testFile) WriteAt(p []byte, off int64) (int, error) { return unix.Pwrite(f.fd, p, off) }

// sync flushes the file's data and the device's write cache.
func (f *testFile) sync() error { return unix.Fdatasync(f.fd) }

func (f *testFile) close() error { return unix.Close(f.fd) }

// isDirectIOError: the filesystem accepted O_DIRECT at open but refuses the request.
func isDirectIOError(err error) bool { return errors.Is(err, unix.EINVAL) }

// dropDirect clears O_DIRECT (Linux lets fcntl change it).
func (f *testFile) dropDirect() error {
	flags, err := unix.FcntlInt(uintptr(f.fd), unix.F_GETFL, 0)
	if err != nil {
		return err
	}
	_, err = unix.FcntlInt(uintptr(f.fd), unix.F_SETFL, flags&^unix.O_DIRECT)
	return err
}

// earlyUnlink removes path while its descriptors stay open: the space is
// freed when they close, even if the process is killed.
func earlyUnlink(path string) bool { return os.Remove(path) == nil }

// linkSpeed is what the OS reports for the interface: /sys/class/net
// speed (Mb/s; virtual NICs report none or a nominal value).
func linkSpeed(_ int, name string) map[string]any {
	b, err := os.ReadFile("/sys/class/net/" + name + "/speed")
	if err != nil {
		return nil
	}
	mbps, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
	if err != nil || mbps <= 0 {
		return nil
	}
	return map[string]any{"link_speed_tx_bps": mbps * 1e6, "link_speed_rx_bps": mbps * 1e6}
}
