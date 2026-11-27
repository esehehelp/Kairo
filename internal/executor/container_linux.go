//go:build linux

package executor

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"debug/elf"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"kairo/internal/imageref"
)

// runContainerHelpers runs a container's runner or init (container.go) when
// args name one, and never returns then.
func runContainerHelpers(args []string) {
	if len(args) < 3 {
		return
	}
	if args[1] == containerInitArg {
		os.Exit(containerInit(args[2]))
	}
	runOCIHelper(args)
}

// incusCommand runs the incus client with stdin (nil: none) and returns
// what it printed; a failure says what it printed on stderr.
var incusCommand = func(ctx context.Context, stdin []byte, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "incus", args...)
	ownProcessGroup(cmd)
	if stdin != nil {
		cmd.Stdin = bytes.NewReader(stdin)
	}
	var out, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &stderr
	if err := cmd.Run(); err != nil {
		return out.Bytes(), fmt.Errorf("incus %s: %w: %s", strings.Join(args[:min(2, len(args))], " "), err, strings.TrimSpace(stderr.String()))
	}
	return out.Bytes(), nil
}

// incusNotFound reports whether a failed incus call failed for want of
// the instance.
func incusNotFound(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "not found")
}

// incusRuntime runs attempts' containers with Incus (oci.go): a container
// is an instance named by its instance name.
type incusRuntime struct{}

func (incusRuntime) run(ctx context.Context, spec containerSpec, logf func(string, ...any), starting func(string) bool) (string, int, error) {
	handle := ""
	exit, err := runInstance(ctx, spec, logf, func() { handle = spec.Instance }, func() bool { return starting(spec.Instance) })
	return handle, exit, err
}

func (incusRuntime) stop(ctx context.Context, name string) error {
	_, err := incusCommand(ctx, nil, "stop", name, "--timeout", strconv.Itoa(wslcStopSeconds))
	return err
}

func (incusRuntime) kill(ctx context.Context, name string) error {
	if _, err := incusCommand(ctx, nil, "stop", name, "--force"); err != nil && !incusNotFound(err) {
		return err
	}
	return nil
}

// stats reads the instance's state: its memory in use (Incus says nothing
// of the bytes it wrote).
func (incusRuntime) stats(ctx context.Context, name string) (*int64, *int64, error) {
	out, err := incusCommand(ctx, nil, "query", "/1.0/instances/"+name+"/state")
	if err != nil {
		return nil, nil, err
	}
	var st struct {
		Memory struct {
			Usage int64 `json:"usage"`
		} `json:"memory"`
	}
	if err := json.Unmarshal(out, &st); err != nil {
		return nil, nil, err
	}
	ram := st.Memory.Usage
	return &ram, nil, nil
}

// remove deletes an instance (whatever the container wrote outside what
// Kairo handed it goes with it).
func (incusRuntime) remove(ctx context.Context, name string) error {
	if _, err := incusCommand(ctx, nil, "delete", "--force", name); err != nil && !incusNotFound(err) {
		return err
	}
	return nil
}

func (incusRuntime) containers(ctx context.Context) ([]ociContainer, error) {
	out, err := incusCommand(ctx, nil, "list", "--format", "json")
	if err != nil {
		return nil, err
	}
	var instances []struct {
		Name   string `json:"name"`
		Status string `json:"status"`
	}
	if err := json.Unmarshal(out, &instances); err != nil {
		return nil, err
	}
	var list []ociContainer
	for _, in := range instances {
		if id, ok := attemptOfInstance(in.Name); ok {
			list = append(list, ociContainer{attempt: id, running: in.Status != "Stopped", handle: in.Name})
		}
	}
	return list, nil
}

// runInstance makes, starts and waits for the attempt's instance; made is
// called once it exists.
func runInstance(ctx context.Context, spec containerSpec, logf func(string, ...any), made func(), starting func() bool) (int, error) {
	ref, err := imageref.ParsePinned(spec.Image)
	if err != nil {
		return shimNotStarted, err
	}
	remote, err := ociRemote(ctx, ref.Registry)
	if err != nil {
		return shimNotStarted, err
	}
	// On a host the container is unprivileged: its root is the first of
	// root's subordinate ids (as a Proxmox container's), and what it
	// writes to is handed to its attempt user (containerUID) before it
	// starts, so that what earlier containers on the node wrote it can write
	// again. Nested in
	// an unprivileged container (a Proxmox LXC) it cannot have a uid map of
	// its own and is privileged instead: the outer container's root,
	// unprivileged on the host.
	nested := inUserNamespace()
	devices := map[string]map[string]string{}
	for i, m := range spec.Mounts {
		devices[fmt.Sprintf("kairo-%d", i)] = map[string]string{"type": "disk", "source": m.Source, "path": m.Path, "readonly": strconv.FormatBool(m.ReadOnly)}
	}
	if len(spec.GPUs) > 0 {
		gpus, driver, err := gpuDevices(spec.GPUs, spec.DriverRoot)
		if err != nil {
			return shimNotStarted, err
		}
		for name, d := range gpus {
			devices[name] = d
		}
		devices["kairo-nvidia"] = map[string]string{"type": "disk", "source": driver, "path": containerNVIDIA, "readonly": "true"}
	}
	if spec.APIProxy != "" {
		// the daemon's API on the fabric, through the host: listening in
		// the container (on the port; the init puts the address on lo),
		// connecting from the host
		if _, port, err := net.SplitHostPort(spec.APIProxy); err == nil {
			devices["kairo-api"] = map[string]string{"type": "proxy", "bind": "instance", "listen": "tcp:0.0.0.0:" + port, "connect": "tcp:" + spec.APIProxy}
		}
	}
	if g := spec.Gang; g != nil {
		// a gang member: a second NIC on its generation's segment (its
		// init gives it its address)
		devices["kairo-gang"] = map[string]string{"type": "nic", "nictype": "bridged", "parent": g.Device, "mtu": strconv.Itoa(g.MTU), "name": gangNIC}
	}
	body, err := json.Marshal(map[string]any{
		"config": map[string]string{
			"security.privileged": strconv.FormatBool(nested),
			// Incus's own init is the container's first process and runs
			// Kairo's: it passes a stop of the container on to the argv and
			// writes the argv's exit
			"oci.entrypoint": containerRoot + "/bin/kairo " + containerInitArg + " " + containerSpecIn,
			"raw.lxc":        "lxc.mount.entry = tmpfs dev/shm tmpfs rw,nosuid,nodev,size=" + containerShm + ",create=dir 0 0",
			"boot.autostart": "false",
		},
		"devices": devices,
	})
	if err != nil {
		return shimNotStarted, err
	}
	logf("instance %s of %s", spec.Instance, spec.Image)
	if _, err := incusCommand(ctx, body, "init", remote+":"+ref.Repository+"@"+ref.Digest, spec.Instance, "--quiet"); err != nil {
		return shimNotStarted, err
	}
	made()
	if !nested {
		ids, err := instanceIDMap(ctx, spec.Instance)
		if err != nil {
			return shimNotStarted, err
		}
		for _, m := range spec.Mounts {
			if !m.ReadOnly {
				if err := chownTree(m.Source, ids.toAttemptUser); err != nil {
					return shimNotStarted, fmt.Errorf("hand %s to the container: %w", m.Source, err)
				}
			}
		}
	}
	// a stop asked for before now: it is not started
	if !starting() {
		logf("asked to stop before %s started: not started", spec.Instance)
		return exitStoppedBeforeStart, nil
	}
	if _, err := incusCommand(ctx, nil, "start", spec.Instance); err != nil {
		return shimNotStarted, err
	}
	warned, failures := false, 0
	for {
		st, err := instanceState(ctx, spec.Instance)
		if incusNotFound(err) {
			break
		}
		// while it stops, its state may fail to read ("Invalid PID"): only a
		// failure that lasts is told
		if err != nil {
			failures++
		} else {
			failures = 0
		}
		if failures >= 30 && !warned {
			logf("state of %s not read: %v", spec.Instance, err)
			warned = true
		}
		if err == nil && st.Status == "Stopped" {
			break
		}
		time.Sleep(time.Second)
	}
	code, ok := readExitFile(filepath.Join(spec.RunDir, "exit"))
	if !ok {
		logf("the container stopped without writing its exit")
		return -1, nil
	}
	return code, nil
}

// idMap is where an unprivileged instance's ids are on the host: its root's
// host uid and gid and how many ids follow each, as Incus maps them.
type idMap struct {
	uid, gid       uint32
	uidLen, gidLen uint32
}

func (m idMap) ownsUID(id uint32) bool { return id >= m.uid && id-m.uid < m.uidLen }
func (m idMap) ownsGID(id uint32) bool { return id >= m.gid && id-m.gid < m.gidLen }

// instanceIDMap is the map Incus gives an unprivileged instance when it
// starts (volatile.idmap.next).
func instanceIDMap(ctx context.Context, instance string) (idMap, error) {
	out, err := incusCommand(ctx, nil, "query", "/1.0/instances/"+instance)
	if err != nil {
		return idMap{}, err
	}
	var inst struct {
		Config map[string]string `json:"config"`
	}
	if err := json.Unmarshal(out, &inst); err != nil {
		return idMap{}, err
	}
	var entries []struct {
		Isuid  bool   `json:"Isuid"`
		Isgid  bool   `json:"Isgid"`
		Hostid uint32 `json:"Hostid"`
		Nsid   uint32 `json:"Nsid"`
		Range  uint32 `json:"Maprange"`
	}
	if err := json.Unmarshal([]byte(inst.Config["volatile.idmap.next"]), &entries); err != nil {
		return idMap{}, fmt.Errorf("instance %s: id map: %w", instance, err)
	}
	var m idMap
	for _, e := range entries {
		if e.Nsid != 0 || e.Range == 0 {
			continue
		}
		if e.Isuid && m.uidLen == 0 {
			m.uid, m.uidLen = e.Hostid, e.Range
		}
		if e.Isgid && m.gidLen == 0 {
			m.gid, m.gidLen = e.Hostid, e.Range
		}
	}
	if m.uidLen <= containerUID || m.gidLen <= containerGID {
		return idMap{}, fmt.Errorf("instance %s: no map of its root's and its attempt user's uid and gid", instance)
	}
	return m, nil
}

// chownTree hands root and all under it to the owners to gives them
// (idMap.toAttemptUser, toAttemptUserHere). It walks by descriptors and
// follows no link, so that a tree a running container changes cannot lead
// it outside root. root may be a file (an attempt's log), handed over
// itself. What is not there is no error.
func chownTree(root string, to ownerFor) error {
	var lst unix.Stat_t
	if err := unix.Lstat(root, &lst); err != nil {
		if errors.Is(err, unix.ENOENT) {
			return nil
		}
		return &os.PathError{Op: "stat", Path: root, Err: err}
	}
	if lst.Mode&unix.S_IFMT != unix.S_IFDIR {
		// a file (an attempt's log): itself, a link's own owner
		if err := hand(lst, to, func(uid, gid int) error {
			return unix.Fchownat(unix.AT_FDCWD, root, uid, gid, unix.AT_SYMLINK_NOFOLLOW)
		}); err != nil {
			return &os.PathError{Op: "chown", Path: root, Err: err}
		}
		return nil
	}
	fd, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return &os.PathError{Op: "open", Path: root, Err: err}
	}
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		_ = unix.Close(fd)
		return &os.PathError{Op: "stat", Path: root, Err: err}
	}
	if err := hand(st, to, func(uid, gid int) error { return unix.Fchown(fd, uid, gid) }); err != nil {
		_ = unix.Close(fd)
		return &os.PathError{Op: "chown", Path: root, Err: err}
	}
	return chownDir(fd, root, to)
}

// chownDir is chownTree under the directory fd (which it closes).
func chownDir(fd int, dir string, to ownerFor) error {
	f := os.NewFile(uintptr(fd), dir)
	defer f.Close()
	names, err := f.Readdirnames(-1)
	if err != nil {
		return err
	}
	for _, name := range names {
		p := filepath.Join(dir, name)
		var st unix.Stat_t
		if err := unix.Fstatat(fd, name, &st, unix.AT_SYMLINK_NOFOLLOW); err != nil {
			if errors.Is(err, unix.ENOENT) {
				continue
			}
			return &os.PathError{Op: "stat", Path: p, Err: err}
		}
		// a link's own owner, never its target's
		if err := hand(st, to, func(uid, gid int) error { return unix.Fchownat(fd, name, uid, gid, unix.AT_SYMLINK_NOFOLLOW) }); err != nil && !errors.Is(err, unix.ENOENT) {
			return &os.PathError{Op: "chown", Path: p, Err: err}
		}
		if st.Mode&unix.S_IFMT != unix.S_IFDIR {
			continue
		}
		child, err := unix.Openat(fd, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if err != nil {
			// gone, or no directory now (a link put in its place): nothing
			// of root's to walk
			if errors.Is(err, unix.ENOENT) || errors.Is(err, unix.ELOOP) || errors.Is(err, unix.ENOTDIR) {
				continue
			}
			return &os.PathError{Op: "open", Path: p, Err: err}
		}
		if err := chownDir(child, p, to); err != nil {
			return err
		}
	}
	return nil
}

// ownerFor is the uid and gid what st describes goes to, each -1 where it
// stays.
type ownerFor func(st unix.Stat_t) (uid, gid int)

// hand changes what st describes to the owner to gives it.
func hand(st unix.Stat_t, to ownerFor, chown func(uid, gid int) error) error {
	uid, gid := to(st)
	if uid == -1 && gid == -1 {
		return nil
	}
	return chown(uid, gid)
}

// toAttemptUser gives the container's attempt user (containerUID,
// containerGID: its ids on the host, by the map) all of it: what an id
// outside the map owns (the host's, as what an agent wrote), and what the
// container's root or another of its users owns (what attempts wrote while
// they ran as root or switched to a user of their own, before issue #46:
// handed once). Every attempt now runs as that user.
func (m idMap) toAttemptUser(st unix.Stat_t) (int, int) {
	uid, gid := -1, -1
	if st.Uid != m.uid+containerUID {
		uid = int(m.uid + containerUID)
	}
	if st.Gid != m.gid+containerGID {
		gid = int(m.gid + containerGID)
	}
	return uid, gid
}

// toAttemptUserHere gives the attempt user all of it, in a container whose
// runner hands it nothing (wslc, or one nested in a Proxmox container).
func toAttemptUserHere(st unix.Stat_t) (int, int) {
	uid, gid := -1, -1
	if st.Uid != containerUID {
		uid = containerUID
	}
	if st.Gid != containerGID {
		gid = containerGID
	}
	return uid, gid
}

// inUserNamespace reports whether this process runs in a user namespace
// other than the host's (an unprivileged container): its uid map is not the
// whole range mapped to itself.
func inUserNamespace() bool {
	b, err := os.ReadFile("/proc/self/uid_map")
	if err != nil {
		return false
	}
	f := strings.Fields(string(b))
	return !(len(f) == 3 && f[0] == "0" && f[1] == "0" && f[2] == "4294967295")
}

type incusState struct {
	Status string `json:"status"`
}

func instanceState(ctx context.Context, instance string) (incusState, error) {
	var st incusState
	out, err := incusCommand(ctx, nil, "query", "/1.0/instances/"+instance+"/state")
	if err != nil {
		return st, err
	}
	return st, json.Unmarshal(out, &st)
}

// ociRemote is the incus remote of an OCI registry, added when there is
// none.
func ociRemote(ctx context.Context, registry string) (string, error) {
	want := "https://" + registry
	find := func() (string, error) {
		out, err := incusCommand(ctx, nil, "remote", "list", "--format", "json")
		if err != nil {
			return "", err
		}
		return findOCIRemote(out, want)
	}
	if name, err := find(); err != nil || name != "" {
		return name, err
	}
	name := "kairo-" + strings.NewReplacer(".", "-", ":", "-").Replace(registry)
	if _, err := incusCommand(ctx, nil, "remote", "add", name, want, "--protocol=oci"); err != nil {
		// another runner may have added it meanwhile
		if found, ferr := find(); ferr == nil && found != "" {
			return found, nil
		}
		return "", err
	}
	return name, nil
}

// gpuDevices are the devices of the leased GPUs (by UUID) and the NVIDIA
// control devices, and the directory of this node's driver files.
func gpuDevices(uuids []string, driverRoot string) (map[string]map[string]string, string, error) {
	minors, err := nvidiaMinors()
	if err != nil {
		return nil, "", err
	}
	devices := map[string]map[string]string{}
	for i, u := range uuids {
		minor, ok := minors[u]
		if !ok {
			return nil, "", fmt.Errorf("GPU %s is not on this node", u)
		}
		dev := "/dev/nvidia" + minor
		// the attempt user opens them (Incus makes them 0660 root's)
		devices[fmt.Sprintf("kairo-gpu-%d", i)] = map[string]string{"type": "unix-char", "source": dev, "path": dev, "mode": "0666"}
	}
	for _, name := range []string{"nvidiactl", "nvidia-uvm", "nvidia-uvm-tools"} {
		if _, err := os.Stat("/dev/" + name); err == nil {
			devices["kairo-"+name] = map[string]string{"type": "unix-char", "source": "/dev/" + name, "path": "/dev/" + name, "mode": "0666"}
		}
	}
	dir, err := driverDir(driverRoot)
	if err != nil {
		return nil, "", err
	}
	return devices, dir, nil
}

// nvidiaMinors maps each GPU's UUID to its device minor (/dev/nvidia<minor>).
func nvidiaMinors() (map[string]string, error) {
	infos, _ := filepath.Glob("/proc/driver/nvidia/gpus/*/information")
	if len(infos) == 0 {
		return nil, errors.New("no NVIDIA GPU on this node (/proc/driver/nvidia/gpus)")
	}
	out := map[string]string{}
	for _, p := range infos {
		f, err := os.Open(p)
		if err != nil {
			return nil, err
		}
		var uuid, minor string
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			key, value, ok := strings.Cut(sc.Text(), ":")
			if !ok {
				continue
			}
			switch strings.TrimSpace(key) {
			case "GPU UUID":
				uuid = strings.TrimSpace(value)
			case "Device Minor":
				minor = strings.TrimSpace(value)
			}
		}
		f.Close()
		if uuid != "" && minor != "" {
			out[uuid] = minor
		}
	}
	return out, nil
}

var driverVersion = regexp.MustCompile(`Kernel Module(?:\s+for\s+\S+)?\s+([0-9]+(?:\.[0-9]+)+)`)

// driverDir puts together, once per driver version, the driver's libraries
// and programs a container with a GPU is handed (mounted read-only at
// /kairo/nvidia: lib on LD_LIBRARY_PATH, bin on PATH), as
// nvidia-container-cli lists them: each linked (copied across
// filesystems), with links of their sonames.
func driverDir(root string) (string, error) {
	body, err := os.ReadFile("/proc/driver/nvidia/version")
	if err != nil {
		return "", fmt.Errorf("NVIDIA driver version: %w", err)
	}
	m := driverVersion.FindSubmatch(body)
	if m == nil {
		return "", fmt.Errorf("NVIDIA driver version not found in %q", strings.TrimSpace(string(body)))
	}
	dir := filepath.Join(root, string(m[1]))
	if _, err := os.Stat(filepath.Join(dir, ".complete")); err == nil {
		return dir, nil
	}
	list := func(what string) ([]string, error) {
		out, err := exec.Command("nvidia-container-cli", "list", what).Output()
		if err != nil {
			return nil, fmt.Errorf("nvidia-container-cli list %s (GPU containers need libnvidia-container-tools): %w", what, err)
		}
		return strings.Fields(string(out)), nil
	}
	libs, err := list("--libraries")
	if err != nil {
		return "", err
	}
	bins, err := list("--binaries")
	if err != nil {
		return "", err
	}
	tmp := fmt.Sprintf("%s.partial-%d", dir, os.Getpid())
	_ = os.RemoveAll(tmp)
	for _, d := range []string{filepath.Join(tmp, "lib"), filepath.Join(tmp, "bin")} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return "", err
		}
	}
	for _, lib := range libs {
		base := filepath.Base(lib)
		if err := linkOrCopy(lib, filepath.Join(tmp, "lib", base)); err != nil {
			return "", err
		}
		soname := elfSoname(lib)
		for _, name := range []string{soname, devName(soname)} {
			if name == "" || name == base {
				continue
			}
			if _, err := os.Lstat(filepath.Join(tmp, "lib", name)); err == nil {
				continue
			}
			if err := os.Symlink(base, filepath.Join(tmp, "lib", name)); err != nil {
				return "", err
			}
		}
	}
	for _, bin := range bins {
		if err := linkOrCopy(bin, filepath.Join(tmp, "bin", filepath.Base(bin))); err != nil {
			return "", err
		}
	}
	if err := os.WriteFile(filepath.Join(tmp, ".complete"), []byte(string(m[1])+"\n"), 0o644); err != nil {
		return "", err
	}
	if err := os.Rename(tmp, dir); err != nil {
		_ = os.RemoveAll(tmp)
		if _, serr := os.Stat(filepath.Join(dir, ".complete")); serr == nil {
			return dir, nil // made meanwhile by another runner
		}
		return "", err
	}
	return dir, nil
}

// elfSoname is a shared library's DT_SONAME ("" when it has none).
func elfSoname(p string) string {
	f, err := elf.Open(p)
	if err != nil {
		return ""
	}
	defer f.Close()
	names, err := f.DynString(elf.DT_SONAME)
	if err != nil || len(names) == 0 {
		return ""
	}
	return names[0]
}

// devName is libfoo.so of libfoo.so.1 ("" for a name that has no version).
func devName(soname string) string {
	if i := strings.Index(soname, ".so."); i >= 0 {
		return soname[:i+3]
	}
	return ""
}

// containerInit runs under Incus's init, the container's first process, as
// the container's root: it names the attempt's user (containerUID) in the
// image, confines the container, hands the user what Kairo lets the attempt
// write, starts the attempt's argv as that user in its own process group,
// reaps whatever ends in the container and, once the argv has exited,
// writes its exit and exits with it (which stops the container and ends
// whatever still runs there). A signal that stops the attempt reaches the
// argv directly (runInstance); the init outlives it.
func containerInit(specPath string) int {
	spec, err := readContainerSpec(specPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "kairo container:", err)
		return writeShimExit(containerExitIn, shimNotStarted)
	}
	stdout, stderr := openContainerLog(containerStdout, os.Stdout), openContainerLog(containerStderr, os.Stderr)
	// named while the image can still be written (before the confinement)
	user := nameAttemptUser(stderr)
	// a wslc container cannot mount: only its user keeps the argv off the
	// image's files (container_wslc.go)
	if spec.Runtime != "wslc" {
		if err := confineMounts(spec.Writable, spec.Tmp); err != nil {
			fmt.Fprintln(stderr, "kairo container: confine:", err)
			return writeShimExit(containerExitIn, shimNotConfined)
		}
	}
	// what Kairo lets the attempt write is its user's: the runner hands it
	// on a host (runInstance); here, where it could not (wslc, or nested in
	// a Proxmox container)
	for _, p := range spec.Writable {
		var st unix.Stat_t
		if unix.Stat(p, &st) == nil && st.Uid == containerUID && st.Gid == containerGID {
			continue
		}
		if err := chownTree(p, toAttemptUserHere); err != nil {
			fmt.Fprintf(stderr, "kairo container: hand %s to the attempt's user: %v\n", p, err)
		}
	}
	if !waitForNetwork("/sys/class/net", "/proc/net/route", "/proc/net/ipv6_route", networkWait) {
		fmt.Fprintln(stderr, "kairo container: no default route after", networkWait, "(starting without one)")
	}
	// the daemon's API on the fabric, at its own address, through the
	// proxy device listening on every address of the container
	if spec.APIProxy != "" {
		if err := apiOnLoopback(spec.APIProxy); err != nil {
			fmt.Fprintln(stderr, "kairo container: the daemon's API through the host:", err)
		}
	}
	// caught, not ignored: the argv starts with the default dispositions.
	// A stop of the container comes to this init (its runtime stops it: the
	// runner asks it to), which passes it on to the argv's group; lxc's halt
	// signal (SIGPWR: pct, Incus) is passed on as SIGTERM. One that comes
	// before the argv starts (at a gang's barrier) ends the init.
	sigs := make(chan os.Signal, 8)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP, syscall.SIGPWR)
	// a gang member: on its generation's segment, every other member
	// reached, both ways (container_gang.go)
	gangIface := ""
	if spec.Gang != nil {
		iface, err := gangJoin(spec.Gang, stderr, sigs)
		if err != nil {
			fmt.Fprintln(stderr, "kairo container: gang:", err)
			if errors.Is(err, errStoppedAtBarrier) {
				return writeShimExit(containerExitIn, exitStoppedBeforeStart)
			}
			return writeShimExit(containerExitIn, shimGangUnmet)
		}
		gangIface = iface
	}
	env := containerEnv(os.Environ(), spec.Env, len(spec.GPUs) > 0)
	env = replaceEnv(replaceEnv(env, "USER", user), "LOGNAME", user)
	if gangIface != "" {
		env = replaceEnv(replaceEnv(env, "NCCL_SOCKET_IFNAME", gangIface), "GLOO_SOCKET_IFNAME", gangIface)
	}
	// argv[0] is looked up on the attempt's PATH
	_ = os.Setenv("PATH", containerEnvValue(env, "PATH"))
	argv0, err := exec.LookPath(spec.Argv[0])
	if err != nil && !errors.Is(err, exec.ErrDot) {
		fmt.Fprintln(stderr, "kairo container:", err)
		return writeShimExit(containerExitIn, shimNotStarted)
	}
	// each stream's loader, its socket ready before the argv starts
	loaders, env, err := startLoaders(spec, env, stderr)
	if err != nil {
		stopLoaders(loaders)
		fmt.Fprintln(stderr, "kairo container:", err)
		return writeShimExit(containerExitIn, shimNotStarted)
	}
	defer endTheRest()
	defer stopLoaders(loaders)
	cmd := &exec.Cmd{Path: argv0, Args: spec.Argv, Dir: spec.CWD, Env: env, Stdout: stdout, Stderr: stderr,
		SysProcAttr: &syscall.SysProcAttr{Setpgid: true, Credential: attemptCredential()}}
	if err := cmd.Start(); err != nil {
		fmt.Fprintln(stderr, "kairo container: start:", err)
		return writeShimExit(containerExitIn, shimNotStarted)
	}
	// once the container is being stopped, a loader that ends (the stop
	// reached it too) is not started again
	var stopping atomic.Bool
	go func() {
		for sig := range sigs {
			stopping.Store(true)
			s := sig.(syscall.Signal)
			if s == syscall.SIGPWR {
				s = syscall.SIGTERM
			}
			_ = syscall.Kill(-cmd.Process.Pid, s)
		}
	}()
	exit, sig := reapUntil(cmd.Process.Pid, func(pid int) { loaders.ended(pid, !stopping.Load()) })
	if sig != 0 {
		fmt.Fprintf(stderr, "kairo container: %s ended by signal %d (%v)\n", spec.Argv[0], int(sig), sig)
	}
	return writeShimExit(containerExitIn, exit)
}

// attemptCredential is the attempt user's: its uid and gid, no other group.
func attemptCredential() *syscall.Credential {
	return &syscall.Credential{Uid: containerUID, Gid: containerGID, Groups: []uint32{}}
}

// nameAttemptUser gives the attempt's user (containerUID) a name in the
// image (attemptAccount: /etc/passwd and /etc/group), so that what asks for
// it (getpass, pwd, id -un, git, ssh) finds one, and returns it. Where the
// image's files cannot be written it keeps none, told on logs: the name is
// still the environment's (USER, LOGNAME).
func nameAttemptUser(logs io.Writer) string {
	passwd, _ := os.ReadFile("/etc/passwd")
	group, _ := os.ReadFile("/etc/group")
	name, passwdLine, groupLine := attemptAccount(string(passwd), string(group))
	add := func(p string, body []byte, line string) {
		if line == "" {
			return
		}
		if len(body) > 0 && body[len(body)-1] != '\n' {
			line = "\n" + line
		}
		f, err := os.OpenFile(p, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o644)
		if err == nil {
			_, err = f.WriteString(line + "\n")
			if cerr := f.Close(); err == nil {
				err = cerr
			}
		}
		if err != nil {
			fmt.Fprintf(logs, "kairo container: name the attempt's user in %s: %v\n", p, err)
		}
	}
	add("/etc/passwd", passwd, passwdLine)
	add("/etc/group", group, groupLine)
	return name
}

// containerRestGrace is how long what the argv left running in the
// container has between SIGTERM and SIGKILL once the argv has exited.
const containerRestGrace = 10 * time.Second

// endTheRest ends what the argv left running in the container (a daemon it
// started), when this init is not the container's first process (Incus's
// own init is): the container stops only once nothing is left, as it does
// when its first process exits (pct), which ends the rest at once. SIGTERM
// first, SIGKILL after containerRestGrace.
func endTheRest() {
	if os.Getpid() == 1 || !inOwnPIDNamespace() {
		return
	}
	_ = syscall.Kill(-1, syscall.SIGTERM)
	for deadline := time.Now().Add(containerRestGrace); time.Now().Before(deadline) && othersLeft(); {
		for {
			if pid, _ := syscall.Wait4(-1, nil, syscall.WNOHANG, nil); pid <= 0 {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	_ = syscall.Kill(-1, syscall.SIGKILL)
}

// inOwnPIDNamespace: this process sees a pid namespace of its own (a
// container's), not the host's, where the kernel's threads are seen
// (kthreadd is pid 2): the init signals every process it sees only there.
// With no /proc to tell, it is not.
func inOwnPIDNamespace() bool {
	if _, err := os.Stat("/proc/self/stat"); err != nil {
		return false
	}
	b, err := os.ReadFile("/proc/2/comm")
	return err != nil || strings.TrimSpace(string(b)) != "kthreadd"
}

// othersLeft: a process other than the container's first and this one runs.
func othersLeft() bool {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return false
	}
	me := os.Getpid()
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil || pid == 1 || pid == me {
			continue
		}
		if b, err := os.ReadFile("/proc/" + e.Name() + "/stat"); err == nil {
			// a zombie is gone but for its parent's wait
			if i := bytes.LastIndexByte(b, ')'); i > 0 && i+2 < len(b) && b[i+2] == 'Z' {
				continue
			}
			return true
		}
	}
	return false
}

// loaderReady bounds how long the init waits for a loader's socket.
const loaderReady = 30 * time.Second

// loaderRestarts: times a loader that ended is started again; its readers
// come back where they stand.
const loaderRestarts = 5

// loaderProc is a stream's loader: how it is started, and its process.
type loaderProc struct {
	stream string
	args   []string
	dir    string
	env    []string
	logs   *os.File
	// as is the user it runs as (the attempt's); nil: this process's
	as       *syscall.Credential
	pid      int
	restarts int
}

// loaderProcs are an attempt's loaders.
type loaderProcs []*loaderProc

func (p *loaderProc) start() error {
	cmd := &exec.Cmd{Path: p.args[0], Args: p.args, Dir: p.dir, Env: p.env, Stdout: p.logs, Stderr: p.logs, SysProcAttr: &syscall.SysProcAttr{Setpgid: true, Credential: p.as}}
	if err := cmd.Start(); err != nil {
		return err
	}
	p.pid = cmd.Process.Pid
	return nil
}

// ownedByAttempt hands the attempt user what the init made for it (a
// stream's directory and files), each path itself.
func ownedByAttempt(paths ...string) error {
	for _, p := range paths {
		if err := os.Lchown(p, containerUID, containerGID); err != nil {
			return err
		}
	}
	return nil
}

// ended takes the end of a loader's process pid (reaped): with again, it
// starts it again, up to loaderRestarts times (its readers come back at their
// positions); without (the container is being stopped), it does not.
func (l loaderProcs) ended(pid int, again bool) {
	for _, p := range l {
		if p.pid != pid {
			continue
		}
		p.pid = 0
		_ = syscall.Kill(-pid, syscall.SIGKILL) // its stages
		if !again {
			return
		}
		if p.restarts >= loaderRestarts {
			fmt.Fprintf(p.logs, "kairo container: the loader of stream %s ended %d times: not started again\n", p.stream, p.restarts+1)
			return
		}
		p.restarts++
		if err := p.start(); err != nil {
			fmt.Fprintf(p.logs, "kairo container: the loader of stream %s ended and did not start again: %v\n", p.stream, err)
			return
		}
		fmt.Fprintf(p.logs, "kairo container: the loader of stream %s ended; started again (%d)\n", p.stream, p.restarts)
	}
}

// startLoaders starts a loader (kairo loader serve) for each stream the
// argv reads, its spec, positions, socket and status (StreamStatusFile, read
// by the agent from the node's side) in /kairo/run/streams/<name>,
// its stages run from the argv's directory, what it learns reported to the
// daemon as the attempt; the environment the argv gets has each stream's
// socket (KAIRO_STREAM_<NAME>).
func startLoaders(spec containerSpec, env []string, logs *os.File) (loaderProcs, []string, error) {
	if len(spec.Streams) == 0 {
		return nil, env, nil
	}
	self, err := os.Executable()
	if err != nil {
		return nil, env, err
	}
	var procs loaderProcs
	for _, s := range spec.Streams {
		streams := path.Join(containerRoot, "run", "streams")
		dir := path.Join(streams, s.Name)
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return procs, env, err
		}
		ls := s.Spec
		for i := range ls.Stages {
			if ls.Stages[i].Dir == "" {
				ls.Stages[i].Dir = spec.CWD
			}
		}
		specPath, sock := path.Join(dir, "spec.json"), path.Join(dir, "socket")
		body, err := json.Marshal(ls)
		if err != nil {
			return procs, env, err
		}
		if err := os.WriteFile(specPath, body, 0o600); err != nil {
			return procs, env, err
		}
		// one session for the attempt: takers come back to a restarted
		// loader with it, never with a position of another attempt's
		var session [16]byte
		_, _ = rand.Read(session[:])
		args := []string{self, "loader", "serve", "--spec", specPath, "--listen", "unix:" + sock, "--session", hex.EncodeToString(session[:]), "--state", filepath.Dir(specPath),
			"--status", path.Join(dir, StreamStatusFile), "--order-state", path.Join(containerRoot, "output", StreamOrderStateDir, s.Name)}
		if len(s.Positions) > 0 {
			positions, _ := json.Marshal(s.Positions)
			p := path.Join(dir, "positions.json")
			if err := os.WriteFile(p, positions, 0o600); err != nil {
				return procs, env, err
			}
			args = append(args, "--positions", p, "--world", strconv.Itoa(s.World))
			if err := ownedByAttempt(p); err != nil {
				return procs, env, err
			}
		}
		// the loader runs as the attempt's user: its state, socket and status
		// are written there
		if err := ownedByAttempt(streams, dir, specPath); err != nil {
			return procs, env, err
		}
		if spec.RawDir != "" {
			args = append(args, "--raw", spec.RawDir)
		}
		if spec.RawRequests != "" {
			args = append(args, "--raw-requests", spec.RawRequests)
		}
		p := &loaderProc{stream: s.Name, args: args, dir: spec.CWD, env: env, logs: logs, as: attemptCredential()}
		if err := p.start(); err != nil {
			return procs, env, fmt.Errorf("stream %s: start its loader: %w", s.Name, err)
		}
		procs = append(procs, p)
		deadline := time.Now().Add(loaderReady)
		for {
			if _, err := os.Stat(sock); err == nil {
				break
			}
			if time.Now().After(deadline) {
				return procs, env, fmt.Errorf("stream %s: its loader did not listen within %s", s.Name, loaderReady)
			}
			time.Sleep(50 * time.Millisecond)
		}
		env = append(env, s.Env+"=unix:"+sock)
	}
	return procs, env, nil
}

// stopLoaders ends the loaders' process groups (their stages with them):
// asked to end, then killed.
func stopLoaders(procs loaderProcs) {
	var groups []int
	for _, p := range procs {
		if p.pid > 0 {
			groups = append(groups, p.pid)
			p.restarts = loaderRestarts // not started again
		}
	}
	if len(groups) == 0 {
		return
	}
	for _, g := range groups {
		_ = syscall.Kill(-g, syscall.SIGTERM)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		alive := false
		for _, g := range groups {
			if syscall.Kill(-g, 0) == nil {
				alive = true
			}
		}
		if !alive {
			return
		}
		var ws syscall.WaitStatus
		_, _ = syscall.Wait4(-1, &ws, syscall.WNOHANG, nil)
		time.Sleep(50 * time.Millisecond)
	}
	for _, g := range groups {
		_ = syscall.Kill(-g, syscall.SIGKILL)
	}
}

// networkWait bounds how long the init waits for the container's network:
// a Proxmox container's address comes by DHCP a few seconds after it starts.
const networkWait = 30 * time.Second

// waitForNetwork waits, up to limit, until the container has a default
// route (IPv4 or IPv6) when it has an interface other than loopback; true
// when it has one, or no network to wait for.
func waitForNetwork(netDir, route4, route6 string, limit time.Duration) bool {
	ifaces, _ := os.ReadDir(netDir)
	if !slices.ContainsFunc(ifaces, func(e os.DirEntry) bool { return e.Name() != "lo" }) {
		return true
	}
	deadline := time.Now().Add(limit)
	for {
		if hasDefaultRoute(route4, route6) {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// hasDefaultRoute reports whether /proc/net/route has a default route
// (destination 00000000) or /proc/net/ipv6_route one (destination ::/0 on
// an interface other than lo).
func hasDefaultRoute(route4, route6 string) bool {
	if body, err := os.ReadFile(route4); err == nil {
		for _, line := range strings.Split(string(body), "\n")[1:] {
			if f := strings.Fields(line); len(f) > 1 && f[1] == "00000000" {
				return true
			}
		}
	}
	if body, err := os.ReadFile(route6); err == nil {
		for _, line := range strings.Split(string(body), "\n") {
			if f := strings.Fields(line); len(f) == 10 && f[0] == strings.Repeat("0", 32) && f[1] == "00" && f[9] != "lo" {
				return true
			}
		}
	}
	return false
}

// reapUntil reaps the container's processes until pid exits, and returns
// its exit (-1 when a signal ended it, as exec's ExitCode says, with the
// signal).
func reapUntil(pid int, other func(pid int)) (int, syscall.Signal) {
	for {
		var ws syscall.WaitStatus
		wpid, err := syscall.Wait4(-1, &ws, 0, nil)
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if err != nil {
			return -1, 0
		}
		if wpid != pid && other != nil {
			other(wpid)
		}
		if wpid == pid {
			if ws.Exited() {
				return ws.ExitStatus(), 0
			}
			return -1, ws.Signal()
		}
	}
}

// openContainerLog opens a log mounted in the container to append to it
// (the runner writes at its end too), else falls back to the console.
func openContainerLog(p string, fallback *os.File) *os.File {
	f, err := os.OpenFile(p, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		fmt.Fprintf(fallback, "kairo container: %s: %v\n", p, err)
		return fallback
	}
	return f
}

// The sweep of instances left behind: a runner killed (the kill of a force
// stop, the OOM killer) deletes no instance. Every containerSweepInterval
// the executor deletes the stopped instances of its attempts (kairo-att-...)
// that no longer run here, and what they were made from. One still running
// is left alone: what an exited attempt left running is ended as on a host
// (its processes carry its KAIRO_ATTEMPT_ID), which stops it.
const (
	containerSweepInterval = 10 * time.Minute
	containerSweepTimeout  = 5 * time.Minute
)
