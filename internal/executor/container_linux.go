//go:build linux

package executor

import (
	"bufio"
	"bytes"
	"context"
	"debug/elf"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"kairo/internal/imageref"
)

// runContainerHelpers runs a container's runner or init (container.go) when
// args name one, and never returns then.
func runContainerHelpers(args []string) {
	if len(args) < 3 {
		return
	}
	switch args[1] {
	case containerRunArg:
		os.Exit(containerRun(args[2]))
	case containerInitArg:
		os.Exit(containerInit(args[2]))
	case pctRunArg:
		os.Exit(pctRun(args[2]))
	}
}

func readContainerSpec(p string) (containerSpec, error) {
	var spec containerSpec
	body, err := os.ReadFile(p)
	if err != nil {
		return spec, err
	}
	if err := json.Unmarshal(body, &spec); err != nil {
		return spec, fmt.Errorf("%s: %w", p, err)
	}
	if spec.Instance == "" || len(spec.Argv) == 0 {
		return spec, fmt.Errorf("%s: no instance or argv", p)
	}
	return spec, nil
}

// incusCommand runs the incus client with stdin (nil: none) and returns
// what it printed; a failure says what it printed on stderr.
var incusCommand = func(ctx context.Context, stdin []byte, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "incus", args...)
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

// containerRun is the runner: it makes the attempt's instance, starts it,
// waits for it to stop and exits with the exit the container wrote (-1 when
// it wrote none: killed). It deletes the instance (whatever the container
// wrote outside what Kairo handed it goes with it) and its spec, which holds
// the attempt's token. What stops an attempt (a force stop, a quarantine)
// reaches the container's processes directly, as it reaches a host
// attempt's: they carry its KAIRO_ATTEMPT_ID. The runner only outlives the
// signal it is sent with them, so as to see the container stop.
func containerRun(specPath string) int {
	logf := func(format string, a ...any) {
		// the container appends to the same log: write at its end
		_, _ = os.Stderr.Seek(0, io.SeekEnd)
		fmt.Fprintf(os.Stderr, "kairo container: "+format+"\n", a...)
	}
	spec, err := readContainerSpec(specPath)
	if err != nil {
		logf("%v", err)
		return shimNotStarted
	}
	if os.Getenv("HOME") == "" {
		// incus keeps its remotes under HOME; a service may have none
		_ = os.Setenv("HOME", "/root")
	}
	sigs := make(chan os.Signal, 8)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
	go func() {
		for range sigs {
		}
	}()
	exit, err := runInstance(context.Background(), spec, logf)
	if err != nil {
		logf("%v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if _, err := incusCommand(ctx, nil, "delete", "--force", spec.Instance); err != nil && !incusNotFound(err) {
		logf("instance %s not deleted: %v", spec.Instance, err)
	}
	_ = os.Remove(specPath)
	return exit
}

// runInstance makes, starts and waits for the attempt's instance.
func runInstance(ctx context.Context, spec containerSpec, logf func(string, ...any)) (int, error) {
	ref, err := imageref.ParsePinned(spec.Image)
	if err != nil {
		return shimNotStarted, err
	}
	remote, err := ociRemote(ctx, ref.Registry)
	if err != nil {
		return shimNotStarted, err
	}
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
	body, err := json.Marshal(map[string]any{
		"config": map[string]string{
			// nested in an unprivileged container (a Proxmox LXC), the
			// container cannot have a uid map of its own: it is the outer
			// container's root, unprivileged on the host
			"security.privileged": "true",
			// Incus's own init is the container's first process and runs
			// this one: neither carries the attempt's KAIRO_ATTEMPT_ID (only
			// the argv's environment does), so a stop sent to the attempt's
			// processes reaches the argv and not them (Incus's init would end
			// the container before the exit is written)
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
	if _, err := incusCommand(ctx, nil, "start", spec.Instance); err != nil {
		return shimNotStarted, err
	}
	warned := false
	for {
		st, err := instanceState(ctx, spec.Instance)
		if incusNotFound(err) {
			break
		}
		if err != nil && !warned {
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
		devices[fmt.Sprintf("kairo-gpu-%d", i)] = map[string]string{"type": "unix-char", "source": dev, "path": dev}
	}
	for _, name := range []string{"nvidiactl", "nvidia-uvm", "nvidia-uvm-tools"} {
		if _, err := os.Stat("/dev/" + name); err == nil {
			devices["kairo-"+name] = map[string]string{"type": "unix-char", "source": "/dev/" + name, "path": "/dev/" + name}
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

// containerInit runs under Incus's init, the container's first process: it
// confines the container, starts the attempt's argv in its own process
// group, reaps whatever ends in the container and, once the argv has
// exited, writes its exit and exits with it (which stops the container and
// ends whatever still runs there). A signal that stops the attempt reaches
// the argv directly (runInstance); the init outlives it.
func containerInit(specPath string) int {
	spec, err := readContainerSpec(specPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "kairo container:", err)
		return writeShimExit(containerExitIn, shimNotStarted)
	}
	if err := confineMounts(spec.Writable, spec.Tmp); err != nil {
		fmt.Fprintln(os.Stderr, "kairo container: confine:", err)
		return writeShimExit(containerExitIn, shimNotConfined)
	}
	stdout, stderr := openContainerLog(containerStdout, os.Stdout), openContainerLog(containerStderr, os.Stderr)
	if !waitForNetwork("/sys/class/net", "/proc/net/route", "/proc/net/ipv6_route", networkWait) {
		fmt.Fprintln(stderr, "kairo container: no default route after", networkWait, "(starting without one)")
	}
	env := containerEnv(os.Environ(), spec.Env, len(spec.GPUs) > 0)
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
	defer stopLoaders(loaders)
	cmd := &exec.Cmd{Path: argv0, Args: spec.Argv, Dir: spec.CWD, Env: env, Stdout: stdout, Stderr: stderr,
		SysProcAttr: &syscall.SysProcAttr{Setpgid: true}}
	// caught, not ignored: the argv starts with the default dispositions
	sigs := make(chan os.Signal, 8)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
	go func() {
		for range sigs {
		}
	}()
	if err := cmd.Start(); err != nil {
		fmt.Fprintln(stderr, "kairo container: start:", err)
		return writeShimExit(containerExitIn, shimNotStarted)
	}
	exit, sig := reapUntil(cmd.Process.Pid, loaders.restart)
	if sig != 0 {
		fmt.Fprintf(stderr, "kairo container: %s ended by signal %d (%v)\n", spec.Argv[0], int(sig), sig)
	}
	return writeShimExit(containerExitIn, exit)
}

// loaderReady bounds how long the init waits for a loader's socket.
const loaderReady = 30 * time.Second

// loaderRestarts: times a loader that ended is started again; its readers
// come back where they stand.
const loaderRestarts = 5

// loaderProc is a stream's loader: how it is started, and its process.
type loaderProc struct {
	stream   string
	args     []string
	dir      string
	env      []string
	logs     *os.File
	pid      int
	restarts int
}

// loaderProcs are an attempt's loaders.
type loaderProcs []*loaderProc

func (p *loaderProc) start() error {
	cmd := &exec.Cmd{Path: p.args[0], Args: p.args, Dir: p.dir, Env: p.env, Stdout: p.logs, Stderr: p.logs, SysProcAttr: &syscall.SysProcAttr{Setpgid: true}}
	if err := cmd.Start(); err != nil {
		return err
	}
	p.pid = cmd.Process.Pid
	return nil
}

// restart starts again the loader whose process pid ended (reaped), up to
// loaderRestarts times: its readers come back at their positions.
func (l loaderProcs) restart(pid int) {
	for _, p := range l {
		if p.pid != pid {
			continue
		}
		p.pid = 0
		_ = syscall.Kill(-pid, syscall.SIGKILL) // its stages
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
// argv reads, its spec, positions and socket in /kairo/run/streams/<name>,
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
	report := containerEnvValue(env, "KAIRO_API_URL") != "" && containerEnvValue(env, "KAIRO_ATTEMPT_TOKEN") != ""
	var procs loaderProcs
	for _, s := range spec.Streams {
		dir := path.Join(containerRoot, "run", "streams", s.Name)
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
		args := []string{self, "loader", "serve", "--spec", specPath, "--listen", "unix:" + sock}
		if len(s.Positions) > 0 {
			positions, _ := json.Marshal(s.Positions)
			p := path.Join(dir, "positions.json")
			if err := os.WriteFile(p, positions, 0o600); err != nil {
				return procs, env, err
			}
			args = append(args, "--positions", p)
		}
		if report {
			args = append(args, "--report")
		}
		if spec.RawDir != "" {
			args = append(args, "--raw", spec.RawDir)
		}
		p := &loaderProc{stream: s.Name, args: args, dir: spec.CWD, env: env, logs: logs}
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

func (e *storeWorker) sweepContainers(ctx context.Context) {
	out, err := incusCommand(ctx, nil, "list", "--format", "json")
	if err != nil {
		e.roundProblem(&e.containerSweeps, "", "containers not listed", err)
		return
	}
	var instances []struct {
		Name   string `json:"name"`
		Status string `json:"status"`
	}
	if err := json.Unmarshal(out, &instances); err != nil {
		e.roundProblem(&e.containerSweeps, "", "containers not listed", err)
		return
	}
	e.roundProblem(&e.containerSweeps, "", "", nil)
	for _, in := range instances {
		rest, ok := strings.CutPrefix(in.Name, "kairo-att-")
		if !ok || in.Status != "Stopped" {
			continue
		}
		attempt := "att_" + rest
		if e.attemptRunsHere(attempt) {
			continue
		}
		if _, err := incusCommand(ctx, nil, "delete", "--force", in.Name); err != nil && !incusNotFound(err) {
			e.roundProblem(&e.containerSweeps, in.Name, "container not deleted", err)
			continue
		}
		_ = os.RemoveAll(e.containerDir(attempt))
		e.Logger.Info("container of an attempt no longer running deleted", "instance", in.Name, "attempt_id", attempt)
	}
}
