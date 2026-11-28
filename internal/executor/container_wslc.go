package executor

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// wslc containers (README: Containers). An executor of kind wslc, run by an
// agent on Windows, runs every attempt in a WSL container (wslc) of its
// image, made for it and removed after it: Linux work with the node's GPUs
// (--gpus: the WSL GPU) on a Windows host.
//
// The runner (oci.go) has wslcRuntime pull the image and run `wslc run` in
// the foreground; it returns the exit the container's init wrote. The init is the Linux build of this binary
// (kairo-linux beside the agent's), mounted at /kairo/bin/kairo. A wslc
// container cannot mount (no CAP_SYS_ADMIN), so the init does not confine
// it: it starts as root (wslcUser), and the argv runs as the ordinary user
// every container's does (containerUID), to whom what Kairo hands it is
// writable and the image's files are not, so what an attempt writes outside
// /kairo fails rather than vanishing with the container (only /tmp,
// temporary by intent, stays writable). Its
// processes live in the WSL VM, out of the agent's sight: a stop of the
// attempt reaches the runner, which stops the container (its init passes
// the signal to the argv), and the container's state stands for its
// processes' liveness.
const (
	wslcRunArg = "__kairo-wslc-run"
	wslcUser   = "0:0"
	// wslcShm caps the container's /dev/shm (a tmpfs: only what is
	// written there is memory)
	wslcShm = "16g"
	// wslcStopSeconds is how long a stopped container's init has to end
	// the argv before wslc kills it.
	wslcStopSeconds = 30
	// wslcLinuxBinary is the Linux build of this binary beside it, the
	// container's init.
	wslcLinuxBinary = "kairo-linux"
)

// wslcExe is wslc.exe: on PATH, else where WSL installs it.
func wslcExe() string {
	if p, err := exec.LookPath("wslc"); err == nil {
		return p
	}
	return filepath.Join(os.Getenv("ProgramFiles"), "WSL", "wslc.exe")
}

// wslcCommand runs wslc with args: its output, or why it failed.
var wslcCommand = func(ctx context.Context, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, wslcExe(), args...)
	ownProcessGroup(cmd)
	var out, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &stderr
	if err := cmd.Run(); err != nil {
		return out.Bytes(), fmt.Errorf("wslc %s: %w: %s", args[0], err, strings.TrimSpace(stderr.String()))
	}
	return out.Bytes(), nil
}

// wslcRunArgs are the arguments of `wslc run` for spec.
func wslcRunArgs(spec containerSpec) []string {
	args := []string{"run", "--name", spec.Instance, "--user", wslcUser,
		"--entrypoint", containerRoot + "/bin/kairo",
		"--label", "kairo.attempt=" + spec.AttemptID,
		"--stop-signal", "SIGTERM", "--stop-timeout", fmt.Sprint(wslcStopSeconds),
		"--shm-size", wslcShm, "--pull", "never"}
	if len(spec.GPUs) > 0 {
		args = append(args, "--gpus", "all")
	}
	for _, m := range spec.Mounts {
		v := m.Source + ":" + m.Path
		if m.ReadOnly {
			v += ":ro"
		}
		args = append(args, "--volume", v)
	}
	return append(args, spec.Image, containerInitArg, containerSpecIn)
}

// wslcRuntime runs attempts' containers with wslc (oci.go): a container
// is named by its instance name.
type wslcRuntime struct{}

// run pulls the image, removes a leftover of the same name and runs the
// container in the foreground until it stops.
func (wslcRuntime) run(ctx context.Context, spec containerSpec, logf func(string, ...any), starting func(string) bool) (string, int, error) {
	logf("container %s of %s", spec.Instance, spec.Image)
	if err := wslcGPUsWhole(ctx, spec.GPUs); err != nil {
		return "", shimNotStarted, err
	}
	if _, err := wslcCommand(ctx, "pull", "--quiet", spec.Image); err != nil {
		return "", shimNotStarted, err
	}
	_, _ = wslcCommand(ctx, "remove", "--force", spec.Instance)
	// a stop asked for before now: it is not started
	if !starting(spec.Instance) {
		logf("asked to stop before %s started: not started", spec.Instance)
		return "", exitStoppedBeforeStart, nil
	}
	// a gang member: connected to its generation's segment once it exists
	// (its init waits for the address)
	if spec.Gang != nil {
		go wslcJoinSegment(ctx, spec, logf)
	}
	// wslc sometimes fails a run before it made the container (two runs
	// started at once): made again, a few times, unless a stop was asked
	// for meanwhile
	for try := 1; ; try++ {
		if try > 1 && !starting(spec.Instance) {
			logf("asked to stop before %s started: not started", spec.Instance)
			return "", exitStoppedBeforeStart, nil
		}
		run := exec.Command(wslcExe(), wslcRunArgs(spec)...)
		run.Stdout, run.Stderr = io.Discard, os.Stderr
		ownProcessGroup(run)
		err := run.Run()
		if err == nil {
			break
		}
		logf("wslc run %s: %v", spec.Instance, err)
		if _, written := readExitFile(filepath.Join(spec.RunDir, "exit")); written || try == wslcRunTries || wslcHas(ctx, spec.Instance) {
			break
		}
		time.Sleep(time.Duration(try) * 3 * time.Second)
	}
	// the client may end before its container (killed): the container is
	// seen stopped before its exit is read
	for deadline := time.Now().Add((wslcStopSeconds + 60) * time.Second); wslcRunning(ctx, spec.Instance) && time.Now().Before(deadline); {
		time.Sleep(time.Second)
	}
	exit, ok := readExitFile(filepath.Join(spec.RunDir, "exit"))
	if !ok {
		logf("the container stopped without writing its exit")
		exit = -1
	}
	return spec.Instance, exit, nil
}

// wslcJoinSegment connects spec's container, once wslc has it, to its
// gang's segment network with its address there.
func wslcJoinSegment(ctx context.Context, spec containerSpec, logf func(string, ...any)) {
	address, err := netip.ParsePrefix(spec.Gang.Address)
	if err != nil {
		logf("gang address %q: %v", spec.Gang.Address, err)
		return
	}
	for deadline := time.Now().Add(gangAddressWait); !wslcHas(ctx, spec.Instance); {
		if time.Now().After(deadline) || ctx.Err() != nil {
			logf("container %s not made in %s: not connected to segment network %s", spec.Instance, gangAddressWait, spec.Gang.Device)
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	if _, err := wslcCommand(ctx, "network", "connect", "--ip", address.Addr().String(), spec.Gang.Device, spec.Instance); err != nil {
		logf("connect %s to segment network %s: %v", spec.Instance, spec.Gang.Device, err)
		return
	}
	logf("%s connected to segment network %s as %s", spec.Instance, spec.Gang.Device, address.Addr())
}

// wslcRunning: wslc has a container named name that may still run (an
// error: taken as running; the caller's deadline bounds the wait).
func wslcRunning(ctx context.Context, name string) bool {
	list, err := wslcContainers(ctx)
	if err != nil {
		return true
	}
	for _, c := range list {
		if c.Names == name {
			return wslcAlive(c.State)
		}
	}
	return false
}

// wslcAlive: a container in state may have processes: any state but those
// of a container stopped or never started.
func wslcAlive(state string) bool {
	switch strings.ToLower(state) {
	case "exited", "created", "dead", "stopped":
		return false
	}
	return true
}

// wslcGPUs counts the host's GPUs (nvidia-smi, as the provider reads
// them).
var wslcGPUs = func(ctx context.Context) (int, error) {
	out, err := exec.CommandContext(ctx, "nvidia-smi", "--query-gpu=uuid", "--format=csv,noheader").Output()
	if err != nil {
		return 0, fmt.Errorf("nvidia-smi: %w", err)
	}
	return len(strings.Fields(string(out))), nil
}

// wslcGPUsWhole: a container that leases GPUs leases all of the host's. A
// wslc container is given the GPUs all or none (GPU paravirtualization:
// one /dev/dxg for every adapter), CUDA_VISIBLE_DEVICES only hides the
// others from the attempt, which could unset it and use a GPU another
// attempt holds; on a host with more GPUs than the attempt leases it is
// not started.
func wslcGPUsWhole(ctx context.Context, leased []string) error {
	if len(leased) == 0 {
		return nil
	}
	n, err := wslcGPUs(ctx)
	if err != nil {
		return fmt.Errorf("the host's GPUs not counted (a wslc container is given all of them): %w", err)
	}
	if n > len(leased) {
		return fmt.Errorf("a wslc container is given all %d of the host's GPUs, and this attempt leases %d: lease all of them (count = %d), or run it where containers get only theirs", n, len(leased), n)
	}
	return nil
}

func (wslcRuntime) stop(ctx context.Context, name string) error {
	_, err := wslcCommand(ctx, "stop", "--time", fmt.Sprint(wslcStopSeconds), name)
	return err
}

func (wslcRuntime) kill(ctx context.Context, name string) error {
	_, err := wslcCommand(ctx, "kill", name)
	return err
}

// stats reads `wslc stats`: memory in use and the bytes written (BlockIO's
// second figure).
func (wslcRuntime) stats(ctx context.Context, name string) (*int64, *int64, error) {
	out, err := wslcCommand(ctx, "stats", "--format", "json", name)
	if err != nil {
		return nil, nil, err
	}
	var s struct {
		MemUsage string `json:"MemUsage"`
		BlockIO  string `json:"BlockIO"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(bytes.SplitN(out, []byte("\n"), 2)[0]), &s); err != nil {
		return nil, nil, fmt.Errorf("wslc stats: %w", err)
	}
	var ram, written *int64
	if mem, _, ok := strings.Cut(s.MemUsage, "/"); ok {
		if n, err := parseSize(mem); err == nil {
			ram = &n
		}
	}
	if _, w, ok := strings.Cut(s.BlockIO, "/"); ok {
		if n, err := parseSize(w); err == nil {
			written = &n
		}
	}
	return ram, written, nil
}

// parseSize reads a size as docker-style tools print it: 859MiB, 6.79MB,
// 0B (decimal and binary units).
func parseSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	units := []struct {
		suffix string
		bytes  float64
	}{{"KiB", 1 << 10}, {"MiB", 1 << 20}, {"GiB", 1 << 30}, {"TiB", 1 << 40}, {"kB", 1e3}, {"KB", 1e3}, {"MB", 1e6}, {"GB", 1e9}, {"TB", 1e12}, {"B", 1}}
	for _, u := range units {
		if num, ok := strings.CutSuffix(s, u.suffix); ok {
			v, err := strconv.ParseFloat(strings.TrimSpace(num), 64)
			if err != nil {
				return 0, fmt.Errorf("size %q: %w", s, err)
			}
			return int64(v * u.bytes), nil
		}
	}
	return 0, fmt.Errorf("size %q: no unit", s)
}

func (wslcRuntime) remove(ctx context.Context, name string) error {
	_, err := wslcCommand(ctx, "remove", "--force", name)
	return err
}

func (wslcRuntime) containers(ctx context.Context) ([]ociContainer, error) {
	list, err := wslcContainers(ctx)
	if err != nil {
		return nil, err
	}
	var out []ociContainer
	for _, c := range list {
		if id, ok := attemptOfInstance(c.Names); ok {
			out = append(out, ociContainer{attempt: id, running: wslcAlive(c.State), handle: c.Names})
		}
	}
	return out, nil
}

// wslcRunTries is how many times a run that failed before its container
// was made is made.
const wslcRunTries = 3

// wslcHas: wslc has a container named name (an error: taken as having it).
func wslcHas(ctx context.Context, name string) bool {
	list, err := wslcContainers(ctx)
	if err != nil {
		return true
	}
	for _, c := range list {
		if c.Names == name {
			return true
		}
	}
	return false
}

// wslcContainer is a container as `wslc list --format json` lists it.
type wslcContainer struct {
	Names string `json:"Names"`
	State string `json:"State"`
}

// wslcContainers are the containers wslc has, running or not.
func wslcContainers(ctx context.Context) ([]wslcContainer, error) {
	out, err := wslcCommand(ctx, "list", "--all", "--format", "json")
	if err != nil {
		return nil, err
	}
	var list []wslcContainer
	s := bufio.NewScanner(bytes.NewReader(out))
	s.Buffer(make([]byte, 1<<20), 16<<20)
	for s.Scan() {
		line := bytes.TrimSpace(s.Bytes())
		if len(line) == 0 {
			continue
		}
		var c wslcContainer
		if err := json.Unmarshal(line, &c); err != nil {
			return nil, fmt.Errorf("wslc list: %w", err)
		}
		list = append(list, c)
	}
	return list, s.Err()
}

// attemptOfInstance is the attempt of a container named by
// containerInstance (with this agent's prefix: another installation's is
// none of its).
func attemptOfInstance(name string) (string, bool) {
	rest, ok := strings.CutPrefix(name, ContainerPrefix+"att-")
	if !ok || rest == "" || strings.ContainsAny(rest, "-_") {
		return "", false
	}
	return "att_" + rest, true
}
