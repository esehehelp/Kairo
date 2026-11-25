package executor

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"kairo/internal/processidentity"
	"kairo/internal/store"
)

// wslc containers (README: Containers). An executor of kind wslc, run by an
// agent on Windows, runs every attempt in a WSL container (wslc) of its
// image, made for it and removed after it: Linux work with the node's GPUs
// (--gpus: the WSL GPU) on a Windows host.
//
// The runner (wslcRunArg, under the shim as for the other runtimes) pulls
// the image, runs `wslc run` in the foreground and returns the exit the
// container's init wrote. The init is the Linux build of this binary
// (kairo-linux beside the agent's), mounted at /kairo/bin/kairo. A wslc
// container cannot mount (no CAP_SYS_ADMIN), so the init does not confine
// it; the whole container runs as an ordinary user (wslcUser) instead:
// what Kairo hands it is writable, the image's files are not, so what an
// attempt writes outside /kairo fails rather than vanishing with the
// container (only /tmp, temporary by intent, stays writable). Its
// processes live in the WSL VM, out of the agent's sight: a stop of the
// attempt reaches the runner, which stops the container (its init passes
// the signal to the argv), and the container's state stands for its
// processes' liveness.
const (
	wslcRunArg = "__kairo-wslc-run"
	wslcUser   = "1000:1000"
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

// wslcRun runs an attempt's container (the shim's launcher): it pulls the
// image, removes a leftover of the same name, runs the container until it
// stops, removes it and exits with the exit its init wrote.
func wslcRun(specPath string) int {
	logf := func(format string, a ...any) {
		_, _ = os.Stderr.Seek(0, io.SeekEnd)
		fmt.Fprintf(os.Stderr, "kairo container: "+format+"\n", a...)
	}
	spec, err := readContainerSpec(specPath)
	if err != nil {
		logf("%v", err)
		return shimNotStarted
	}
	ctx := context.Background()
	logf("container %s of %s", spec.Instance, spec.Image)
	if _, err := wslcCommand(ctx, "pull", "--quiet", spec.Image); err != nil {
		logf("%v", err)
		return shimNotStarted
	}
	_, _ = wslcCommand(ctx, "remove", "--force", spec.Instance)
	run := exec.Command(wslcExe(), wslcRunArgs(spec)...)
	run.Stdout, run.Stderr = io.Discard, os.Stderr
	// a stop of the attempt (CTRL_BREAK; SIGTERM elsewhere) stops the
	// container: its init passes it to the argv
	sigs := make(chan os.Signal, 4)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	go func() {
		for range sigs {
			logf("stopping %s", spec.Instance)
			stopCtx, cancel := context.WithTimeout(ctx, (wslcStopSeconds+30)*time.Second)
			_, _ = wslcCommand(stopCtx, "stop", "--time", fmt.Sprint(wslcStopSeconds), spec.Instance)
			cancel()
		}
	}()
	if err := run.Run(); err != nil {
		logf("wslc run %s: %v", spec.Instance, err)
	}
	signal.Stop(sigs)
	exit, ok := readExitFile(filepath.Join(spec.RunDir, "exit"))
	if !ok {
		logf("the container stopped without writing its exit")
		exit = -1
	}
	rmCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	if _, err := wslcCommand(rmCtx, "remove", "--force", spec.Instance); err != nil {
		logf("container %s not removed: %v", spec.Instance, err)
	}
	_ = os.Remove(specPath)
	return exit
}

// runWSLCHelper runs the wslc runner when args name it, and does not return
// then.
func runWSLCHelper(args []string) {
	if len(args) == 3 && args[1] == wslcRunArg {
		os.Exit(wslcRun(args[2]))
	}
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

// wslcBackend runs every attempt in a wslc container of its image
// (above), on Windows.
type wslcBackend struct{ e *Local }

func (b wslcBackend) launcher(launch *store.Launch) (*exec.Cmd, *confinement, error) {
	cmd, err := b.e.containerCommand(launch, containerOptions{runArg: wslcRunArg, dirsOnly: true, wslc: true})
	return cmd, nil, err
}

// liveness: the processes of an attempt in a container are the container's
// (in the WSL VM, out of sight): alive while it is running.
func (b wslcBackend) liveness(ctx context.Context, attemptID string, _ int, _ string) (processidentity.Liveness, error) {
	list, err := wslcContainers(ctx)
	if err != nil {
		return processidentity.LivenessUnknown, err
	}
	name := containerInstance(attemptID)
	for _, c := range list {
		if c.Names == name && c.State == "running" {
			return processidentity.LivenessAlive, nil
		}
	}
	return processidentity.LivenessAbsent, nil
}

func (b wslcBackend) ended(attemptID string) {
	_ = os.RemoveAll(b.e.containerDir(attemptID))
	b.e.storeWork().sweepSoon()
}

func (b wslcBackend) sweep(ctx context.Context) { b.e.storeWork().sweepWSLC(ctx) }

// sweepWSLC removes the containers of attempts that no longer run here (a
// runner killed before it removed its container): stopped ones, and running
// ones whose attempt is not running.
func (e *storeWorker) sweepWSLC(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, containerSweepTimeout)
	defer cancel()
	list, err := wslcContainers(ctx)
	if err != nil {
		e.roundProblem(&e.containerSweeps, "", "containers not listed", err)
		return
	}
	e.roundProblem(&e.containerSweeps, "", "", nil)
	for _, c := range list {
		id, ok := attemptOfInstance(c.Names)
		if !ok || e.attemptRunsHere(id) {
			continue
		}
		if _, err := wslcCommand(ctx, "remove", "--force", c.Names); err != nil {
			e.roundProblem(&e.containerSweeps, c.Names, "container not removed", err)
			continue
		}
		_ = os.RemoveAll(e.containerDir(id))
		e.Logger.Info("container of an attempt no longer running removed", "instance", c.Names, "attempt_id", id, "state", c.State)
	}
}

// attemptOfInstance is the attempt of a container named by
// containerInstance.
func attemptOfInstance(name string) (string, bool) {
	rest, ok := strings.CutPrefix(name, "kairo-att-")
	if !ok || rest == "" || strings.ContainsAny(rest, "-_") {
		return "", false
	}
	return "att_" + rest, true
}
