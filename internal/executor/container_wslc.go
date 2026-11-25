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
	"path/filepath"
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

// wslcRuntime runs attempts' containers with wslc (oci.go): a container
// is named by its instance name.
type wslcRuntime struct{}

// run pulls the image, removes a leftover of the same name and runs the
// container in the foreground until it stops.
func (wslcRuntime) run(ctx context.Context, spec containerSpec, logf func(string, ...any), made func(string)) (string, int, error) {
	logf("container %s of %s", spec.Instance, spec.Image)
	if _, err := wslcCommand(ctx, "pull", "--quiet", spec.Image); err != nil {
		return "", shimNotStarted, err
	}
	_, _ = wslcCommand(ctx, "remove", "--force", spec.Instance)
	made(spec.Instance)
	// wslc sometimes fails a run before it made the container (two runs
	// started at once): made again, a few times
	for try := 1; ; try++ {
		run := exec.Command(wslcExe(), wslcRunArgs(spec)...)
		run.Stdout, run.Stderr = io.Discard, os.Stderr
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
	exit, ok := readExitFile(filepath.Join(spec.RunDir, "exit"))
	if !ok {
		logf("the container stopped without writing its exit")
		exit = -1
	}
	return spec.Instance, exit, nil
}

func (wslcRuntime) stop(ctx context.Context, name string) error {
	_, err := wslcCommand(ctx, "stop", "--time", fmt.Sprint(wslcStopSeconds), name)
	return err
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
			out = append(out, ociContainer{attempt: id, running: c.State == "running", handle: c.Names})
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
// containerInstance.
func attemptOfInstance(name string) (string, bool) {
	rest, ok := strings.CutPrefix(name, "kairo-att-")
	if !ok || rest == "" || strings.ContainsAny(rest, "-_") {
		return "", false
	}
	return "att_" + rest, true
}
