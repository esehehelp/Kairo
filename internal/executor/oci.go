package executor

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"kairo/internal/config"
	"kairo/internal/processidentity"
	"kairo/internal/store"
)

// Container runtimes (README: Containers). Every executor that runs
// containers runs them the same way: its attempt's launcher is the shim,
// which runs this binary as the OCI runner (ociRunArg); the runner has the
// runtime make the attempt's container from its spec, waits for it to stop,
// takes the exit its init wrote and removes the container. What differs
// between runtimes (Incus on Linux, Proxmox's pct, wslc on Windows) is an
// ociRuntime; how an executor treats its containers, its ociCaps.

// ociRunArg is the runner's argument (with the spec's path). The runners'
// earlier arguments (containerRunArg, pctRunArg, wslcRunArg) still run it.
const ociRunArg = "__kairo-oci-run"

// ociRuntime runs attempts' containers with one container runtime.
type ociRuntime interface {
	// run makes the container of spec from its image, starts it and waits
	// until it stops: the exit its init wrote (shimNotStarted when it never
	// ran; -1 when it ran and wrote none) and the handle remove takes (""
	// when nothing was made). made gets the handle as soon as the container
	// exists: a stop may come while it runs.
	run(ctx context.Context, spec containerSpec, logf func(string, ...any), made func(handle string)) (handle string, exit int, err error)
	// stop asks a running container to stop (its init passes it on to the
	// argv).
	stop(ctx context.Context, handle string) error
	// remove removes a container (one run made, or one a sweep found).
	remove(ctx context.Context, handle string) error
	// containers are the attempts' containers the runtime has.
	containers(ctx context.Context) ([]ociContainer, error)
}

// ociContainer is an attempt's container as a runtime lists it.
type ociContainer struct {
	attempt string
	running bool
	handle  string
}

// ociCaps are how an executor treats the containers of a runtime.
type ociCaps struct {
	// argvOnHost: the argv's processes are on the agent's host (a
	// container of the same kernel): a stop reaches them directly, as a
	// host attempt's processes, and their liveness is a host process's. A
	// container in a VM (wslc) is stopped through its runtime instead, and
	// is alive while it runs.
	argvOnHost bool
	// dirsOnly: only directories are mounted (Proxmox and wslc bind no
	// single file): the binary and the logs as directories of links.
	dirsOnly bool
	// vm: the container runs in a VM (wslc): its init is the Linux build
	// beside this binary, it cannot mount (it runs as an ordinary user,
	// unconfined) and it reaches the agent's raw views only as files.
	vm bool
}

// ociCapsOf are the caps of an executor kind's runtime; ok false for a kind
// that runs no containers.
func ociCapsOf(kind string) (ociCaps, bool) {
	switch kind {
	case config.KindIncus:
		return ociCaps{argvOnHost: true}, true
	case config.KindPCT:
		return ociCaps{argvOnHost: true, dirsOnly: true}, true
	case config.KindWSLC:
		return ociCaps{dirsOnly: true, vm: true}, true
	}
	return ociCaps{}, false
}

// specRuntime is the runtime a container spec is of: its Runtime, or, for a
// spec from before it named one, Incus or (with pct settings) pct.
func specRuntime(spec containerSpec) string {
	switch {
	case spec.Runtime != "":
		return spec.Runtime
	case spec.PCT != nil:
		return config.KindPCT
	}
	return config.KindIncus
}

// ociBackend runs every attempt of an executor in a container of its image,
// with the executor kind's runtime (rt nil: not on this system).
type ociBackend struct {
	e    *Local
	kind string
	caps ociCaps
	rt   ociRuntime
}

func (b ociBackend) launcher(launch *store.Launch) (*exec.Cmd, *confinement, error) {
	opts := containerOptions{runArg: ociRunArg, runtime: b.kind, dirsOnly: b.caps.dirsOnly, vm: b.caps.vm}
	if b.kind == config.KindPCT {
		settings, err := pctSettingsOf(b.e.Attributes)
		if err != nil {
			return nil, nil, err
		}
		opts.pct = settings
	}
	cmd, err := b.e.containerCommand(launch, opts)
	return cmd, nil, err
}

// liveness: a process on the host is looked at on the host; the processes
// of a container in a VM are the container's: alive while it runs.
func (b ociBackend) liveness(ctx context.Context, attemptID string, pid int, identity string) (processidentity.Liveness, error) {
	if b.caps.argvOnHost {
		return processidentity.HostProcessLiveness(pid, identity)
	}
	if b.rt == nil {
		return processidentity.LivenessUnknown, fmt.Errorf("executor %s: kind %s does not run here", b.e.ID, b.kind)
	}
	list, err := b.rt.containers(ctx)
	if err != nil {
		return processidentity.LivenessUnknown, err
	}
	for _, c := range list {
		if c.attempt == attemptID && c.running {
			return processidentity.LivenessAlive, nil
		}
	}
	return processidentity.LivenessAbsent, nil
}

// ended removes what the attempt's container was made from (its spec holds
// its token) and has the container, left by a runner that was killed, swept
// at the next tick.
func (b ociBackend) ended(attemptID string) {
	_ = os.RemoveAll(b.e.containerDir(attemptID))
	b.e.storeWork().sweepSoon()
}

func (b ociBackend) sweep(ctx context.Context) {
	if b.rt != nil {
		b.e.storeWork().sweepContainers(ctx, b.rt, b.caps)
	}
}

// sweepContainers removes the containers of attempts that no longer run
// here (a runner killed before it removed its container), and what they
// were made from. A running one whose argv is on the host is left alone:
// what an exited attempt left running is ended as on a host, which stops it.
func (e *storeWorker) sweepContainers(ctx context.Context, rt ociRuntime, caps ociCaps) {
	ctx, cancel := context.WithTimeout(ctx, containerSweepTimeout)
	defer cancel()
	list, err := rt.containers(ctx)
	if err != nil {
		e.roundProblem(&e.containerSweeps, "", "containers not listed", err)
		return
	}
	e.roundProblem(&e.containerSweeps, "", "", nil)
	for _, c := range list {
		if e.attemptRunsHere(c.attempt) || c.running && caps.argvOnHost {
			continue
		}
		if err := rt.remove(ctx, c.handle); err != nil {
			e.roundProblem(&e.containerSweeps, c.handle, "container not removed", err)
			continue
		}
		_ = os.RemoveAll(e.containerDir(c.attempt))
		e.Logger.Info("container of an attempt no longer running removed", "container", c.handle, "attempt_id", c.attempt, "running", c.running)
	}
}

// ociRun is the runner (the shim's launcher): it has the spec's runtime
// make, run and remove the attempt's container, and exits with the exit its
// init wrote. A stop of the attempt reaches the runner too: with the argv on
// the host it reaches the argv directly and the runner stays to see the
// container stop; in a VM the runner stops the container. It removes the
// spec, which holds the attempt's token.
func ociRun(specPath string) int {
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
	kind := specRuntime(spec)
	rt, ok := runtimeFor(kind)
	caps, _ := ociCapsOf(kind)
	if !ok {
		logf("runtime %s does not run on %s", kind, runtime.GOOS)
		return shimNotStarted
	}
	if runtime.GOOS != "windows" && os.Getenv("HOME") == "" {
		// incus keeps its remotes under HOME; a service may have none
		_ = os.Setenv("HOME", "/root")
	}
	ctx := context.Background()
	var mu sync.Mutex
	current := ""
	sigs := make(chan os.Signal, 8)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	go func() {
		for range sigs {
			mu.Lock()
			handle := current
			mu.Unlock()
			if caps.argvOnHost || handle == "" {
				continue
			}
			logf("stopping %s", spec.Instance)
			stopCtx, cancel := context.WithTimeout(ctx, (wslcStopSeconds+30)*time.Second)
			if err := rt.stop(stopCtx, handle); err != nil {
				logf("container %s not stopped: %v", spec.Instance, err)
			}
			cancel()
		}
	}()
	handle, exit, err := rt.run(ctx, spec, logf, func(h string) {
		mu.Lock()
		current = h
		mu.Unlock()
	})
	signal.Stop(sigs)
	if err != nil {
		logf("%v", err)
	}
	if handle != "" {
		rmCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
		if err := rt.remove(rmCtx, handle); err != nil {
			logf("container %s not removed: %v", handle, err)
		}
		cancel()
	}
	_ = os.Remove(specPath)
	return exit
}

// runtimeFor is the runtime of an executor kind here (runtimeNamed; a test
// puts a fake one).
var runtimeFor = runtimeNamed

// runOCIHelper runs the OCI runner when args name it (its argument or an
// earlier runner's), and does not return then.
func runOCIHelper(args []string) {
	if len(args) != 3 {
		return
	}
	switch args[1] {
	case ociRunArg, containerRunArg, pctRunArg, wslcRunArg:
		os.Exit(ociRun(args[2]))
	}
}
