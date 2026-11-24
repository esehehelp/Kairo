package executor

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"kairo/internal/config"
	"kairo/internal/envspec"
	"kairo/internal/processidentity"
	"kairo/internal/store"
)

// An executor runs its attempts one way, its backend, chosen once from its
// kind (Local.backend): on its host (hostBackend) or in an Incus container
// of the attempt's image (incusBackend, container.go). Only what differs
// between them is here; leases, the shim and adoption, logs, usage,
// force-stop orders and the stores are Local's, the same for every backend.
// Both run in the node's namespace: they read the node's paths, reach the
// stores' roots directly and serve the node's stores that name no executor.
type backend interface {
	// launcher builds an attempt's launcher, which its shim runs (shim.go),
	// and the confinement the shim applies to it (nil: none, or the backend
	// confines the attempt itself). On an error the launch is refused; the
	// command may still be returned (argv[0] not found: as built).
	launcher(launch *store.Launch) (*exec.Cmd, *confinement, error)
	// liveness is that of a process an attempt registered in the executor's
	// namespace.
	liveness(ctx context.Context, pid int, identity string) (processidentity.Liveness, error)
	// ended: an attempt it ran has ended (its exit recorded).
	ended(attemptID string)
}

// instanceSweeper is a backend that removes, now and then, what attempts
// that no longer run left behind (incusBackend: stopped instances).
type instanceSweeper interface {
	sweep(ctx context.Context)
}

// backend is this executor's backend, from its kind.
func (e *Local) backend() backend {
	switch e.Kind {
	case config.KindIncus:
		return incusBackend{e}
	case config.KindPCT:
		return pctBackend{e}
	default:
		return hostBackend{e}
	}
}

// refuseImage refuses an execution in an image on a backend that runs no
// containers.
func refuseImage(e *Local, launch *store.Launch) error {
	if launch.Image == "" {
		return nil
	}
	return fmt.Errorf("image %s: executor %s (kind %s) does not run containers", launch.Image, e.ID, e.Kind)
}

// attemptCommandEnv is what a launcher on this node starts from: the
// executor's environment without the names the attempt is given and without
// Kairo's own variables (parent), and the attempt's own (own: declared, then
// Kairo's launch variables).
func (e *Local) attemptCommandEnv(launch *store.Launch) (parent, own []string) {
	own = e.attemptEnv(launch)
	given := map[string]bool{}
	for _, kv := range own {
		given[envKey(envName(kv))] = true
	}
	parent = make([]string, 0, len(os.Environ()))
	for _, kv := range os.Environ() {
		name, _, _ := strings.Cut(kv, "=")
		if !envspec.Reserved(name) && !given[envKey(name)] {
			parent = append(parent, kv)
		}
	}
	return parent, own
}

// hostBackend runs attempts as processes of the executor's host: its
// environment is the executor's, then the declared env, then Kairo's; a
// later layer replaces a name of an earlier one, so declared env overrides
// PATH and the like but never what Kairo sets. The executor's own KAIRO_*
// variables (an operator's KAIRO_TOKEN, say) never reach the attempt. It
// runs in its code snapshot when it has one, confined when it is to be.
type hostBackend struct{ e *Local }

func (b hostBackend) launcher(launch *store.Launch) (*exec.Cmd, *confinement, error) {
	e := b.e
	if err := refuseImage(e, launch); err != nil {
		return nil, nil, err
	}
	parent, own := e.attemptCommandEnv(launch)
	cmd := exec.Command(launch.Argv[0], launch.Argv[1:]...)
	cmd.Dir = launch.CWD
	cmd.Env = append(parent, own...)
	if cmd.Err != nil {
		// argv[0] not found: the command as built, with why
		return cmd, nil, cmd.Err
	}
	if launch.Source != nil {
		// run in its code snapshot, cwd relative to it (source.go)
		dir, err := e.storeWork().ensureSource(context.Background(), launch.Source)
		if err != nil {
			return nil, nil, fmt.Errorf("code snapshot %s: %w", launch.Source.ID, err)
		}
		cmd.Dir = filepath.Join(dir, filepath.FromSlash(launch.CWD))
		cmd.Env = append(cmd.Env, "KAIRO_SOURCE_DIR="+dir, "KAIRO_SOURCE_ID="+launch.Source.ID)
		// Python would write its bytecode beside the modules, into the
		// snapshot (which is then unpacked again at the next launch): not
		// written, unless the attempt's environment says where.
		if !hasEnv(cmd.Env, "PYTHONDONTWRITEBYTECODE") && !hasEnv(cmd.Env, "PYTHONPYCACHEPREFIX") {
			cmd.Env = append(cmd.Env, "PYTHONDONTWRITEBYTECODE=1")
		}
	}
	var conf *confinement
	if launch.Confine {
		c, err := e.confine(cmd)
		if err != nil {
			return nil, nil, err
		}
		conf = c
	}
	return cmd, conf, nil
}

func (hostBackend) liveness(_ context.Context, pid int, identity string) (processidentity.Liveness, error) {
	return processidentity.HostProcessLiveness(pid, identity)
}

func (hostBackend) ended(string) {}

// incusBackend runs every attempt in an Incus container of its image
// (container.go).
type incusBackend struct{ e *Local }

func (b incusBackend) launcher(launch *store.Launch) (*exec.Cmd, *confinement, error) {
	cmd, err := b.e.containerCommand(launch, containerOptions{runArg: containerRunArg})
	return cmd, nil, err
}

func (incusBackend) liveness(_ context.Context, pid int, identity string) (processidentity.Liveness, error) {
	return processidentity.HostProcessLiveness(pid, identity)
}

// ended removes what the attempt's container was made from (its spec holds
// its token) and has its instance, left by a runner that was killed, swept
// at the next tick.
func (b incusBackend) ended(attemptID string) {
	_ = os.RemoveAll(b.e.containerDir(attemptID))
	b.e.storeWork().sweepSoon()
}

func (b incusBackend) sweep(ctx context.Context) { b.e.storeWork().sweepContainers(ctx) }

// pctBackend runs every attempt in a Proxmox VE container of its image, in
// VMIDs 1000-1999, made for it and destroyed after it
// (container_pct_linux.go). Its agent runs on the Proxmox host.
type pctBackend struct{ e *Local }

func (b pctBackend) launcher(launch *store.Launch) (*exec.Cmd, *confinement, error) {
	settings, err := pctSettingsOf(b.e.Attributes)
	if err != nil {
		return nil, nil, err
	}
	cmd, err := b.e.containerCommand(launch, containerOptions{runArg: pctRunArg, dirsOnly: true, pct: settings})
	return cmd, nil, err
}

func (pctBackend) liveness(_ context.Context, pid int, identity string) (processidentity.Liveness, error) {
	return processidentity.HostProcessLiveness(pid, identity)
}

// ended removes what the attempt's container was made from and has its
// container, left by a runner that was killed, swept at the next tick.
func (b pctBackend) ended(attemptID string) {
	_ = os.RemoveAll(b.e.containerDir(attemptID))
	b.e.storeWork().sweepSoon()
}

func (b pctBackend) sweep(ctx context.Context) { b.e.storeWork().sweepPCT(ctx) }
