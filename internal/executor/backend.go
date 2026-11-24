package executor

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"kairo/internal/config"
	"kairo/internal/envspec"
	"kairo/internal/processidentity"
	"kairo/internal/store"
)

// An executor runs its attempts one way, its backend, chosen once from its
// kind (Local.backend): on its host (hostBackend), inside a WSL distro
// (wslBackend: kind wsl2, or the label environment = "wsl2"), or in an Incus
// container of the attempt's image (incusBackend, container.go). Only what
// differs between them is here; leases, the shim and adoption, logs, usage,
// force-stop orders and the stores are Local's, the same for every backend.
type backend interface {
	// launcher builds an attempt's launcher, which its shim runs (shim.go),
	// and the confinement the shim applies to it (nil: none, or the backend
	// confines the attempt itself). On an error the launch is refused; the
	// command may still be returned (argv[0] not found: as built).
	launcher(launch *store.Launch) (*exec.Cmd, *confinement, error)
	// liveness is that of a process an attempt registered in the executor's
	// namespace.
	liveness(ctx context.Context, pid int, identity string) (processidentity.Liveness, error)
	// tree is an attempt's process tree as far as it is known before its
	// launcher's pid: where its processes are.
	tree(attemptID string) processTree
	// readsNodePaths reports whether the node's paths (inputs read in
	// place, in-place sources) are this executor's to check.
	readsNodePaths() bool
	// storeFS is how the executor reaches its stores' roots;
	// servesNodeStores whether it serves the node's stores that name no
	// executor.
	storeFS() storeFS
	servesNodeStores() bool
	// tempNames are the variables naming a temporary directory, set to the
	// scratch with scratch_tmpdir.
	tempNames() []string
	// ended: an attempt it ran has ended (its exit recorded).
	ended(attemptID string)
}

// instanceSweeper is a backend that removes, now and then, what attempts
// that no longer run left behind (incusBackend: stopped instances).
type instanceSweeper interface {
	sweep(ctx context.Context)
}

// usageSampler is a backend that samples its attempts' usage itself
// (wslBackend on Windows: the processes are the distro's).
type usageSampler interface {
	sampleUsage(ctx context.Context, targets []usageTarget) map[string]usageSample
}

// kindWSL is the executor kind that runs attempts inside a WSL distro.
const kindWSL = "wsl2"

// backend is this executor's backend, from its kind and labels.
func (e *Local) backend() backend {
	switch {
	case e.Kind == config.KindIncus:
		return incusBackend{e}
	case e.Kind == kindWSL || e.Attributes["environment"] == kindWSL:
		return wslBackend{e: e, distro: e.Attributes["distro"]}
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
// Kairo's own variables (parent), the attempt's own (own: declared, then
// Kairo's launch variables), and the executor's WSLENV, if any, apart.
func (e *Local) attemptCommandEnv(launch *store.Launch) (parent, own []string, wslenv string) {
	own = e.attemptEnv(launch)
	given := map[string]bool{}
	for _, kv := range own {
		given[envKey(envName(kv))] = true
	}
	parent = make([]string, 0, len(os.Environ()))
	for _, kv := range os.Environ() {
		name, value, _ := strings.Cut(kv, "=")
		switch {
		case strings.EqualFold(name, "WSLENV"):
			wslenv = value
		case envspec.Reserved(name), given[envKey(name)]:
		default:
			parent = append(parent, kv)
		}
	}
	return parent, own, wslenv
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
	parent, own, wslenv := e.attemptCommandEnv(launch)
	cmd := exec.Command(launch.Argv[0], launch.Argv[1:]...)
	cmd.Dir = launch.CWD
	if wslenv != "" {
		parent = append(parent, "WSLENV="+wslenv)
	}
	cmd.Env = append(parent, own...)
	if cmd.Err != nil {
		// argv[0] not found: the command as built, with why
		return cmd, nil, cmd.Err
	}
	if launch.Source != nil {
		// run in its code snapshot, cwd relative to it (source.go)
		dir, err := e.ensureSource(context.Background(), launch.Source)
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

func (hostBackend) tree(attemptID string) processTree { return processTree{attemptID: attemptID} }
func (hostBackend) readsNodePaths() bool              { return true }
func (hostBackend) storeFS() storeFS                  { return localStoreFS{} }
func (hostBackend) servesNodeStores() bool            { return true }
func (hostBackend) tempNames() []string               { return []string{"TMPDIR", "TMP", "TEMP"} }
func (hostBackend) ended(string)                      {}

// wslBackend runs attempts inside a WSL distro through wsl.exe. The attempt
// gets its variables through WSLENV rather than on the wsl.exe command line,
// which other tools can read and log, and runs its argv with --exec: without
// it wsl.exe joins the arguments into a line for the default shell, which
// expands $VAR and re-splits quoted words. Its paths are the distro's: the
// executor does not look at them, serves only the stores that name it, and
// neither runs code snapshots nor confines there yet.
type wslBackend struct {
	e      *Local
	distro string
}

func (b wslBackend) launcher(launch *store.Launch) (*exec.Cmd, *confinement, error) {
	e := b.e
	if err := refuseImage(e, launch); err != nil {
		return nil, nil, err
	}
	parent, own, wslenv := e.attemptCommandEnv(launch)
	args := []string{}
	if b.distro != "" {
		args = append(args, "-d", b.distro)
	}
	args = append(args, "--cd", launch.CWD, "--exec")
	args = append(args, launch.Argv...)
	cmd := exec.Command("wsl.exe", args...)
	shared := []string{}
	if wslenv != "" {
		shared = append(shared, wslenv)
	}
	for _, kv := range own {
		shared = append(shared, envName(kv)+"/u")
	}
	cmd.Env = append(append(parent, own...), "WSLENV="+strings.Join(shared, ":"))
	if launch.Source != nil {
		return nil, nil, fmt.Errorf("code snapshot %s: %w", launch.Source.ID, errors.New("code snapshots are not run inside WSL yet"))
	}
	if launch.Confine {
		return nil, nil, fmt.Errorf("%w: confinement inside WSL is not done yet", errConfineHere)
	}
	return cmd, nil, nil
}

func (b wslBackend) liveness(ctx context.Context, pid int, identity string) (processidentity.Liveness, error) {
	return processidentity.WSLProcessLiveness(ctx, b.distro, pid, identity)
}

func (b wslBackend) tree(attemptID string) processTree {
	return processTree{attemptID: attemptID, wsl: true, distro: b.distro}
}
func (wslBackend) readsNodePaths() bool   { return false }
func (b wslBackend) storeFS() storeFS     { return wslStoreFS{distro: b.distro} }
func (wslBackend) servesNodeStores() bool { return false }
func (wslBackend) tempNames() []string    { return []string{"TMPDIR"} } // TMP and TEMP would reach wsl.exe, a Windows program
func (wslBackend) ended(string)           {}

// incusBackend runs every attempt in an Incus container of its image
// (container.go).
type incusBackend struct{ e *Local }

func (b incusBackend) launcher(launch *store.Launch) (*exec.Cmd, *confinement, error) {
	cmd, err := b.e.containerCommand(launch)
	return cmd, nil, err
}

func (incusBackend) liveness(_ context.Context, pid int, identity string) (processidentity.Liveness, error) {
	return processidentity.HostProcessLiveness(pid, identity)
}

func (incusBackend) tree(attemptID string) processTree { return processTree{attemptID: attemptID} }
func (incusBackend) readsNodePaths() bool              { return true }
func (incusBackend) storeFS() storeFS                  { return localStoreFS{} }
func (incusBackend) servesNodeStores() bool            { return true }
func (incusBackend) tempNames() []string               { return []string{"TMPDIR", "TMP", "TEMP"} }

// ended removes what the attempt's container was made from (its spec holds
// its token) and has its instance, left by a runner that was killed, swept
// at the next tick.
func (b incusBackend) ended(attemptID string) {
	_ = os.RemoveAll(b.e.containerDir(attemptID))
	b.e.mu.Lock()
	b.e.containerSweeps.at = time.Time{}
	b.e.mu.Unlock()
}

func (b incusBackend) sweep(ctx context.Context) { b.e.sweepContainers(ctx) }
