package executor

import (
	"context"
	"os"
	"os/exec"
	"strings"

	"kairo/internal/envspec"
	"kairo/internal/processidentity"
	"kairo/internal/store"
)

// procBackend runs an attempt's argv as a plain process, its launcher, in
// the launcher's process group (its job object on Windows), as a
// container's runner is run. It is for tests only: the tests of what Local
// does for any backend (leases, the shim and adoption, force stops,
// quiescence, logs) run their attempts with it, on an executor without a
// kind (kindlessBackend). Production never uses it: every configured
// executor has a container kind.
type procBackend struct{ e *Local }

func (b procBackend) launcher(launch *store.Launch) (*exec.Cmd, error) {
	own := b.e.attemptEnv(launch)
	given := map[string]bool{}
	for _, kv := range own {
		given[envKey(envName(kv))] = true
	}
	var env []string
	for _, kv := range os.Environ() {
		name, _, _ := strings.Cut(kv, "=")
		if !envspec.Reserved(name) && !given[envKey(name)] {
			env = append(env, kv)
		}
	}
	cmd := exec.Command(launch.Argv[0], launch.Argv[1:]...)
	cmd.Dir = launch.CWD
	cmd.Env = append(env, own...)
	return cmd, cmd.Err
}

func (procBackend) liveness(_ context.Context, _ string, pid int, identity string) (processidentity.Liveness, error) {
	return processidentity.HostProcessLiveness(pid, identity)
}

func (procBackend) ended(string) {}

func (b procBackend) stop(ctx context.Context, t processTree) error {
	return b.e.stopLauncherTree(ctx, t)
}

func (b procBackend) kill(ctx context.Context, t processTree) error {
	return b.e.killLauncherTree(ctx, t)
}

func (b procBackend) alive(ctx context.Context, t processTree) (bool, error) {
	return b.e.launcherTreeAlive(ctx, t)
}

func (procBackend) sample(context.Context, []usageTarget) map[string]usageSample {
	return map[string]usageSample{}
}

// ForTestsRunKindlessAsProcesses makes executors without a kind run their
// attempts as plain processes (procBackend): for tests only, here and in
// other packages (an agent's end-to-end test).
func ForTestsRunKindlessAsProcesses() {
	kindlessBackend = func(e *Local) backend { return procBackend{e} }
}
