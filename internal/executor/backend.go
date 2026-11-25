package executor

import (
	"context"
	"os/exec"

	"kairo/internal/processidentity"
	"kairo/internal/store"
)

// An executor runs its attempts in containers of their images (ociBackend,
// oci.go), through the container runtime its kind names. Only what is the
// runtime's is here; leases, the shim and adoption, logs, usage rounds,
// force-stop orders and the stores are Local's. A backend runs in the
// node's namespace: it reads the node's paths, reaches the stores' roots
// directly and serves the node's stores that name no executor.
type backend interface {
	// launcher builds an attempt's launcher, which its shim runs (shim.go).
	// On an error the launch is refused.
	launcher(launch *store.Launch) (*exec.Cmd, error)
	// liveness is that of a process attempt attemptID registered.
	liveness(ctx context.Context, attemptID string, pid int, identity string) (processidentity.Liveness, error)
	// ended: an attempt it ran has ended (its exit recorded).
	ended(attemptID string)
	// stop asks an attempt to stop; kill ends what is left of it at once;
	// alive reports whether anything of it runs.
	stop(ctx context.Context, t processTree) error
	kill(ctx context.Context, t processTree) error
	alive(ctx context.Context, t processTree) (bool, error)
	// sample is what each target attempt uses (unknown: left out).
	sample(ctx context.Context, targets []usageTarget) map[string]usageSample
}

// instanceSweeper is a backend that removes, now and then, what attempts
// that no longer run left behind (ociBackend: their containers).
type instanceSweeper interface {
	sweep(ctx context.Context)
}

// kindlessBackend is the backend of an executor without a kind: none in
// production, where every executor has a container kind (its configuration
// refuses any other); tests run attempts there as plain processes.
var kindlessBackend func(*Local) backend

// backend is this executor's backend, from its kind.
func (e *Local) backend() backend {
	if e.Kind == "" && kindlessBackend != nil {
		return kindlessBackend(e)
	}
	caps, _ := ociCapsOf(e.Kind)
	rt, _ := runtimeFor(e.Kind)
	return ociBackend{e: e, kind: e.Kind, caps: caps, rt: rt}
}
