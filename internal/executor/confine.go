package executor

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"

	"kairo/internal/store"
)

// Every attempt launched by an executor that serves a usable volatile store
// gets its project's cache directory there, <root>/cache/<project scope id>
// (marker .kairo-cache naming the project), named by KAIRO_CACHE_DIR: what
// attempts of the project keep for one another (compiled kernels, package
// caches), on the node, from one attempt to the next. Kairo does not delete
// it.
//
// A confined attempt (store.Launch.Confine) may write only to its scratch,
// its run's output directory and its project's cache directory; everything
// else it reads as before but cannot change. TMPDIR, TMP and TEMP name its
// scratch. Without a scratch and an output directory (no usable volatile
// store here) a confined launch is refused. How it is confined is the
// platform's (confine_linux.go: its own mount namespace, everything read-only
// but those directories, /tmp a directory of its scratch, and /dev/shm;
// confine_windows.go: a token at Low integrity, those directories labelled
// Low). Not done inside WSL yet: a confined launch there is refused.

// kindCache: a project's cache directory, <root>/cache/<project scope id>.
var kindCache = dirKind{dir: "cache", marker: ".kairo-cache"}

// errConfineHere: a confined launch this executor cannot carry out.
var errConfineHere = errors.New("cannot confine the attempt here")

// makeCache makes (or finds) the project's cache directory of an attempt
// about to start on the store its scratch is made on and keeps its path for
// the attempt's environment. Without one the attempt starts all the same
// (unless confined).
func (e *Local) makeCache(launch *store.Launch) {
	project := launch.Execution.ProjectScopeID
	if project == "" || !validScratchID(project) {
		return
	}
	s, ok := e.storeFor(launch)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), scratchCallTimeout)
	defer cancel()
	dir, err := s.fs.CreateKind(ctx, s.decl.Root, kindCache, project)
	if err != nil {
		e.Logger.Warn("attempt starts without a cache directory: not made", "attempt_id", launch.Attempt.ID, "store_id", s.decl.ID, "error", err)
		return
	}
	e.mu.Lock()
	if e.caches == nil {
		e.caches = map[string]string{}
	}
	e.caches[launch.Attempt.ID] = dir
	e.mu.Unlock()
}

// cacheEnv is KAIRO_CACHE_DIR of an attempt, once (the path is dropped).
func (e *Local) cacheEnv(launch *store.Launch) []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	dir := e.caches[launch.Attempt.ID]
	delete(e.caches, launch.Attempt.ID)
	if dir == "" {
		return nil
	}
	return []string{"KAIRO_CACHE_DIR=" + dir}
}

// confinedDirs are the directories a confined attempt may write, from its
// environment: scratch, output directory, cache directory ("" when it has
// none).
func confinedDirs(env []string) (scratch, output, cache string) {
	for _, kv := range env {
		name, value, _ := strings.Cut(kv, "=")
		switch name {
		case "KAIRO_SCRATCH":
			scratch = value
		case "KAIRO_OUTPUT_DIR":
			output = value
		case "KAIRO_CACHE_DIR":
			cache = value
		}
	}
	return scratch, output, cache
}

// setEnv replaces (or adds) name in env, as the OS compares names.
func setEnv(env []string, name, value string) []string {
	out := env[:0:0]
	for _, kv := range env {
		if envKey(envName(kv)) != envKey(name) {
			out = append(out, kv)
		}
	}
	return append(out, name+"="+value)
}

// confinement is what a confined attempt may write; its shim confines the
// command with it (confineCommand).
type confinement struct {
	writable []string
	scratch  string
}

// confine readies cmd, an attempt's command, to be confined (see above): its
// temporary directory is its scratch, and the directories it may write are
// made ready as confineCommand makes them (which also tells, before
// anything starts, that it can be confined here).
func (e *Local) confine(cmd *exec.Cmd) (*confinement, error) {
	if e.isWSL() {
		return nil, fmt.Errorf("%w: confinement inside WSL is not done yet", errConfineHere)
	}
	scratch, output, cache := confinedDirs(cmd.Env)
	if scratch == "" || output == "" {
		return nil, fmt.Errorf("%w: no scratch and output directory here (no usable volatile store)", errConfineHere)
	}
	for _, name := range []string{"TMPDIR", "TMP", "TEMP"} {
		cmd.Env = setEnv(cmd.Env, name, scratch)
	}
	writable := []string{scratch, output}
	if cache != "" {
		writable = append(writable, cache)
	}
	release, err := confineCommand(&exec.Cmd{Path: cmd.Path, Args: cmd.Args}, writable, scratch)
	if err != nil {
		return nil, err
	}
	if release != nil {
		release()
	}
	return &confinement{writable: writable, scratch: scratch}, nil
}
