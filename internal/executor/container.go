package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"kairo/internal/config"
	"kairo/internal/store"
)

// Containers (README: Containers). An executor of kind incus runs every
// attempt in an Incus container of the attempt's image (store.Launch.Image,
// pinned by digest), and nothing else: the planner gives it only executions
// in an image, and only it takes them (store.selectorMatches).
//
// The attempt's launcher is still its shim (shim.go), which runs this
// binary as containerRunArg, the runner (container_linux.go): it makes the
// instance kairo-<attempt id> from the image, starts it, passes on what it is
// sent to stop it, waits for it to stop, reads the exit the container wrote,
// deletes the instance and exits with that exit; so the shim, adoption,
// force stop and quiescence are as for a host attempt. In the container
// this binary runs as containerInitArg, its first process: it confines the
// container as a confined host attempt is confined (confineMounts: all
// read-only but what Kairo hands it), starts the attempt's argv with its
// environment and directory, its output going to the attempt's logs, passes
// signals to its process group, reaps orphans, and writes the argv's exit to
// /kairo/run/exit before it exits, which stops the container.
//
// What Kairo hands an attempt is mounted under /kairo, at the same paths
// on every node, so a continuation finds its run's files where its
// checkpoint names them:
//
//	/kairo/bin/kairo        this binary (read-only)
//	/kairo/run              spec.json (what the init runs) and exit
//	/kairo/log/stdout       the attempt's logs
//	/kairo/log/stderr
//	/kairo/scratch          KAIRO_SCRATCH, TMPDIR (its tmp/ is /tmp)
//	/kairo/output           KAIRO_OUTPUT_DIR
//	/kairo/cache            KAIRO_CACHE_DIR
//	/kairo/src              KAIRO_SOURCE_DIR, the code snapshot (read-only)
//	/kairo/inputs/<name>    KAIRO_INPUT_<NAME> (read-only)
//	/kairo/nvidia           the node's NVIDIA driver files, with a GPU (read-only)
//
// The variables naming them are rewritten to those paths; the rest of the
// environment is the image's, then the declared env, then Kairo's, as on a
// host. The agent's own environment does not reach the container.

const (
	containerRunArg  = "__kairo-incus-run"
	containerInitArg = "__kairo-container-init"
	containerRoot    = "/kairo"
	containerSpecIn  = "/kairo/run/spec.json"
	containerExitIn  = "/kairo/run/exit"
	containerStdout  = "/kairo/log/stdout"
	containerStderr  = "/kairo/log/stderr"
	containerNVIDIA  = "/kairo/nvidia"
	// containerShm is the size of the container's /dev/shm (a tmpfs: what
	// is written there is memory; PyTorch's data loader workers share
	// batches through it).
	containerShm = "50%"
)

// containerSpec is what an attempt in a container is: written by the
// executor into the attempt's run directory as spec.json, read by the
// runner on the node and by the init in the container.
type containerSpec struct {
	Instance  string `json:"instance"`
	AttemptID string `json:"attempt_id"`
	// Image is the image, pinned by digest (registry/repository@sha256:...).
	Image  string           `json:"image"`
	Mounts []containerMount `json:"mounts"`
	// GPUs are the leased GPUs by UUID; DriverRoot is the node directory
	// where the driver files a container is handed are put together.
	GPUs       []string `json:"gpus,omitempty"`
	DriverRoot string   `json:"driver_root,omitempty"`
	// RunDir is the node path of /kairo/run.
	RunDir string `json:"run_dir"`

	// In the container: the argv, its directory and environment (declared,
	// then Kairo's), the paths it may write and its /tmp.
	Argv     []string `json:"argv"`
	CWD      string   `json:"cwd"`
	Env      []string `json:"env"`
	Writable []string `json:"writable"`
	Tmp      string   `json:"tmp"`
}

// containerMount is a node path mounted in the container.
type containerMount struct {
	Source   string `json:"source"`
	Path     string `json:"path"`
	ReadOnly bool   `json:"read_only,omitempty"`
}

// runsContainers reports whether this executor runs attempts in containers.
func (e *Local) runsContainers() bool { return e.Kind == config.KindIncus }

// containerInstance is the Incus instance name of an attempt (letters,
// digits and dashes).
func containerInstance(attemptID string) string {
	return "kairo-" + strings.ReplaceAll(attemptID, "_", "-")
}

// containerDir is where the executor keeps what an attempt's container is
// made from.
func (e *Local) containerDir(attemptID string) string {
	return filepath.Join(e.LogDir, "containers", attemptID)
}

// containerCommand builds the launcher of an attempt in a container (the
// runner, under the shim): it checks the attempt can run here, writes its
// spec and returns the runner's command.
func (e *Local) containerCommand(launch *store.Launch) (*exec.Cmd, error) {
	if !e.runsContainers() {
		return nil, fmt.Errorf("image %s: executor %s (kind %s) does not run containers", launch.Image, e.ID, e.Kind)
	}
	if launch.Image == "" {
		return nil, fmt.Errorf("executor %s runs attempts in containers: the execution names no image", e.ID)
	}
	if launch.Gang != nil {
		return nil, errors.New("a gang does not run in an image yet")
	}
	id := launch.Attempt.ID
	env := e.attemptEnv(launch)
	scratch, output, cache := confinedDirs(env)
	if scratch == "" || output == "" {
		return nil, fmt.Errorf("%w: a container needs a scratch and an output directory (no usable volatile store)", errConfineHere)
	}
	tmp := filepath.Join(scratch, "tmp")
	if err := os.Mkdir(tmp, 0o777); err != nil && !os.IsExist(err) {
		return nil, fmt.Errorf("make %s: %w", tmp, err)
	}
	if err := os.Chmod(tmp, 0o777|os.ModeSticky); err != nil {
		return nil, err
	}
	self, err := os.Executable()
	if err != nil {
		return nil, err
	}
	dir := e.containerDir(id)
	runDir := filepath.Join(dir, "run")
	if err := os.MkdirAll(runDir, 0o700); err != nil {
		return nil, err
	}
	logs := [2]string{filepath.Join(e.LogDir, id+".stdout.log"), filepath.Join(e.LogDir, id+".stderr.log")}
	for _, p := range logs {
		f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			return nil, err
		}
		f.Close()
	}
	spec := containerSpec{
		Instance:  containerInstance(id),
		AttemptID: id,
		Image:     launch.Image,
		RunDir:    runDir,
		Mounts: []containerMount{
			{Source: self, Path: containerRoot + "/bin/kairo", ReadOnly: true},
			{Source: runDir, Path: containerRoot + "/run"},
			{Source: logs[0], Path: containerStdout},
			{Source: logs[1], Path: containerStderr},
			{Source: scratch, Path: containerRoot + "/scratch"},
			{Source: output, Path: containerRoot + "/output"},
		},
		Writable: []string{containerRoot + "/run", containerStdout, containerStderr, containerRoot + "/scratch", containerRoot + "/output"},
		Tmp:      containerRoot + "/scratch/tmp",
		Argv:     launch.Argv,
	}
	paths := map[string]string{scratch: containerRoot + "/scratch", output: containerRoot + "/output"}
	if cache != "" {
		spec.Mounts = append(spec.Mounts, containerMount{Source: cache, Path: containerRoot + "/cache"})
		spec.Writable = append(spec.Writable, containerRoot+"/cache")
		paths[cache] = containerRoot + "/cache"
	}
	for _, in := range launch.Inputs {
		if in.Path == "" {
			continue
		}
		p := containerRoot + "/inputs/" + strings.ToLower(in.Name)
		spec.Mounts = append(spec.Mounts, containerMount{Source: in.Path, Path: p, ReadOnly: true})
		paths[in.Path] = p
	}
	spec.CWD = launch.CWD
	if launch.Source != nil {
		src, err := e.ensureSource(context.Background(), launch.Source)
		if err != nil {
			return nil, fmt.Errorf("code snapshot %s: %w", launch.Source.ID, err)
		}
		spec.Mounts = append(spec.Mounts, containerMount{Source: src, Path: containerRoot + "/src", ReadOnly: true})
		paths[src] = containerRoot + "/src"
		spec.CWD = path.Join(containerRoot+"/src", filepath.ToSlash(launch.CWD))
		env = append(env, "KAIRO_SOURCE_DIR="+src, "KAIRO_SOURCE_ID="+launch.Source.ID)
		if !hasEnv(env, "PYTHONDONTWRITEBYTECODE") && !hasEnv(env, "PYTHONPYCACHEPREFIX") {
			env = append(env, "PYTHONDONTWRITEBYTECODE=1")
		}
	} else if !path.IsAbs(spec.CWD) {
		return nil, fmt.Errorf("cwd %q is no absolute path in the image", spec.CWD)
	}
	// as a confined attempt on a host: temporary files go to the scratch
	for _, name := range []string{"TMPDIR", "TMP", "TEMP"} {
		env = setEnv(env, name, scratch)
	}
	spec.Env = translatePaths(env, paths)
	for _, r := range launch.Resources {
		if r.Kind == "gpu" {
			spec.GPUs = append(spec.GPUs, resourceBinding(r))
		}
	}
	if len(spec.GPUs) > 0 {
		spec.DriverRoot = filepath.Join(e.LogDir, "containers", "nvidia")
	}
	body, err := json.Marshal(spec)
	if err != nil {
		return nil, err
	}
	specPath := filepath.Join(runDir, "spec.json")
	// it holds the attempt's token: the agent's and the attempt's alone
	if err := os.WriteFile(specPath, body, 0o600); err != nil {
		return nil, err
	}
	cmd := exec.Command(self, containerRunArg, specPath)
	cmd.Dir = runDir
	// the runner needs the agent's environment (PATH, HOME for incus), not
	// the attempt's; it carries the attempt's id, so it is found as the
	// attempt's when its processes are looked for
	for _, kv := range os.Environ() {
		if name := envName(kv); !strings.HasPrefix(strings.ToUpper(name), "KAIRO_") {
			cmd.Env = append(cmd.Env, kv)
		}
	}
	cmd.Env = append(cmd.Env, "KAIRO_ATTEMPT_ID="+id)
	return cmd, nil
}

// translatePaths rewrites the values of env that are a node path in paths,
// or under one, to the path in the container (the longest match first).
func translatePaths(env []string, paths map[string]string) []string {
	nodes := make([]string, 0, len(paths))
	for p := range paths {
		nodes = append(nodes, p)
	}
	sort.Slice(nodes, func(i, j int) bool { return len(nodes[i]) > len(nodes[j]) })
	out := make([]string, len(env))
	for i, kv := range env {
		name, value, _ := strings.Cut(kv, "=")
		out[i] = kv
		for _, node := range nodes {
			if value == node {
				out[i] = name + "=" + paths[node]
				break
			}
			for _, sep := range []string{"/", string(filepath.Separator)} {
				if rest, ok := strings.CutPrefix(value, strings.TrimSuffix(node, sep)+sep); ok {
					out[i] = name + "=" + paths[node] + "/" + filepath.ToSlash(rest)
					break
				}
			}
			if out[i] != kv {
				break
			}
		}
	}
	return out
}

// containerEnv is the attempt's environment in its container: the image's
// (what the container's init started with), then the attempt's, a later
// value replacing an earlier one of the same name; with a GPU, the driver's
// libraries go first on LD_LIBRARY_PATH and its programs last on PATH.
func containerEnv(image, attempt []string, gpu bool) []string {
	var out []string
	index := map[string]int{}
	for _, list := range [][]string{image, attempt} {
		for _, kv := range list {
			name := envName(kv)
			if i, ok := index[name]; ok {
				out[i] = kv
				continue
			}
			index[name] = len(out)
			out = append(out, kv)
		}
	}
	if gpu {
		lib := containerNVIDIA + "/lib"
		if v := containerEnvValue(out, "LD_LIBRARY_PATH"); v != "" {
			lib += ":" + v
		}
		bin := containerEnvValue(out, "PATH")
		if bin == "" {
			bin = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
		}
		out = replaceEnv(out, "LD_LIBRARY_PATH", lib)
		out = replaceEnv(out, "PATH", bin+":"+containerNVIDIA+"/bin")
	}
	return out
}

// containerEnvValue is name's value in a container's env ("" when unset);
// names compare exactly, as on Linux.
func containerEnvValue(env []string, name string) string {
	for i := len(env) - 1; i >= 0; i-- {
		if n, v, _ := strings.Cut(env[i], "="); n == name {
			return v
		}
	}
	return ""
}

// replaceEnv sets name in a container's env, in place when it is there.
func replaceEnv(env []string, name, value string) []string {
	for i, kv := range env {
		if envName(kv) == name {
			env[i] = name + "=" + value
			return env
		}
	}
	return append(env, name+"="+value)
}
