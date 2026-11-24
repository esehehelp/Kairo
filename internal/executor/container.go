package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"kairo/internal/store"
)

// Containers (README: Containers). An executor of kind incus or pct runs
// every attempt in a container of the attempt's image (store.Launch.Image,
// pinned by digest), and nothing else: the planner gives it only executions
// in an image, and only it takes them (store.selectorMatches). Kind incus
// makes an Incus instance (container_linux.go); kind pct, run by an agent on
// a Proxmox VE host, makes a Proxmox container in VMIDs 1000-1999
// (container_pct_linux.go).
//
// The attempt's launcher is still its shim (shim.go), which runs this
// binary as the backend's runner: it makes the container kairo-<attempt id>
// from the image, starts it, waits for it to stop, reads the exit the
// container wrote, destroys the container and exits with that exit; so the
// shim, adoption and quiescence are as for a host attempt. In the container this binary runs as containerInitArg under
// Incus's own init: it confines the container as a confined host attempt is
// confined (confineMounts: all read-only but what Kairo hands it), starts
// the attempt's argv with its environment and directory, its output going
// to the attempt's logs, reaps orphans, and writes the argv's exit to
// /kairo/run/exit before it exits, which stops the container. The argv's
// processes carry the attempt's KAIRO_ATTEMPT_ID and neither init does: a
// force stop reaches the argv as it reaches a host attempt's processes,
// while the inits stay to see it end.
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
	pctRunArg        = "__kairo-pct-run"
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
	// PCT is how a Proxmox container is made (kind pct only).
	PCT *pctSettings `json:"pct,omitempty"`

	// In the container: the argv, its directory and environment (declared,
	// then Kairo's), the paths it may write and its /tmp.
	Argv     []string `json:"argv"`
	CWD      string   `json:"cwd"`
	Env      []string `json:"env"`
	Writable []string `json:"writable"`
	Tmp      string   `json:"tmp"`
	// Streams the argv reads: a loader each, started before it in the
	// container (startLoaders).
	Streams []store.LaunchStream `json:"streams,omitempty"`
}

// containerMount is a node path mounted in the container.
type containerMount struct {
	Source   string `json:"source"`
	Path     string `json:"path"`
	ReadOnly bool   `json:"read_only,omitempty"`
}

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

// containerOptions say how a backend's containers take what an attempt is
// handed: runArg is the runner's argument; dirsOnly mounts directories only
// (Proxmox bind-mounts no single file): the binary as a directory
// (/kairo/bin) holding a hard link to it (a copy across filesystems), the
// logs as one (/kairo/log) of hard links to them; pct is what a Proxmox
// container is made with.
type containerOptions struct {
	runArg   string
	dirsOnly bool
	pct      *pctSettings
}

// containerCommand builds the launcher of an attempt in a container (the
// runner, under the shim): it checks the attempt can run here, writes its
// spec and returns the runner's command.
func (e *Local) containerCommand(launch *store.Launch, opts containerOptions) (*exec.Cmd, error) {
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
	binMount := containerMount{Source: self, Path: containerRoot + "/bin/kairo", ReadOnly: true}
	logMounts := []containerMount{{Source: logs[0], Path: containerStdout}, {Source: logs[1], Path: containerStderr}}
	writableLogs := []string{containerStdout, containerStderr}
	if opts.dirsOnly {
		binDir := filepath.Join(dir, "bin")
		if err := os.MkdirAll(binDir, 0o755); err != nil {
			return nil, err
		}
		if _, err := os.Stat(filepath.Join(binDir, "kairo")); err != nil {
			if err := linkOrCopy(self, filepath.Join(binDir, "kairo")); err != nil {
				return nil, fmt.Errorf("hand the binary to the container: %w", err)
			}
		}
		binMount = containerMount{Source: binDir, Path: containerRoot + "/bin", ReadOnly: true}
		logDir, err := linkLogs(filepath.Join(dir, "log"), logs)
		if err != nil {
			return nil, err
		}
		logMounts = []containerMount{{Source: logDir, Path: path.Dir(containerStdout)}}
		writableLogs = []string{path.Dir(containerStdout)}
	}
	spec := containerSpec{
		Instance:  containerInstance(id),
		AttemptID: id,
		Image:     launch.Image,
		RunDir:    runDir,
		PCT:       opts.pct,
		Mounts: append(append([]containerMount{
			binMount,
			{Source: runDir, Path: containerRoot + "/run"},
		}, logMounts...),
			containerMount{Source: scratch, Path: containerRoot + "/scratch"},
			containerMount{Source: output, Path: containerRoot + "/output"},
		),
		Writable: append(append([]string{containerRoot + "/run"}, writableLogs...), containerRoot+"/scratch", containerRoot+"/output"),
		Tmp:      containerRoot + "/scratch/tmp",
		Argv:     launch.Argv,
		Streams:  launch.Streams,
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
		src, err := e.storeWork().ensureSource(context.Background(), launch.Source)
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
	cmd := exec.Command(self, opts.runArg, specPath)
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

// pctSettings are what a Proxmox container is made with, from the
// executor's attributes (pctSettingsOf).
type pctSettings struct {
	// Storage holds the containers' root disks (lvmthin: its thin pool);
	// TemplateStorage the images, as OCI archives.
	Storage         string `json:"storage"`
	TemplateStorage string `json:"template_storage"`
	// Net is the container's net0 (empty: none).
	Net string `json:"net,omitempty"`
	// RootfsGiB is the root disk's size.
	RootfsGiB int `json:"rootfs_gib"`
}

// pctSettingsOf reads an executor's pct settings from its attributes
// (pct_storage, pct_template_storage, pct_net, pct_rootfs_gib), with
// Proxmox's defaults.
func pctSettingsOf(attrs map[string]string) (*pctSettings, error) {
	s := &pctSettings{Storage: "local-lvm", TemplateStorage: "local", Net: "name=eth0,bridge=vmbr0,ip=dhcp", RootfsGiB: 8}
	if v := attrs["pct_storage"]; v != "" {
		s.Storage = v
	}
	if v := attrs["pct_template_storage"]; v != "" {
		s.TemplateStorage = v
	}
	if v, ok := attrs["pct_net"]; ok {
		s.Net = v
	}
	if v := attrs["pct_rootfs_gib"]; v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return nil, fmt.Errorf("pct_rootfs_gib %q is no positive number of GiB", v)
		}
		s.RootfsGiB = n
	}
	return s, nil
}

// linkLogs makes dir with hard links stdout and stderr to an attempt's logs,
// so the container appends to the logs the executor ships.
func linkLogs(dir string, logs [2]string) (string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	for i, name := range []string{"stdout", "stderr"} {
		link := filepath.Join(dir, name)
		if fi, err := os.Stat(link); err == nil {
			if li, lerr := os.Stat(logs[i]); lerr == nil && os.SameFile(fi, li) {
				continue
			}
			_ = os.Remove(link)
		}
		if err := os.Link(logs[i], link); err != nil {
			return "", fmt.Errorf("link the attempt's log: %w", err)
		}
	}
	return dir, nil
}

// linkOrCopy hard-links src to dst, or copies it when they are on
// different filesystems.
func linkOrCopy(src, dst string) error {
	if err := os.Link(src, dst); err == nil {
		return nil
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	fi, err := in.Stat()
	if err != nil {
		return err
	}
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, fi.Mode().Perm())
	if err != nil {
		return err
	}
	if _, err = io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

// translatePaths rewrites the values of env that are a node path in paths,
// or under one, to the path in the container (the longest match first;
// node paths are a Linux node's).
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
			if rest, ok := strings.CutPrefix(value, strings.TrimSuffix(node, "/")+"/"); ok {
				out[i] = name + "=" + paths[node] + "/" + rest
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

// findOCIRemote is the first (by name) OCI remote at addr in what `incus
// remote list --format json` printed ("" when there is none): its address
// is Addrs in Incus 7, Addr before.
func findOCIRemote(list []byte, addr string) (string, error) {
	var remotes map[string]struct {
		Addr     string   `json:"Addr"`
		Addrs    []string `json:"Addrs"`
		Protocol string   `json:"Protocol"`
	}
	if err := json.Unmarshal(list, &remotes); err != nil {
		return "", fmt.Errorf("incus remote list: %w", err)
	}
	names := make([]string, 0, len(remotes))
	for name := range remotes {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		r := remotes[name]
		if r.Protocol != "oci" {
			continue
		}
		for _, a := range append(r.Addrs, r.Addr) {
			if a != "" && strings.TrimSuffix(a, "/") == addr {
				return name, nil
			}
		}
	}
	return "", nil
}
