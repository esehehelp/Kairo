package executor

import (
	"context"
	"encoding/json"
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

// Containers (README: Containers). Every executor runs every attempt in a
// container of the attempt's image (store.Launch.Image, pinned by digest):
// kind incus makes an Incus instance (container_linux.go), kind wslc a WSL
// container (container_wslc.go).
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
//	/kairo/stream-src       the snapshot its streams' stages run in, when not
//	                        /kairo/src's (read-only; their KAIRO_SOURCE_DIR)
//	/kairo/inputs/<name>    KAIRO_INPUT_<NAME> (read-only)
//	/kairo/nvidia           the node's NVIDIA driver files, with a GPU (read-only)
//	/kairo/bench-disk       KAIRO_BENCH_DISK: of a storage bench, a directory
//	                        (.kairo-bench) of the filesystem it measures
//
// The variables naming them are rewritten to those paths; the rest of the
// environment is the image's, then the declared env, then Kairo's, as on a
// host. The agent's own environment does not reach the container.

const (
	containerRunArg  = "__kairo-incus-run"
	containerInitArg = "__kairo-container-init"
	containerRoot    = "/kairo"
	// containerRunDir is the init's: its spec and the exit it writes, the
	// attempt's streams below (README: the container's layout)
	containerRunDir = "/kairo/run"
	containerSpecIn = "/kairo/run/spec.json"
	containerExitIn = "/kairo/run/exit"
	containerStdout = "/kairo/log/stdout"
	containerStderr = "/kairo/log/stderr"
	containerNVIDIA = "/kairo/nvidia"
	// containerStreamSource is where the snapshot of a stream's stages is
	// when it is not the program's (containerSpec.StreamCWD).
	containerStreamSource = containerRoot + "/stream-src"
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
	// Streams the argv reads: a loader each, started before it in the
	// container (startLoaders), reading raw files through the store's raw
	// cache mounted at RawDir (rawcache.go); RawRequests, when set, is where
	// a loader asks the node's agent for them as files (a container in a
	// VM cannot reach the agent's socket: wslc).
	Streams     []store.LaunchStream `json:"streams,omitempty"`
	RawDir      string               `json:"raw_dir,omitempty"`
	RawRequests string               `json:"raw_requests,omitempty"`
	// StreamCWD is the directory the streams' stages run from when not CWD:
	// in the snapshot StreamSource (/kairo/stream-src), where the stages of
	// the checkpoint the attempt goes on from ran (store.Launch.StreamSource).
	StreamCWD    string `json:"stream_cwd,omitempty"`
	StreamSource string `json:"stream_source,omitempty"`
	// Runtime is the container's runtime when it matters to the init:
	// "wslc", whose containers cannot mount (the init does not confine them:
	// only the ordinary user the argv runs as keeps it off the image's files)
	// and whose processes the agent cannot reach (the init passes a stop on
	// to the argv).
	Runtime string `json:"runtime,omitempty"`
}

// streamSourceOf is the snapshot launch's streams' stages run in when it is
// not the one its program runs in (nil when they run in that one): a
// continuation run in newer code than its checkpoint was made in runs its
// stages where the checkpoint's ran (store.Launch.StreamSource).
func streamSourceOf(launch *store.Launch) *store.LaunchSource {
	s := launch.StreamSource
	if s == nil || len(launch.Streams) == 0 || (launch.Source != nil && s.ID == launch.Source.ID) {
		return nil
	}
	return s
}

// stagePlace is the directory and environment the streams' loaders, and
// their stages, run with: the argv's, or (StreamCWD) the snapshot's where
// the checkpoint's stages ran, as their KAIRO_SOURCE_DIR and
// KAIRO_SOURCE_ID.
func stagePlace(spec containerSpec, env []string) (string, []string) {
	if spec.StreamCWD == "" {
		return spec.CWD, env
	}
	return spec.StreamCWD, setEnv(setEnv(env, "KAIRO_SOURCE_DIR", containerStreamSource), "KAIRO_SOURCE_ID", spec.StreamSource)
}

// containerMount is a node path mounted in the container.
type containerMount struct {
	Source   string `json:"source"`
	Path     string `json:"path"`
	ReadOnly bool   `json:"read_only,omitempty"`
}

// containerUID and containerGID are the ordinary user every attempt runs as
// in a container, on every runtime (issue #46): the container's init starts
// as root, gives the user a name in the image (attemptAccount), hands it what
// Kairo lets the attempt write and starts the argv (and its streams'
// loaders) as it. containerHome is its home, in the attempt's scratch.
const (
	containerUID  = 1000
	containerGID  = 1000
	containerHome = containerRoot + "/scratch/home"
)

// containerUserName is the attempt user's name when the image has none for
// its uid.
const containerUserName = "kairo"

// attemptAccount is the attempt user's name in an image whose /etc/passwd
// and /etc/group are passwd and group: the image's own for its uid, else
// containerUserName (kairo-attempt if the image uses that name for another
// id), with the lines to add to each file ("" when it has one already).
func attemptAccount(passwd, group string) (name, passwdLine, groupLine string) {
	names := map[string]bool{}
	for line := range strings.SplitSeq(passwd, "\n") {
		f := strings.Split(line, ":")
		if len(f) < 3 {
			continue
		}
		if f[2] == strconv.Itoa(containerUID) && name == "" {
			name = f[0]
		}
		names[f[0]] = true
	}
	groupNames := map[string]bool{}
	hasGroup := false
	for line := range strings.SplitSeq(group, "\n") {
		f := strings.Split(line, ":")
		if len(f) < 3 {
			continue
		}
		if f[2] == strconv.Itoa(containerGID) {
			hasGroup = true
		}
		groupNames[f[0]] = true
	}
	if name == "" {
		name = containerUserName
		if names[name] {
			name = containerUserName + "-attempt"
		}
		passwdLine = fmt.Sprintf("%s:x:%d:%d:Kairo attempt:%s:/bin/sh", name, containerUID, containerGID, containerHome)
	}
	if !hasGroup {
		g := name
		if groupNames[g] {
			g = containerUserName + "-attempt"
		}
		groupLine = fmt.Sprintf("%s:x:%d:", g, containerGID)
	}
	return name, passwdLine, groupLine
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

// benchDiskDir is the directory a storage bench writes in on the
// filesystem it measures.
const benchDiskDir = ".kairo-bench"

// containerOptions say how a backend's containers take what an attempt is
// handed: runArg is the runner's argument; dirsOnly mounts directories only
// (a runtime that mounts no single file): the binary as a directory
// (/kairo/bin) holding a hard link to it (a copy across filesystems), the
// logs as one (/kairo/log) of hard links to them.
type containerOptions struct {
	runArg   string
	dirsOnly bool
	// runtime names the container's runtime (spec.Runtime); vm: it runs in
	// a VM (ociCaps.vm): its init is the Linux build beside this binary and
	// it asks for raw files as files.
	runtime string
	vm      bool
}

// containerCommand builds the launcher of an attempt in a container (the
// runner, under the shim): it checks the attempt can run here, writes its
// spec and returns the runner's command.
func (e *Local) containerCommand(launch *store.Launch, opts containerOptions) (*exec.Cmd, error) {
	if launch.Image == "" {
		return nil, fmt.Errorf("executor %s runs attempts in containers: the execution names no image", e.ID)
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
	// what runs in the container: this binary, or (wslc, on Windows) its
	// Linux build beside it
	initBinary := self
	if opts.vm {
		initBinary = filepath.Join(filepath.Dir(self), wslcLinuxBinary)
		if _, err := os.Stat(initBinary); err != nil {
			return nil, fmt.Errorf("a wslc container's init is the Linux build of kairo beside the agent: %w", err)
		}
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
	binMount := containerMount{Source: initBinary, Path: containerRoot + "/bin/kairo", ReadOnly: true}
	logMounts := []containerMount{{Source: logs[0], Path: containerStdout}, {Source: logs[1], Path: containerStderr}}
	writableLogs := []string{containerStdout, containerStderr}
	if opts.dirsOnly {
		binDir := filepath.Join(dir, "bin")
		if err := os.MkdirAll(binDir, 0o755); err != nil {
			return nil, err
		}
		if _, err := os.Stat(filepath.Join(binDir, "kairo")); err != nil {
			if err := linkOrCopy(initBinary, filepath.Join(binDir, "kairo")); err != nil {
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
		Mounts: append(append([]containerMount{
			binMount,
			{Source: runDir, Path: containerRunDir},
		}, logMounts...),
			containerMount{Source: scratch, Path: containerRoot + "/scratch"},
			containerMount{Source: output, Path: containerRoot + "/output"},
		),
		Writable: append(append([]string{containerRunDir}, writableLogs...), containerRoot+"/scratch", containerRoot+"/output"),
		Tmp:      containerRoot + "/scratch/tmp",
		Argv:     launch.Argv,
		Streams:  launch.Streams,
	}
	spec.Runtime = opts.runtime
	// the ordinary user it runs as (containerUID) has no home in the image:
	// one in the scratch, unless the execution declares its own
	if _, declared := launch.Env["HOME"]; !declared {
		home := filepath.Join(scratch, "home")
		if err := os.MkdirAll(home, 0o777); err != nil {
			return nil, err
		}
		env = setEnv(env, "HOME", home)
	}
	paths := map[string]string{scratch: containerRoot + "/scratch", output: containerRoot + "/output"}
	if cache != "" {
		spec.Mounts = append(spec.Mounts, containerMount{Source: cache, Path: containerRoot + "/cache"})
		spec.Writable = append(spec.Writable, containerRoot+"/cache")
		paths[cache] = containerRoot + "/cache"
	}
	if launch.BenchDisk != "" {
		// a storage bench writes its test files on the filesystem it
		// measures, not on the container's own disk
		dir := filepath.Join(launch.BenchDisk+string(filepath.Separator), benchDiskDir)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("the storage bench's directory on %s: %w", launch.BenchDisk, err)
		}
		spec.Mounts = append(spec.Mounts, containerMount{Source: dir, Path: containerRoot + "/bench-disk"})
		spec.Writable = append(spec.Writable, containerRoot+"/bench-disk")
		env = setEnv(env, "KAIRO_BENCH_DISK", containerRoot+"/bench-disk")
	}
	if len(launch.Streams) > 0 {
		s, ok := e.storeWork().storeFor(launch)
		if !ok {
			return nil, fmt.Errorf("%w: streams are read through a volatile store's raw cache (none here)", errConfineHere)
		}
		raw, err := e.storeWork().rawCacheFor(s)
		if err != nil {
			return nil, fmt.Errorf("the raw cache of store %s: %w", s.decl.ID, err)
		}
		view, requests, err := raw.attach(launch, opts.vm)
		if err != nil {
			return nil, fmt.Errorf("the raw cache of store %s: %w", s.decl.ID, err)
		}
		spec.Mounts = append(spec.Mounts, containerMount{Source: view, Path: containerRoot + "/raw", ReadOnly: true})
		spec.RawDir = containerRoot + "/raw"
		if requests != "" {
			spec.Mounts = append(spec.Mounts, containerMount{Source: requests, Path: containerRoot + "/raw-requests"})
			spec.Writable = append(spec.Writable, containerRoot+"/raw-requests")
			spec.RawRequests = containerRoot + "/raw-requests"
		}
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
	if s := streamSourceOf(launch); s != nil {
		src, err := e.storeWork().ensureSource(context.Background(), s)
		if err != nil {
			return nil, fmt.Errorf("code snapshot %s (of its streams' stages): %w", s.ID, err)
		}
		spec.Mounts = append(spec.Mounts, containerMount{Source: src, Path: containerStreamSource, ReadOnly: true})
		spec.StreamCWD = path.Join(containerStreamSource, filepath.ToSlash(launch.CWD))
		spec.StreamSource = s.ID
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
	if len(spec.GPUs) > 0 && !opts.vm {
		// wslc hands its containers the WSL GPU's driver itself
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
			// a node path of an agent on Windows (a drive letter) is
			// compared with either separator; the container's is a Linux path
			v, n := value, node
			if len(node) >= 2 && node[1] == ':' {
				v, n = strings.ReplaceAll(value, `\`, "/"), strings.ReplaceAll(node, `\`, "/")
			}
			if v == n {
				out[i] = name + "=" + paths[node]
				break
			}
			if rest, ok := strings.CutPrefix(v, strings.TrimSuffix(n, "/")+"/"); ok {
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

func readContainerSpec(p string) (containerSpec, error) {
	var spec containerSpec
	body, err := os.ReadFile(p)
	if err != nil {
		return spec, err
	}
	if err := json.Unmarshal(body, &spec); err != nil {
		return spec, fmt.Errorf("%s: %w", p, err)
	}
	if spec.Instance == "" || len(spec.Argv) == 0 {
		return spec, fmt.Errorf("%s: no instance or argv", p)
	}
	return spec, nil
}
