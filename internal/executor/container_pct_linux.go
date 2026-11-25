//go:build linux

package executor

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"kairo/internal/imageref"
)

// Proxmox containers (kind pct; README: Containers). The agent runs on the
// Proxmox VE host. Each attempt's runner (pctRun, under the shim):
//
//   - brings the attempt's image to the template storage, fetched by its
//     pinned digest with skopeo as an OCI archive kairo-<digest>.tar (a
//     Proxmox pull takes only name:tag);
//   - makes the container from it in the first free VMID of 1000-1999, the
//     only VMIDs Kairo uses (pctFirstVMID-pctLastVMID), tagged kairo, its
//     hostname kairo-<attempt id> and its description saying whose it is:
//     unprivileged, its root disk on the executor's storage, the node's
//     memory and cores (admission accounts, the container does not limit),
//     what the attempt is handed bind-mounted under /kairo (owned by the
//     container's root, the first id of root's subordinate range), its GPUs
//     passed as devices with the driver's files at /kairo/nvidia;
//   - sets its init to Kairo's (containerInit), which confines it, runs the
//     argv and writes its exit to /kairo/run/exit; starts it, waits for it to
//     stop, reads that exit and destroys the container.
//
// A runner that is killed leaves its container: the sweep destroys a
// stopped container in the range, tagged kairo, whose attempt no longer runs
// here. Anything else (another VMID, no tag) is never Kairo's and is never
// touched.

const (
	pctFirstVMID = 1000
	pctLastVMID  = 1999
	pctTag       = "kairo"
)

var (
	// pctLock serializes the choice of a free VMID and the creation that
	// takes it, between the runners of one host.
	pctLock = "/run/lock/kairo-pct.lock"
	// pctConfigDir holds the host's container configurations.
	pctConfigDir = "/etc/pve/lxc"
	// resolvConf is the host's resolver configuration, whose nameservers a
	// container is given.
	resolvConf = "/etc/resolv.conf"
	// pctDestroyRetry spaces out the tries to destroy a container whose disk
	// is still in use.
	pctDestroyRetry = 3 * time.Second
)

// pctTool runs a Proxmox or image tool (pct, pvesh, pvesm, skopeo) and
// returns what it printed; a failure says what it printed on stderr.
var pctTool = func(ctx context.Context, name string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	var out, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &stderr
	if err := cmd.Run(); err != nil {
		return out.Bytes(), fmt.Errorf("%s %s: %w: %s", name, strings.Join(args[:min(2, len(args))], " "), err, strings.TrimSpace(stderr.String()))
	}
	return out.Bytes(), nil
}

// pctRuntime runs attempts' containers with Proxmox's pct (oci.go): a
// container is named by its VMID. What stops the attempt reaches its
// processes in the container directly.
type pctRuntime struct{}

func (pctRuntime) run(ctx context.Context, spec containerSpec, logf func(string, ...any), made func(string)) (string, int, error) {
	if spec.PCT == nil {
		return "", shimNotStarted, errors.New("a pct container's spec has no pct settings")
	}
	vmid, exit, err := runPCT(ctx, spec, logf, func(vmid int) { made(strconv.Itoa(vmid)) })
	if vmid == 0 {
		return "", exit, err
	}
	return strconv.Itoa(vmid), exit, err
}

func (pctRuntime) stop(ctx context.Context, vmid string) error {
	_, err := pctTool(ctx, "pct", "shutdown", vmid, "--timeout", strconv.Itoa(wslcStopSeconds), "--forceStop", "1")
	return err
}

func (pctRuntime) kill(ctx context.Context, vmid string) error {
	_, err := pctTool(ctx, "pct", "stop", vmid)
	return err
}

// stats reads the container's status: memory in use and bytes written.
func (pctRuntime) stats(ctx context.Context, vmid string) (*int64, *int64, error) {
	node, err := os.Hostname()
	if err != nil {
		return nil, nil, err
	}
	out, err := pctTool(ctx, "pvesh", "get", "/nodes/"+node+"/lxc/"+vmid+"/status/current", "--output-format", "json")
	if err != nil {
		return nil, nil, err
	}
	var st struct {
		Mem       int64 `json:"mem"`
		DiskWrite int64 `json:"diskwrite"`
	}
	if err := json.Unmarshal(out, &st); err != nil {
		return nil, nil, err
	}
	return &st.Mem, &st.DiskWrite, nil
}

func (pctRuntime) remove(ctx context.Context, vmid string) error {
	n, err := strconv.Atoi(vmid)
	if err != nil {
		return err
	}
	return destroyPCT(ctx, n)
}

// containers are the containers of Kairo's range tagged kairo.
func (pctRuntime) containers(ctx context.Context) ([]ociContainer, error) {
	node, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	out, err := pctTool(ctx, "pvesh", "get", "/nodes/"+node+"/lxc", "--output-format", "json")
	if err != nil {
		return nil, err
	}
	var cts []struct {
		VMID   json.Number `json:"vmid"`
		Name   string      `json:"name"`
		Status string      `json:"status"`
		Tags   string      `json:"tags"`
	}
	if err := json.Unmarshal(out, &cts); err != nil {
		return nil, err
	}
	var list []ociContainer
	for _, ct := range cts {
		vmid, err := strconv.Atoi(ct.VMID.String())
		if err != nil || vmid < pctFirstVMID || vmid > pctLastVMID {
			continue
		}
		if !slices.Contains(strings.FieldsFunc(ct.Tags, func(r rune) bool { return r == ';' || r == ',' || r == ' ' }), pctTag) {
			continue
		}
		if id, ok := attemptOfInstance(ct.Name); ok {
			list = append(list, ociContainer{attempt: id, running: ct.Status != "stopped", handle: strconv.Itoa(vmid)})
		}
	}
	return list, nil
}

// runPCT makes, starts and waits for the attempt's container; vmid is the
// container made (0: none), also told to made as soon as it exists.
func runPCT(ctx context.Context, spec containerSpec, logf func(string, ...any), made func(int)) (vmid, exit int, err error) {
	ref, err := imageref.ParsePinned(spec.Image)
	if err != nil {
		return 0, shimNotStarted, err
	}
	template, err := pctTemplate(ctx, spec.PCT.TemplateStorage, ref)
	if err != nil {
		return 0, shimNotStarted, err
	}
	uid, err := containerRootID()
	if err != nil {
		return 0, shimNotStarted, err
	}
	args, err := pctCreateArgs(spec, template)
	if err != nil {
		return 0, shimNotStarted, err
	}
	// what the container writes is its root's: the directories it writes to
	for _, m := range spec.Mounts {
		if !m.ReadOnly {
			if err := chownTree(m.Source, uid); err != nil {
				return 0, shimNotStarted, fmt.Errorf("hand %s to the container: %w", m.Source, err)
			}
		}
	}
	logf("container of %s", spec.Image)
	vmid, err = createPCT(ctx, args)
	if err != nil {
		return 0, shimNotStarted, err
	}
	made(vmid)
	logf("container %d (%s)", vmid, spec.Instance)
	// /dev/shm as large as a data loader's batches need, and Kairo's init;
	// written after the creation, which takes the image's entrypoint
	if err := appendPCTConfig(vmid, "lxc.mount.entry: tmpfs dev/shm tmpfs rw,nosuid,nodev,size="+containerShm+",create=dir 0 0"); err != nil {
		return vmid, shimNotStarted, err
	}
	if _, err := pctTool(ctx, "pct", "set", strconv.Itoa(vmid), "--entrypoint", containerRoot+"/bin/kairo "+containerInitArg+" "+containerSpecIn); err != nil {
		return vmid, shimNotStarted, err
	}
	if _, err := pctTool(ctx, "pct", "start", strconv.Itoa(vmid)); err != nil {
		// an attempt that ends at once stops the container before pct start
		// has seen it run: it ran when its init wrote its exit
		if _, ran := readExitFile(filepath.Join(spec.RunDir, "exit")); !ran {
			return vmid, shimNotStarted, err
		}
	}
	warned := false
	for {
		status, err := pctStatus(ctx, vmid)
		if err != nil && !warned {
			logf("status of %d not read: %v", vmid, err)
			warned = true
		}
		if err == nil && status != "running" {
			break
		}
		time.Sleep(time.Second)
	}
	code, ok := readExitFile(filepath.Join(spec.RunDir, "exit"))
	if !ok {
		logf("the container stopped without writing its exit")
		return vmid, -1, nil
	}
	return vmid, code, nil
}

// pctTemplate is the volume of the image's OCI archive on the template
// storage, fetched by digest when it is not there yet.
func pctTemplate(ctx context.Context, storage string, ref imageref.Ref) (string, error) {
	hex, ok := strings.CutPrefix(ref.Digest, "sha256:")
	if !ok || len(hex) != 64 {
		return "", fmt.Errorf("image digest %q is no sha256", ref.Digest)
	}
	volume := storage + ":vztmpl/kairo-" + hex + ".tar"
	out, err := pctTool(ctx, "pvesm", "path", volume)
	if err != nil {
		return "", err
	}
	p := strings.TrimSpace(string(out))
	if _, err := os.Stat(p); err == nil {
		return volume, nil
	}
	partial := fmt.Sprintf("%s.partial-%d", p, os.Getpid())
	_ = os.Remove(partial)
	src := "docker://" + ref.Registry + "/" + ref.Repository + "@" + ref.Digest
	if _, err := pctTool(ctx, "skopeo", "copy", "--quiet", src, "oci-archive:"+partial); err != nil {
		_ = os.Remove(partial)
		return "", fmt.Errorf("fetch %s: %w", src, err)
	}
	if err := os.Rename(partial, p); err != nil {
		_ = os.Remove(partial)
		return "", err
	}
	return volume, nil
}

// pctCreateArgs are pct create's arguments after the VMID.
func pctCreateArgs(spec containerSpec, template string) ([]string, error) {
	s := spec.PCT
	memMiB, err := hostMemoryMiB()
	if err != nil {
		return nil, err
	}
	args := []string{template,
		"--hostname", spec.Instance,
		"--tags", pctTag,
		"--description", "Kairo attempt " + spec.AttemptID + " (image " + spec.Image + "): made for it and destroyed after it.",
		"--rootfs", s.Storage + ":" + strconv.Itoa(s.RootfsGiB),
		"--unprivileged", "1",
		"--memory", strconv.Itoa(memMiB),
		"--swap", "0",
		"--cores", strconv.Itoa(runtime.NumCPU()),
		"--onboot", "0",
	}
	if s.Net != "" {
		args = append(args, "--net0", s.Net)
		// an application container is not handed the host's resolvers
		if ns := hostNameservers(resolvConf); len(ns) > 0 {
			args = append(args, "--nameserver", strings.Join(ns, " "))
		}
	}
	mounts := slices.Clone(spec.Mounts)
	var devices []string
	if len(spec.GPUs) > 0 {
		gpus, driver, err := gpuDevices(spec.GPUs, spec.DriverRoot)
		if err != nil {
			return nil, err
		}
		names := make([]string, 0, len(gpus))
		for name := range gpus {
			names = append(names, name)
		}
		slices.Sort(names)
		for _, name := range names {
			devices = append(devices, gpus[name]["source"])
		}
		mounts = append(mounts, containerMount{Source: driver, Path: containerNVIDIA, ReadOnly: true})
	}
	for i, m := range mounts {
		if strings.ContainsAny(m.Source+m.Path, ",=") {
			return nil, fmt.Errorf("%s cannot be mounted in a Proxmox container (a comma or '=' in the path)", m.Source)
		}
		v := m.Source + ",mp=" + m.Path
		if m.ReadOnly {
			v += ",ro=1"
		}
		args = append(args, fmt.Sprintf("--mp%d", i), v)
	}
	for i, d := range devices {
		args = append(args, fmt.Sprintf("--dev%d", i), "path="+d+",mode=0666")
	}
	return args, nil
}

// createPCT makes the container in the first free VMID of Kairo's range.
func createPCT(ctx context.Context, args []string) (int, error) {
	lock, err := os.OpenFile(pctLock, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return 0, err
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		return 0, err
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	used, err := usedVMIDs(ctx)
	if err != nil {
		return 0, err
	}
	for vmid := pctFirstVMID; vmid <= pctLastVMID; vmid++ {
		if used[vmid] {
			continue
		}
		if _, err := pctTool(ctx, "pct", append([]string{"create", strconv.Itoa(vmid)}, args...)...); err != nil {
			if strings.Contains(err.Error(), "already exists") {
				continue // taken meanwhile outside Kairo
			}
			// a half-made container is destroyed by the caller's cleanup
			if exists, _ := pctExists(ctx, vmid); exists {
				return vmid, err
			}
			return 0, err
		}
		return vmid, nil
	}
	return 0, fmt.Errorf("no free VMID in %d-%d", pctFirstVMID, pctLastVMID)
}

// usedVMIDs are the VMIDs of the cluster's guests (or the host's).
func usedVMIDs(ctx context.Context) (map[int]bool, error) {
	out, err := pctTool(ctx, "pvesh", "get", "/cluster/resources", "--type", "vm", "--output-format", "json")
	if err != nil {
		return nil, err
	}
	var guests []struct {
		VMID int `json:"vmid"`
	}
	if err := json.Unmarshal(out, &guests); err != nil {
		return nil, fmt.Errorf("guests: %w", err)
	}
	used := map[int]bool{}
	for _, g := range guests {
		used[g.VMID] = true
	}
	return used, nil
}

// pctStatus is a container's status (running, stopped).
func pctStatus(ctx context.Context, vmid int) (string, error) {
	out, err := pctTool(ctx, "pct", "status", strconv.Itoa(vmid))
	if err != nil {
		return "", err
	}
	_, status, ok := strings.Cut(strings.TrimSpace(string(out)), "status:")
	if !ok {
		return "", fmt.Errorf("pct status %d: %q", vmid, strings.TrimSpace(string(out)))
	}
	return strings.TrimSpace(status), nil
}

func pctExists(ctx context.Context, vmid int) (bool, error) {
	_, err := os.Stat(pctConfigPath(vmid))
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	return err == nil, err
}

func pctConfigPath(vmid int) string { return filepath.Join(pctConfigDir, strconv.Itoa(vmid)+".conf") }

// appendPCTConfig appends a raw LXC line to a container's configuration.
func appendPCTConfig(vmid int, line string) error {
	f, err := os.OpenFile(pctConfigPath(vmid), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		return err
	}
	if _, err = f.WriteString(line + "\n"); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// destroyPCT stops and destroys a container of Kairo's range, with its
// disk; one already gone is no error.
func destroyPCT(ctx context.Context, vmid int) error {
	if vmid < pctFirstVMID || vmid > pctLastVMID {
		return fmt.Errorf("VMID %d is not Kairo's", vmid)
	}
	if exists, err := pctExists(ctx, vmid); err != nil || !exists {
		return err
	}
	// a container that has just stopped may still hold its disk a moment
	var err error
	for try := 0; try < 10; try++ {
		if try > 0 {
			select {
			case <-ctx.Done():
				return errors.Join(err, ctx.Err())
			case <-time.After(pctDestroyRetry):
			}
		}
		_, err = pctTool(ctx, "pct", "destroy", strconv.Itoa(vmid), "--force", "1", "--purge", "1")
		if err == nil || strings.Contains(err.Error(), "does not exist") {
			return nil
		}
		if !strings.Contains(err.Error(), "in use") {
			return err
		}
	}
	return err
}

// containerRootID is the host id of an unprivileged container's root: the
// first of root's subordinate ids (/etc/subuid), 100000 by default.
func containerRootID() (int, error) {
	f, err := os.Open("/etc/subuid")
	if errors.Is(err, fs.ErrNotExist) {
		return 100000, nil
	}
	if err != nil {
		return 0, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Split(strings.TrimSpace(sc.Text()), ":")
		if len(fields) == 3 && fields[0] == "root" {
			return strconv.Atoi(fields[1])
		}
	}
	return 100000, nil
}

// chownTree gives root (the directory and all under it, links not followed)
// to uid, as the container's root; what is not there is no error.
func chownTree(root string, uid int) error {
	return filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		return os.Lchown(p, uid, uid)
	})
}

// hostMemoryMiB is the host's memory (MemTotal): a container gets it all,
// as a host attempt does; admission accounts for it.
func hostMemoryMiB() (int, error) {
	body, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(body), "\n") {
		if rest, ok := strings.CutPrefix(line, "MemTotal:"); ok {
			kib, err := strconv.Atoi(strings.TrimSuffix(strings.TrimSpace(rest), " kB"))
			if err != nil {
				return 0, fmt.Errorf("MemTotal %q: %w", rest, err)
			}
			return kib / 1024, nil
		}
	}
	return 0, errors.New("no MemTotal in /proc/meminfo")
}

// hostNameservers are the nameservers of the host's resolver configuration
// a container can reach: not the host's own loopback ones.
func hostNameservers(path string) []string {
	body, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var out []string
	for _, line := range strings.Split(string(body), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 || fields[0] != "nameserver" {
			continue
		}
		if ip := net.ParseIP(fields[1]); ip != nil && !ip.IsLoopback() {
			out = append(out, fields[1])
		}
	}
	return out
}
