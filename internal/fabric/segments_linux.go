//go:build linux

package fabric

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"kairo/internal/store"
)

var vxlanDevice = regexp.MustCompile(`^\d+:\s+([^:@\s]+)[@:]`)

func (nlLink) segments(ctx context.Context, names string) ([]int, error) {
	return linuxSegments(ctx, names)
}

func linuxSegments(ctx context.Context, names string) ([]int, error) {
	out, err := run(ctx, "ip", "-o", "link", "show", "type", "vxlan")
	if err != nil {
		return nil, err
	}
	var vnis []int
	for _, line := range strings.Split(string(out), "\n") {
		m := vxlanDevice.FindStringSubmatch(line)
		if m == nil || !strings.HasPrefix(m[1], names+"v") {
			continue
		}
		if vni, err := strconv.Atoi(strings.TrimPrefix(m[1], names+"v")); err == nil {
			vnis = append(vnis, vni)
		}
	}
	sort.Ints(vnis)
	return vnis, nil
}

func (nlLink) ensureSegment(ctx context.Context, names string, seg store.FabricSegment, local string, port int) error {
	return linuxEnsureSegment(ctx, names, seg.VNI, seg.MTU, local, port)
}

// noIPv6 keeps dev without IPv6 (no link-local address): a segment does
// not reach the host it crosses.
func noIPv6(dev string) {
	_ = os.WriteFile("/proc/sys/net/ipv6/conf/"+dev+"/disable_ipv6", []byte("1\n"), 0o644)
}

// linuxEnsureSegment makes segment vni's bridge (unless it is there: on a
// wslc VM, docker made it for the segment's network) and its VXLAN device
// (again when it is there with another local address or port), a port of
// the bridge that learns nothing (EVPN says where MACs are) and answers
// ARP from what EVPN said.
func linuxEnsureSegment(ctx context.Context, names string, vni, mtu int, local string, port int) error {
	bridge, vxlan := segmentDevices(names, vni)
	m := strconv.Itoa(mtu)
	if _, err := run(ctx, "ip", "link", "show", "dev", bridge); err != nil {
		if _, err := run(ctx, "ip", "link", "add", "dev", bridge, "type", "bridge"); err != nil {
			return err
		}
	}
	noIPv6(bridge)
	if _, err := run(ctx, "ip", "link", "set", "dev", bridge, "mtu", m, "up"); err != nil {
		return err
	}
	if out, err := run(ctx, "ip", "-d", "-o", "link", "show", "dev", vxlan); err == nil {
		d := string(out)
		if !strings.Contains(d, "vxlan id "+strconv.Itoa(vni)+" ") || !strings.Contains(d, "local "+local+" ") || !strings.Contains(d, "dstport "+strconv.Itoa(port)+" ") {
			if _, err := run(ctx, "ip", "link", "del", "dev", vxlan); err != nil {
				return err
			}
		}
	}
	if _, err := run(ctx, "ip", "link", "show", "dev", vxlan); err != nil {
		if _, err := run(ctx, "ip", "link", "add", "dev", vxlan, "type", "vxlan", "id", strconv.Itoa(vni), "local", local, "dstport", strconv.Itoa(port), "nolearning"); err != nil {
			return err
		}
	}
	noIPv6(vxlan)
	if _, err := run(ctx, "ip", "link", "set", "dev", vxlan, "master", bridge, "mtu", m, "up"); err != nil {
		return err
	}
	_, err := run(ctx, "bridge", "link", "set", "dev", vxlan, "neigh_suppress", "on", "learning", "off")
	return err
}

func (nlLink) removeSegment(ctx context.Context, names string, vni int) error {
	return linuxRemoveSegment(ctx, names, vni, true)
}

func linuxRemoveSegment(ctx context.Context, names string, vni int, bridgeToo bool) error {
	bridge, vxlan := segmentDevices(names, vni)
	if _, err := run(ctx, "ip", "link", "show", "dev", vxlan); err == nil {
		if _, err := run(ctx, "ip", "link", "del", "dev", vxlan); err != nil {
			return err
		}
	}
	if bridgeToo {
		if _, err := run(ctx, "ip", "link", "show", "dev", bridge); err == nil {
			if _, err := run(ctx, "ip", "link", "del", "dev", bridge); err != nil {
				return err
			}
		}
	}
	return nil
}

// writeFiles puts files in dir, saying whether any was not so already.
func writeFiles(dir string, files map[string]string) (bool, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return false, err
	}
	changed := false
	for name, content := range files {
		p := filepath.Join(dir, name)
		if old, err := os.ReadFile(p); err == nil && bytes.Equal(old, []byte(content)) {
			continue
		}
		if err := os.WriteFile(p+".new", []byte(content), 0o644); err != nil {
			return false, err
		}
		if err := os.Rename(p+".new", p); err != nil {
			return false, err
		}
		changed = true
	}
	return changed, nil
}

// incusRouter is FRR in a privileged Incus OCI container that shares the
// host's network namespace (lxc.namespace.share.net) and has no NIC of its
// own, its /etc/frr a directory of the agent's.
type incusRouter struct {
	name, dir string
}

func (nlLink) router(name, keyFile string) router {
	return incusRouter{name: name, dir: filepath.Join(filepath.Dir(keyFile), "frr")}
}

// frrRemote is the Incus remote FRR's image comes from.
const frrRemote = "kairo-quay-io"

func incus(ctx context.Context, args ...string) ([]byte, error) { return run(ctx, "incus", args...) }

// hostFRR: the host's own FRR service would fight Kairo's over the same
// kernel state.
func hostFRR(ctx context.Context) bool {
	out, _ := exec.CommandContext(ctx, "systemctl", "is-active", "frr").Output()
	return strings.TrimSpace(string(out)) == "active"
}

func (r incusRouter) state(ctx context.Context) (string, error) {
	out, err := incus(ctx, "list", "^"+regexp.QuoteMeta(r.name)+"$", "-f", "csv", "-c", "s")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

func (r incusRouter) apply(ctx context.Context, files map[string]string) error {
	if hostFRR(ctx) {
		return errors.New("the host's own frr service is active: Kairo's FRR is not started beside it (systemctl disable --now frr)")
	}
	changed, err := writeFiles(r.dir, files)
	if err != nil {
		return err
	}
	state, err := r.state(ctx)
	if err != nil {
		return err
	}
	switch {
	case state == "":
		if out, _ := incus(ctx, "remote", "list", "-f", "csv"); !strings.Contains("\n"+string(out), "\n"+frrRemote+",") {
			if _, err := incus(ctx, "remote", "add", frrRemote, "https://quay.io", "--protocol=oci", "--public"); err != nil {
				return err
			}
		}
		image := frrRemote + ":" + strings.TrimPrefix(FRRRepository, "quay.io/") + "@" + FRRDigest
		if _, err := incus(ctx, "init", image, r.name, "--quiet", "-c", "security.privileged=true",
			"-c", "raw.lxc=lxc.namespace.share.net=1", "-c", "boot.autostart=true"); err != nil {
			return err
		}
		if _, err := incus(ctx, "config", "device", "add", r.name, "eth0", "none"); err != nil {
			return err
		}
		if _, err := incus(ctx, "config", "device", "add", r.name, "frr", "disk", "source="+r.dir, "path=/etc/frr"); err != nil {
			return err
		}
		_, err = incus(ctx, "start", r.name)
		return err
	case state != "RUNNING":
		_, err = incus(ctx, "start", r.name)
		return err
	case changed:
		_, err = incus(ctx, "restart", r.name)
		return err
	}
	return nil
}

func (r incusRouter) remove(ctx context.Context) error {
	if state, err := r.state(ctx); err != nil || state == "" {
		return err
	}
	_, err := incus(ctx, "delete", "--force", r.name)
	return err
}

func (r incusRouter) show(ctx context.Context, command string) ([]byte, error) {
	return incus(ctx, "exec", r.name, "--", "vtysh", "-c", command)
}

// dockerRouter is FRR as a docker container on the host's network,
// privileged: in a wslc VM (the fabric-link command, for the Windows
// agent), where dockerd runs.
type dockerRouter struct {
	name, dir string
}

func docker(ctx context.Context, args ...string) ([]byte, error) { return run(ctx, "docker", args...) }

func (r dockerRouter) apply(ctx context.Context, files map[string]string) error {
	changed, err := writeFiles(r.dir, files)
	if err != nil {
		return err
	}
	out, err := docker(ctx, "inspect", "-f", "{{.State.Running}}", r.name)
	switch {
	case err != nil:
		_, err = docker(ctx, "run", "-d", "--name", r.name, "--network", "host", "--privileged", "--restart", "always",
			"-v", r.dir+":/etc/frr", FRRRepository+"@"+FRRDigest)
		return err
	case strings.TrimSpace(string(out)) != "true":
		_, err = docker(ctx, "start", r.name)
		return err
	case changed:
		_, err = docker(ctx, "restart", r.name)
		return err
	}
	return nil
}

func (r dockerRouter) remove(ctx context.Context) error {
	if _, err := docker(ctx, "inspect", r.name); err != nil {
		return nil
	}
	_, err := docker(ctx, "rm", "-f", r.name)
	return err
}

func (r dockerRouter) show(ctx context.Context, command string) ([]byte, error) {
	return docker(ctx, "exec", r.name, "vtysh", "-c", command)
}

