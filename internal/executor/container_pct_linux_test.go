//go:build linux

package executor

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakePCT records the Proxmox tools' calls and answers them from answer.
type fakePCT struct {
	mu     sync.Mutex
	calls  [][]string
	answer func(args []string) ([]byte, error)
}

func (f *fakePCT) install(t *testing.T) {
	t.Helper()
	old, oldLock, oldDir := pctTool, pctLock, pctConfigDir
	dir := t.TempDir()
	pctLock, pctConfigDir = filepath.Join(dir, "lock"), filepath.Join(dir, "lxc")
	if err := os.Mkdir(pctConfigDir, 0o755); err != nil {
		t.Fatal(err)
	}
	pctTool = func(_ context.Context, name string, args ...string) ([]byte, error) {
		f.mu.Lock()
		f.calls = append(f.calls, append([]string{name}, args...))
		f.mu.Unlock()
		return f.answer(append([]string{name}, args...))
	}
	t.Cleanup(func() { pctTool, pctLock, pctConfigDir = old, oldLock, oldDir })
}

func (f *fakePCT) called(prefix ...string) [][]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out [][]string
	for _, c := range f.calls {
		if len(c) >= len(prefix) && slices.Equal(c[:len(prefix)], prefix) {
			out = append(out, c)
		}
	}
	return out
}

func writeCTConfig(t *testing.T, vmid int) {
	t.Helper()
	if err := os.WriteFile(pctConfigPath(vmid), []byte("arch: amd64\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

// An executor's pct settings come from its attributes, with Proxmox's
// defaults; a root disk size must be a positive number of GiB.
func TestPCTSettingsFromAttributes(t *testing.T) {
	s, err := pctSettingsOf(nil)
	if err != nil || *s != (pctSettings{Storage: "local-lvm", TemplateStorage: "local", Net: "name=eth0,bridge=vmbr0,ip=dhcp", RootfsGiB: 8}) {
		t.Fatalf("defaults %+v %v", s, err)
	}
	s, err = pctSettingsOf(map[string]string{"pct_storage": "fast", "pct_template_storage": "tmpl", "pct_net": "", "pct_rootfs_gib": "20"})
	if err != nil || *s != (pctSettings{Storage: "fast", TemplateStorage: "tmpl", RootfsGiB: 20}) {
		t.Fatalf("set %+v %v", s, err)
	}
	for _, bad := range []string{"0", "-1", "x"} {
		if _, err := pctSettingsOf(map[string]string{"pct_rootfs_gib": bad}); err == nil {
			t.Fatalf("rootfs %q accepted", bad)
		}
	}
}

// A container is made unprivileged from the image's template, tagged kairo
// and named after its attempt, with what the attempt is handed bind-mounted
// (read-only where it is to be) and no path Proxmox would misread.
func TestPCTCreateArgs(t *testing.T) {
	spec := containerSpec{
		Instance: "kairo-att-1", AttemptID: "att_1", Image: "docker.io/library/python@sha256:" + strings.Repeat("a", 64),
		PCT: &pctSettings{Storage: "local-lvm", TemplateStorage: "local", Net: "name=eth0,bridge=vmbr0,ip=dhcp", RootfsGiB: 8},
		Mounts: []containerMount{
			{Source: "/srv/kairo/bin/kairo", Path: "/kairo/bin/kairo", ReadOnly: true},
			{Source: "/srv/store/scratch/att_1", Path: "/kairo/scratch"},
		},
	}
	args, err := pctCreateArgs(spec, "local:vztmpl/kairo-x.tar")
	if err != nil {
		t.Fatal(err)
	}
	line := strings.Join(args, " ")
	for _, want := range []string{"local:vztmpl/kairo-x.tar --hostname kairo-att-1 --tags kairo", "--rootfs local-lvm:8", "--unprivileged 1", "--swap 0",
		"--net0 name=eth0,bridge=vmbr0,ip=dhcp", "--mp0 /srv/kairo/bin/kairo,mp=/kairo/bin/kairo,ro=1", "--mp1 /srv/store/scratch/att_1,mp=/kairo/scratch"} {
		if !strings.Contains(line, want) {
			t.Fatalf("%q not in %s", want, line)
		}
	}
	if strings.Contains(line, "--mp1 /srv/store/scratch/att_1,mp=/kairo/scratch,ro=1") {
		t.Fatalf("scratch read-only: %s", line)
	}
	spec.Mounts = append(spec.Mounts, containerMount{Source: "/srv/a,b", Path: "/kairo/inputs/x", ReadOnly: true})
	if _, err := pctCreateArgs(spec, "local:vztmpl/kairo-x.tar"); err == nil {
		t.Fatal("a path with a comma was taken")
	}
}

// A container takes the first VMID of 1000-1999 no guest has, and the next
// when another took it meanwhile; never one outside that range.
func TestCreatePCTTakesTheFirstFreeVMIDOfKairosRange(t *testing.T) {
	f := &fakePCT{answer: func(args []string) ([]byte, error) {
		switch {
		case args[0] == "pvesh":
			return []byte(`[{"vmid":100},{"vmid":999},{"vmid":1000},{"vmid":1001}]`), nil
		case args[0] == "pct" && args[1] == "create" && args[2] == "1002":
			return nil, errors.New("pct create: exit status 255: CT 1002 already exists on node 'pve'")
		}
		return nil, nil
	}}
	f.install(t)
	vmid, err := createPCT(context.Background(), []string{"local:vztmpl/x.tar"})
	if err != nil || vmid != 1003 {
		t.Fatalf("vmid %d %v", vmid, err)
	}
	if creates := f.called("pct", "create"); len(creates) != 2 {
		t.Fatalf("creates %v", creates)
	}
	f.answer = func(args []string) ([]byte, error) {
		used := `[`
		for id := pctFirstVMID; id <= pctLastVMID; id++ {
			if id > pctFirstVMID {
				used += ","
			}
			used += `{"vmid":` + strconv.Itoa(id) + `}`
		}
		return []byte(used + `]`), nil
	}
	if _, err := createPCT(context.Background(), nil); err == nil || !strings.Contains(err.Error(), "no free VMID") {
		t.Fatalf("full range: %v", err)
	}
}

// Destroying refuses a VMID outside Kairo's range and takes one already
// gone as destroyed.
func TestDestroyPCTOnlyInKairosRange(t *testing.T) {
	f := &fakePCT{answer: func([]string) ([]byte, error) { return nil, nil }}
	f.install(t)
	if err := destroyPCT(context.Background(), 999); err == nil {
		t.Fatal("999 destroyed")
	}
	if err := destroyPCT(context.Background(), 1000); err != nil || len(f.called("pct", "destroy")) != 0 {
		t.Fatalf("a container that is gone: %v %v", err, f.calls)
	}
	writeCTConfig(t, 1000)
	if err := destroyPCT(context.Background(), 1000); err != nil || len(f.called("pct", "destroy", "1000", "--force", "1", "--purge", "1")) != 1 {
		t.Fatalf("destroy: %v %v", err, f.calls)
	}
}

// The sweep destroys only Kairo's containers: in its range, tagged kairo,
// named for an attempt that no longer runs here (running or not).
func TestPCTSweepDestroysOnlyKairosStoppedContainersOfEndedAttempts(t *testing.T) {
	f := &fakePCT{answer: func(args []string) ([]byte, error) {
		if args[0] == "pvesh" {
			return []byte(`[
				{"vmid":999,"name":"kairo-att-a","status":"stopped","tags":"kairo"},
				{"vmid":1000,"name":"kairo-att-b","status":"stopped","tags":"kairo"},
				{"vmid":1001,"name":"kairo-att-c","status":"running","tags":"kairo"},
				{"vmid":1002,"name":"kairo-att-d","status":"stopped","tags":""},
				{"vmid":1003,"name":"other","status":"stopped","tags":"kairo"},
				{"vmid":1004,"name":"kairo-att-run","status":"stopped","tags":"x;kairo"}
			]`), nil
		}
		return nil, nil
	}}
	f.install(t)
	for _, id := range []int{999, 1000, 1001, 1002, 1003, 1004} {
		writeCTConfig(t, id)
	}
	var logs bytes.Buffer
	e := NewLocal("ctr", nil, t.TempDir(), slog.New(slog.NewTextHandler(&logs, nil)))
	e.running = map[string]int{"att_run": 1}
	e.storeWork().sweepContainers(context.Background(), pctRuntime{})
	destroyed := f.called("pct", "destroy")
	if len(destroyed) != 2 || destroyed[0][2] != "1000" || destroyed[1][2] != "1001" {
		t.Fatalf("destroyed %v\n%s", destroyed, logs.String())
	}
}

// A container whose disk is still in use just after it stopped is destroyed
// at a later try.
func TestDestroyPCTRetriesWhileItsDiskIsInUse(t *testing.T) {
	tries := 0
	f := &fakePCT{answer: func(args []string) ([]byte, error) {
		if args[0] == "pct" && args[1] == "destroy" {
			tries++
			if tries < 3 {
				return nil, errors.New("lvremove 'pve/vm-1000-disk-0' error: Logical volume pve/vm-1000-disk-0 contains a filesystem in use.")
			}
		}
		return nil, nil
	}}
	f.install(t)
	old := pctDestroyRetry
	pctDestroyRetry = time.Millisecond
	t.Cleanup(func() { pctDestroyRetry = old })
	writeCTConfig(t, 1000)
	if err := destroyPCT(context.Background(), 1000); err != nil || tries != 3 {
		t.Fatalf("destroy after %d tries: %v", tries, err)
	}
}

// A container is given the host's nameservers, but not its loopback ones.
func TestHostNameservers(t *testing.T) {
	p := filepath.Join(t.TempDir(), "resolv.conf")
	if err := os.WriteFile(p, []byte("search lan\nnameserver 127.0.0.53\nnameserver 192.0.2.1\nnameserver ::1\nnameserver 2001:db8::1\noptions edns0\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := hostNameservers(p); !slices.Equal(got, []string{"192.0.2.1", "2001:db8::1"}) {
		t.Fatalf("nameservers %v", got)
	}
	if got := hostNameservers(filepath.Join(t.TempDir(), "none")); got != nil {
		t.Fatalf("no file: %v", got)
	}
}
