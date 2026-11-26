//go:build linux

package provider

import (
	"os"
	"path/filepath"
	"testing"
)

// The node's CPU time comes from /proc/stat (ticks of 1/100 s; idle and
// iowait idle) and Kairo's from its Incus containers' cgroups
// (lxc.payload.kairo-*, cgroup v2); other containers' are not Kairo's.
func TestCPUTimesReadsTheNodeAndKairosContainers(t *testing.T) {
	dir := t.TempDir()
	stat := filepath.Join(dir, "stat")
	if err := os.WriteFile(stat, []byte("cpu  1000 0 500 8000 500 0 0 0 0 0\ncpu0 1 2 3 4\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cg := filepath.Join(dir, "cgroup")
	for name, usec := range map[string]string{"lxc.payload.kairo-att_a": "2500000", "lxc.payload.kairo-att_b": "500000", "lxc.payload.other": "9000000"} {
		if err := os.MkdirAll(filepath.Join(cg, name), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(cg, name, "cpu.stat"), []byte("usage_usec "+usec+"\nuser_usec 1\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	got, err := readCPUTimesFrom(stat, cg)
	if err != nil || got.kairoKnown {
		t.Fatalf("without cgroup v2: %+v %v", got, err)
	}
	if err := os.WriteFile(filepath.Join(cg, "cgroup.controllers"), []byte("cpu memory\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err = readCPUTimesFrom(stat, cg)
	if err != nil || !got.kairoKnown || got.total != 100 || got.idle != 85 || got.kairo != 3 {
		t.Fatalf("%+v %v", got, err)
	}
}
