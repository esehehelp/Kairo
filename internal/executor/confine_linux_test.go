//go:build linux

package executor

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// A confined attempt writes its scratch, output and cache directories,
// /tmp (a directory of its scratch) and /dev/shm, but nothing else, and
// still reads everything; nothing it mounts reaches the node. Needs root.
func TestAConfinedAttemptWritesOnlyWhereItIsHanded(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("confining needs root (mounts in a namespace of its own)")
	}
	// not under /tmp, which the attempt sees as a directory of its scratch
	base, err := os.MkdirTemp("/var/tmp", "kairo-confine-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(base) })
	scratch, output, cache, other := filepath.Join(base, "scratch"), filepath.Join(base, "output"), filepath.Join(base, "cache"), filepath.Join(base, "other")
	for _, d := range []string{scratch, output, cache, other} {
		if err := os.Mkdir(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(other, "read.txt"), []byte("readable"), 0o644); err != nil {
		t.Fatal(err)
	}
	e := &Local{ID: "exec", Kind: "local"}
	script := `echo o > ` + output + `/o.txt && echo c > ` + cache + `/c.txt && echo t > /tmp/t.txt && cat ` + other + `/read.txt && (echo x > ` + other + `/x.txt 2>/dev/null || echo DENIED) && (touch /etc/kairo-confine-probe 2>/dev/null || echo ETC-DENIED)`
	cmd := exec.Command("sh", "-c", script)
	cmd.Env = append(os.Environ(), "KAIRO_SCRATCH="+scratch, "KAIRO_OUTPUT_DIR="+output, "KAIRO_CACHE_DIR="+cache)
	prepareProcessTree(cmd)
	conf, err := e.confine(cmd)
	if err != nil {
		t.Fatal(err)
	}
	// as the attempt's shim confines its command
	if _, err := confineCommand(cmd, conf.writable, conf.scratch); err != nil {
		t.Fatal(err)
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	for _, want := range []string{"readable", "DENIED", "ETC-DENIED"} {
		if !strings.Contains(string(out), want) {
			t.Fatalf("output lacks %q: %s", want, out)
		}
	}
	for _, p := range []string{filepath.Join(output, "o.txt"), filepath.Join(cache, "c.txt"), filepath.Join(scratch, "tmp", "t.txt")} {
		if !exists(p) {
			t.Fatalf("%s was not written (%s)", p, out)
		}
	}
	if exists(filepath.Join(other, "x.txt")) || exists("/etc/kairo-confine-probe") || exists("/tmp/t.txt") {
		t.Fatal("wrote outside what it was handed, or its mounts reached the node")
	}
}
