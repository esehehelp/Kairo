//go:build windows

package executor

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// A confined attempt writes its scratch, output and cache directories (and
// its TEMP is its scratch) but nothing else, and still reads everything.
func TestAConfinedAttemptWritesOnlyWhereItIsHanded(t *testing.T) {
	base := t.TempDir()
	scratch, output, cache, other := filepath.Join(base, "scratch"), filepath.Join(base, "output"), filepath.Join(base, "cache"), filepath.Join(base, "other")
	for _, d := range []string{scratch, output, cache, other} {
		if err := os.Mkdir(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(other, "read.txt"), []byte("readable"), 0o644); err != nil {
		t.Fatal(err)
	}
	e := &Local{ID: "exec", Kind: "local", Attributes: map[string]string{"environment": "windows"}}
	script := `echo o> "` + output + `\o.txt" & echo c> "` + cache + `\c.txt" & echo t> "%TEMP%\t.txt" & type "` + other + `\read.txt" & (echo x> "` + other + `\x.txt") 2>nul || echo DENIED`
	cmd := exec.Command("cmd", "/c", script)
	cmd.Env = append(os.Environ(), "KAIRO_SCRATCH="+scratch, "KAIRO_OUTPUT_DIR="+output, "KAIRO_CACHE_DIR="+cache)
	prepareProcessTree(cmd)
	cmd.SysProcAttr.CmdLine = `cmd /s /c "` + script + `"` // as written: cmd reads quotes its own way
	conf, err := e.confine(cmd)
	if err != nil {
		t.Fatal(err)
	}
	// as the attempt's shim confines its command
	release, err := confineCommand(cmd, conf.writable, conf.scratch)
	if err != nil {
		t.Fatal(err)
	}
	out, err := cmd.CombinedOutput()
	if release != nil {
		release()
	}
	if err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if !strings.Contains(string(out), "readable") || !strings.Contains(string(out), "DENIED") {
		t.Fatalf("output: %s", out)
	}
	for _, p := range []string{filepath.Join(output, "o.txt"), filepath.Join(cache, "c.txt"), filepath.Join(scratch, "t.txt")} {
		if !exists(p) {
			t.Fatalf("%s was not written (%s)", p, out)
		}
	}
	if exists(filepath.Join(other, "x.txt")) {
		t.Fatal("wrote outside what it was handed")
	}
}

// A confined launch without a scratch and an output directory is refused.
func TestAConfinedLaunchNeedsAStore(t *testing.T) {
	e := &Local{ID: "exec", Kind: "local"}
	cmd := exec.Command("cmd", "/c", "echo")
	if _, err := e.confine(cmd); err == nil {
		t.Fatal("confined without a store")
	}
	wsl := &Local{ID: "wsl", Kind: "wsl2"}
	cmd.Env = []string{"KAIRO_SCRATCH=x", "KAIRO_OUTPUT_DIR=y"}
	if _, err := wsl.confine(cmd); err == nil || !strings.Contains(err.Error(), "WSL") {
		t.Fatalf("confined inside WSL: %v", err)
	}
}
