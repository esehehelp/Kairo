//go:build linux

package processidentity

import (
	"os"
	"os/exec"
	"reflect"
	"testing"
	"time"
)

func TestLineageOfAChildNamesThisProcessAndItsAttempt(t *testing.T) {
	cmd := exec.Command("sleep", "30")
	cmd.Env = append(os.Environ(), "KAIRO_ATTEMPT_ID=att-lineage")
	if err := cmd.Start(); err != nil {
		t.Skipf("cannot start sleep: %v", err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()
	self, err := ForPID(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	child, err := ForPID(cmd.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	// Just after exec the environment may read empty for a moment (the new
	// image has not set it up yet): read again until it is there.
	var got Lineage
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		got = LineageOf(cmd.Process.Pid, child)
		if got.AttemptID != "" || time.Now().After(deadline) {
			break
		}
	}
	if len(got.Ancestors) == 0 || got.Ancestors[0] != self || got.AttemptID != "att-lineage" {
		t.Fatalf("lineage = %+v, want attempt att-lineage and ancestors starting with %s", got, self)
	}
	if stale := LineageOf(cmd.Process.Pid, child+"-stale"); !reflect.DeepEqual(stale, Lineage{}) {
		t.Fatalf("lineage under a stale identity: %+v", stale)
	}
}
