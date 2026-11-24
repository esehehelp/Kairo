//go:build windows

package processidentity

import (
	"os"
	"os/exec"
	"reflect"
	"testing"
)

func TestHostProcessAliveUsesCreationIdentity(t *testing.T) {
	pid := os.Getpid()
	identity, err := ForPID(pid)
	if err != nil {
		t.Fatal(err)
	}
	state, err := HostProcessLiveness(pid, identity)
	if err != nil || state != LivenessAlive {
		t.Fatal("current process was not recognized by its creation identity")
	}
	state, err = HostProcessLiveness(pid, identity+"-stale")
	if err != nil || state != LivenessAbsent {
		t.Fatal("mismatched creation identity was accepted")
	}
}

func TestLineageOfAChildNamesThisProcess(t *testing.T) {
	cmd := exec.Command("ping", "-n", "30", "127.0.0.1")
	if err := cmd.Start(); err != nil {
		t.Skipf("cannot start ping: %v", err)
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
	got := LineageOf(cmd.Process.Pid, child)
	if len(got.Ancestors) == 0 || got.Ancestors[0] != self || got.AttemptID != "" {
		t.Fatalf("lineage = %+v, want ancestors starting with %s", got, self)
	}
	if stale := LineageOf(cmd.Process.Pid, child+"-stale"); !reflect.DeepEqual(stale, Lineage{}) {
		t.Fatalf("lineage under a stale identity: %+v", stale)
	}
}
