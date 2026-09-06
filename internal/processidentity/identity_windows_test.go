//go:build windows

package processidentity

import (
	"os"
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
