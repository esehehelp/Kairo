//go:build !windows

package executor

import (
	"os/exec"
	"syscall"
	"testing"
	"time"

	"kairo/internal/processidentity"
)

// The members of a process group are listed with their creation identities,
// which the delayed SIGKILL checks before it signals anything.
func TestGroupMembersListsTheGroupWithIdentities(t *testing.T) {
	cmd := exec.Command("sh", "-c", "sleep 60 & wait")
	prepareProcessTree(cmd)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		_ = cmd.Wait()
	})
	time.Sleep(300 * time.Millisecond)
	members := groupMembers(cmd.Process.Pid)
	leader, err := processidentity.ForPID(cmd.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	if members[cmd.Process.Pid] != leader || len(members) < 2 {
		t.Fatalf("members %v, leader %s", members, leader)
	}
	for pid, identity := range members {
		if l, _ := processidentity.HostProcessLiveness(pid, identity); l != processidentity.LivenessAlive {
			t.Fatalf("member %d (%s) not alive", pid, identity)
		}
	}
	if other := groupMembers(1 << 22); len(other) != 0 {
		t.Fatalf("an unused group has members: %v", other)
	}
}
