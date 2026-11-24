//go:build !windows

package executor

import (
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

func startWithAttemptID(t *testing.T, attemptID string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command("sleep", "60")
	cmd.Env = append(os.Environ(), "KAIRO_ATTEMPT_ID="+attemptID)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	deadline := time.Now().Add(5 * time.Second)
	for { // until it has exec'd sleep with its environment
		if body, err := os.ReadFile("/proc/" + strconv.Itoa(cmd.Process.Pid) + "/environ"); err == nil && strings.Contains(string(body), "KAIRO_ATTEMPT_ID="+attemptID) {
			return cmd
		}
		if time.Now().After(deadline) {
			t.Fatal("the process never carried its attempt id")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// attemptPIDs remembers what it found while it is there, and forgets an
// attempt once nothing of it is found.
func TestAttemptPIDsForgetsWhatIsGone(t *testing.T) {
	const attemptID = "att_members_test"
	cmd := startWithAttemptID(t, attemptID)
	pid := cmd.Process.Pid
	pids, err := attemptPIDs(attemptID)
	if err != nil || !slices.Equal(pids, []int{pid}) {
		t.Fatalf("attemptPIDs = %v %v", pids, err)
	}
	if known := attemptMembers.known(attemptID); len(known) != 1 || known[pid] == "" {
		t.Fatalf("remembered %v", known)
	}
	_ = cmd.Process.Kill()
	_ = cmd.Wait()
	if pids, err = attemptPIDs(attemptID); err != nil || len(pids) != 0 {
		t.Fatalf("after it was reaped: %v %v", pids, err)
	}
	if known := attemptMembers.known(attemptID); known != nil {
		t.Fatalf("still remembered: %v", known)
	}
}
