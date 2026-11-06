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

// statStart is the starttime (field 22) of pid's /proc/PID/stat.
func statStart(t *testing.T, pid int) string {
	t.Helper()
	body, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		t.Fatal(err)
	}
	s := string(body)
	return strings.Fields(s[strings.LastIndexByte(s, ')')+1:])[19]
}

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

// The script WSL runs is plain sh: run here, it finds the attempt's process
// by its environment with its starttime, leaves out remembered processes
// that are gone, have another starttime or still have an environment (so are
// not exiting), and signals what it found.
func TestAttemptScriptFindsRemembersAndSignals(t *testing.T) {
	const attemptID = "att_script_test"
	cmd := startWithAttemptID(t, attemptID)
	pid := cmd.Process.Pid
	run := func(signal string, remembered ...string) ([]int, map[int]string) {
		t.Helper()
		args := append([]string{"-c", wslAttemptScript, "kairo-force-stop", attemptID, signal}, remembered...)
		out, err := exec.Command("sh", args...).Output()
		if err != nil {
			t.Fatalf("script: %v", err)
		}
		pids, found, err := parseAttemptMembers(string(out))
		if err != nil {
			t.Fatal(err)
		}
		return pids, found
	}
	self := os.Getpid()
	pids, found := run("0",
		"999999999:1",                             // gone
		strconv.Itoa(self)+":1",                   // another starttime
		strconv.Itoa(self)+":"+statStart(t, self), // there, but with an environment
	)
	if !slices.Equal(pids, []int{pid}) || found[pid] != statStart(t, pid) {
		t.Fatalf("found %v %v, want %d:%s", pids, found, pid, statStart(t, pid))
	}
	run("TERM", strconv.Itoa(pid)+":"+found[pid])
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("not signalled")
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
