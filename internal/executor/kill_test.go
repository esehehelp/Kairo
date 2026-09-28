package executor

import (
	"os/exec"
	"runtime"
	"testing"
	"time"

	"kairo/internal/processidentity"
)

// A node quarantine must stop the launcher and what it spawned (uv -> python).
func TestKillProcessTreeStopsDescendants(t *testing.T) {
	var cmd *exec.Cmd
	if runtime.GOOS == "windows" {
		cmd = exec.Command("cmd", "/c", "ping -n 60 127.0.0.1 >NUL")
	} else {
		cmd = exec.Command("sh", "-c", "sleep 60 & sleep 60; wait")
	}
	prepareProcessTree(cmd)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	identity, err := processidentity.ForPID(cmd.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond) // let the child start
	children := childPIDs(t, cmd.Process.Pid)
	if len(children) == 0 {
		t.Fatal("test process has no child")
	}
	if err = killProcessTree(cmd.Process.Pid); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { _ = cmd.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("launcher still running")
	}
	if l, _ := processidentity.HostProcessLiveness(cmd.Process.Pid, identity); l == processidentity.LivenessAlive {
		t.Fatal("launcher alive after kill")
	}
	deadline := time.Now().Add(15 * time.Second)
	for _, pid := range children {
		for processAlive(pid) {
			if time.Now().After(deadline) {
				t.Fatalf("child %d survived", pid)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}
