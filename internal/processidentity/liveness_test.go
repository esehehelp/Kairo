package processidentity

import (
	"os/exec"
	"runtime"
	"testing"
	"time"
)

// A process that exited is absent although it is not reaped yet (a Linux
// zombie) or its object is still open (Windows: this test holds its handle
// until Wait), which keeps its pid and creation identity readable.
func TestExitedProcessIsAbsentWhileItsObjectIsKept(t *testing.T) {
	cmd := exec.Command("sleep", "1")
	if runtime.GOOS == "windows" {
		cmd = exec.Command("cmd", "/c", "ping -n 2 127.0.0.1 >NUL")
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Wait() })
	identity, err := ForPID(cmd.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	if l, err := HostProcessLiveness(cmd.Process.Pid, identity); err != nil || l != LivenessAlive {
		t.Fatalf("running process: %v %v", l, err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		l, err := HostProcessLiveness(cmd.Process.Pid, identity)
		if err != nil {
			t.Fatal(err)
		}
		if l == LivenessAbsent {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("an exited, unreaped process is still alive")
		}
		time.Sleep(100 * time.Millisecond)
	}
}
