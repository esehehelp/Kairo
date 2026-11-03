//go:build !windows

package executor

import (
	"context"
	"log/slog"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"kairo/internal/processidentity"
	"kairo/internal/store"
)

// A child that dropped KAIRO_ATTEMPT_ID from its environment is still found
// through the launcher's process group once the launcher is gone, and the
// group stops counting when it is empty.
func TestLeftBehindFindsTheLaunchersGroupWithoutTheEnvironment(t *testing.T) {
	attemptID := "att-noenv"
	cmd := exec.Command("sh", "-c", "env -u KAIRO_ATTEMPT_ID sleep 60 & echo $!")
	cmd.Env = append(os.Environ(), "KAIRO_ATTEMPT_ID="+attemptID)
	prepareProcessTree(cmd)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	launcher := cmd.Process.Pid
	identity, err := processidentity.ForPID(launcher)
	if err != nil {
		t.Fatal(err)
	}
	var line [32]byte
	n, err := stdout.Read(line[:])
	if err != nil {
		t.Fatal(err)
	}
	child, err := strconv.Atoi(strings.TrimSpace(string(line[:n])))
	if err != nil {
		t.Fatalf("child pid: %q", line[:n])
	}
	t.Cleanup(func() { _ = syscall.Kill(child, syscall.SIGKILL) })
	_ = cmd.Wait() // the launcher is gone; its child runs on in its group
	e := NewLocal("e", nil, t.TempDir(), slog.Default())
	attempt := store.Attempt{ID: attemptID, State: "exited", PID: &launcher, ProcessIdentity: &identity}
	ctx := context.Background()
	if left, err := e.leftBehind(ctx, attempt); err != nil || !left {
		t.Fatalf("a child without KAIRO_ATTEMPT_ID was not found: %v %v", left, err)
	}
	if err = syscall.Kill(child, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		left, err := e.leftBehind(ctx, attempt)
		if err != nil {
			t.Fatal(err)
		}
		if !left {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("an empty group still counts")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// A group number now held by a process of another identity (the launcher's
// pid given to an unrelated process) is not the attempt's group.
func TestGroupWhoseNumberAnotherProcessHoldsIsNotTheAttempts(t *testing.T) {
	cmd := exec.Command("sleep", "60")
	prepareProcessTree(cmd)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	pid := cmd.Process.Pid
	identity, err := processidentity.ForPID(pid)
	if err != nil {
		t.Fatal(err)
	}
	if pids, err := groupPIDs(processTree{group: pid, identity: identity}); err != nil || len(pids) != 1 || pids[0] != pid {
		t.Fatalf("own group: %v %v", pids, err)
	}
	if pids, err := groupPIDs(processTree{group: pid, identity: "proc:" + strconv.Itoa(pid) + ":starttime:1"}); err != nil || len(pids) != 0 {
		t.Fatalf("another process's group counted: %v %v", pids, err)
	}
}
