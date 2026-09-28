//go:build !windows

package executor

import (
	"context"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"kairo/internal/processidentity"
)

// A descendant that outlives its launcher and ignores SIGTERM keeps the tree
// alive until the forced kill.
func TestProcessTreeAliveFollowsOrphanedGroupUntilForcedKill(t *testing.T) {
	cmd := exec.Command("sh", "-c", `sh -c 'trap "" TERM; sleep 60' & sleep 0.3; exit 0`)
	prepareProcessTree(cmd)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	pid := cmd.Process.Pid
	identity, err := processidentity.ForPID(pid)
	if err != nil {
		t.Fatal(err)
	}
	if err = cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	if l, _ := processidentity.HostProcessLiveness(pid, identity); l != processidentity.LivenessAbsent {
		t.Fatalf("launcher liveness %v after exit", l)
	}
	t.Cleanup(func() { _ = signalProcessTree(pid, true) })
	if alive, err := processTreeAlive(pid, identity); err != nil || !alive {
		t.Fatalf("orphaned descendant not seen: alive=%v err=%v", alive, err)
	}
	if err = signalProcessTree(pid, false); err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)
	if alive, err := processTreeAlive(pid, identity); err != nil || !alive {
		t.Fatalf("SIGTERM-ignoring descendant reported gone: alive=%v err=%v", alive, err)
	}
	if err = signalProcessTree(pid, true); err != nil {
		t.Fatal(err)
	}
	waitTreeGone(t, pid, identity)
}

// The Linux side of a WSL2 attempt is found by its KAIRO_ATTEMPT_ID. The
// script is run by sh directly in place of wsl.exe.
func TestWSLSignalAttemptFindsAndStopsAttemptProcesses(t *testing.T) {
	var ran [][]string
	orig := wslRun
	wslRun = func(ctx context.Context, argv []string) (int, error) {
		ran = append(ran, argv)
		for i, arg := range argv {
			if arg == "--exec" {
				return orig(ctx, argv[i+1:])
			}
		}
		t.Fatalf("no --exec in %v", argv)
		return 0, nil
	}
	t.Cleanup(func() { wslRun = orig })

	attemptID := "att_wsl_test_" + strconv.Itoa(os.Getpid())
	cmd := exec.Command("sh", "-c", "sleep 60 & sleep 60; wait")
	cmd.Env = append(os.Environ(), "KAIRO_ATTEMPT_ID="+attemptID)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { _ = cmd.Wait(); close(done) }()
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	time.Sleep(300 * time.Millisecond)
	ctx := context.Background()

	if found, err := wslSignalAttempt(ctx, "Ubuntu", "att_other", "0"); err != nil || found {
		t.Fatalf("another attempt matched: %v %v", found, err)
	}
	if found, err := wslSignalAttempt(ctx, "Ubuntu", attemptID, "0"); err != nil || !found {
		t.Fatalf("attempt not found: %v %v", found, err)
	}
	if found, err := wslSignalAttempt(ctx, "Ubuntu", attemptID, "TERM"); err != nil || !found {
		t.Fatalf("attempt not signalled: %v %v", found, err)
	}
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("attempt survived SIGTERM")
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		found, err := wslSignalAttempt(ctx, "Ubuntu", attemptID, "0")
		if err != nil {
			t.Fatal(err)
		}
		if !found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("attempt processes still found")
		}
		time.Sleep(100 * time.Millisecond)
	}
	if got := ran[0][:4]; got[0] != "wsl.exe" || got[1] != "-d" || got[2] != "Ubuntu" || got[3] != "--exec" {
		t.Fatalf("wsl invocation %v", ran[0])
	}
}
