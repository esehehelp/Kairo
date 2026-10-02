//go:build windows

package executor

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// Host-specific: KAIRO_TEST_WSL=1 runs it against the default WSL distro.
func TestWSLAttemptProcessesSignalsTheAttempt(t *testing.T) {
	if os.Getenv("KAIRO_TEST_WSL") != "1" {
		t.Skip("set KAIRO_TEST_WSL=1 to run against WSL")
	}
	ctx := context.Background()
	attempt := "att_wsl_force_stop_test"
	cmd := exec.Command("wsl.exe", "--exec", "env", "KAIRO_ATTEMPT_ID="+attempt, "sh", "-c", "sleep 120 & sleep 120; wait")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	var pids []int
	deadline := time.Now().Add(20 * time.Second)
	for len(pids) < 3 {
		var err error
		if pids, err = wslAttemptProcesses(ctx, "", attempt, "0"); err != nil {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("attempt processes in WSL: %v", pids)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if _, err := wslAttemptProcesses(ctx, "", attempt, "TERM"); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(20 * time.Second)
	for {
		left, err := wslAttemptProcesses(ctx, "", attempt, "0")
		if err != nil {
			t.Fatal(err)
		}
		if len(left) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("survived SIGTERM: %v", left)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// Host-specific: an attempt launched by command() receives its variables
// through WSLENV, and a force stop still finds its processes by
// KAIRO_ATTEMPT_ID.
func TestWSLLaunchEnvironmentReachesTheAttempt(t *testing.T) {
	if os.Getenv("KAIRO_TEST_WSL") != "1" {
		t.Skip("set KAIRO_TEST_WSL=1 to run against WSL")
	}
	ctx := context.Background()
	launch := testLaunch()
	launch.Attempt.ID = "att_wsl_launch_env_test"
	launch.CWD = "/tmp"
	launch.Argv = []string{"sh", "-c", `echo "$KAIRO_ATTEMPT_TOKEN|$CUDA_VISIBLE_DEVICES|$RANK|${KAIRO_TOKEN:-none}"; sleep 120 & sleep 120; wait`}
	t.Setenv("KAIRO_TOKEN", "kairo_admin_operator")
	cmd := (&Local{ID: "wsl", Kind: "wsl2", Attributes: map[string]string{}}).command(launch)
	var out strings.Builder
	cmd.Stdout = &out
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	deadline := time.Now().Add(20 * time.Second)
	for {
		pids, err := wslAttemptProcesses(ctx, "", launch.Attempt.ID, "0")
		if err != nil {
			t.Fatal(err)
		}
		if len(pids) >= 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("attempt processes in WSL: %v", pids)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if _, err := wslAttemptProcesses(ctx, "", launch.Attempt.ID, "TERM"); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	if got := strings.TrimSpace(out.String()); got != "kairo_worker_secret|0,1|1|none" {
		t.Fatalf("attempt saw %q", got)
	}
}
