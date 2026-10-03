//go:build windows

package processidentity

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
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

func TestWSLProcessLivenessRecognizesAbsentPID(t *testing.T) {
	if _, err := exec.LookPath("wsl.exe"); err != nil {
		t.Skip("WSL is unavailable")
	}
	state, err := WSLProcessLiveness(context.Background(), "Ubuntu", 2147483647, "proc:2147483647:starttime:1")
	if err != nil {
		t.Fatal(err)
	}
	if state != LivenessAbsent {
		t.Fatalf("state = %v, want absent", state)
	}
}

func TestWSLProcessLivenessRecognizesLivePID(t *testing.T) {
	if _, err := exec.LookPath("wsl.exe"); err != nil {
		t.Skip("WSL is unavailable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "wsl.exe", "-d", "Ubuntu", "--exec", "sh", "-c", "printf '%s\\n' $$; exec sleep 10")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = cmd.Start(); err != nil {
		t.Skipf("Ubuntu distro is unavailable: %v", err)
	}
	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() {
		_ = cmd.Wait()
		t.Fatalf("read WSL test PID: %v", scanner.Err())
	}
	pid, err := strconv.Atoi(strings.TrimSpace(scanner.Text()))
	if err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatalf("parse WSL test PID: %v", err)
	}
	defer func() {
		_ = exec.Command("wsl.exe", "-d", "Ubuntu", "--exec", "kill", "-TERM", strconv.Itoa(pid)).Run()
		_ = cmd.Wait()
	}()

	stat, err := exec.CommandContext(ctx, "wsl.exe", wslArgs("Ubuntu", "cat", fmt.Sprintf("/proc/%d/stat", pid))...).Output()
	if err != nil {
		t.Fatal(err)
	}
	expected, err := LinuxIdentityFromStat(pid, string(stat))
	if err != nil {
		t.Fatal(err)
	}
	state, err := WSLProcessLiveness(ctx, "Ubuntu", pid, expected)
	if err != nil {
		t.Fatal(err)
	}
	if state != LivenessAlive {
		t.Fatalf("state = %v, want alive", state)
	}
}

func TestWSLArgsUseDirectExecAndPreserveArguments(t *testing.T) {
	got := wslArgs("Ubuntu", "test", "!", "-e", "/proc/42/stat")
	want := []string{"-d", "Ubuntu", "--exec", "test", "!", "-e", "/proc/42/stat"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("wslArgs() = %#v, want %#v", got, want)
	}
}
