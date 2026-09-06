//go:build windows

package processidentity

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"

	"golang.org/x/sys/windows"
)

type Liveness int

const (
	LivenessUnknown Liveness = iota
	LivenessAlive
	LivenessAbsent
)

func ForPID(pid int) (string, error) {
	handle, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(pid))
	if err != nil {
		return "", err
	}
	defer windows.CloseHandle(handle)
	var created, exited, kernel, user windows.Filetime
	if err = windows.GetProcessTimes(handle, &created, &exited, &kernel, &user); err != nil {
		return "", err
	}
	return fmt.Sprintf("pid:%d:start:%d", pid, created.Nanoseconds()), nil
}

// HostProcessLiveness distinguishes PID reuse and keeps observation errors
// separate from positive proof of absence.
func HostProcessLiveness(pid int, expected string) (Liveness, error) {
	actual, err := ForPID(pid)
	if err != nil {
		if errors.Is(err, windows.ERROR_INVALID_PARAMETER) || errors.Is(err, windows.ERROR_NOT_FOUND) {
			return LivenessAbsent, nil
		}
		return LivenessUnknown, err
	}
	if actual == expected {
		return LivenessAlive, nil
	}
	return LivenessAbsent, nil
}

// WSLProcessLiveness checks the Linux /proc start-time tick recorded by the
// Python SDK. It invokes cat and test directly so wsl.exe does not have to
// preserve a quoted shell program across the Windows/Linux command boundary.
func WSLProcessLiveness(ctx context.Context, distro string, pid int, expected string) (Liveness, error) {
	path := fmt.Sprintf("/proc/%d/stat", pid)
	out, err := exec.CommandContext(ctx, "wsl.exe", wslArgs(distro, "cat", path)...).CombinedOutput()
	if err != nil {
		absentOut, absentErr := exec.CommandContext(ctx, "wsl.exe", wslArgs(distro, "test", "!", "-e", path)...).CombinedOutput()
		if absentErr == nil {
			return LivenessAbsent, nil
		}
		return LivenessUnknown, fmt.Errorf(
			"query WSL process identity: %w: %s (absence check: %v: %s)",
			err, strings.TrimSpace(string(out)), absentErr, strings.TrimSpace(string(absentOut)),
		)
	}
	actual, err := linuxIdentityFromStat(pid, string(out))
	if err != nil {
		return LivenessUnknown, err
	}
	if actual == expected {
		return LivenessAlive, nil
	}
	return LivenessAbsent, nil
}

func wslArgs(distro string, command ...string) []string {
	args := make([]string, 0, len(command)+3)
	if distro != "" {
		args = append(args, "-d", distro)
	}
	// --exec bypasses WSL's command-line mode and preserves argv boundaries.
	// This matters for test's "!" argument and paths or distro-independent
	// commands that contain shell metacharacters.
	args = append(args, "--exec")
	return append(args, command...)
}

func linuxIdentityFromStat(pid int, body string) (string, error) {
	end := strings.LastIndexByte(body, ')')
	if end < 0 {
		return "", fmt.Errorf("malformed WSL %s", fmt.Sprintf("/proc/%d/stat", pid))
	}
	fields := strings.Fields(body[end+1:])
	if len(fields) <= 19 {
		return "", fmt.Errorf("malformed WSL %s", fmt.Sprintf("/proc/%d/stat", pid))
	}
	return fmt.Sprintf("proc:%d:starttime:%s", pid, fields[19]), nil
}
