//go:build windows

package processidentity

import (
	"context"
	"errors"
	"fmt"
	"time"

	"golang.org/x/sys/windows"

	"kairo/internal/extcmd"
)

// wslLivenessTimeout bounds each wsl.exe run of the liveness check, so a WSL
// that stopped answering makes the identity unknown instead of blocking.
const wslLivenessTimeout = 15 * time.Second

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
	return WindowsIdentityFromFiletime(pid, uint64(created.HighDateTime)<<32|uint64(created.LowDateTime)), nil
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
	out, err := wslRun(ctx, distro, "cat", path)
	if err != nil {
		_, absentErr := wslRun(ctx, distro, "test", "!", "-e", path)
		if absentErr == nil {
			return LivenessAbsent, nil
		}
		return LivenessUnknown, fmt.Errorf("query WSL process identity: %w (absence check: %v)", err, absentErr)
	}
	actual, err := LinuxIdentityFromStat(pid, string(out))
	if err != nil {
		return LivenessUnknown, fmt.Errorf("WSL: %w", err)
	}
	if actual == expected {
		return LivenessAlive, nil
	}
	return LivenessAbsent, nil
}

// wslRun runs one command in the distro under wslLivenessTimeout.
func wslRun(ctx context.Context, distro string, command ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, wslLivenessTimeout)
	defer cancel()
	return extcmd.Output(ctx, "wsl.exe", wslArgs(distro, command...)...)
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
