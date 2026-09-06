//go:build windows

package processidentity

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strconv"
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
// Python SDK. The PID is passed as a positional shell argument rather than
// interpolated into the script, and WSL command failures remain unknown.
func WSLProcessLiveness(ctx context.Context, distro string, pid int, expected string) (Liveness, error) {
	args := make([]string, 0, 8)
	if distro != "" {
		args = append(args, "-d", distro)
	}
	script := `pid=$1; [ -d /proc/self ] || exit 2; if [ ! -d "/proc/$pid" ]; then printf absent; exit 0; fi; [ -r "/proc/$pid/stat" ] || exit 2; line=$(cat "/proc/$pid/stat") || exit 2; rest=${line##*) }; set -- $rest; [ -n "${20}" ] || exit 2; printf 'proc:%s:starttime:%s' "$pid" "${20}"`
	args = append(args, "--", "sh", "-c", script, "sh", strconv.Itoa(pid))
	out, err := exec.CommandContext(ctx, "wsl.exe", args...).Output()
	if err != nil {
		return LivenessUnknown, fmt.Errorf("query WSL process identity: %w", err)
	}
	actual := strings.TrimSpace(string(out))
	if actual == "absent" {
		return LivenessAbsent, nil
	}
	if actual == expected {
		return LivenessAlive, nil
	}
	if strings.HasPrefix(actual, fmt.Sprintf("proc:%d:starttime:", pid)) {
		return LivenessAbsent, nil
	}
	return LivenessUnknown, fmt.Errorf("unexpected WSL process identity response %q", actual)
}
