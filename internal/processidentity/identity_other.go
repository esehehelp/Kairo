//go:build !windows

package processidentity

import (
	"context"
	"fmt"
	"os"
)

func ForPID(pid int) (string, error) {
	body, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return "", err
	}
	return LinuxIdentityFromStat(pid, string(body))
}

// LineageOf reads the lineage of process pid, named identity, from /proc: its
// KAIRO_ATTEMPT_ID (readable for a process of the same user, or by root) and
// its parent chain.
func LineageOf(pid int, identity string) Lineage {
	return lineage(pid, identity, readProcess, func(pid int) ([]byte, error) {
		return os.ReadFile(fmt.Sprintf("/proc/%d/environ", pid))
	})
}

func readProcess(pid int) (process, error) {
	body, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return process{}, err
	}
	return linuxProcessFromStat(pid, string(body))
}

func HostProcessLiveness(pid int, expected string) (Liveness, error) {
	actual, err := ForPID(pid)
	if err != nil {
		if os.IsNotExist(err) {
			if _, procErr := os.Stat("/proc/self/stat"); procErr != nil {
				return LivenessUnknown, fmt.Errorf("procfs is unavailable: %w", procErr)
			}
			return LivenessAbsent, nil
		}
		return LivenessUnknown, err
	}
	if actual == expected {
		return LivenessAlive, nil
	}
	return LivenessAbsent, nil
}

func WSLProcessLiveness(_ context.Context, _ string, pid int, expected string) (Liveness, error) {
	return HostProcessLiveness(pid, expected)
}
