//go:build !windows

package processidentity

import (
	"context"
	"fmt"
	"os"
	"strings"
)

type Liveness int

const (
	LivenessUnknown Liveness = iota
	LivenessAlive
	LivenessAbsent
)

func ForPID(pid int) (string, error) {
	body, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return "", err
	}
	end := strings.LastIndexByte(string(body), ')')
	if end < 0 {
		return "", fmt.Errorf("malformed /proc/%d/stat", pid)
	}
	fields := strings.Fields(string(body[end+1:]))
	// The suffix starts at proc field 3, so starttime (field 22) is index 19.
	if len(fields) <= 19 {
		return "", fmt.Errorf("malformed /proc/%d/stat", pid)
	}
	return fmt.Sprintf("proc:%d:starttime:%s", pid, fields[19]), nil
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
