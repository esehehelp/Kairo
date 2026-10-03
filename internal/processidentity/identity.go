// Package processidentity names a process by its PID and start time, so that
// a reused PID is never mistaken for the process that registered it. Workers
// (the Python SDK) produce the same strings; the daemon compares them byte for
// byte (sdk/conformance/identity.json).
package processidentity

import (
	"fmt"
	"strconv"
	"strings"
)

type Liveness int

const (
	LivenessUnknown Liveness = iota
	LivenessAlive
	LivenessAbsent
)

// LinuxIdentityFromStat returns "proc:PID:starttime:T" from the text of
// /proc/PID/stat. T is field 22, i.e. index 19 of the fields after the last
// ')' (the command name may contain spaces and parentheses), and must be a
// positive integer.
func LinuxIdentityFromStat(pid int, stat string) (string, error) {
	malformed := fmt.Errorf("malformed /proc/%d/stat", pid)
	end := strings.LastIndexByte(stat, ')')
	if end < 0 {
		return "", malformed
	}
	fields := strings.Fields(stat[end+1:])
	// The suffix starts at proc field 3, so starttime (field 22) is index 19.
	if len(fields) <= 19 {
		return "", malformed
	}
	start := fields[19]
	if ticks, err := strconv.ParseUint(start, 10, 64); err != nil || ticks == 0 {
		return "", fmt.Errorf("/proc/%d/stat: invalid starttime %q", pid, start)
	}
	return fmt.Sprintf("proc:%d:starttime:%s", pid, start), nil
}

// filetimeUnixEpoch is 1970-01-01 in FILETIME units (100 ns since 1601).
const filetimeUnixEpoch = 116444736000000000

// WindowsIdentityFromFiletime returns "pid:PID:start:NS", NS being the
// process creation FILETIME in Unix nanoseconds (as windows.Filetime's
// Nanoseconds computes it).
func WindowsIdentityFromFiletime(pid int, creation uint64) string {
	return fmt.Sprintf("pid:%d:start:%d", pid, (int64(creation)-filetimeUnixEpoch)*100)
}
