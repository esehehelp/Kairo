//go:build !windows

package executor

import (
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
)

func childPIDs(t *testing.T, pid int) []int {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		t.Fatal(err)
	}
	var pids []int
	for _, e := range entries {
		n, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		stat, err := os.ReadFile("/proc/" + e.Name() + "/stat")
		if err != nil {
			continue
		}
		fields := strings.Fields(string(stat[strings.LastIndexByte(string(stat), ')')+1:]))
		if len(fields) > 1 && fields[1] == strconv.Itoa(pid) {
			pids = append(pids, n)
		}
	}
	return pids
}

func processAlive(pid int) bool {
	if err := syscall.Kill(pid, 0); err != nil {
		return false
	}
	stat, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return false
	}
	s := string(stat)
	fields := strings.Fields(s[strings.LastIndexByte(s, ')')+1:])
	return len(fields) > 0 && fields[0] != "Z"
}
