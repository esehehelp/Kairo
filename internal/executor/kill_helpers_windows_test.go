//go:build windows

package executor

import (
	"os/exec"
	"strconv"
	"strings"
	"testing"
)

func childPIDs(t *testing.T, pid int) []int {
	out, err := exec.Command("powershell", "-NoProfile", "-Command",
		"Get-CimInstance Win32_Process -Filter 'ParentProcessId="+strconv.Itoa(pid)+"' | ForEach-Object { $_.ProcessId }").Output()
	if err != nil {
		t.Fatal(err)
	}
	var pids []int
	for _, f := range strings.Fields(string(out)) {
		if n, err := strconv.Atoi(f); err == nil {
			pids = append(pids, n)
		}
	}
	return pids
}

func processAlive(pid int) bool {
	out, _ := exec.Command("powershell", "-NoProfile", "-Command", "Get-Process -Id "+strconv.Itoa(pid)+" -ErrorAction SilentlyContinue | Measure-Object | ForEach-Object { $_.Count }").Output()
	return strings.TrimSpace(string(out)) == "1"
}
