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

// hideTestConsole gives a test attempt its own hidden console, so CTRL_BREAK
// can reach it whether or not `go test` itself has a console.
func hideTestConsole(cmd *exec.Cmd) {
	cmd.SysProcAttr.CreationFlags |= 0x00000010 // CREATE_NEW_CONSOLE
	cmd.SysProcAttr.HideWindow = true
}
