//go:build windows

package executor

import (
	"os/exec"
	"strconv"
)

// prepareProcessTree is a no-op on Windows: killProcessTree walks the tree.
func prepareProcessTree(*exec.Cmd) {}

// killProcessTree terminates pid and every descendant (taskkill /T /F), for a
// node quarantine of an attempt that cannot checkpoint.
func killProcessTree(pid int) error {
	return exec.Command("taskkill", "/T", "/F", "/PID", strconv.Itoa(pid)).Run()
}
