//go:build windows

package executor

import (
	"os/exec"
	"strconv"
	"syscall"

	"golang.org/x/sys/windows"
)

// prepareProcessTree starts the launcher as the leader of its own console
// process group, so a force stop can send CTRL_BREAK to the attempt alone
// (attachTree adds it to a job object once it runs).
func prepareProcessTree(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.CreationFlags |= windows.CREATE_NEW_PROCESS_GROUP
}

// killProcessTree terminates pid and every descendant (taskkill /T /F), for a
// node quarantine of an attempt that cannot checkpoint.
func killProcessTree(pid int) error {
	return exec.Command("taskkill", "/T", "/F", "/PID", strconv.Itoa(pid)).Run()
}
