//go:build windows

package executor

import (
	"os/exec"
	"syscall"
)

// ownProcessGroup starts cmd in a process group of its own: a CTRL_BREAK
// sent to the attempt's launcher (its runner) does not reach the runtime's
// tools the runner runs (wslc), which would end before the container does.
func ownProcessGroup(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.CreationFlags |= syscall.CREATE_NEW_PROCESS_GROUP
}
