//go:build !windows

package executor

import (
	"os/exec"
	"syscall"
)

// ownProcessGroup starts cmd in a process group of its own: a stop sent to
// the attempt's launcher group (its runner) does not reach the runtime's
// tools the runner runs (pct shutdown, incus), which would end before the
// container does.
func ownProcessGroup(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Setpgid = true
}
