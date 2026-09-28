//go:build !windows

package executor

import (
	"errors"
	"os/exec"
	"syscall"
	"time"
)

// killGrace is how long a quarantined process group gets between SIGTERM and SIGKILL.
const killGrace = 10 * time.Second

// prepareProcessTree starts the attempt in its own process group so a node
// quarantine can signal the launcher and everything it spawned together.
func prepareProcessTree(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Setpgid = true
}

// killProcessTree sends SIGTERM to pid's process group (or to pid alone when it
// leads no group, as for attempts started before process groups were used),
// then SIGKILL after killGrace if anything is left.
func killProcessTree(pid int) error {
	target := -pid
	if pgid, err := syscall.Getpgid(pid); err != nil || pgid != pid {
		target = pid
	}
	if err := syscall.Kill(target, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	go func() {
		time.Sleep(killGrace)
		_ = syscall.Kill(target, syscall.SIGKILL)
	}()
	return nil
}
