//go:build windows

package executor

import (
	"context"
	"os/exec"
	"strconv"
	"syscall"
	"time"

	"golang.org/x/sys/windows"

	"kairo/internal/extcmd"
)

// taskkillTimeout bounds one taskkill. One that does not finish in time fails,
// so the caller keeps the tree as not yet killed.
const taskkillTimeout = 30 * time.Second

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
	ctx, cancel := context.WithTimeout(context.Background(), taskkillTimeout)
	defer cancel()
	_, err := extcmd.Output(ctx, "taskkill", "/T", "/F", "/PID", strconv.Itoa(pid))
	return err
}
