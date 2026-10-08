//go:build windows

package extcmd

import (
	"context"
	"os/exec"
	"strconv"
	"time"
)

// killTreeTimeout bounds the taskkill of killTree, which must not hang either.
const killTreeTimeout = 10 * time.Second

// killTree ends the command and everything it started: Process.Kill alone
// would leave the descendants running. taskkill /T reaches them.
func killTree(cmd *exec.Cmd) error {
	ctx, cancel := context.WithTimeout(context.Background(), killTreeTimeout)
	defer cancel()
	if err := exec.CommandContext(ctx, "taskkill", "/T", "/F", "/PID", strconv.Itoa(cmd.Process.Pid)).Run(); err != nil {
		return cmd.Process.Kill()
	}
	return nil
}
