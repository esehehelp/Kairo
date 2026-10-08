//go:build !windows

package extcmd

import "os/exec"

// killTree ends the command. Its own children are not signalled: the executors
// that use this package run on Windows, where the tree matters.
func killTree(cmd *exec.Cmd) error {
	return cmd.Process.Kill()
}
