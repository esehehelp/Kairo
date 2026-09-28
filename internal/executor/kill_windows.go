//go:build windows

package executor

import (
	"errors"
	"os/exec"
	"strconv"

	"kairo/internal/processidentity"
)

// prepareProcessTree is a no-op on Windows: signalProcessTree walks the tree.
func prepareProcessTree(*exec.Cmd) {}

// processTreeAlive reports whether the launcher identified by pid and identity
// is still running. Windows keeps no process group: descendants are reached
// through the launcher, so the tree counts as gone with it.
func processTreeAlive(pid int, identity string) (bool, error) {
	liveness, err := processidentity.HostProcessLiveness(pid, identity)
	if err != nil || liveness == processidentity.LivenessUnknown {
		return false, errors.Join(errors.New("launcher liveness is unknown"), err)
	}
	return liveness == processidentity.LivenessAlive, nil
}

// signalProcessTree terminates pid and every descendant (taskkill /T /F); the
// termination is always forced. Call it only after processTreeAlive reported
// the tree alive.
func signalProcessTree(pid int, _ bool) error {
	return exec.Command("taskkill", "/T", "/F", "/PID", strconv.Itoa(pid)).Run()
}
