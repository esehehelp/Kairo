//go:build !windows

package executor

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"strconv"
	"syscall"

	"kairo/internal/processidentity"
)

// treeHandle: on Linux the launcher leads its own process group
// (prepareProcessTree), which is all that is kept.
type treeHandle struct{}

func attachTree(*exec.Cmd) (*treeHandle, error) { return nil, nil }

func (h *treeHandle) close() {}

// RunHelper has no helper subcommands outside Windows.
func RunHelper([]string) {}

// stopTreeGracefully sends SIGTERM to the launcher's process group and to every
// process carrying the attempt's KAIRO_ATTEMPT_ID (which also reaches children
// that left the group).
func (e *Local) stopTreeGracefully(_ context.Context, t processTree) error {
	return signalTree(t, syscall.SIGTERM)
}

// killTree sends SIGKILL to the same processes.
func (e *Local) killTree(_ context.Context, t processTree) error {
	return signalTree(t, syscall.SIGKILL)
}

func (e *Local) treeAlive(_ context.Context, t processTree) (bool, error) {
	alive, err := t.launcherAlive()
	if err != nil || alive {
		return alive, err
	}
	return len(attemptPIDs(t.attemptID)) > 0, nil
}

func signalTree(t processTree, sig syscall.Signal) error {
	var errs []error
	if t.pid != 0 {
		if alive, _ := t.launcherAlive(); alive {
			target := t.pid
			if pgid, err := syscall.Getpgid(t.pid); err == nil && pgid == t.pid {
				target = -t.pid
			}
			if err := syscall.Kill(target, sig); err != nil && !errors.Is(err, syscall.ESRCH) {
				errs = append(errs, err)
			}
		}
	}
	for _, pid := range attemptPIDs(t.attemptID) {
		if err := syscall.Kill(pid, sig); err != nil && !errors.Is(err, syscall.ESRCH) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// attemptPIDs lists the live processes whose environment carries
// KAIRO_ATTEMPT_ID=attemptID (zombies have an empty environment).
func attemptPIDs(attemptID string) []int {
	want := []byte("KAIRO_ATTEMPT_ID=" + attemptID)
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil
	}
	self := os.Getpid()
	var pids []int
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid == self {
			continue
		}
		environ, err := os.ReadFile("/proc/" + entry.Name() + "/environ")
		if err != nil {
			continue
		}
		for _, kv := range bytes.Split(environ, []byte{0}) {
			if bytes.Equal(kv, want) {
				pids = append(pids, pid)
				break
			}
		}
	}
	return pids
}

// launcherRunning reports whether pid (with that creation identity, when
// known) is still running.
func launcherRunning(pid int, identity string) (bool, error) {
	if identity == "" {
		return syscall.Kill(pid, 0) == nil, nil
	}
	liveness, err := processidentity.HostProcessLiveness(pid, identity)
	if err != nil || liveness == processidentity.LivenessUnknown {
		return false, errors.Join(errors.New("launcher liveness unknown"), err)
	}
	return liveness == processidentity.LivenessAlive, nil
}
