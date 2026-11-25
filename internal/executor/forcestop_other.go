//go:build !windows

package executor

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"

	"kairo/internal/processidentity"
)

// treeHandle: on Linux the launcher leads its own process group
// (prepareProcessTree), which is all that is kept.
type treeHandle struct{}

func attachTree(*exec.Cmd, string) (*treeHandle, error) { return nil, nil }

func (h *treeHandle) close() {}

// RunHelper runs a helper subcommand of this binary when args name one (an
// attempt's shim, shim.go; a container's runner and init, oci.go,
// container_linux.go) and does not return then.
func RunHelper(args []string) {
	runShim(args)
	runContainerHelpers(args)
}

// stopLauncherTree sends SIGTERM to the launcher's process group alone (an
// attempt in a container: its runner, which stops the container).
func (e *Local) stopLauncherTree(_ context.Context, t processTree) error {
	return signalTree(t, syscall.SIGTERM)
}

// killLauncherTree sends SIGKILL to the launcher's process group alone.
func (e *Local) killLauncherTree(_ context.Context, t processTree) error {
	return signalTree(t, syscall.SIGKILL)
}

// launcherTreeAlive reports whether the launcher or a process of its group
// runs.
func (e *Local) launcherTreeAlive(_ context.Context, t processTree) (bool, error) {
	alive, err := t.launcherAlive()
	if err != nil || alive {
		return alive, err
	}
	members, err := groupPIDs(t)
	return len(members) > 0, err
}

// signalTree signals the launcher's process group (an attempt's runner).
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
	group, err := groupPIDs(t)
	if err != nil {
		errs = append(errs, err)
	}
	for _, pid := range group {
		if err := syscall.Kill(pid, sig); err != nil && !errors.Is(err, syscall.ESRCH) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// groupPIDs lists the live (not zombie) processes of the launcher's process
// group t.group, whose number is the launcher's pid. While a member lives the
// number is not given to another process; once none does it can be, so when
// a process holds that pid with another creation identity than the
// launcher's (t.identity), the group is not the attempt's.
func groupPIDs(t processTree) ([]int, error) {
	if t.group <= 0 {
		return nil, nil
	}
	if t.identity != "" {
		if body, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", t.group)); err == nil {
			if identity, err := processidentity.LinuxIdentityFromStat(t.group, string(body)); err == nil && identity != t.identity {
				return nil, nil
			}
		}
	}
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, fmt.Errorf("list processes: %w", err)
	}
	self := os.Getpid()
	want := strconv.Itoa(t.group)
	var pids []int
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid == self {
			continue
		}
		body, err := os.ReadFile("/proc/" + entry.Name() + "/stat")
		if err != nil {
			continue // exited meanwhile
		}
		stat := string(body)
		end := strings.LastIndexByte(stat, ')')
		if end < 0 {
			continue
		}
		// After the comm: state, ppid, pgrp, ...
		fields := strings.Fields(stat[end+1:])
		if len(fields) < 3 || fields[2] != want || fields[0] == "Z" || fields[0] == "X" {
			continue
		}
		pids = append(pids, pid)
	}
	return pids, nil
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
