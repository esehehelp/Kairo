//go:build !windows

package executor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
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

// treeAlive reports whether anything of the attempt runs: its launcher, a
// process of the launcher's process group (also once the launcher is gone,
// and also one that dropped KAIRO_ATTEMPT_ID from its environment), or a
// process carrying the attempt's KAIRO_ATTEMPT_ID (also one that left the
// group). A /proc it cannot read makes the answer unknown, not "nothing".
func (e *Local) treeAlive(_ context.Context, t processTree) (bool, error) {
	alive, err := t.launcherAlive()
	if err != nil || alive {
		return alive, err
	}
	members, err := groupPIDs(t)
	if err != nil || len(members) > 0 {
		return len(members) > 0, err
	}
	pids, err := attemptPIDs(t.attemptID)
	return len(pids) > 0, err
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
	members, err := groupPIDs(t)
	if err != nil {
		errs = append(errs, err)
	}
	pids, err := attemptPIDs(t.attemptID)
	if err != nil {
		errs = append(errs, err)
	}
	for _, pid := range append(members, pids...) {
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

// attemptPIDs lists the live processes whose environment carries
// KAIRO_ATTEMPT_ID=attemptID (zombies have an empty environment). A process
// that exited meanwhile, or one of another user (the attempt's processes run
// as this executor's user), is skipped; any other failure to read an
// environment is an error, not a process found absent.
func attemptPIDs(attemptID string) ([]int, error) {
	want := []byte("KAIRO_ATTEMPT_ID=" + attemptID)
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, fmt.Errorf("list processes: %w", err)
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
			if errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ESRCH) || errors.Is(err, fs.ErrPermission) {
				continue
			}
			return nil, fmt.Errorf("read the environment of process %d: %w", pid, err)
		}
		for _, kv := range bytes.Split(environ, []byte{0}) {
			if bytes.Equal(kv, want) {
				pids = append(pids, pid)
				break
			}
		}
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
