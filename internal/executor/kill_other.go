//go:build !windows

package executor

import (
	"errors"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"

	"kairo/internal/processidentity"
)

// prepareProcessTree starts the attempt in its own process group so a node
// quarantine can signal the launcher and everything it spawned together.
func prepareProcessTree(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Setpgid = true
}

// processTreeAlive reports whether the launcher identified by pid and identity,
// or anything left in its process group, is still running. The kernel does not
// reuse a pid while a process group of that id exists, so a pid now held by a
// different process means the group is gone; with no process at pid, a
// remaining group of that id is the launcher's own, orphaned descendants.
func processTreeAlive(pid int, identity string) (bool, error) {
	liveness, err := processidentity.HostProcessLiveness(pid, identity)
	if err != nil || liveness == processidentity.LivenessUnknown {
		return false, errors.Join(errors.New("launcher liveness is unknown"), err)
	}
	if liveness == processidentity.LivenessAlive {
		return true, nil
	}
	if _, err := processidentity.ForPID(pid); err == nil {
		return false, nil // pid reused
	}
	return groupAlive(pid)
}

// groupAlive reports whether process group pgid has a member that is not a
// zombie (an orphan's zombie lingers until its new parent reaps it).
func groupAlive(pgid int) (bool, error) {
	if err := syscall.Kill(-pgid, 0); errors.Is(err, syscall.ESRCH) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return true, nil // no procfs: the group exists
	}
	want := strconv.Itoa(pgid)
	for _, entry := range entries {
		if _, err := strconv.Atoi(entry.Name()); err != nil {
			continue
		}
		stat, err := os.ReadFile("/proc/" + entry.Name() + "/stat")
		if err != nil {
			continue // exited meanwhile
		}
		// Fields after the parenthesized command: state, ppid, pgrp, ...
		fields := strings.Fields(string(stat[strings.LastIndexByte(string(stat), ')')+1:]))
		if len(fields) > 2 && fields[2] == want && fields[0] != "Z" {
			return true, nil
		}
	}
	return false, nil
}

// signalProcessTree sends SIGTERM (SIGKILL when force) to pid's process group,
// or to pid alone when it is alive and leads no group, as for attempts started
// before process groups were used. Call it only after processTreeAlive
// reported the tree alive.
func signalProcessTree(pid int, force bool) error {
	sig := syscall.SIGTERM
	if force {
		sig = syscall.SIGKILL
	}
	target := -pid
	if pgid, err := syscall.Getpgid(pid); err == nil && pgid != pid {
		target = pid
	}
	if err := syscall.Kill(target, sig); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	return nil
}
