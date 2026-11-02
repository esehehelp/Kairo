//go:build !windows

package executor

import (
	"errors"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"

	"kairo/internal/processidentity"
)

// killGrace is how long a killed process group gets between SIGTERM and SIGKILL.
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
// then SIGKILL after killGrace to what is left. The SIGKILL goes only to
// processes that are still the ones signalled (same creation identity): by
// then the group may be gone and its number reused by an unrelated process.
func killProcessTree(pid int) error {
	leader, _ := processidentity.ForPID(pid)
	members := map[int]string{}
	group := false
	if pgid, err := syscall.Getpgid(pid); err == nil && pgid == pid {
		group = true
		members = groupMembers(pid)
	} else if leader != "" {
		members[pid] = leader
	}
	target := pid
	if group {
		target = -pid
	}
	if err := syscall.Kill(target, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	go func() {
		time.Sleep(killGrace)
		if group && leader != "" {
			if l, _ := processidentity.HostProcessLiveness(pid, leader); l == processidentity.LivenessAlive {
				// The leader still runs, so the group is the one signalled.
				_ = syscall.Kill(-pid, syscall.SIGKILL)
			}
		}
		for member, identity := range members {
			if l, _ := processidentity.HostProcessLiveness(member, identity); l == processidentity.LivenessAlive {
				_ = syscall.Kill(member, syscall.SIGKILL)
			}
		}
	}()
	return nil
}

// groupMembers lists the processes of process group pgid with their
// creation identities.
func groupMembers(pgid int) map[int]string {
	out := map[int]string{}
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return out
	}
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		body, err := os.ReadFile("/proc/" + entry.Name() + "/stat")
		if err != nil {
			continue
		}
		stat := string(body)
		end := strings.LastIndexByte(stat, ')')
		if end < 0 {
			continue
		}
		// After the comm: state, ppid, pgrp, ...
		fields := strings.Fields(stat[end+1:])
		if len(fields) < 3 || fields[2] != strconv.Itoa(pgid) {
			continue
		}
		if identity, err := processidentity.LinuxIdentityFromStat(pid, stat); err == nil {
			out[pid] = identity
		}
	}
	return out
}
