// Package processidentity names a process by its PID and start time, so that
// a reused PID is never mistaken for the process that registered it. Workers
// (the Python SDK) produce the same strings; the daemon compares them byte for
// byte (sdk/conformance/identity.json).
package processidentity

import (
	"fmt"
	"strconv"
	"strings"
)

type Liveness int

const (
	LivenessUnknown Liveness = iota
	LivenessAlive
	LivenessAbsent
)

// LinuxIdentityFromStat returns "proc:PID:starttime:T" from the text of
// /proc/PID/stat. T is field 22, i.e. index 19 of the fields after the last
// ')' (the command name may contain spaces and parentheses), and must be a
// positive integer.
func LinuxIdentityFromStat(pid int, stat string) (string, error) {
	p, err := linuxProcessFromStat(pid, stat)
	return p.identity, err
}

// linuxProcessFromStat reads the identity, the parent (field 4) and the
// starttime of a process from the text of its /proc/PID/stat.
func linuxProcessFromStat(pid int, stat string) (process, error) {
	malformed := fmt.Errorf("malformed /proc/%d/stat", pid)
	end := strings.LastIndexByte(stat, ')')
	if end < 0 {
		return process{}, malformed
	}
	fields := strings.Fields(stat[end+1:])
	// The suffix starts at proc field 3, so starttime (field 22) is index 19.
	if len(fields) <= 19 {
		return process{}, malformed
	}
	start := fields[19]
	ticks, err := strconv.ParseUint(start, 10, 64)
	if err != nil || ticks == 0 {
		return process{}, fmt.Errorf("/proc/%d/stat: invalid starttime %q", pid, start)
	}
	// An unreadable parent ends an ancestry walk; the identity stands.
	parent, _ := strconv.Atoi(fields[1])
	exited := fields[0] == "Z" || fields[0] == "X"
	return process{identity: fmt.Sprintf("proc:%d:starttime:%s", pid, start), parent: parent, start: ticks, exited: exited}, nil
}

// filetimeUnixEpoch is 1970-01-01 in FILETIME units (100 ns since 1601).
const filetimeUnixEpoch = 116444736000000000

// WindowsIdentityFromFiletime returns "pid:PID:start:NS", NS being the
// process creation FILETIME in Unix nanoseconds (as windows.Filetime's
// Nanoseconds computes it).
func WindowsIdentityFromFiletime(pid int, creation uint64) string {
	return fmt.Sprintf("pid:%d:start:%d", pid, (int64(creation)-filetimeUnixEpoch)*100)
}

// Lineage is what ties a process to the attempt that started it: the
// KAIRO_ATTEMPT_ID it carries (Linux; empty when it has none or its
// environment cannot be read) and the identities of its living ancestors,
// nearest first. The daemon decides from it whether the process is the work
// of the attempt holding the GPU it uses (store.ApplyObservationBatch).
type Lineage struct {
	AttemptID string
	Ancestors []string
}

// maxAncestors bounds an ancestry walk.
const maxAncestors = 64

// process is one process as an ancestry walk sees it: its identity, its
// parent's pid, its start (Linux starttime ticks, Windows creation FILETIME),
// comparable between the processes of one host, and whether it has exited
// (a Linux zombie, a Windows process whose object is still open).
type process struct {
	identity string
	parent   int
	start    uint64
	exited   bool
}

// lineage returns the lineage of process pid while it is still the one named
// identity (nothing otherwise). environ (nil where it is not read) is followed
// by a second read of the process, so that the environment is known to be
// that process's and not a later one's given the same pid.
func lineage(pid int, identity string, read func(int) (process, error), environ func(int) ([]byte, error)) Lineage {
	p, err := read(pid)
	if err != nil || p.exited || p.identity != identity {
		return Lineage{}
	}
	var l Lineage
	if environ != nil {
		body, err := environ(pid)
		if again, againErr := read(pid); err == nil && againErr == nil && again.identity == identity {
			l.AttemptID = attemptIDFromEnviron(body)
		}
	}
	l.Ancestors = ancestors(p, read)
	return l
}

// ancestors returns the identities of the living ancestors of p, nearest
// first, as far as read can tell. A parent that started after its child is a
// later process given a dead parent's pid (Windows keeps a child's parent pid
// after the parent exits), so the walk stops there, as it does at an exited
// parent and at one read cannot read.
func ancestors(p process, read func(int) (process, error)) []string {
	var out []string
	for len(out) < maxAncestors && p.parent > 0 {
		parent, err := read(p.parent)
		if err != nil || parent.exited || parent.start > p.start {
			break
		}
		out = append(out, parent.identity)
		p = parent
	}
	return out
}

// attemptIDFromEnviron returns KAIRO_ATTEMPT_ID from the NUL-separated text of
// /proc/PID/environ, or "".
func attemptIDFromEnviron(environ []byte) string {
	for _, kv := range strings.Split(string(environ), "\x00") {
		if value, ok := strings.CutPrefix(kv, "KAIRO_ATTEMPT_ID="); ok {
			return value
		}
	}
	return ""
}
