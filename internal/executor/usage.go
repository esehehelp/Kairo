package executor

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"strconv"
	"strings"
	"syscall"
	"time"

	"kairo/internal/processidentity"
	"kairo/internal/store"
)

// Attempt usage: what the process tree of each attempt this executor runs
// takes up now, RAM in use and bytes written, for admission to count a lease's
// reservation once (store.RecordAttemptUsage). A round of samples runs beside
// the tick, at most every usageInterval and never two at once, so a slow or
// hung read cannot hold the tick. What cannot be read is reported as unknown
// (admission then counts the whole reservation as still to come), never
// guessed.

const (
	// usageInterval spaces out the rounds of usage samples.
	usageInterval = 15 * time.Second
	// usageTimeout bounds one round: its wsl.exe run and its reports.
	usageTimeout = 15 * time.Second
)

// usageState is what the executor keeps of a running attempt for sampling:
// its launcher and the most bytes written seen so far (a tree's live
// processes write less once some of them have exited).
type usageState struct {
	pid      int
	identity string
	written  int64
}

// usageTarget is one attempt to sample: its launcher (pid and creation
// identity) and, on Windows, a duplicate of its job object handle (closed
// after the round).
type usageTarget struct {
	attemptID string
	pid       int
	identity  string
	job       usageJob
}

// usageSample is what one attempt's tree uses; nil is not known.
type usageSample struct {
	ram, written *int64
}

// errProcessGone is a process that exited while it was being read: it is
// left out of its tree's totals.
var errProcessGone = errors.New("process is gone")

// trackUsage starts sampling a launched attempt.
func (e *Local) trackUsage(attemptID string, pid int, identity string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.usage == nil {
		e.usage = map[string]*usageState{}
	}
	e.usage[attemptID] = &usageState{pid: pid, identity: identity}
}

// untrackUsage stops sampling an attempt whose launcher has exited.
func (e *Local) untrackUsage(attemptID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.usage, attemptID)
}

// sampleUsage starts a round of usage samples of this executor's running
// attempts when one is due and none is running, and returns at once.
func (e *Local) sampleUsage(ctx context.Context) {
	e.mu.Lock()
	if e.usageRunning || len(e.usage) == 0 || time.Since(e.usageSampled) < usageInterval {
		e.mu.Unlock()
		return
	}
	targets := make([]usageTarget, 0, len(e.usage))
	for attemptID, u := range e.usage {
		targets = append(targets, usageTarget{attemptID: attemptID, pid: u.pid, identity: u.identity, job: duplicateJob(e.trees[attemptID])})
	}
	e.usageRunning, e.usageSampled = true, time.Now()
	e.mu.Unlock()
	go func() {
		defer func() {
			for _, t := range targets {
				t.job.close()
			}
			e.mu.Lock()
			e.usageRunning = false
			e.mu.Unlock()
		}()
		ctx, cancel := context.WithTimeout(ctx, usageTimeout)
		defer cancel()
		sample := e.usageSampler
		if sample == nil {
			sample = e.sampleTrees
		}
		samples := sample(ctx, targets)
		for _, t := range targets {
			s, tracked := e.withHighWater(t.attemptID, samples[t.attemptID])
			if !tracked {
				continue // exited during the round
			}
			if err := e.Store.RecordAttemptUsage(ctx, t.attemptID, s.ram, s.written); err != nil && !errors.Is(err, store.ErrNotFound) {
				e.Logger.Warn("attempt usage not recorded", "attempt_id", t.attemptID, "error", err)
			}
		}
	}()
}

// withHighWater replaces a sample's bytes written by the most seen so far for
// the attempt, and reports whether the attempt is still tracked.
func (e *Local) withHighWater(attemptID string, s usageSample) (usageSample, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	u := e.usage[attemptID]
	if u == nil {
		return s, false
	}
	if s.written != nil {
		u.written = max(u.written, *s.written)
		written := u.written
		s.written = &written
	}
	return s, true
}

// sumTree adds up what the processes of a tree use. A process that is gone is
// left out; one that cannot be read makes that total unknown. written may be
// nil when the total comes from elsewhere (a job object).
func sumTree(pids []int, ram, written func(int) (int64, error)) usageSample {
	var ramTotal, writtenTotal int64
	ramKnown, writtenKnown := true, written != nil
	for _, pid := range pids {
		if ramKnown {
			n, err := ram(pid)
			if errors.Is(err, errProcessGone) {
				continue
			}
			ramTotal, ramKnown = ramTotal+n, err == nil
		}
		if writtenKnown {
			n, err := written(pid)
			if errors.Is(err, errProcessGone) {
				continue
			}
			writtenTotal, writtenKnown = writtenTotal+n, err == nil
		}
	}
	var s usageSample
	if ramKnown {
		s.ram = &ramTotal
	}
	if writtenKnown {
		s.written = &writtenTotal
	}
	return s
}

// procEntry is one process of a /proc table.
type procEntry struct {
	identity string
	parent   int
	attempt  string // its KAIRO_ATTEMPT_ID, "" when it has none or it cannot be read
}

// sampleProcTrees samples attempts' trees from a Linux /proc (fsys): an
// attempt's processes are those carrying its KAIRO_ATTEMPT_ID (as force stop
// finds them) and the descendants of its launcher, while the launcher is
// still the process it launched. RAM is the sum of their PSS
// (smaps_rollup), bytes written of their write_bytes less
// cancelled_write_bytes (io); both need the processes to be the executor's
// user's, or the executor to run as root. A launcher that is gone with no
// process carrying the attempt's id gives an unknown sample.
func sampleProcTrees(fsys fs.FS, self int, targets []usageTarget) map[string]usageSample {
	procs := readProcTable(fsys, self)
	children := map[int][]int{}
	for pid, p := range procs {
		children[p.parent] = append(children[p.parent], pid)
	}
	out := map[string]usageSample{}
	for _, t := range targets {
		members := map[int]bool{}
		for pid, p := range procs {
			if p.attempt == t.attemptID {
				members[pid] = true
			}
		}
		if root, ok := procs[t.pid]; ok && root.identity == t.identity {
			visited := map[int]bool{}
			for stack := []int{t.pid}; len(stack) > 0; {
				pid := stack[len(stack)-1]
				stack = stack[:len(stack)-1]
				if !visited[pid] {
					visited[pid], members[pid] = true, true
					stack = append(stack, children[pid]...)
				}
			}
		}
		if len(members) == 0 {
			out[t.attemptID] = usageSample{}
			continue
		}
		pids := make([]int, 0, len(members))
		for pid := range members {
			pids = append(pids, pid)
		}
		out[t.attemptID] = sumTree(pids, func(pid int) (int64, error) {
			kb, err := procField(fsys, pid, "smaps_rollup", "Pss:")
			return kb * 1024, err
		}, func(pid int) (int64, error) {
			written, err := procField(fsys, pid, "io", "write_bytes:")
			if err != nil {
				return 0, err
			}
			cancelled, err := procField(fsys, pid, "io", "cancelled_write_bytes:")
			return max(written-cancelled, 0), err
		})
	}
	return out
}

// readProcTable reads the live processes of a /proc, but self (zombies have
// exited; a process that exits while being read is left out).
func readProcTable(fsys fs.FS, self int) map[int]procEntry {
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return nil
	}
	procs := map[int]procEntry{}
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid == self {
			continue
		}
		stat, err := fs.ReadFile(fsys, entry.Name()+"/stat")
		if err != nil {
			continue
		}
		identity, err := processidentity.LinuxIdentityFromStat(pid, string(stat))
		if err != nil {
			continue
		}
		// The fields after the command name: state, then the parent's pid.
		fields := strings.Fields(string(stat[bytes.LastIndexByte(stat, ')')+1:]))
		if fields[0] == "Z" || fields[0] == "X" {
			continue
		}
		parent, _ := strconv.Atoi(fields[1])
		p := procEntry{identity: identity, parent: parent}
		if environ, err := fs.ReadFile(fsys, entry.Name()+"/environ"); err == nil {
			for _, kv := range bytes.Split(environ, []byte{0}) {
				if value, ok := bytes.CutPrefix(kv, []byte("KAIRO_ATTEMPT_ID=")); ok {
					p.attempt = string(value)
					break
				}
			}
		}
		procs[pid] = p
	}
	return procs
}

// procField returns the number after key in /proc/PID/name, errProcessGone
// when the process has exited.
func procField(fsys fs.FS, pid int, name, key string) (int64, error) {
	body, err := fs.ReadFile(fsys, strconv.Itoa(pid)+"/"+name)
	if errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ESRCH) {
		return 0, errProcessGone
	}
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(body), "\n") {
		if fields := strings.Fields(line); len(fields) >= 2 && fields[0] == key {
			return strconv.ParseInt(fields[1], 10, 64)
		}
	}
	return 0, errors.New("/proc/" + strconv.Itoa(pid) + "/" + name + " has no " + key)
}
