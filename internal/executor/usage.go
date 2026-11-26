package executor

import (
	"context"
	"errors"
	"time"

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
	// usageTimeout bounds one round: its samples and its reports.
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
		targets = append(targets, usageTarget{attemptID: attemptID, pid: u.pid, identity: u.identity})
	}
	e.usageRunning, e.usageSampled = true, time.Now()
	e.mu.Unlock()
	go func() {
		defer func() {
			e.mu.Lock()
			e.usageRunning = false
			e.mu.Unlock()
		}()
		ctx, cancel := context.WithTimeout(ctx, usageTimeout)
		defer cancel()
		sample := e.usageSampler
		if sample == nil {
			sample = e.backend().sample
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
			if streams := e.streamStatuses(t.attemptID); len(streams) > 0 {
				if err := e.Store.RecordAttemptStreams(ctx, t.attemptID, streams); err != nil && !errors.Is(err, store.ErrNotFound) {
					e.Logger.Warn("attempt streams not recorded", "attempt_id", t.attemptID, "error", err)
				}
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
