package executor

import (
	"context"
	"fmt"
	"time"

	"kairo/internal/store"
)

// An attempt's stdout and stderr are files in the executor's log directory,
// written by the attempt's processes themselves (they keep running when the
// executor restarts). The executor cannot see a write of theirs fail, so it
// guards the filesystem instead: it does not launch while that has less than
// the minimum free, and while its attempts run it records on each of them when
// the space falls below the minimum or runs out.

// DefaultMinLogFreeBytes is the free space the filesystem holding the attempt
// logs must have for an executor to launch (log_min_free_mib).
const DefaultMinLogFreeBytes = 1 << 30

// logSpaceExhaustedBelow is the free space below which writes to the logs
// fail, or are about to.
const logSpaceExhaustedBelow = 1 << 20

// defaultLogCheckInterval is how often a running executor checks the space
// left for its attempts' logs; freeSpaceTimeout bounds one check.
const (
	defaultLogCheckInterval = 15 * time.Second
	freeSpaceTimeout        = 10 * time.Second
)

func (e *Local) minLogFree() int64 {
	if e.MinLogFreeBytes > 0 {
		return e.MinLogFreeBytes
	}
	return DefaultMinLogFreeBytes
}

// logFree measures the free space of the filesystem holding the log
// directory, waiting at most freeSpaceTimeout. A measurement that has not
// returned by then (a hung network filesystem) is abandoned, and no other
// starts until it returns.
func (e *Local) logFree() (int64, error) {
	measure := e.FreeSpace
	if measure == nil {
		measure = diskFree
	}
	e.mu.Lock()
	stuck := e.freeSpaceStuck
	e.mu.Unlock()
	if stuck {
		return 0, fmt.Errorf("an earlier free space check of %s has not returned", e.LogDir)
	}
	type result struct {
		free int64
		err  error
	}
	done := make(chan result, 1)
	go func() {
		free, err := measure(e.LogDir)
		// Under the lock, so that the waiter either sees the result or has
		// already marked the check abandoned (and the mark is cleared here).
		e.mu.Lock()
		e.freeSpaceStuck = false
		done <- result{free, err}
		e.mu.Unlock()
	}()
	timer := time.NewTimer(freeSpaceTimeout)
	defer timer.Stop()
	select {
	case r := <-done:
		return r.free, r.err
	case <-timer.C:
		e.mu.Lock()
		defer e.mu.Unlock()
		select {
		case r := <-done:
			return r.free, r.err
		default:
		}
		e.freeSpaceStuck = true
		return 0, fmt.Errorf("free space check of %s did not return within %s", e.LogDir, freeSpaceTimeout)
	}
}

// checkLaunchLogSpace refuses a launch while the log filesystem has less than
// the minimum free: the attempt's output would be lost.
func (e *Local) checkLaunchLogSpace() error {
	free, err := e.logFree()
	if err != nil {
		return fmt.Errorf("the free space for attempt logs is unknown: %w", err)
	}
	if minimum := e.minLogFree(); free < minimum {
		return fmt.Errorf("the filesystem of the attempt log directory %s has %s free, below the minimum of %s (log_min_free_mib)", e.LogDir, formatBytes(free), formatBytes(minimum))
	}
	return nil
}

// logSpaceProblem is the problem free space means for running attempts' logs
// (nil: none).
func (e *Local) logSpaceProblem(free int64) *store.LogProblem {
	minimum := e.minLogFree()
	switch {
	case free < logSpaceExhaustedBelow:
		return &store.LogProblem{Problem: store.LogSpaceExhausted, Detail: fmt.Sprintf("%s has %s free; writes to the logs fail", e.LogDir, formatBytes(free))}
	case free < minimum:
		return &store.LogProblem{Problem: store.LogSpaceLow, Detail: fmt.Sprintf("%s has %s free, below the minimum of %s", e.LogDir, formatBytes(free), formatBytes(minimum))}
	}
	return nil
}

// reportLogProblem records a problem on an attempt once; a report the store
// did not take is made again at the next check.
func (e *Local) reportLogProblem(ctx context.Context, attemptID string, p store.LogProblem) {
	key := p.Problem + "/" + p.Stream
	e.mu.Lock()
	done := e.logProblems[attemptID][key]
	e.mu.Unlock()
	if done {
		return
	}
	if err := e.Store.RecordAttemptLogProblem(ctx, attemptID, p); err != nil {
		e.Logger.Warn("attempt log problem not recorded", "attempt_id", attemptID, "problem", p.Problem, "error", err)
		return
	}
	e.mu.Lock()
	if e.logProblems == nil {
		e.logProblems = map[string]map[string]bool{}
	}
	if e.logProblems[attemptID] == nil {
		e.logProblems[attemptID] = map[string]bool{}
	}
	e.logProblems[attemptID][key] = true
	e.mu.Unlock()
}

// watchLogSpace checks, every LogCheckInterval while attempts of this
// executor run, the space left for their logs, and records a shortage on each
// of them.
func (e *Local) watchLogSpace(ctx context.Context) {
	interval := e.LogCheckInterval
	if interval == 0 {
		interval = defaultLogCheckInterval
	}
	if time.Since(e.lastLogCheck) < interval {
		return
	}
	e.mu.Lock()
	attempts := make([]string, 0, len(e.running))
	for id := range e.running {
		attempts = append(attempts, id)
	}
	e.mu.Unlock()
	if len(attempts) == 0 {
		return
	}
	e.lastLogCheck = time.Now()
	free, err := e.logFree()
	if err != nil {
		e.Logger.Warn("attempt log space check failed", "executor_id", e.ID, "error", err)
		return
	}
	problem := e.logSpaceProblem(free)
	if problem == nil {
		return
	}
	e.Logger.Warn("attempt log filesystem short of space", "executor_id", e.ID, "problem", problem.Problem, "detail", problem.Detail)
	for _, id := range attempts {
		e.reportLogProblem(ctx, id, *problem)
	}
}

// checkLogSpaceAtExit records a shortage of log space seen as an attempt
// exits: a short run may end between two checks.
func (e *Local) checkLogSpaceAtExit(attemptID string) {
	free, err := e.logFree()
	if err != nil {
		e.Logger.Warn("attempt log space check failed", "attempt_id", attemptID, "error", err)
		return
	}
	if problem := e.logSpaceProblem(free); problem != nil {
		problem.Detail += " (at exit)"
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		e.reportLogProblem(ctx, attemptID, *problem)
	}
}

// forgetLogProblems drops what was reported for an attempt that has ended.
func (e *Local) forgetLogProblems(attemptID string) {
	e.mu.Lock()
	delete(e.logProblems, attemptID)
	e.mu.Unlock()
}

// formatBytes is 512 B, 12.0 MiB, 1.0 GiB.
func formatBytes(n int64) string {
	switch {
	case n < 1<<10:
		return fmt.Sprintf("%d B", n)
	case n < 1<<20:
		return fmt.Sprintf("%.1f KiB", float64(n)/(1<<10))
	case n < 1<<30:
		return fmt.Sprintf("%.1f MiB", float64(n)/(1<<20))
	default:
		return fmt.Sprintf("%.1f GiB", float64(n)/(1<<30))
	}
}
