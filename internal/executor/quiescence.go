package executor

import (
	"context"
	"time"

	"kairo/internal/store"
)

// leftBehindReportInterval spaces out the reports of one attempt whose lease
// is held for what it left running.
const leftBehindReportInterval = time.Minute

// leftBehind reports whether anything of an exited attempt's tree still runs
// although its registered processes are gone: the job object's processes
// (Windows), the launcher's process group (Linux, also for a process that
// dropped KAIRO_ATTEMPT_ID from its environment), or the processes carrying
// its KAIRO_ATTEMPT_ID (inside WSL too). Its launcher has exited, so only
// those count.
func (e *Local) leftBehind(ctx context.Context, attempt store.Attempt) (bool, error) {
	wsl := e.Kind == "wsl2" || e.Attributes["environment"] == "wsl2"
	t := processTree{attemptID: attempt.ID, wsl: wsl, distro: e.Attributes["distro"]}
	if !wsl {
		// For WSL the work is the Linux processes, found by their
		// KAIRO_ATTEMPT_ID; the Windows side of wsl.exe is not counted (its
		// helpers may outlive it without running anything of the attempt).
		e.mu.Lock()
		t.handle = e.trees[attempt.ID]
		e.mu.Unlock()
		if attempt.PID != nil {
			t.group = *attempt.PID
		}
		if attempt.ProcessIdentity != nil {
			t.identity = *attempt.ProcessIdentity
		}
	}
	left, err := e.treeAlive(ctx, t)
	if err == nil && !left {
		e.mu.Lock()
		delete(e.leftReported, attempt.ID)
		e.mu.Unlock()
	}
	return left, err
}

// warnLeftBehind reports, at most every leftBehindReportInterval, that an
// attempt's lease is kept for processes it left (or whose absence is unknown).
func (e *Local) warnLeftBehind(attemptID string, err error) {
	e.mu.Lock()
	if e.leftReported == nil {
		e.leftReported = map[string]time.Time{}
	}
	last, seen := e.leftReported[attemptID]
	due := !seen || time.Since(last) >= leftBehindReportInterval
	if due {
		e.leftReported[attemptID] = time.Now()
	}
	e.mu.Unlock()
	if !due {
		return
	}
	if err != nil {
		e.Logger.Warn("liveness of what the attempt left is unknown; retaining lease", "attempt_id", attemptID, "error", err)
		return
	}
	e.Logger.Warn("the attempt's registered processes exited but processes it started still run; retaining lease", "attempt_id", attemptID)
}
