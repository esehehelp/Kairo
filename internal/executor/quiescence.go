package executor

import (
	"context"
	"time"

	"kairo/internal/store"
)

// leftBehindReportInterval spaces out the reports of one attempt whose lease
// is held for what it left running.
const leftBehindReportInterval = time.Minute

// What an exited attempt left running is not its work any more: its launcher
// exited and its exit is recorded, so no one reads what the rest does. It is
// asked to stop (SIGTERM; inside WSL too; on Windows CTRL_BREAK, which no
// longer reaches it once its launcher is gone) once it has been seen for
// leftoverGrace, and killed (the job object, the process group, the
// processes carrying the attempt's KAIRO_ATTEMPT_ID) leftoverKillAfter after
// that. Only an attempt whose exit was recorded is a candidate: one lost
// with an agent restart is not touched. Variables for tests.
var (
	leftoverGrace     = 10 * time.Second
	leftoverKillAfter = 30 * time.Second
)

// leftover is how far ending what an attempt left has gone.
type leftover struct {
	seen, asked time.Time // first seen; asked to stop (zero: not yet)
	killed      bool
}

// leftBehind reports whether anything of an exited attempt's tree still runs
// although its registered processes are gone: the job object's processes
// (Windows), the launcher's process group (Linux, also for a process that
// dropped KAIRO_ATTEMPT_ID from its environment), or the processes carrying
// its KAIRO_ATTEMPT_ID (inside WSL too). Its launcher has exited, so only
// those count.
func (e *Local) leftBehind(ctx context.Context, attempt store.Attempt) (bool, error) {
	left, err := e.treeAlive(ctx, e.exitedTree(attempt))
	if err == nil && !left {
		e.mu.Lock()
		delete(e.leftReported, attempt.ID)
		delete(e.exitedLeftovers, attempt.ID)
		e.mu.Unlock()
	}
	return left, err
}

// exitedTree is what this executor can reach of an exited attempt's tree.
func (e *Local) exitedTree(attempt store.Attempt) processTree {
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
	return t
}

// endLeftBehind ends what an exited attempt left running (see leftoverGrace),
// one step per call: noted when first seen, asked to stop once it has been
// seen for leftoverGrace, killed leftoverKillAfter after that (and again at
// each later call while anything is left).
func (e *Local) endLeftBehind(ctx context.Context, attempt store.Attempt) {
	now := time.Now()
	e.mu.Lock()
	if e.exitedLeftovers == nil {
		e.exitedLeftovers = map[string]leftover{}
	}
	l, known := e.exitedLeftovers[attempt.ID]
	if !known {
		e.exitedLeftovers[attempt.ID] = leftover{seen: now}
	}
	e.mu.Unlock()
	if !known {
		return
	}
	t := e.exitedTree(attempt)
	switch {
	case l.asked.IsZero() && now.Sub(l.seen) >= leftoverGrace:
		err := e.stopTreeGracefully(ctx, t)
		e.Logger.Info("asking what the exited attempt left running to stop", "attempt_id", attempt.ID, "error", err)
		l.asked = now
	case !l.asked.IsZero() && now.Sub(l.asked) >= leftoverKillAfter:
		err := e.killTree(ctx, t)
		if !l.killed {
			e.Logger.Warn("killed what the exited attempt left running", "attempt_id", attempt.ID, "error", err)
		}
		l.killed = true
	default:
		return
	}
	e.mu.Lock()
	e.exitedLeftovers[attempt.ID] = l
	e.mu.Unlock()
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
