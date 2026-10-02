package executor

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"kairo/internal/processidentity"
	"kairo/internal/store"
)

// processTree is what this executor knows of an attempt's processes: the
// launcher (pid 0 when it is gone or cannot be identified), the tree handle
// taken at launch (nil for an attempt launched by an earlier executor
// instance), and whether the work runs inside WSL.
type processTree struct {
	attemptID string
	pid       int
	identity  string
	handle    *treeHandle
	wsl       bool
	distro    string
}

// forceStopCheckInterval spaces out liveness checks of a signalled tree (a WSL
// check runs wsl.exe).
const forceStopCheckInterval = time.Second

// carryOutForceStops carries out the force stops (pause --force) of this
// executor's attempts: a graceful stop of the whole process tree first, a
// kill of everything left once the grace period the daemon reports is over.
// It acknowledges 'signalled' after the graceful stop and 'terminated' once no
// process of the tree is left. Quiescence and lease release still follow from
// reconcileQuiescence. Failures are logged, never fatal to the tick.
func (e *Local) carryOutForceStops(ctx context.Context) {
	orders, err := e.Store.ForceStopOrders(ctx, e.ID)
	if err != nil {
		if !e.forceOrdersFailing {
			e.Logger.Warn("cannot read force stop orders", "executor_id", e.ID, "error", err)
		}
		e.forceOrdersFailing = true
		return
	}
	e.forceOrdersFailing = false
	for _, o := range orders {
		t := e.attemptTree(o)
		switch o.State {
		case "pending":
			e.beginForceStop(ctx, o, t)
		case "signalled":
			e.finishForceStop(ctx, o, t)
		}
	}
}

func (e *Local) beginForceStop(ctx context.Context, o store.ForceStopOrder, t processTree) {
	detail := map[string]any{"executor_id": e.ID}
	alive, err := e.treeAlive(ctx, t)
	if err != nil {
		e.Logger.Warn("force stop: process liveness unknown; retrying", "attempt_id", o.AttemptID, "error", err)
		return
	}
	if alive {
		if err := e.stopTreeGracefully(ctx, t); err != nil {
			// Nothing will act on a graceful stop that was not delivered: kill now.
			detail["graceful_error"] = err.Error()
			e.Logger.Warn("force stop: graceful stop unavailable; killing the process tree", "attempt_id", o.AttemptID, "error", err)
			if err := e.killTree(ctx, t); err != nil {
				detail["kill_error"] = err.Error()
			}
			e.markForceKilled(o.AttemptID)
			detail["killed"] = true
		} else {
			e.Logger.Info("force stop: graceful stop sent", "attempt_id", o.AttemptID, "grace_seconds", o.GraceSeconds)
		}
	} else {
		detail["already_gone"] = true
	}
	if err := e.ackForceStop(ctx, o.AttemptID, "signalled", detail); err != nil {
		e.Logger.Warn("force stop acknowledgement failed", "attempt_id", o.AttemptID, "error", err)
	}
}

func (e *Local) finishForceStop(ctx context.Context, o store.ForceStopOrder, t processTree) {
	e.mu.Lock()
	if e.forceChecked == nil {
		e.forceChecked = map[string]time.Time{}
	}
	if time.Since(e.forceChecked[o.AttemptID]) < forceStopCheckInterval {
		e.mu.Unlock()
		return
	}
	e.forceChecked[o.AttemptID] = time.Now()
	killed := e.forceKilled[o.AttemptID]
	e.mu.Unlock()
	alive, err := e.treeAlive(ctx, t)
	if err != nil {
		e.Logger.Warn("force stop: process liveness unknown; retrying", "attempt_id", o.AttemptID, "error", err)
		return
	}
	if !alive {
		if err := e.ackForceStop(ctx, o.AttemptID, "terminated", map[string]any{"killed": killed}); err != nil {
			e.Logger.Warn("force stop acknowledgement failed", "attempt_id", o.AttemptID, "error", err)
			return
		}
		e.mu.Lock()
		delete(e.forceChecked, o.AttemptID)
		delete(e.forceKilled, o.AttemptID)
		e.mu.Unlock()
		e.Logger.Info("force stop: process tree gone", "attempt_id", o.AttemptID, "killed", killed)
		return
	}
	if o.KillAfterSeconds > 0 {
		return
	}
	if err := e.killTree(ctx, t); err != nil {
		e.Logger.Warn("force stop: kill failed; retrying", "attempt_id", o.AttemptID, "error", err)
	} else {
		e.Logger.Info("force stop: grace period over; process tree killed", "attempt_id", o.AttemptID)
	}
	e.markForceKilled(o.AttemptID)
}

func (e *Local) markForceKilled(attemptID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.forceKilled == nil {
		e.forceKilled = map[string]bool{}
	}
	e.forceKilled[attemptID] = true
}

func (e *Local) ackForceStop(ctx context.Context, attemptID, phase string, detail map[string]any) error {
	body, err := json.Marshal(detail)
	if err != nil {
		return err
	}
	err = e.Store.AckForceStop(ctx, attemptID, phase, body)
	if errors.Is(err, store.ErrNotFound) {
		return nil
	}
	return err
}

// attemptTree finds the attempt's launcher through this executor's own handle,
// or else through the recorded pid when its creation identity still matches
// (an executor restarted since the launch).
func (e *Local) attemptTree(o store.ForceStopOrder) processTree {
	t := processTree{attemptID: o.AttemptID, identity: deref(o.ProcessIdentity), wsl: e.Kind == "wsl2" || e.Attributes["environment"] == "wsl2", distro: e.Attributes["distro"]}
	e.mu.Lock()
	if cmd := e.running[o.AttemptID]; cmd != nil && cmd.Process != nil {
		t.pid = cmd.Process.Pid
	}
	t.handle = e.trees[o.AttemptID]
	e.mu.Unlock()
	if t.pid == 0 && o.PID != nil && o.ProcessIdentity != nil {
		if liveness, _ := processidentity.HostProcessLiveness(*o.PID, *o.ProcessIdentity); liveness == processidentity.LivenessAlive {
			t.pid = *o.PID
		}
	}
	return t
}

// releaseTree drops the tree handle of an attempt that has quiesced.
func (e *Local) releaseTree(attemptID string) {
	e.mu.Lock()
	h := e.trees[attemptID]
	delete(e.trees, attemptID)
	e.mu.Unlock()
	h.close()
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// launcherAlive reports whether the tree's launcher still runs.
func (t processTree) launcherAlive() (bool, error) {
	if t.pid == 0 {
		return false, nil
	}
	return launcherRunning(t.pid, t.identity)
}
