package executor

import (
	"context"
	"errors"
	"fmt"
	"time"

	"kairo/internal/machine"
	"kairo/internal/store"
)

// An attempt launched by an executor that serves a usable volatile store
// gets a scratch directory there, <root>/scratch/<attempt id> holding the
// marker .kairo-scratch, recorded at the daemon and named to the attempt by
// KAIRO_SCRATCH (and, for an execution with scratch_tmpdir, by TMPDIR, TMP
// and TEMP). Without such a store, or when making or recording it fails,
// the attempt starts without one. Once the attempt has quiesced or was lost
// the executor settles its scratch as the daemon says
// (store.ScratchToSettle): it deletes it (removeScratch's rules), keeps it
// (measured once), or leaves it untouched (kept, not even measured: the
// declaration no longer makes its path a volatile store's scratch). A
// deletion refused for safety (not the recorded directory, a link, a marker
// naming another attempt) keeps the scratch, saying why; one that failed
// otherwise is tried again.

const (
	// scratchSettleInterval: how often the scratch to settle is asked for.
	scratchSettleInterval = 10 * time.Second
	// scratchCallTimeout bounds making and recording a scratch at launch.
	scratchCallTimeout = 2 * time.Minute
	// scratchSettleTimeout bounds one settling round (deleting trees).
	scratchSettleTimeout = time.Hour
)

// scratchStore is the store an attempt's scratch is made on: the first
// usable volatile store this executor serves (by id).
func (e *Local) scratchStore() (servedStore, bool) {
	for _, s := range e.servedStores() {
		if s.usable && s.decl.Class == machine.StoreVolatile {
			return s, true
		}
	}
	return servedStore{}, false
}

// makeScratch makes and records the scratch of an attempt about to start
// and keeps its path for the attempt's environment (scratchDir). Without
// one (no volatile store, or making or recording it failed, which is
// logged) the attempt starts all the same.
func (e *Local) makeScratch(launch *store.Launch) {
	s, ok := e.scratchStore()
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), scratchCallTimeout)
	defer cancel()
	attemptID := launch.Attempt.ID
	dir, err := s.fs.Create(ctx, s.decl.Root, attemptID)
	if err != nil {
		e.Logger.Warn("attempt starts without scratch: not made", "attempt_id", attemptID, "store_id", s.decl.ID, "error", err)
		return
	}
	record := store.AttemptScratch{AttemptID: attemptID, LeaseID: launch.Lease.ID, Epoch: launch.Lease.CoordinationEpoch,
		StoreID: s.decl.ID, ExecutorID: e.ID, Root: s.decl.Root, Path: dir}
	if err = e.retryUnanswered(ctx, func() error { return e.Store.RecordAttemptScratch(ctx, record) }); err != nil {
		e.Logger.Warn("attempt starts without scratch: not recorded", "attempt_id", attemptID, "store_id", s.decl.ID, "error", err)
		// Nothing has run in it: it goes now (and, were it recorded after
		// all, its settlement finds it gone).
		if removeErr := s.fs.Remove(ctx, s.decl.Root, dir, attemptID, false); removeErr != nil {
			e.Logger.Warn("unrecorded scratch not removed", "attempt_id", attemptID, "path", dir, "error", removeErr)
		}
		return
	}
	e.mu.Lock()
	if e.scratch == nil {
		e.scratch = map[string]string{}
	}
	e.scratch[attemptID] = dir
	e.mu.Unlock()
}

// scratchDir is the scratch made for an attempt this instance is launching
// ("" for none).
func (e *Local) scratchDir(attemptID string) string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.scratch[attemptID]
}

// forgetScratch drops an attempt's scratch path once its environment is
// built.
func (e *Local) forgetScratch(attemptID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.scratch, attemptID)
}

// scratchEnv is what an attempt is told of its scratch: KAIRO_SCRATCH, and
// with scratch_tmpdir its temporary directory too (TMPDIR, plus TMP and
// TEMP except for a WSL attempt, whose variables are also set on wsl.exe,
// a Windows program that must not get a Linux path as its TMP).
func (e *Local) scratchEnv(launch *store.Launch) []string {
	dir := e.scratchDir(launch.Attempt.ID)
	if dir == "" {
		return nil
	}
	env := []string{"KAIRO_SCRATCH=" + dir}
	if launch.ScratchTmpdir {
		env = append(env, "TMPDIR="+dir)
		if !e.isWSL() {
			env = append(env, "TMP="+dir, "TEMP="+dir)
		}
	}
	return env
}

// settleScratch settles, in the background, this executor's scratch that
// is due (at most every scratchSettleInterval), once the daemon has
// answered for stores: one older than stores knows no scratch.
func (e *Local) settleScratch() {
	e.mu.Lock()
	due := e.storesKnown && !e.scratchSettling && time.Since(e.scratchSettleAt) >= scratchSettleInterval
	if due {
		e.scratchSettling = true
		e.scratchSettleAt = time.Now()
	}
	e.mu.Unlock()
	if !due {
		return
	}
	go func() {
		defer func() {
			e.mu.Lock()
			e.scratchSettling = false
			e.mu.Unlock()
		}()
		ctx, cancel := context.WithTimeout(context.Background(), scratchSettleTimeout)
		defer cancel()
		e.settleDueScratch(ctx)
	}()
}

// settleDueScratch settles every scratch the daemon says is due. A failure
// is warned once per scratch (and again when it changes) and tried again
// at the next round.
func (e *Local) settleDueScratch(ctx context.Context) {
	due, err := e.Store.ScratchToSettle(ctx, e.ID)
	if err != nil {
		e.scratchProblem("", "scratch to settle not read", err)
		return
	}
	e.scratchProblem("", "", nil)
	fs := e.fsForStores()
	for _, k := range due {
		settled, err := settleOnDisk(ctx, fs, k)
		if err == nil {
			err = e.Store.SettleScratch(ctx, settled)
		}
		if err != nil {
			e.scratchProblem(k.AttemptID, "scratch not settled", err)
			continue
		}
		e.scratchProblem(k.AttemptID, "", nil)
		e.Logger.Info("scratch "+settled.State, "attempt_id", k.AttemptID, "path", k.Path, "reason", k.Reason)
	}
}

// settleOnDisk carries out a settlement and says what became of the
// scratch; an error leaves it to be tried again.
func settleOnDisk(ctx context.Context, fs storeFS, k store.ScratchSettlement) (store.ScratchSettled, error) {
	settled := store.ScratchSettled{AttemptID: k.AttemptID}
	keep := func(reason string) (store.ScratchSettled, error) {
		settled.State, settled.Reason = "kept", reason
		if size, err := fs.Size(ctx, k.Path); err == nil {
			settled.Bytes = &size
		}
		return settled, nil
	}
	switch k.Action {
	case store.ScratchLeave:
		settled.State = "kept"
		return settled, nil
	case store.ScratchKeep:
		return keep("")
	case store.ScratchDelete:
	default:
		return settled, fmt.Errorf("unknown scratch action %q", k.Action)
	}
	err := fs.Remove(ctx, k.Root, k.Path, k.AttemptID, false)
	switch {
	case err == nil:
		settled.State = "deleted"
		return settled, nil
	case errors.Is(err, errOutsideRoot), errors.Is(err, errScratchLink), errors.Is(err, errScratchMarker):
		return keep(k.Reason + "; not deleted: " + err.Error())
	}
	return settled, err
}

// scratchProblem warns of a scratch problem once (again when it changes);
// an empty message clears it. attemptID "" is the round itself.
func (e *Local) scratchProblem(attemptID, message string, err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if message == "" {
		delete(e.scratchProblems, attemptID)
		return
	}
	text := message + ": " + err.Error()
	if e.scratchProblems[attemptID] == text {
		return
	}
	if e.scratchProblems == nil {
		e.scratchProblems = map[string]string{}
	}
	e.scratchProblems[attemptID] = text
	e.Logger.Warn(message, "executor_id", e.ID, "attempt_id", attemptID, "error", err)
}
