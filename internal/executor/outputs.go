package executor

import (
	"context"
	"errors"

	"kairo/internal/store"
)

// An attempt launched by an executor that serves a usable volatile store
// gets its run's output directory there (the store its scratch is made on):
// <root>/outputs/<run id> (store.Launch.RunID) holding the marker
// .kairo-output, recorded at the daemon (store.RecordRunOutput) and named to
// the attempt by KAIRO_OUTPUT_DIR. Every attempt of the run launched on that
// store gets the same directory: the first makes it, the later ones find it
// (a continuation finds the checkpoint its run wrote before). Without such a
// store, without a run (a daemon older than output directories), or when
// making or recording it fails (logged), the attempt starts without one; a
// directory made but not recorded is left as it is (it is counted as
// unrecorded, and a later attempt of the run records it), unless the daemon
// refused it for good (the run's directory is packed already, or the attempt
// is no longer to launch): then one holding only its marker, just made, is
// removed again. Nothing else here deletes an output directory.

// makeOutput makes (or finds) and records the output directory of an attempt
// about to start and keeps its path for the attempt's environment.
func (e *Local) makeOutput(launch *store.Launch) {
	if launch.RunID == "" {
		return
	}
	s, ok := e.storeFor(launch)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), scratchCallTimeout)
	defer cancel()
	attemptID := launch.Attempt.ID
	dir, err := s.fs.CreateKind(ctx, s.decl.Root, kindOutput, launch.RunID)
	if err != nil {
		e.Logger.Warn("attempt starts without an output directory: not made", "attempt_id", attemptID, "run_id", launch.RunID, "store_id", s.decl.ID, "error", err)
		return
	}
	record := store.RunOutput{AttemptID: attemptID, LeaseID: launch.Lease.ID, Epoch: launch.Lease.CoordinationEpoch,
		RunID: launch.RunID, StoreID: s.decl.ID, ExecutorID: e.ID, Root: s.decl.Root, Path: dir}
	if err = e.retryUnanswered(ctx, func() error { return e.Store.RecordRunOutput(ctx, record) }); err != nil {
		e.Logger.Warn("attempt starts without an output directory: not recorded", "attempt_id", attemptID, "run_id", launch.RunID, "path", dir, "error", err)
		if errors.Is(err, store.ErrStaleEpoch) {
			_ = s.fs.RemoveKind(ctx, s.decl.Root, kindOutput, dir, launch.RunID, true)
		}
		return
	}
	e.mu.Lock()
	if e.outputs == nil {
		e.outputs = map[string]string{}
	}
	if e.outputRuns == nil {
		e.outputRuns = map[string]string{}
	}
	e.outputs[attemptID] = dir
	e.outputRuns[attemptID] = launch.RunID
	// written again: measured again
	delete(e.outputSized, launch.RunID)
	e.mu.Unlock()
}

// outputEnv is what an attempt is told of its output directory:
// KAIRO_OUTPUT_DIR, when it has one. The path is dropped once the
// environment is built (the run is kept for measuring, outputSize).
func (e *Local) outputEnv(launch *store.Launch) []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	dir := e.outputs[launch.Attempt.ID]
	delete(e.outputs, launch.Attempt.ID)
	if dir == "" {
		return nil
	}
	return []string{"KAIRO_OUTPUT_DIR=" + dir}
}

// outputSize measures a run's output directory: at every report while an
// attempt of the run runs on this executor, once per executor instance
// otherwise (nothing writes there then). ok is false when it could not be
// measured now.
func (e *Local) outputSize(ctx context.Context, s servedStore, runID string) (int64, bool) {
	e.mu.Lock()
	running := false
	for _, run := range e.outputRuns {
		running = running || run == runID
	}
	size, measured := e.outputSized[runID]
	e.mu.Unlock()
	if measured && !running {
		return size, true
	}
	size, err := s.fs.Size(ctx, kindPath(s.fs, s.decl.Root, kindOutput, runID))
	if err != nil {
		return 0, false
	}
	if !running {
		e.mu.Lock()
		if e.outputSized == nil {
			e.outputSized = map[string]int64{}
		}
		e.outputSized[runID] = size
		e.mu.Unlock()
	}
	return size, true
}
