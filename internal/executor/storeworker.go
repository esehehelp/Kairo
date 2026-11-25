package executor

import (
	"sync"
	"time"
)

// storeWorker is an executor's work on its node's stores (README: Data
// stores): it knows the stores it serves and runs the rounds that keep them
// (packs, copies, fetches, deletions, evictions, read-backs, inventories,
// code-snapshot cleanups, container sweeps, scratch settlement and store
// reports). It holds the state of that work under a lock of its own; of its
// executor it uses only the identity, the coordinator, the logger and the
// backend, and asks what runs there (attemptRunsHere, runRunsHere), never
// reading the executor's attempts under its own lock.
//
// What a launch needs of the stores (the store an attempt's directories go
// on, a code snapshot unpacked) it asks the worker; the directories it makes
// for an attempt stay the executor's (scratch.go, outputs.go, confine.go).
type storeWorker struct {
	*Local

	mu sync.Mutex

	// The stores this executor serves (datastore.go), when they were last
	// asked for, whether the daemon ever answered (storesKnown) and whether
	// the last ask failed (warned once), when they were last reported and
	// whether a report runs.
	stores         []servedStore
	storesAt       time.Time
	storesKnown    bool
	storesWarned   bool
	storeReportAt  time.Time
	storeReporting bool

	// scratchSized is the size, measured once, of each scratch directory of
	// an attempt not running here, by attempt; when scratch was last settled
	// and whether a round runs; the problems already warned of, by attempt.
	scratchSized    map[string]int64
	scratchSettleAt time.Time
	scratchSettling bool
	scratchProblems map[string]string

	// outputSized is the size, measured once, of each run's output
	// directory none of whose attempts runs here, by run.
	outputSized map[string]int64

	// raws are the raw caches of the volatile stores, by store (rawcache.go).
	raws map[string]*rawCache

	// The rounds: packing (pack.go), copies to a durable store
	// (transfer.go), fetches (fetch.go), deleting copied sources and reading
	// durable copies back (retire.go), evictions (evict.go), durable
	// inventories (inventory.go), unpacked code snapshots no longer used
	// (source.go) and instances of attempts no longer running here
	// (container_linux.go).
	packs           round
	copies          round
	fetches         round
	deletions       round
	evictions       round
	scrubs          round
	inventories     round
	sourceCleanups  round
	containerSweeps round
}

// storeWork is the executor's store worker.
func (e *Local) storeWork() *storeWorker {
	e.swOnce.Do(func() { e.sw = &storeWorker{Local: e} })
	return e.sw
}

// attemptRunsHere reports whether attemptID runs on this executor (launched
// or adopted).
func (e *Local) attemptRunsHere(attemptID string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	_, ok := e.running[attemptID]
	return ok
}

// runRunsHere reports whether an attempt of run runID runs on this executor
// with the run's output directory.
func (e *Local) runRunsHere(runID string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, run := range e.outputRuns {
		if run == runID {
			return true
		}
	}
	return false
}

// sweepSoon has instances swept at the next tick (an attempt ended).
func (e *storeWorker) sweepSoon() {
	e.mu.Lock()
	e.containerSweeps.at = time.Time{}
	e.mu.Unlock()
}

// forgetOutputSize has a run's output directory measured again (an attempt
// of the run starts writing there).
func (e *storeWorker) forgetOutputSize(runID string) {
	e.mu.Lock()
	delete(e.outputSized, runID)
	e.mu.Unlock()
}
