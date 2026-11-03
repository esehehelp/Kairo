package executor

import (
	"context"
	"sort"
	"time"

	"kairo/internal/machine"
	"kairo/internal/store"
)

// An executor serves the stores declared on its node in its namespace: a
// store naming it as executor, else (for an executor that is not WSL) a
// store naming none. It makes each root if missing and checks it is on the
// declared filesystem (a store that fails is not used, and is reported
// unusable), and reports each one's filesystem total and free and the bytes
// of the Kairo scratch directories under its root. A daemon without stores
// (404) leaves the executor without any, warned once.

const (
	// storeRefreshInterval: how often the node's stores are asked for again.
	storeRefreshInterval = 5 * time.Minute
	// storeReportInterval: how often what the stores hold is reported.
	storeReportInterval = 5 * time.Minute
	// storeReportTimeout bounds one report round (sizing scratch trees).
	storeReportTimeout = 30 * time.Minute
)

// servedStore is a store this executor serves.
type servedStore struct {
	decl    machine.Store
	fs      storeFS
	usable  bool
	problem string
}

func (e *Local) isWSL() bool { return e.Kind == "wsl2" || e.Attributes["environment"] == "wsl2" }

// fsForStores is how this executor reaches its stores' roots.
func (e *Local) fsForStores() storeFS {
	if e.storeFSOverride != nil {
		return e.storeFSOverride
	}
	if e.isWSL() {
		return wslStoreFS{distro: e.Attributes["distro"]}
	}
	return localStoreFS{}
}

// serves reports whether a store of this executor's node is in its
// namespace.
func (e *Local) serves(s machine.Store) bool {
	if s.Executor != "" {
		return s.Executor == e.ID
	}
	return !e.isWSL()
}

// refreshStores asks for the node's stores (at most every
// storeRefreshInterval) and makes ready the ones this executor serves.
func (e *Local) refreshStores(ctx context.Context) {
	if !e.storesAt.IsZero() && time.Since(e.storesAt) < storeRefreshInterval {
		return
	}
	e.storesAt = time.Now()
	decls, err := e.Store.NodeStores(ctx, e.NodeID)
	if err != nil {
		if !e.storesWarned {
			e.Logger.Warn("cannot read this node's stores (a daemon older than stores answers 404); running without stores and scratch", "executor_id", e.ID, "error", err)
			e.storesWarned = true
		}
		return
	}
	e.storesWarned = false
	sort.Slice(decls, func(i, j int) bool { return decls[i].ID < decls[j].ID })
	fs := e.fsForStores()
	var served []servedStore
	for _, decl := range decls {
		if !e.serves(decl) {
			continue
		}
		s := servedStore{decl: decl.Normalized(), fs: fs, usable: true}
		if err := fs.Ensure(ctx, decl.Root, decl.Filesystem); err != nil {
			s.usable, s.problem = false, err.Error()
			e.Logger.Warn("store not usable", "executor_id", e.ID, "store_id", decl.ID, "root", decl.Root, "error", err)
		}
		served = append(served, s)
	}
	e.mu.Lock()
	e.stores = served
	e.storesKnown = true
	e.mu.Unlock()
	e.storeReportAt = time.Time{} // report what is new soon
}

// servedStores is a copy of the stores this executor serves.
func (e *Local) servedStores() []servedStore {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]servedStore(nil), e.stores...)
}

// reportStores reports every served store (at most every
// storeReportInterval), in the background: sizing scratch trees may take a
// while and must not hold the tick.
func (e *Local) reportStores() {
	e.mu.Lock()
	due := !e.storeReporting && time.Since(e.storeReportAt) >= storeReportInterval && len(e.stores) != 0
	if due {
		e.storeReporting = true
		e.storeReportAt = time.Now()
	}
	stores := append([]servedStore(nil), e.stores...)
	e.mu.Unlock()
	if !due {
		return
	}
	go func() {
		defer func() {
			e.mu.Lock()
			e.storeReporting = false
			e.mu.Unlock()
		}()
		ctx, cancel := context.WithTimeout(context.Background(), storeReportTimeout)
		defer cancel()
		for _, s := range stores {
			obs := e.observeStore(ctx, s)
			if err := e.Store.RecordStoreObservation(ctx, obs); err != nil {
				e.Logger.Warn("store observation not recorded", "executor_id", e.ID, "store_id", s.decl.ID, "error", err)
			}
		}
	}()
}

// observeStore is what a served store holds now.
func (e *Local) observeStore(ctx context.Context, s servedStore) store.StoreObservation {
	obs := store.StoreObservation{StoreID: s.decl.ID, ExecutorID: e.ID, Usable: s.usable, Problem: s.problem}
	if !s.usable {
		return obs
	}
	if total, free, err := s.fs.Usage(ctx, s.decl.Root); err == nil {
		obs.TotalBytes, obs.FreeBytes = &total, &free
	} else {
		obs.Problem = "usage: " + err.Error()
	}
	started := time.Now()
	ids, err := s.fs.List(ctx, s.decl.Root)
	if err != nil {
		if obs.Problem == "" {
			obs.Problem = "scratch: " + err.Error()
		}
		return obs
	}
	obs.Scratch = map[string]int64{}
	for _, id := range ids {
		if size, ok := e.scratchSize(ctx, s, id); ok {
			obs.Scratch[id] = size
		} else {
			obs.Scratch[id] = -1 // found, not measured this time
		}
	}
	// The whole of <root>/scratch was read (and the report is not cut by its
	// deadline): a record not found is missing.
	obs.ScratchScanned = ctx.Err() == nil
	obs.ScanSeconds = time.Since(started).Seconds()
	return obs
}

// scratchSize measures a scratch directory: an attempt running here at every
// report, any other (kept, or not yet settled) once per executor instance,
// nothing being written there any more. ok is false when it could not be
// measured now.
func (e *Local) scratchSize(ctx context.Context, s servedStore, id string) (int64, bool) {
	e.mu.Lock()
	_, running := e.running[id]
	size, measured := e.scratchSized[id]
	e.mu.Unlock()
	if measured && !running {
		return size, true
	}
	size, err := s.fs.Size(ctx, scratchPath(s.fs, s.decl.Root, id))
	if err != nil {
		return 0, false
	}
	if !running {
		e.mu.Lock()
		if e.scratchSized == nil {
			e.scratchSized = map[string]int64{}
		}
		e.scratchSized[id] = size
		e.mu.Unlock()
	}
	return size, true
}
