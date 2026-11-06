package executor

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"time"

	"kairo/internal/machine"
	"kairo/internal/store"
)

// Once an object's copy on a durable store is verified, the executor that
// packed it deletes what it was made from (store.DeletionsToMake): the run's
// output directory or the scratch, by the store's rules (removeKindDir: the
// directory derived from the root, its kind and its id, its marker naming
// it, nothing reached through a link), then the pack and its file list. The
// deletion is claimed first (store.SettleDeletion deleting) and settled
// deleted once done; one refused for safety is settled kept, with why; one
// that failed otherwise stays claimed and is tried again.
//
// An executor serving a durable store reads its verified copies back now and
// then (store.ScrubsToMake), around the cache, and reports what it read; a
// copy missing or unreadable is reported damaged. Nothing is deleted or
// rewritten on a durable store for it.

const (
	deleteInterval     = time.Minute
	deleteRoundTimeout = 6 * time.Hour
	scrubEvery         = 10 * time.Minute
	scrubRoundTimeout  = 12 * time.Hour
)

// round is a background round's state: when it last started, whether one
// runs, and the problems already warned of, by object.
type round struct {
	at       time.Time
	running  bool
	problems map[string]string
}

// startRound runs fn in the background, at most every interval, once the
// daemon has answered for stores, one round at a time.
func (e *Local) startRound(r *round, interval, timeout time.Duration, fn func(context.Context)) {
	e.mu.Lock()
	due := e.storesKnown && !r.running && time.Since(r.at) >= interval
	if due {
		r.running, r.at = true, time.Now()
	}
	e.mu.Unlock()
	if !due {
		return
	}
	go func() {
		defer func() {
			e.mu.Lock()
			r.running = false
			e.mu.Unlock()
		}()
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		fn(ctx)
	}()
}

// roundProblem warns of a round's problem once per object (again when it
// changes); an empty message clears it. objectID "" is the round itself.
func (e *Local) roundProblem(r *round, objectID, message string, err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if message == "" {
		delete(r.problems, objectID)
		return
	}
	text := message + ": " + err.Error()
	if r.problems[objectID] == text {
		return
	}
	if r.problems == nil {
		r.problems = map[string]string{}
	}
	r.problems[objectID] = text
	e.Logger.Warn(message, "executor_id", e.ID, "object_id", objectID, "error", err)
}

func (e *Local) makeDeletions() {
	e.startRound(&e.deletions, deleteInterval, deleteRoundTimeout, e.deleteDue)
}

func (e *Local) makeScrubs() {
	e.startRound(&e.scrubs, scrubEvery, scrubRoundTimeout, e.scrubDue)
}

// deleteDue deletes every source the daemon says is due on a usable store
// this executor serves at the ordered root.
func (e *Local) deleteDue(ctx context.Context) {
	orders, err := e.Store.DeletionsToMake(ctx, e.ID)
	if err != nil {
		e.roundProblem(&e.deletions, "", "deletions to make not read", err)
		return
	}
	e.roundProblem(&e.deletions, "", "", nil)
	served := map[string]servedStore{}
	for _, s := range e.servedStores() {
		served[s.decl.ID] = s
	}
	for _, o := range orders {
		s, ok := served[o.StoreID]
		kind, known := kindOf(o.Kind)
		if !known || !ok || !s.usable || s.decl.Root != o.Root {
			continue
		}
		settle := func(state, reason string) error {
			return e.Store.SettleDeletion(ctx, store.ObjectDeletion{ObjectID: o.ObjectID, ExecutorID: e.ID, State: state, Reason: reason})
		}
		if err := settle(store.SourceDeleting, ""); err != nil {
			e.roundProblem(&e.deletions, o.ObjectID, "source not claimed for deletion", err)
			continue
		}
		err := s.fs.RemoveKind(ctx, s.decl.Root, kind, kindPath(s.fs, s.decl.Root, kind, o.SourceID), o.SourceID, false)
		if err == nil {
			err = removePack(s.fs, s.decl.Root, o.ObjectID)
		}
		switch {
		case err == nil:
			err = settle(store.SourceDeleted, "")
		case errors.Is(err, errOutsideRoot), errors.Is(err, errScratchLink), errors.Is(err, errScratchMarker):
			err = settle(store.SourceKept, "not deleted: "+err.Error())
		}
		if err != nil {
			e.roundProblem(&e.deletions, o.ObjectID, "source not deleted", err)
			continue
		}
		e.roundProblem(&e.deletions, o.ObjectID, "", nil)
		e.Logger.Info("source deleted once copied", "object_id", o.ObjectID, "kind", o.Kind, "source_id", o.SourceID, "store_id", o.StoreID)
	}
}

// removePack removes an object's pack and file list from <root>/packs
// through handles (a link there is removed itself); ones already gone are no
// error.
func removePack(sfs storeFS, root, objectID string) error {
	if _, ok := sfs.(localStoreFS); !ok {
		return errPackNotHere
	}
	if !validScratchID(objectID) {
		return fmt.Errorf("%w: object %q", errOutsideRoot, objectID)
	}
	r, err := openDirPath(filepath.Clean(root), false)
	if err != nil {
		return err
	}
	defer r.Close()
	pr, err := openPlainDir(r, packsDirName)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer pr.Close()
	for _, name := range []string{objectID + packSuffix + manifestSuffix, objectID + packSuffix} {
		if err := pr.Remove(name); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}
	return nil
}

// scrubDue reads back every copy the daemon says is due on a usable durable
// store this executor serves at the ordered root, and reports each.
func (e *Local) scrubDue(ctx context.Context) {
	orders, err := e.Store.ScrubsToMake(ctx, e.ID)
	if err != nil {
		e.roundProblem(&e.scrubs, "", "copies to read back not read", err)
		return
	}
	e.roundProblem(&e.scrubs, "", "", nil)
	served := map[string]servedStore{}
	for _, s := range e.servedStores() {
		served[s.decl.ID] = s
	}
	for _, o := range orders {
		s, ok := served[o.StoreID]
		if !ok || !s.usable || s.decl.Class != machine.StoreDurable || s.decl.Root != o.Root {
			continue
		}
		if _, isLocal := s.fs.(localStoreFS); !isLocal {
			continue
		}
		result, reachable := readBack(s.decl.Root, o)
		if !reachable {
			// the store's root or objects directory is not there now: not
			// the copy's fault; asked again later
			continue
		}
		result.ExecutorID = e.ID
		if err := e.Store.RecordScrub(ctx, result); err != nil {
			e.roundProblem(&e.scrubs, o.ObjectID, "read back not recorded", err)
			continue
		}
		e.roundProblem(&e.scrubs, o.ObjectID, "", nil)
		if result.Problem != "" || result.SHA256 != o.PackSHA256 || result.Bytes != o.PackBytes || result.ManifestSHA256 != o.ManifestSHA256 {
			e.Logger.Warn("copy damaged", "object_id", o.ObjectID, "store_id", o.StoreID, "problem", result.Problem, "sha256", result.SHA256, "bytes", result.Bytes)
		}
	}
}

// readBack reads a copy's pack and list back around the cache. reachable is
// false when the store's objects directory cannot be opened at all (a drive
// away, say): nothing is known of the copy then.
func readBack(root string, o store.ScrubOrder) (store.ScrubResult, bool) {
	result := store.ScrubResult{ObjectID: o.ObjectID, StoreID: o.StoreID}
	if !validScratchID(o.ObjectID) || !machine.ValidMonth(o.Month) {
		result.Problem = "not a copy's name"
		return result, true
	}
	root = filepath.Clean(root)
	r, err := openDirPath(root, false)
	if err != nil {
		return result, false
	}
	defer r.Close()
	or, err := openPlainDir(r, objectsDirName)
	if err != nil {
		return result, false
	}
	defer or.Close()
	dir, err := openPlainDir(or, o.Month)
	if err != nil {
		result.Problem = "its month directory: " + err.Error()
		return result, true
	}
	defer dir.Close()
	dirPath := filepath.Join(root, objectsDirName, o.Month)
	packName := o.ObjectID + packSuffix
	if result.ManifestSHA256, _, err = hashUncached(dir, packName+manifestSuffix, filepath.Join(dirPath, packName+manifestSuffix)); err != nil {
		result.Problem = "its file list: " + err.Error()
		return result, true
	}
	if result.SHA256, result.Bytes, err = hashUncached(dir, packName, filepath.Join(dirPath, packName)); err != nil {
		result.Problem = "its pack: " + err.Error()
	}
	return result, true
}
