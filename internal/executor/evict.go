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

// Evicting (store/evictions.go): when a volatile store this executor serves
// runs short of room, the daemon orders deleted from it what can be had
// again, each already claimed (evicting): an object brought there
// (<root>/inputs/<object id>, removed through the inputs directory's handle;
// it can be brought again from its durable copy), or temporal data (a run's
// output directory by the store's rules, as a copied source is deleted, or an
// imported directory through its parent's handle; it can be made again by
// its task), the latter only while it holds just what its identity lists
// (checkPacked, checkPackedDir: each regular file listed with the size and
// mtime it was identified with, nothing else but directories) and, for a
// run's output, no attempt this executor launched still runs in it. Each is
// settled evicted, or kept with why (changed since, not Kairo's, in use);
// one that failed otherwise (unreadable now) stays claimed and is tried
// again later.

const (
	evictInterval     = time.Minute
	evictRoundTimeout = 6 * time.Hour
)

// errEvictInUse: an attempt this executor launched still runs in a temporal
// run output directory.
var errEvictInUse = errors.New("an attempt still runs in it")

func (e *Local) makeEvictions() {
	e.startRound(&e.evictions, evictInterval, evictRoundTimeout, e.evictDue)
}

// evictDue evicts every object the daemon orders from a usable volatile
// store this executor serves at the ordered root, and settles each.
func (e *Local) evictDue(ctx context.Context) {
	orders, err := e.Store.EvictionsToMake(ctx, e.ID)
	if err != nil {
		e.roundProblem(&e.evictions, "", "evictions to make not read", err)
		return
	}
	e.roundProblem(&e.evictions, "", "", nil)
	served := map[string]servedStore{}
	for _, s := range e.servedStores() {
		served[s.decl.ID] = s
	}
	for _, o := range orders {
		s, ok := served[o.StoreID]
		if !ok || !s.usable || s.decl.Class != machine.StoreVolatile || s.decl.Root != o.Root || !e.due(&e.evictions, o.ObjectID) {
			continue
		}
		if _, isLocal := s.fs.(localStoreFS); !isLocal {
			continue
		}
		switch o.Kind {
		case store.EvictPlacement:
			err = removeBrought(s.decl.Root, o.ObjectID)
		case store.EvictTemporal:
			err = e.evictTemporal(ctx, s, o)
		default:
			continue
		}
		settled := store.EvictionSettled{ObjectID: o.ObjectID, Kind: o.Kind, StoreID: o.StoreID, ExecutorID: e.ID, Evicted: true}
		switch {
		case err == nil:
			err = e.Store.SettleEviction(ctx, settled)
		case errors.Is(err, errOutsideRoot), errors.Is(err, errScratchLink), errors.Is(err, errScratchMarker), errors.Is(err, errSourceChanged), errors.Is(err, errEvictInUse):
			settled.Evicted, settled.Reason = false, "not evicted: "+err.Error()
			err = e.Store.SettleEviction(ctx, settled)
		}
		if err != nil {
			e.roundProblem(&e.evictions, o.ObjectID, "not evicted", err)
			continue
		}
		e.roundProblem(&e.evictions, o.ObjectID, "", nil)
		if !settled.Evicted {
			e.Logger.Warn("kept instead of evicted", "object_id", o.ObjectID, "kind", o.Kind, "store_id", o.StoreID, "reason", settled.Reason)
			continue
		}
		e.Logger.Info("evicted for room", "object_id", o.ObjectID, "kind", o.Kind, "store_id", o.StoreID, "bytes", o.Bytes)
	}
}

// evictTemporal deletes a temporal object's directory (see above), then its
// identity's file list.
func (e *Local) evictTemporal(ctx context.Context, s servedStore, o store.EvictionOrder) error {
	root := s.decl.Root
	if o.ObjectKind == store.ObjectImport {
		err := checkPackedDir(ctx, root, o.Path, o.ObjectID)
		if err == nil {
			err = onSameFilesystem(o.Path, root)
		}
		if err == nil {
			err = removeImportedDir(o.Path)
		}
		if err == nil {
			err = removePack(s.fs, root, o.ObjectID)
		}
		return err
	}
	kind, known := kindOf(o.ObjectKind)
	if !known || kind.dir != kindOutput.dir {
		return fmt.Errorf("%w: a temporal object of kind %q", errOutsideRoot, o.ObjectKind)
	}
	if e.inUse(kind, o.SourceID) {
		return errEvictInUse
	}
	err := checkPacked(ctx, root, kind, o.SourceID, o.ObjectID)
	if err == nil {
		err = s.fs.RemoveKind(ctx, root, kind, kindPath(s.fs, root, kind, o.SourceID), o.SourceID, false)
	}
	if err == nil {
		err = removePack(s.fs, root, o.ObjectID)
	}
	return err
}

// removeBrought removes <root>/inputs/<object id> through the inputs
// directory's handle: a plain directory (a link there is refused), with what
// is under it (a link under it removed itself). One gone is no error.
func removeBrought(root, objectID string) error {
	if !validScratchID(objectID) {
		return fmt.Errorf("%w: object %q", errOutsideRoot, objectID)
	}
	r, err := openDirPath(filepath.Clean(root), false)
	if err != nil {
		return err
	}
	defer r.Close()
	dir, err := openPlainDir(r, inputsDirName)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer dir.Close()
	fi, err := dir.Lstat(objectID)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if isLink(fi) || !fi.IsDir() {
		return fmt.Errorf("%w: %s", errScratchLink, filepath.Join(root, inputsDirName, objectID))
	}
	return dir.RemoveAll(objectID)
}
