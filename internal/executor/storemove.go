package executor

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"kairo/internal/machine"
)

// Moving a store (store/stores_moved.go): a store declared with moved_from
// is renamed by the executor serving it from that root to its declared one,
// once the daemon says nothing uses it there any more (MoveWait empty), and
// only by a rename (one filesystem: nothing is copied). A marker written
// into the old root before the rename and found in the new one after says
// it is this store moved there (and not a directory made there otherwise);
// the observation then says so (MovedFrom) and the daemon records the store
// there. Until then the store is not usable.

const (
	// storeMoveMarker is the marker a store being moved carries at its root.
	storeMoveMarker = ".kairo-store-move"
	storeMoveHeader = "kairo store move 1"
)

// storeMoveMarkerBody is the marker of decl's move.
func storeMoveMarkerBody(decl machine.Store) string {
	return fmt.Sprintf("%s\nstore\t%s\nfrom\t%s\nto\t%s\n", storeMoveHeader, decl.ID, machine.StoreRootKey(decl.MovedFrom), machine.StoreRootKey(decl.Root))
}

// moveStore brings decl, a store being moved, to its declared root: moved
// when it is there (moved now, or before), else why not (it is not usable
// meanwhile).
func (e *storeWorker) moveStore(sfs storeFS, decl machine.Store) (moved bool, problem string) {
	from, to := filepath.Clean(decl.MovedFrom), filepath.Clean(decl.Root)
	switch r, err := openDirPath(to, false); {
	case err == nil:
		ok := hasMoveMarker(r, decl)
		r.Close()
		if ok {
			return true, ""
		}
		return false, fmt.Sprintf("being moved from %s, but %s is there already and was not moved there by Kairo (no marker of this move in it)", from, to)
	case !errors.Is(err, fs.ErrNotExist):
		return false, fmt.Sprintf("being moved from %s to %s: %v", from, to, err)
	}
	if decl.MoveWait != "" {
		return false, fmt.Sprintf("being moved from %s once nothing uses it there: %s", from, decl.MoveWait)
	}
	src, err := openDirPath(from, false)
	if err != nil {
		return false, fmt.Sprintf("being moved from %s: %v", from, err)
	}
	err = writeRootFile(src, storeMoveMarker, []byte(storeMoveMarkerBody(decl)))
	src.Close()
	if err != nil {
		return false, fmt.Sprintf("being moved from %s: its marker: %v", from, err)
	}
	parent, err := openDirPath(filepath.Dir(to), false)
	if err != nil {
		return false, fmt.Sprintf("being moved to %s: %v", to, err)
	}
	parent.Close()
	// One rename: never across filesystems (Windows refuses another
	// volume, Linux another device), and on Windows not while anything
	// there is open; tried again at the next refresh.
	if err = os.Rename(from, to); err != nil {
		return false, fmt.Sprintf("being moved from %s to %s: %v", from, to, err)
	}
	r, err := openDirPath(to, false)
	if err != nil {
		return false, fmt.Sprintf("moved from %s to %s, but: %v", from, to, err)
	}
	ok := hasMoveMarker(r, decl)
	r.Close()
	if !ok {
		return false, fmt.Sprintf("moved from %s to %s, but its marker is not there", from, to)
	}
	e.Logger.Info("store moved", "executor_id", e.ID, "store_id", decl.ID, "from", from, "to", to)
	return true, ""
}

// hasMoveMarker reports whether root (opened) carries the marker of decl's
// move.
func hasMoveMarker(root *os.Root, decl machine.Store) bool {
	fi, err := root.Lstat(storeMoveMarker)
	if err != nil || isLink(fi) || !fi.Mode().IsRegular() || fi.Size() > 4096 {
		return false
	}
	body, err := root.ReadFile(storeMoveMarker)
	return err == nil && string(body) == storeMoveMarkerBody(decl)
}

// dropMoveMarker removes the marker a store moved here keeps once its move
// is over (the declaration no longer says moved_from).
func dropMoveMarker(root string) {
	r, err := openDirPath(filepath.Clean(root), false)
	if err != nil {
		return
	}
	defer r.Close()
	if fi, err := r.Lstat(storeMoveMarker); err == nil && fi.Mode().IsRegular() {
		_ = r.Remove(storeMoveMarker)
	}
}
