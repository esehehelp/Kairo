package executor

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"kairo/internal/machine"
	"kairo/internal/store"
)

// A run's output directory brought here (store/outputlandings.go: a
// continuation moved to this node, or a rerun from another run's
// checkpoint) is a packed object fetched and checked as an input is
// (fetch.go), under names of its own in <root>/inputs (land-<run id>: the
// same object may be an input on this store too), unpacked there with every
// file checked against its list, given its marker, and renamed
// <root>/outputs/<run id>: the run's output directory on this store, which
// the run's next attempt here finds (outputs.go). One already there whose
// marker names the run is taken as it is (brought before, recorded or not);
// anything else there is refused.

// landingPrefix names what is written in <root>/inputs while a run's output
// directory is brought.
const landingPrefix = "land-"

// landOne brings a run's output directory (o.RunID; see above) and returns
// what to record.
func (e *storeWorker) landOne(ctx context.Context, s servedStore, o store.FetchOrder, served map[string]servedStore) (store.FetchMade, error) {
	made := store.FetchMade{ObjectID: o.ObjectID, StoreID: o.StoreID, ExecutorID: e.ID, RunID: o.RunID}
	if !validScratchID(o.ObjectID) || !validScratchID(o.RunID) || o.Source.Month != "" && !machine.ValidMonth(o.Source.Month) {
		return made, fmt.Errorf("%w: object %q, run %q, month %q", errOutsideRoot, o.ObjectID, o.RunID, o.Source.Month)
	}
	root := filepath.Clean(s.decl.Root)
	made.Path = kindPath(s.fs, root, kindOutput, o.RunID)
	r, err := openDirPath(root, false)
	if err != nil {
		return made, err
	}
	defer r.Close()
	outs, err := openMade(r, kindOutput.dir)
	if err != nil {
		return made, err
	}
	there, err := landedThere(outs, o.RunID)
	outs.Close()
	if err != nil {
		return made, fmt.Errorf("%s: %w", made.Path, err)
	}
	if there {
		made.Bytes, err = s.fs.Size(ctx, made.Path)
		return made, err
	}
	if err = checkParentNotMounted(root, kindOutput); err != nil {
		return made, err
	}
	dir, err := openMade(r, inputsDirName)
	if err != nil {
		return made, err
	}
	defer dir.Close()
	name := landingPrefix + o.RunID
	packName, listName := name+packSuffix, name+packSuffix+manifestSuffix
	needed := fetchBytesNeeded(dir, packName, o)
	if free := freeOn(ctx, s); free >= 0 && free-needed < headroom(s) {
		return made, fmt.Errorf("%d bytes free on %s, %d to bring, %d to leave free", free, s.decl.ID, needed, headroom(s))
	}
	copyOrder := store.CopyOrder{ObjectID: o.ObjectID, PackBytes: o.PackBytes, PackSHA256: o.PackSHA256, ManifestSHA256: o.ManifestSHA256, Source: o.Source}
	if err = e.fetchAs(ctx, copyOrder, served, dir, o.ObjectID+packSuffix, o.ObjectID+packSuffix+manifestSuffix, packName, listName); err != nil {
		return made, err
	}
	dirPath := filepath.Join(root, inputsDirName)
	listSHA, _, _, err := hashUncachedFile(dir, listName+partialSuffix, filepath.Join(dirPath, listName+partialSuffix))
	if err != nil {
		return made, err
	}
	packSHA, packBytes, _, err := hashUncachedFile(dir, packName+partialSuffix, filepath.Join(dirPath, packName+partialSuffix))
	if err != nil {
		return made, err
	}
	if packSHA != o.PackSHA256 || packBytes != o.PackBytes || listSHA != o.ManifestSHA256 {
		_ = dir.Remove(packName + partialSuffix)
		_ = dir.Remove(listName + partialSuffix)
		return made, fmt.Errorf("%w: %d bytes, sha256 %s (list %s); the pack has %d bytes, sha256 %s (list %s)",
			errCopyMismatch, packBytes, packSHA, listSHA, o.PackBytes, o.PackSHA256, o.ManifestSHA256)
	}
	unpacking := name + partialSuffix
	if err = dir.RemoveAll(unpacking); err != nil {
		return made, err
	}
	if err = dir.Mkdir(unpacking, 0o755); err != nil {
		return made, err
	}
	into, err := openPlainDir(dir, unpacking)
	if err != nil {
		return made, err
	}
	var skipped []string
	made.Bytes, err = unpackChecked(ctx, dir, packName+partialSuffix, listName+partialSuffix, into, false, &skipped)
	if err == nil {
		// the pack leaves the marker out: the directory is the run's here
		err = writeMarkerOf(into, kindOutput.marker, o.RunID)
	}
	into.Close()
	if err != nil {
		_ = dir.RemoveAll(unpacking)
		return made, err
	}
	if err = renameSettled(ctx, r, filepath.Join(inputsDirName, unpacking), filepath.Join(kindOutput.dir, o.RunID), nil); err != nil {
		return made, err
	}
	_ = dir.Remove(packName + partialSuffix)
	_ = dir.Remove(listName + partialSuffix)
	if len(skipped) != 0 {
		e.Logger.Warn("output entries not made: symbolic links, reserved Windows names", "object_id", o.ObjectID, "run_id", o.RunID, "paths", skipped)
	}
	e.forgetOutputSize(o.RunID)
	return made, nil
}

// landedThere reports whether outs (<root>/outputs) holds runID's output
// directory already: a plain directory whose marker names the run. Anything
// else there is an error.
func landedThere(outs *os.Root, runID string) (bool, error) {
	fi, err := outs.Lstat(runID)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !fi.IsDir() || isLink(fi) {
		return false, errors.New("something other than a directory is there")
	}
	ar, err := openPlainDir(outs, runID)
	if err != nil {
		return false, err
	}
	defer ar.Close()
	if named, err := readMarkerOf(ar, kindOutput.marker); err != nil || named != runID {
		return false, fmt.Errorf("%w: a directory not the run's is there (%v)", errScratchMarker, err)
	}
	return true, nil
}
