package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"kairo/internal/machine"
	"kairo/internal/store"
)

// The inventory of a durable store (store/inventory_durable.go): every
// inventoryEvery each durable store this executor serves on its own
// filesystems is listed (<root>/objects/<YYYYMM>/<object id>.tar.zst with
// its file list beside it; partial files left out) and the listing reported;
// what the daemon does not know there is adopted from each copy's
// description (.object.json), adoptBatch at a time. A store whose root is not
// there is not listed (a disk not mounted is not an empty store).
const (
	inventoryEvery        = time.Hour
	inventoryRoundTimeout = 30 * time.Minute
	adoptBatch            = 200
)

func (e *Local) makeInventories() {
	e.startRound(&e.inventories, inventoryEvery, inventoryRoundTimeout, e.inventoryDue)
}

func (e *Local) inventoryDue(ctx context.Context) {
	e.checkInPlaceSources(ctx)
	for _, s := range e.servedStores() {
		if !s.usable || s.decl.Class != machine.StoreDurable || s.decl.MovedFrom != "" {
			continue
		}
		if _, isLocal := s.fs.(localStoreFS); !isLocal {
			continue
		}
		listedAt := time.Now().UTC().Format(time.RFC3339Nano)
		copies, err := listDurableCopies(ctx, s.decl.Root)
		if err != nil {
			e.roundProblem(&e.inventories, s.decl.ID, "durable store not listed", err)
			continue
		}
		inv, err := e.Store.InventoryDurable(ctx, store.DurableListing{ExecutorID: e.ID, StoreID: s.decl.ID, Root: s.decl.Root, ListedAt: listedAt, Copies: copies})
		if err != nil {
			e.roundProblem(&e.inventories, s.decl.ID, "durable store's listing not taken", err)
			continue
		}
		if inv.Missing > 0 {
			e.Logger.Warn("copies recorded on a durable store are not in its directory: marked damaged", "store_id", s.decl.ID, "missing", inv.Missing)
		}
		adopted, refused := e.adoptCopies(ctx, s, copies, inv.Unknown)
		if adopted > 0 || refused > 0 {
			e.Logger.Info("copies found on a durable store adopted", "store_id", s.decl.ID, "adopted", adopted, "not adopted", refused)
		}
		e.roundProblem(&e.inventories, s.decl.ID, "", nil)
	}
}

// checkInPlaceSources reports each path of this node inputs are read in place
// at that is gone (its volume there): store.InputGone, so inputs are brought
// from a durable copy instead (README: Data stores); and each that is a
// regular file, an import of one file (an input is a directory):
// store.InputIsFile, so it is brought as a directory holding it. A WSL
// executor's paths are the distro's: not looked at from here.
func (e *Local) checkInPlaceSources(ctx context.Context) {
	if e.Kind == "wsl2" || e.Attributes["environment"] == "wsl2" {
		return
	}
	sources, err := e.Store.InPlaceSources(ctx, e.ID)
	if err != nil {
		e.roundProblem(&e.inventories, "", "in-place sources not read", err)
		return
	}
	gone, files := 0, 0
	for _, src := range sources {
		info, err := os.Lstat(src.Path)
		if err == nil && info.Mode().IsRegular() {
			if err := e.Store.InputIsFile(ctx, e.ID, src.ObjectID, src.Path); err != nil {
				e.roundProblem(&e.inventories, src.ObjectID, "file source not recorded", err)
				continue
			}
			files++
			continue
		}
		if err == nil || !errors.Is(sourceGone(src.Path, err), errSourceGone) {
			continue
		}
		if err := e.Store.InputGone(ctx, e.ID, src.ObjectID, src.Path); err != nil {
			e.roundProblem(&e.inventories, src.ObjectID, "gone source not recorded", err)
			continue
		}
		gone++
	}
	if gone > 0 {
		e.Logger.Warn("sources read in place are gone: recorded, brought from durable copies instead", "count", gone)
	}
	if files > 0 {
		e.Logger.Info("imports of one file read in place: recorded, brought as a directory holding the file instead", "count", files)
	}
}

// adoptCopies adopts the copies ids of store s from their descriptions,
// batched; a batch refused is tried a copy at a time, so one bad description
// keeps out only its own copy.
func (e *Local) adoptCopies(ctx context.Context, s servedStore, months map[string]string, ids []string) (adopted, refused int) {
	var batch []store.AdoptedCopy
	send := func(copies []store.AdoptedCopy) error {
		n, err := e.Store.AdoptCopies(ctx, e.ID, s.decl.ID, s.decl.Root, copies)
		adopted += n
		return err
	}
	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := send(batch); err != nil {
			for _, c := range batch {
				if err := send([]store.AdoptedCopy{c}); err != nil {
					refused++
					e.roundProblem(&e.inventories, c.Description.ObjectID, "copy found on a durable store not adopted", err)
				}
			}
		}
		batch = batch[:0]
	}
	for _, id := range ids {
		if ctx.Err() != nil {
			break
		}
		desc, err := readDescription(s.decl.Root, months[id], id)
		if err != nil {
			refused++
			e.roundProblem(&e.inventories, id, "copy found on a durable store not adopted", err)
			continue
		}
		batch = append(batch, store.AdoptedCopy{Month: months[id], Description: desc})
		if len(batch) == adoptBatch {
			flush()
		}
	}
	flush()
	return adopted, refused
}

// listDurableCopies lists the copies under <root>/objects: object id to
// month, each a <id>.tar.zst with its <id>.tar.zst.files.tsv beside it. The
// root not there is an error; no objects directory, no copies.
func listDurableCopies(ctx context.Context, root string) (map[string]string, error) {
	r, err := openDirPath(root, false)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	out := map[string]string{}
	var months []fs.FileInfo
	if err = eachEntry(ctx, r, "objects", func(info fs.FileInfo) { months = append(months, info) }); err != nil {
		return nil, err
	}
	for _, m := range months {
		if !m.IsDir() || !machine.ValidMonth(m.Name()) {
			continue
		}
		names := map[string]bool{}
		if err = eachEntry(ctx, r, filepath.Join("objects", m.Name()), func(info fs.FileInfo) {
			if info.Mode().IsRegular() {
				names[info.Name()] = true
			}
		}); err != nil {
			return nil, err
		}
		for name := range names {
			id, ok := strings.CutSuffix(name, packSuffix)
			if ok && machine.ScratchIDValid(id) && names[name+manifestSuffix] {
				out[id] = m.Name()
			}
		}
	}
	return out, nil
}

// readDescription reads the description beside the copy of id made in month
// on the store at root.
func readDescription(root, month, id string) (store.ObjectDescription, error) {
	var d store.ObjectDescription
	if !machine.ValidMonth(month) || !machine.ScratchIDValid(id) {
		return d, fmt.Errorf("%w: object %q, month %q", errOutsideRoot, id, month)
	}
	r, err := openDirPath(filepath.Join(root, "objects", month), false)
	if err != nil {
		return d, err
	}
	defer r.Close()
	f, err := r.Open(id + packSuffix + descriptionSuffix)
	if errors.Is(err, fs.ErrNotExist) {
		return d, fmt.Errorf("no description beside the copy of %s (made before copies described themselves, or being written)", id)
	}
	if err != nil {
		return d, err
	}
	defer f.Close()
	body, err := io.ReadAll(io.LimitReader(f, 1<<20))
	if err != nil {
		return d, err
	}
	if err = json.Unmarshal(body, &d); err != nil {
		return d, fmt.Errorf("description of %s: %w", id, err)
	}
	if d.ObjectID != id {
		return d, fmt.Errorf("the description beside the copy of %s describes %s", id, d.ObjectID)
	}
	return d, nil
}
