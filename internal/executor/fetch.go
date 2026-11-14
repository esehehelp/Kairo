package executor

import (
	"archive/tar"
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"

	"kairo/internal/machine"
	"kairo/internal/store"
)

// An input not on this node is brought here (store/placements.go): the
// executor serving the volatile store it is ordered to fetches the object's
// pack, from a verified durable copy or from the pack where it was made, on
// a store it serves (read through handles) or from another node's agent
// (its pack server, with a token the daemon issued for the fetch), into
// <root>/inputs/<object id>.tar.zst.partial with its file list beside it,
// checks both against the pack's size and sha256s, and unpacks it into
// <root>/inputs/<object id>.partial: only directories and regular files,
// each at a relative path inside it (written through a handle on that
// directory, so nothing escapes it), each file's size and sha256 checked
// against the file list and its mtime set as packed, every file listed
// there; then renames it <root>/inputs/<object id>, removes the pack and
// list and records it (record-fetch). A failure is warned once per object and
// tried again later; a fetch cut short resumes the pack where it stopped.
// What is brought stays for the executions of this node that read it.

const (
	inputsDirName = "inputs"
	// fetchInterval: how often what is to be brought is asked for.
	fetchInterval = time.Minute
	// fetchRoundTimeout bounds one fetching round.
	fetchRoundTimeout = 12 * time.Hour
)

// errInputMismatch: an unpacked file, or the pack, not as its file list says.
var errInputMismatch = errors.New("the object does not unpack as its file list says")

func (e *Local) makeFetches() {
	e.startRound(&e.fetches, fetchInterval, fetchRoundTimeout, e.fetchDue)
}

// fetchDue brings every object the daemon says is due to a usable volatile
// store this executor serves at the ordered root, one at a time, and
// records each.
func (e *Local) fetchDue(ctx context.Context) {
	orders, err := e.Store.FetchesToMake(ctx, e.ID)
	if err != nil {
		e.roundProblem(&e.fetches, "", "fetches to make not read", err)
		return
	}
	e.roundProblem(&e.fetches, "", "", nil)
	served := map[string]servedStore{}
	for _, s := range e.servedStores() {
		served[s.decl.ID] = s
	}
	for _, o := range orders {
		s, ok := served[o.StoreID]
		if !ok || !s.usable || s.decl.Class != machine.StoreVolatile || s.decl.Root != o.Root || !e.due(&e.fetches, o.ObjectID) {
			continue
		}
		started := time.Now()
		made, err := e.fetchOne(ctx, s, o, served)
		if err == nil {
			err = e.Store.RecordFetch(ctx, made)
		}
		if err != nil {
			e.roundProblem(&e.fetches, o.ObjectID, "input not brought here", err)
			continue
		}
		e.roundProblem(&e.fetches, o.ObjectID, "", nil)
		e.Logger.Info("input brought here", "object_id", o.ObjectID, "store_id", o.StoreID, "path", made.Path, "bytes", made.Bytes,
			"local", o.Source.Local, "seconds", time.Since(started).Seconds())
	}
}

// fetchOne brings one object (see above) and returns what to record. One
// already unpacked (recorded after all, or not yet) is taken as it is.
func (e *Local) fetchOne(ctx context.Context, s servedStore, o store.FetchOrder, served map[string]servedStore) (store.FetchMade, error) {
	made := store.FetchMade{ObjectID: o.ObjectID, StoreID: o.StoreID, ExecutorID: e.ID}
	if _, ok := s.fs.(localStoreFS); !ok {
		return made, errPackNotHere
	}
	if !validScratchID(o.ObjectID) || o.Source.Month != "" && !machine.ValidMonth(o.Source.Month) {
		return made, fmt.Errorf("%w: object %q, month %q", errOutsideRoot, o.ObjectID, o.Source.Month)
	}
	root := filepath.Clean(s.decl.Root)
	made.Path = filepath.Join(root, inputsDirName, o.ObjectID)
	if free := freeOn(ctx, s); free >= 0 && free < o.PackBytes+o.ContentBytes {
		return made, fmt.Errorf("%d bytes free on %s, %d to bring", free, s.decl.ID, o.PackBytes+o.ContentBytes)
	}
	r, err := openDirPath(root, false)
	if err != nil {
		return made, err
	}
	defer r.Close()
	dir, err := openMade(r, inputsDirName)
	if err != nil {
		return made, err
	}
	defer dir.Close()
	if fi, err := dir.Lstat(o.ObjectID); err == nil && fi.IsDir() && !isLink(fi) {
		bytes, err := s.fs.Size(ctx, made.Path)
		made.Bytes = bytes
		return made, err
	}
	packName := o.ObjectID + packSuffix
	listName := packName + manifestSuffix
	copyOrder := store.CopyOrder{ObjectID: o.ObjectID, PackBytes: o.PackBytes, PackSHA256: o.PackSHA256, ManifestSHA256: o.ManifestSHA256, Source: o.Source}
	if err = e.fetch(ctx, copyOrder, served, dir, packName, listName); err != nil {
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
	unpacking := o.ObjectID + partialSuffix
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
	made.Bytes, err = unpackChecked(ctx, dir, packName+partialSuffix, listName+partialSuffix, into, false)
	into.Close()
	if err != nil {
		_ = dir.RemoveAll(unpacking)
		return made, err
	}
	if err = dir.Rename(unpacking, o.ObjectID); err != nil {
		return made, err
	}
	_ = dir.Remove(packName + partialSuffix)
	_ = dir.Remove(listName + partialSuffix)
	return made, nil
}

// freeOn is the free bytes of a store's filesystem (-1 when unknown).
func freeOn(ctx context.Context, s servedStore) int64 {
	if _, free, err := s.fs.Usage(ctx, s.decl.Root); err == nil {
		return free
	}
	return -1
}

// unpackChecked unpacks the pack packName (in dir) into into, checking each
// regular file against the file list listName and that every file listed is
// there, and returns the bytes unpacked. With execBits (a code snapshot) a
// file packed executable, or one beginning with "#!" (a pack made on Windows
// has no execute bits), is made executable (0755).
func unpackChecked(ctx context.Context, dir *os.Root, packName, listName string, into *os.Root, execBits bool) (int64, error) {
	listFile, err := dir.Open(listName)
	if err != nil {
		return 0, err
	}
	type listed struct {
		size, mtime int64
		sha         string
	}
	want := map[string]listed{}
	lines := bufio.NewScanner(listFile)
	lines.Buffer(make([]byte, 1<<20), 1<<20)
	first := true
	for lines.Scan() {
		if first {
			first = false
			if lines.Text() != manifestHeader {
				listFile.Close()
				return 0, fmt.Errorf("%w: not a file list Kairo wrote", errInputMismatch)
			}
			continue
		}
		fields := strings.Split(lines.Text(), "\t")
		if len(fields) != 4 {
			listFile.Close()
			return 0, fmt.Errorf("%w: line %q", errInputMismatch, lines.Text())
		}
		size, err1 := strconv.ParseInt(fields[1], 10, 64)
		mtime, err2 := strconv.ParseInt(fields[3], 10, 64)
		if err1 != nil || err2 != nil {
			listFile.Close()
			return 0, fmt.Errorf("%w: line %q", errInputMismatch, lines.Text())
		}
		want[unescapeListPath(fields[0])] = listed{size, mtime, fields[2]}
	}
	listFile.Close()
	if err := lines.Err(); err != nil {
		return 0, err
	}
	pack, err := dir.Open(packName)
	if err != nil {
		return 0, err
	}
	defer pack.Close()
	zr, err := zstd.NewReader(pack)
	if err != nil {
		return 0, err
	}
	defer zr.Close()
	tr := tar.NewReader(zr)
	var total int64
	seen := map[string]bool{}
	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return total, err
		}
		name := strings.TrimSuffix(h.Name, "/")
		if name == "" || path.IsAbs(name) || path.Clean(name) != name || name == ".." || strings.HasPrefix(name, "../") || strings.Contains(name, "\\") {
			return total, fmt.Errorf("%w: entry %q", errInputMismatch, h.Name)
		}
		local := filepath.FromSlash(name)
		switch h.Typeflag {
		case tar.TypeDir:
			if err := into.MkdirAll(local, 0o755); err != nil {
				return total, err
			}
		case tar.TypeReg:
			w, ok := want[name]
			if !ok {
				return total, fmt.Errorf("%w: %s is not in the file list", errInputMismatch, name)
			}
			if parent := filepath.Dir(local); parent != "." {
				if err := into.MkdirAll(parent, 0o755); err != nil {
					return total, err
				}
			}
			f, err := into.OpenFile(local, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
			if err != nil {
				return total, err
			}
			hash, head := sha256.New(), &headWriter{}
			n, err := io.Copy(io.MultiWriter(f, hash, head), io.LimitReader(tr, w.size+1))
			if err == nil {
				err = f.Sync()
			}
			if closeErr := f.Close(); err == nil {
				err = closeErr
			}
			if err != nil {
				return total, err
			}
			if n != w.size || hex.EncodeToString(hash.Sum(nil)) != w.sha {
				return total, fmt.Errorf("%w: %s unpacked as %d bytes, sha256 %s; listed %d bytes, sha256 %s", errInputMismatch, name, n, hex.EncodeToString(hash.Sum(nil)), w.size, w.sha)
			}
			if execBits && (h.Mode&0o111 != 0 || bytes.HasPrefix(head.b, []byte("#!"))) {
				if err := into.Chmod(local, 0o755); err != nil {
					return total, err
				}
			}
			mtime := time.Unix(0, w.mtime)
			if err := into.Chtimes(local, mtime, mtime); err != nil {
				return total, err
			}
			seen[name] = true
			total += n
		default:
			return total, fmt.Errorf("%w: %s is neither a directory nor a regular file", errInputMismatch, name)
		}
	}
	for name := range want {
		if !seen[name] {
			return total, fmt.Errorf("%w: %s is listed but not in the pack", errInputMismatch, name)
		}
	}
	return total, nil
}

// headWriter keeps the first two bytes written to it.
type headWriter struct{ b []byte }

func (h *headWriter) Write(p []byte) (int, error) {
	if n := 2 - len(h.b); n > 0 {
		h.b = append(h.b, p[:min(n, len(p))]...)
	}
	return len(p), nil
}

// openObjectFile opens <root>/objects/<month>/name of a durable store: the
// root reached one directory at a time, the file a regular file (not a link)
// and the one checked.
func openObjectFile(root, month, name string) (*os.File, error) {
	if !machine.ValidMonth(month) || !strings.HasSuffix(name, packSuffix) && !strings.HasSuffix(name, packSuffix+manifestSuffix) ||
		!validScratchID(strings.TrimSuffix(strings.TrimSuffix(name, manifestSuffix), packSuffix)) {
		return nil, fmt.Errorf("%w: object %q of %q", errOutsideRoot, name, month)
	}
	r, err := openDirPath(filepath.Clean(root), false)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	or, err := openPlainDir(r, objectsDirName)
	if err != nil {
		return nil, err
	}
	defer or.Close()
	mr, err := openPlainDir(or, month)
	if err != nil {
		return nil, err
	}
	defer mr.Close()
	fi, err := mr.Lstat(name)
	if err != nil {
		return nil, err
	}
	if isLink(fi) || !fi.Mode().IsRegular() {
		return nil, fmt.Errorf("%w: %s is not a regular file", errScratchLink, name)
	}
	f, err := mr.OpenFile(name, os.O_RDONLY|markerOpenFlags, 0)
	if err != nil {
		return nil, err
	}
	opened, err := f.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(fi, opened) {
		f.Close()
		if err == nil {
			err = fmt.Errorf("%w: %s", errRaced, name)
		}
		return nil, err
	}
	return f, nil
}
