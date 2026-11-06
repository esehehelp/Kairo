package executor

import (
	"archive/tar"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"

	"kairo/internal/store"
)

// What Kairo keeps is packed where it was written (store.PacksToMake): the
// executor that made a run's output directory or a scratch packs it, once
// the daemon says so, into <root>/packs/<object id>.tar.zst (tar, PAX,
// compressed with zstd) with its file list <object id>.tar.zst.files.tsv: a
// header line "path\tbytes\tsha256\tmtime_ns", then one line per regular
// file, sorted by path (relative, "/"-separated; a backslash, tab, CR or LF
// in it written \\, \t, \r, \n), the format neo-ime's own packs on the
// durable store use. Both are written under a ".partial" name and renamed
// once complete and synced (the file list first), so a pack that is there is
// whole; a round cut short starts the object again.
//
// The directory is reached as the store's rules say (storefs.go): derived
// from the root, its kind and its id (never a path the daemon sends), opened
// one directory at a time through handles, its marker checked to name it.
// Inside, regular files and directories are packed; links, special files
// and (on Linux) mount points are not followed or entered, only counted as
// skipped; the marker itself is left out. A file must be the one listed and
// keep its size while it is read (nothing writes there any more), else the
// round fails and is tried again. Packing is not done for a store inside a
// WSL distro yet (its files are Linux files the executor cannot read through
// handles): such an object stays to be packed.

const (
	packsDirName   = "packs"
	packSuffix     = ".tar.zst"
	manifestSuffix = ".files.tsv"
	partialSuffix  = ".partial"
	// packInterval: how often what is to be packed is asked for.
	packInterval = time.Minute
	// packRoundTimeout bounds one packing round.
	packRoundTimeout = 12 * time.Hour
	// manifestHeader is the file list's first line.
	manifestHeader = "path\tbytes\tsha256\tmtime_ns"
)

var (
	// errPackNotHere: the store is one this executor does not pack (inside
	// a WSL distro).
	errPackNotHere = errors.New("directories of a store inside a WSL distro are not packed yet")
	// errPackChanged: a file changed while it was packed.
	errPackChanged = errors.New("a file changed while it was packed")
)

// packResult is a pack made: its paths, sizes, contents and sha256s.
type packResult struct {
	packPath, manifestPath   string
	packBytes, contentBytes  int64
	files, skipped           int64
	packSHA256, manifestHash string
}

// kindOf is the directory kind of an object kind.
func kindOf(objectKind string) (dirKind, bool) {
	switch objectKind {
	case store.ObjectOutput:
		return kindOutput, true
	case store.ObjectScratch:
		return kindScratch, true
	}
	return dirKind{}, false
}

func (localStoreFS) Pack(ctx context.Context, root string, kind dirKind, id, objectID string) (packResult, error) {
	var res packResult
	if !validScratchID(id) || !validScratchID(objectID) {
		return res, fmt.Errorf("%w: id %q, object %q", errOutsideRoot, id, objectID)
	}
	root = filepath.Clean(root)
	r, err := openDirPath(root, false)
	if err != nil {
		return res, err
	}
	defer r.Close()
	kr, err := openPlainDir(r, kind.dir)
	if err != nil {
		return res, err
	}
	defer kr.Close()
	sr, err := openPlainDir(kr, id)
	if err != nil {
		return res, err
	}
	defer sr.Close()
	if named, err := readMarkerOf(sr, kind.marker); err != nil || named != id {
		return res, fmt.Errorf("%w: %s (%v)", errScratchMarker, filepath.Join(root, kind.dir, id), err)
	}
	src := filepath.Join(root, kind.dir, id)
	mounts, err := mountsUnderFn(src)
	if err != nil {
		return res, err
	}
	if err = r.Mkdir(packsDirName, 0o755); err != nil && !errors.Is(err, fs.ErrExist) {
		return res, err
	}
	pr, err := openPlainDir(r, packsDirName)
	if err != nil {
		return res, err
	}
	defer pr.Close()
	if err = checkParentNotMounted(root, dirKind{dir: packsDirName}); err != nil {
		return res, err
	}
	res, err = writePack(ctx, sr, src, mounts, kind.marker, pr, objectID+packSuffix)
	if err != nil {
		return res, err
	}
	res.packPath = filepath.Join(root, packsDirName, objectID+packSuffix)
	res.manifestPath = res.packPath + manifestSuffix
	return res, nil
}

func (wslStoreFS) Pack(context.Context, string, dirKind, string, string) (packResult, error) {
	return packResult{}, errPackNotHere
}

// countWriter counts the bytes written through it.
type countWriter struct{ n int64 }

func (c *countWriter) Write(p []byte) (int, error) {
	c.n += int64(len(p))
	return len(p), nil
}

// writePack packs src (opened at srcPath) into packName and its file list in
// packs, leaving out the marker at its top. See above for what goes in.
func writePack(ctx context.Context, src *os.Root, srcPath string, mounts []string, marker string, packs *os.Root, packName string) (packResult, error) {
	var res packResult
	listName := packName + manifestSuffix
	for _, name := range []string{packName + partialSuffix, listName + partialSuffix} {
		if err := packs.Remove(name); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return res, err
		}
	}
	f, err := packs.OpenFile(packName+partialSuffix, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return res, err
	}
	written := false
	defer func() {
		if !written {
			f.Close()
			_ = packs.Remove(packName + partialSuffix)
		}
	}()
	packHash, counted := sha256.New(), &countWriter{}
	zw, err := zstd.NewWriter(io.MultiWriter(f, packHash, counted), zstd.WithEncoderLevel(zstd.SpeedBetterCompression))
	if err != nil {
		return res, err
	}
	tw := tar.NewWriter(zw)
	mounted := map[string]bool{}
	for _, m := range mounts {
		mounted[filepath.Clean(m)] = true
	}
	var lines []string
	pending := []string{"."}
	for len(pending) != 0 {
		dir := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		var infos []fs.FileInfo
		if err := eachEntry(ctx, src, filepath.FromSlash(dir), func(info fs.FileInfo) { infos = append(infos, info) }); err != nil {
			return res, err
		}
		sort.Slice(infos, func(i, j int) bool { return infos[i].Name() < infos[j].Name() })
		var subdirs []string
		for _, info := range infos {
			if err := ctx.Err(); err != nil {
				return res, err
			}
			rel := path.Join(dir, info.Name())
			switch {
			case dir == "." && info.Name() == marker:
			case isLink(info):
				res.skipped++
			case info.IsDir():
				if mounted[filepath.Join(srcPath, filepath.FromSlash(rel))] {
					res.skipped++
					continue
				}
				if err := tw.WriteHeader(&tar.Header{Typeflag: tar.TypeDir, Name: rel + "/", Mode: int64(info.Mode().Perm()), ModTime: info.ModTime(), Format: tar.FormatPAX}); err != nil {
					return res, err
				}
				subdirs = append(subdirs, rel)
			case info.Mode().IsRegular():
				line, size, err := packFile(src, tw, rel, info)
				if err != nil {
					return res, err
				}
				lines = append(lines, line)
				res.files++
				res.contentBytes += size
			default:
				res.skipped++
			}
		}
		for i := len(subdirs) - 1; i >= 0; i-- {
			pending = append(pending, subdirs[i])
		}
	}
	if err = tw.Close(); err != nil {
		return res, err
	}
	if err = zw.Close(); err != nil {
		return res, err
	}
	if err = f.Sync(); err != nil {
		return res, err
	}
	if err = f.Close(); err != nil {
		return res, err
	}
	written = true
	res.packBytes, res.packSHA256 = counted.n, hex.EncodeToString(packHash.Sum(nil))
	sort.Strings(lines)
	body := manifestHeader + "\n"
	if len(lines) != 0 {
		body += strings.Join(lines, "\n") + "\n"
	}
	listHash := sha256.Sum256([]byte(body))
	res.manifestHash = hex.EncodeToString(listHash[:])
	if err = writeNew(packs, listName+partialSuffix, []byte(body)); err != nil {
		_ = packs.Remove(packName + partialSuffix)
		return res, err
	}
	// The file list first: a pack that is there has its list beside it.
	if err = packs.Rename(listName+partialSuffix, listName); err != nil {
		return res, err
	}
	if err = packs.Rename(packName+partialSuffix, packName); err != nil {
		return res, err
	}
	return res, nil
}

// packFile writes the regular file rel (as info listed it) to tw and returns
// its file list line and size. The file opened must be the one listed and
// must keep its size while it is read.
func packFile(src *os.Root, tw *tar.Writer, rel string, info fs.FileInfo) (string, int64, error) {
	fh, err := src.OpenFile(filepath.FromSlash(rel), os.O_RDONLY|markerOpenFlags, 0)
	if err != nil {
		return "", 0, err
	}
	defer fh.Close()
	opened, err := fh.Stat()
	if err != nil {
		return "", 0, err
	}
	if !opened.Mode().IsRegular() || !os.SameFile(info, opened) {
		return "", 0, fmt.Errorf("%w: %s is not the file listed", errPackChanged, rel)
	}
	size := opened.Size()
	if err = tw.WriteHeader(&tar.Header{Typeflag: tar.TypeReg, Name: rel, Size: size, Mode: int64(opened.Mode().Perm()), ModTime: opened.ModTime(), Format: tar.FormatPAX}); err != nil {
		return "", 0, err
	}
	var h hash.Hash = sha256.New()
	n, err := io.Copy(tw, io.TeeReader(io.LimitReader(fh, size), h))
	if err != nil {
		return "", 0, err
	}
	if n != size {
		return "", 0, fmt.Errorf("%w: %s shrank from %d to %d bytes", errPackChanged, rel, size, n)
	}
	return fmt.Sprintf("%s\t%d\t%s\t%d", escapeListPath(rel), size, hex.EncodeToString(h.Sum(nil)), opened.ModTime().UnixNano()), size, nil
}

// escapeListPath writes a backslash, tab, CR or LF in a path as \\, \t, \r,
// \n, so each file is one line of the file list.
func escapeListPath(p string) string {
	if !strings.ContainsAny(p, "\\\t\r\n") {
		return p
	}
	return strings.NewReplacer(`\`, `\\`, "\t", `\t`, "\r", `\r`, "\n", `\n`).Replace(p)
}

// writeNew creates name in dir (nothing may be there) with body, synced.
func writeNew(dir *os.Root, name string, body []byte) error {
	f, err := dir.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	if _, err = f.Write(body); err != nil {
		f.Close()
		_ = dir.Remove(name)
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		_ = dir.Remove(name)
		return err
	}
	return f.Close()
}

// makePacks packs, in the background, what the daemon says is to be packed
// now (at most every packInterval), once the daemon has answered for stores.
func (e *Local) makePacks() {
	e.mu.Lock()
	due := e.storesKnown && !e.packing && time.Since(e.packAt) >= packInterval
	if due {
		e.packing = true
		e.packAt = time.Now()
	}
	e.mu.Unlock()
	if !due {
		return
	}
	go func() {
		defer func() {
			e.mu.Lock()
			e.packing = false
			e.mu.Unlock()
		}()
		ctx, cancel := context.WithTimeout(context.Background(), packRoundTimeout)
		defer cancel()
		e.packDue(ctx)
	}()
}

// packDue packs every object the daemon says is due on a usable store this
// executor serves, one at a time, and records each. A failure is warned once
// per object (again when it changes) and tried again at the next round.
func (e *Local) packDue(ctx context.Context) {
	orders, err := e.Store.PacksToMake(ctx, e.ID)
	if err != nil {
		e.packProblem("", "packs to make not read", err)
		return
	}
	e.packProblem("", "", nil)
	served := map[string]servedStore{}
	for _, s := range e.servedStores() {
		served[s.decl.ID] = s
	}
	for _, o := range orders {
		s, ok := served[o.StoreID]
		kind, known := kindOf(o.Kind)
		switch {
		case !known:
			e.packProblem(o.ObjectID, "not packed", fmt.Errorf("unknown object kind %q", o.Kind))
			continue
		case !ok || !s.usable || s.decl.Root != o.Root:
			// not served here as ordered now: asked again next round
			continue
		}
		if o.Bytes != nil {
			if _, free, err := s.fs.Usage(ctx, s.decl.Root); err == nil && free < *o.Bytes {
				e.packProblem(o.ObjectID, "not packed", fmt.Errorf("%d bytes free on %s, %d to pack", free, s.decl.ID, *o.Bytes))
				continue
			}
		}
		started := time.Now()
		res, err := s.fs.Pack(ctx, s.decl.Root, kind, o.SourceID, o.ObjectID)
		if err == nil {
			made := store.PackMade{ObjectID: o.ObjectID, ExecutorID: e.ID, PackPath: res.packPath, ManifestPath: res.manifestPath,
				PackBytes: res.packBytes, ContentBytes: res.contentBytes, Files: res.files, Skipped: res.skipped,
				PackSHA256: res.packSHA256, ManifestSHA256: res.manifestHash}
			err = e.Store.RecordPack(ctx, made)
		}
		if err != nil {
			e.packProblem(o.ObjectID, "not packed", err)
			continue
		}
		e.packProblem(o.ObjectID, "", nil)
		e.Logger.Info("packed", "object_id", o.ObjectID, "kind", o.Kind, "source_id", o.SourceID, "files", res.files,
			"content_bytes", res.contentBytes, "pack_bytes", res.packBytes, "skipped", res.skipped, "seconds", time.Since(started).Seconds())
	}
}

// packProblem warns of a packing problem once (again when it changes); an
// empty message clears it. objectID "" is the round itself.
func (e *Local) packProblem(objectID, message string, err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if message == "" {
		delete(e.packProblems, objectID)
		return
	}
	text := message + ": " + err.Error()
	if e.packProblems[objectID] == text {
		return
	}
	if e.packProblems == nil {
		e.packProblems = map[string]string{}
	}
	e.packProblems[objectID] = text
	e.Logger.Warn(message, "executor_id", e.ID, "object_id", objectID, "error", err)
}
