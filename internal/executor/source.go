package executor

import (
	"archive/tar"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/klauspost/compress/zstd"

	"kairo/internal/sourcesnap"
	"kairo/internal/store"
)

// Code snapshots (kairo source, store/sources.go): kairo project apply packs
// the files of a source's declared paths (PackSource) as a data object is
// packed, the daemon keeps the pack, and an attempt of a task bound to the
// snapshot runs in it: its executor brings it to the node once (fetched from
// the daemon, checked against its size, its sha256 and each file against
// the file list, as an input is: ensureSource) under <root>/sources/<id> of
// the first volatile store it serves, and starts the attempt there.

// sourcesDirName is the directory of a volatile store snapshots are
// unpacked in.
const sourcesDirName = "sources"

// SourcePack is a snapshot PackSource made: the pack and its file list
// written in a directory, and what they hold.
type SourcePack struct {
	ID           string
	PackPath     string
	ListPath     string
	List         string
	PackBytes    int64
	PackSHA256   string
	ListSHA256   string
	Files        int64
	ContentBytes int64
	// Skipped counts what was not packed: links and special files.
	Skipped int64
}

// PackSource packs the regular files of paths (relative to root; a file or
// a directory, taken whole) but those exclude names, into dir: <id>.tar.zst
// and its file list <id>.tar.zst.files.tsv. An exclude pattern without a
// slash is matched against every path element (".venv", "__pycache__",
// "*.pyc"); one with a slash against the whole relative path.
func PackSource(ctx context.Context, root string, paths, exclude []string, dir string) (SourcePack, error) {
	var sp SourcePack
	for _, pattern := range exclude {
		if _, err := path.Match(pattern, ""); err != nil {
			return sp, fmt.Errorf("exclude %q: %w", pattern, err)
		}
	}
	r, err := os.OpenRoot(root)
	if err != nil {
		return sp, err
	}
	defer r.Close()
	type file struct {
		rel  string
		info fs.FileInfo
	}
	var files []file
	seen := map[string]bool{}
	var add func(rel string) error
	add = func(rel string) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if sourceExcluded(rel, exclude) || seen[rel] {
			return nil
		}
		info, err := r.Lstat(filepath.FromSlash(rel))
		if err != nil {
			return err
		}
		switch {
		case isLink(info):
			sp.Skipped++
		case info.Mode().IsRegular():
			if !utf8.ValidString(rel) {
				return fmt.Errorf("%w: %q", errPackName, rel)
			}
			seen[rel] = true
			files = append(files, file{rel, info})
		case info.IsDir():
			var entries []fs.FileInfo
			if err := eachEntry(ctx, r, filepath.FromSlash(rel), func(info fs.FileInfo) { entries = append(entries, info) }); err != nil {
				return err
			}
			for _, e := range entries {
				if err := add(path.Join(rel, e.Name())); err != nil {
					return err
				}
			}
		default:
			sp.Skipped++
		}
		return nil
	}
	for _, p := range paths {
		rel := path.Clean(filepath.ToSlash(p))
		if rel == "." || rel == ".." || strings.HasPrefix(rel, "../") || path.IsAbs(rel) {
			return sp, fmt.Errorf("path %q is not under the source's root", p)
		}
		if err := add(rel); err != nil {
			return sp, fmt.Errorf("%s: %w", p, err)
		}
	}
	if len(files) == 0 {
		return sp, errors.New("nothing to snapshot: the paths hold no regular file")
	}
	sort.Slice(files, func(i, j int) bool { return files[i].rel < files[j].rel })
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return sp, err
	}
	tmp, err := os.CreateTemp(dir, "snapshot-*.tar.zst")
	if err != nil {
		return sp, err
	}
	defer func() {
		tmp.Close()
		_ = os.Remove(tmp.Name())
	}()
	packHash, counted := sha256.New(), &countWriter{}
	zw, err := zstd.NewWriter(io.MultiWriter(tmp, packHash, counted), zstd.WithEncoderLevel(zstd.SpeedBetterCompression))
	if err != nil {
		return sp, err
	}
	tw := tar.NewWriter(zw)
	var list strings.Builder
	list.WriteString(manifestHeader + "\n")
	for _, f := range files {
		line, size, err := packFile(r, tw, f.rel, f.info)
		if err != nil {
			return sp, err
		}
		list.WriteString(line + "\n")
		sp.Files++
		sp.ContentBytes += size
	}
	if err := tw.Close(); err != nil {
		return sp, err
	}
	if err := zw.Close(); err != nil {
		return sp, err
	}
	if err := tmp.Close(); err != nil {
		return sp, err
	}
	sp.List = list.String()
	if sp.ID, err = sourcesnap.ID(sp.List); err != nil {
		return sp, err
	}
	sp.PackBytes, sp.PackSHA256 = counted.n, hex.EncodeToString(packHash.Sum(nil))
	listHash := sha256.Sum256([]byte(sp.List))
	sp.ListSHA256 = hex.EncodeToString(listHash[:])
	sp.PackPath = filepath.Join(dir, sp.ID+packSuffix)
	sp.ListPath = sp.PackPath + manifestSuffix
	if err := os.WriteFile(sp.ListPath, []byte(sp.List), 0o644); err != nil {
		return sp, err
	}
	if err := os.Rename(tmp.Name(), sp.PackPath); err != nil {
		return sp, err
	}
	return sp, nil
}

// sourceExcluded reports whether an exclude pattern names rel (see
// PackSource).
func sourceExcluded(rel string, exclude []string) bool {
	for _, pattern := range exclude {
		if strings.Contains(pattern, "/") {
			if ok, _ := path.Match(pattern, rel); ok {
				return true
			}
			continue
		}
		for _, element := range strings.Split(rel, "/") {
			if ok, _ := path.Match(pattern, element); ok {
				return true
			}
		}
	}
	return false
}

// SourceFetcher brings a snapshot's pack and file list from the daemon.
type SourceFetcher interface {
	FetchSource(ctx context.Context, id, file string, w io.Writer) error
}

// ensureSource is where snapshot src is unpacked on this executor's node,
// bringing it there first if it is not: under <root>/sources/<id> of the
// first usable volatile store this executor serves. One at a time per
// executor; a directory there is the snapshot (it is renamed into place only
// once every file was checked).
func (e *Local) ensureSource(ctx context.Context, src *store.LaunchSource) (string, error) {
	if e.isWSL() {
		return "", errors.New("code snapshots are not run inside WSL yet")
	}
	fetch, ok := e.Store.(SourceFetcher)
	if !ok {
		return "", errors.New("this executor cannot fetch code snapshots")
	}
	var s *servedStore
	for _, served := range e.servedStores() {
		if served.usable && served.decl.Class == "volatile" {
			s = &served
			break
		}
	}
	if s == nil {
		return "", errors.New("no usable volatile store to unpack the code snapshot on")
	}
	if _, ok := s.fs.(localStoreFS); !ok {
		return "", errors.New("code snapshots are unpacked on a local store only")
	}
	e.sourceMu.Lock()
	defer e.sourceMu.Unlock()
	base := filepath.Join(s.decl.Root, sourcesDirName)
	final := filepath.Join(base, src.ID)
	if info, err := os.Lstat(final); err == nil && info.IsDir() {
		return final, nil
	}
	if err := os.MkdirAll(base, 0o755); err != nil {
		return "", err
	}
	dir, err := os.OpenRoot(base)
	if err != nil {
		return "", err
	}
	defer dir.Close()
	packName, listName, partial := src.ID+packSuffix+partialSuffix, src.ID+packSuffix+manifestSuffix+partialSuffix, src.ID+partialSuffix
	defer func() {
		_ = dir.Remove(packName)
		_ = dir.Remove(listName)
		_ = dir.RemoveAll(partial)
	}()
	for _, f := range []struct {
		name, file, sha string
		bytes           int64
	}{{listName, "list", src.ListSHA256, -1}, {packName, "pack", src.PackSHA256, src.PackBytes}} {
		w, err := dir.OpenFile(f.name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
		if err != nil {
			return "", err
		}
		h, counted := sha256.New(), &countWriter{}
		err = fetch.FetchSource(ctx, src.ID, f.file, io.MultiWriter(w, h, counted))
		if err == nil {
			err = w.Sync()
		}
		if closeErr := w.Close(); err == nil {
			err = closeErr
		}
		if err != nil {
			return "", fmt.Errorf("fetch the snapshot's %s: %w", f.file, err)
		}
		if got := hex.EncodeToString(h.Sum(nil)); got != f.sha || f.bytes >= 0 && counted.n != f.bytes {
			return "", fmt.Errorf("%w: the snapshot's %s is %d bytes, sha256 %s; recorded %d bytes, sha256 %s", errInputMismatch, f.file, counted.n, got, f.bytes, f.sha)
		}
	}
	_ = dir.RemoveAll(partial)
	if err := dir.Mkdir(partial, 0o755); err != nil {
		return "", err
	}
	into, err := dir.OpenRoot(partial)
	if err != nil {
		return "", err
	}
	_, err = unpackChecked(ctx, dir, packName, listName, into)
	into.Close()
	if err != nil {
		return "", fmt.Errorf("unpack the snapshot: %w", err)
	}
	if err := dir.Rename(partial, src.ID); err != nil {
		return "", err
	}
	e.Logger.Info("code snapshot brought", "source_id", src.ID, "path", final)
	return final, nil
}
