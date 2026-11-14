package api

import (
	"archive/tar"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"

	"github.com/klauspost/compress/zstd"

	"kairo/internal/sourcesnap"
	"kairo/internal/store"
)

// Code snapshots (store/sources.go): kairo project apply sends each one the
// daemon lacks (POST /api/sources, multipart: a "meta" part, the
// SourceUpload, then a "pack" part, the tar.zst), which the daemon checks
// (the pack's size and sha256, each file in it against the file list, and
// nothing else in it), keeps in SourceDirectory (<id>.tar.zst and
// <id>.tar.zst.files.tsv) and records. Agents fetch them (GET
// /api/agent/sources/{id}/pack and /list) to run attempts in them.

// maxSourcePack bounds a snapshot's pack.
const maxSourcePack = 4 << 30

// SourceUpload is the "meta" part of a snapshot sent.
type SourceUpload struct {
	ID           string `json:"id"`
	List         string `json:"list"`
	ListSHA256   string `json:"list_sha256"`
	PackBytes    int64  `json:"pack_bytes"`
	PackSHA256   string `json:"pack_sha256"`
	Files        int64  `json:"files"`
	ContentBytes int64  `json:"content_bytes"`
	Skipped      int64  `json:"skipped"`
	Project      string `json:"project,omitempty"`
	SourceName   string `json:"source_name,omitempty"`
	OriginHost   string `json:"origin_host,omitempty"`
	OriginRoot   string `json:"origin_root,omitempty"`
	GitHead      string `json:"git_head,omitempty"`
	GitDirty     *bool  `json:"git_dirty,omitempty"`
}

func (s *Server) sourceDir() (string, error) {
	if s.SourceDirectory == "" {
		return "", errors.New("this daemon keeps no code snapshots (source_directory is not set)")
	}
	if err := os.MkdirAll(s.SourceDirectory, 0o755); err != nil {
		return "", err
	}
	return s.SourceDirectory, nil
}

// uploadSource receives a snapshot (see above). One already kept is
// answered 200 without reading its pack.
func (s *Server) uploadSource(w http.ResponseWriter, r *http.Request) {
	if s.ObserveOnly {
		writeError(w, errObserveOnly)
		return
	}
	dir, err := s.sourceDir()
	if err != nil {
		writeError(w, err)
		return
	}
	reader, err := r.MultipartReader()
	if err != nil {
		writeError(w, fmt.Errorf("a snapshot is sent as multipart/form-data: %w", err))
		return
	}
	part, err := reader.NextPart()
	if err != nil || part.FormName() != "meta" {
		writeError(w, errors.New(`a snapshot's first part is "meta"`))
		return
	}
	var meta SourceUpload
	dec := json.NewDecoder(io.LimitReader(part, 512<<20))
	dec.DisallowUnknownFields()
	if err = dec.Decode(&meta); err != nil {
		writeError(w, fmt.Errorf("meta: %w", err))
		return
	}
	if !sourcesnap.ValidID(meta.ID) {
		writeError(w, fmt.Errorf("%q is not a snapshot id", meta.ID))
		return
	}
	if _, err := s.Store.SourceSnapshotByID(r.Context(), meta.ID); err == nil {
		writeJSON(w, http.StatusOK, map[string]any{"id": meta.ID, "stored": false})
		return
	} else if !errors.Is(err, store.ErrNotFound) {
		writeError(w, err)
		return
	}
	if part, err = reader.NextPart(); err != nil || part.FormName() != "pack" {
		writeError(w, errors.New(`a snapshot's second part is "pack"`))
		return
	}
	tmp, err := os.CreateTemp(dir, meta.ID+"-*.partial")
	if err != nil {
		writeError(w, err)
		return
	}
	defer func() {
		tmp.Close()
		_ = os.Remove(tmp.Name())
	}()
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(tmp, h), io.LimitReader(part, maxSourcePack+1))
	if err == nil {
		err = tmp.Sync()
	}
	if err != nil {
		writeError(w, err)
		return
	}
	if n != meta.PackBytes || hex.EncodeToString(h.Sum(nil)) != meta.PackSHA256 {
		writeError(w, fmt.Errorf("%w: the pack sent is %d bytes, sha256 %s; said %d bytes, sha256 %s", store.ErrSourceInvalid, n, hex.EncodeToString(h.Sum(nil)), meta.PackBytes, meta.PackSHA256))
		return
	}
	if err = checkSourcePack(tmp.Name(), meta.List); err != nil {
		writeError(w, fmt.Errorf("%w: %v", store.ErrSourceInvalid, err))
		return
	}
	pack := filepath.Join(dir, meta.ID+".tar.zst")
	if err = os.WriteFile(pack+".files.tsv", []byte(meta.List), 0o644); err != nil {
		writeError(w, err)
		return
	}
	tmp.Close()
	if err = os.Rename(tmp.Name(), pack); err != nil {
		writeError(w, err)
		return
	}
	snap := store.SourceSnapshot{ID: meta.ID, PackBytes: meta.PackBytes, PackSHA256: meta.PackSHA256, ListSHA256: meta.ListSHA256, List: meta.List,
		Files: meta.Files, ContentBytes: meta.ContentBytes, Skipped: meta.Skipped, Project: meta.Project, SourceName: meta.SourceName,
		OriginHost: meta.OriginHost, OriginRoot: meta.OriginRoot, GitHead: meta.GitHead, GitDirty: meta.GitDirty, CreatedBy: tokenName(r)}
	if err = s.Store.RecordSourceSnapshot(r.Context(), snap); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"id": meta.ID, "stored": true})
}

// checkSourcePack checks that the pack at path holds exactly the regular
// files list names, each of its size and sha256 (directories aside).
func checkSourcePack(path, list string) error {
	files, err := sourcesnap.Parse(list)
	if err != nil {
		return err
	}
	want := make(map[string]sourcesnap.File, len(files))
	for _, f := range files {
		want[sourcesnap.Unescape(f.Path)] = f
	}
	fh, err := os.Open(path)
	if err != nil {
		return err
	}
	defer fh.Close()
	zr, err := zstd.NewReader(fh)
	if err != nil {
		return err
	}
	defer zr.Close()
	tr := tar.NewReader(zr)
	seen := map[string]bool{}
	for {
		hd, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		switch hd.Typeflag {
		case tar.TypeDir:
			continue
		case tar.TypeReg:
		default:
			return fmt.Errorf("%s is neither a directory nor a regular file", hd.Name)
		}
		f, ok := want[hd.Name]
		if !ok || seen[hd.Name] {
			return fmt.Errorf("%s is not in the file list (or twice in the pack)", hd.Name)
		}
		h := sha256.New()
		n, err := io.Copy(h, tr)
		if err != nil {
			return err
		}
		if fmt.Sprint(n) != f.Bytes || hex.EncodeToString(h.Sum(nil)) != f.SHA256 {
			return fmt.Errorf("%s is %d bytes, sha256 %s; listed %s bytes, sha256 %s", hd.Name, n, hex.EncodeToString(h.Sum(nil)), f.Bytes, f.SHA256)
		}
		seen[hd.Name] = true
	}
	if len(seen) != len(want) {
		return fmt.Errorf("%d files listed, %d in the pack", len(want), len(seen))
	}
	return nil
}

// listSources lists snapshots newest first: ?project=, ?limit=.
func (s *Server) listSources(w http.ResponseWriter, r *http.Request) {
	values, err := s.Store.ListSourceSnapshots(r.Context(), r.URL.Query().Get("project"), queryInt(r, "limit", 100))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"sources": values})
}

// getSource is a snapshot with its file list and the tasks bound to it.
func (s *Server) getSource(w http.ResponseWriter, r *http.Request) {
	value, err := s.Store.SourceSnapshotByID(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"source": value})
}

// agentSourceFile streams a snapshot's pack or file list to an agent.
func (s *Server) agentSourceFile(w http.ResponseWriter, r *http.Request) {
	id, file := r.PathValue("id"), r.PathValue("file")
	if !sourcesnap.ValidID(id) || file != "pack" && file != "list" {
		writeError(w, store.ErrNotFound)
		return
	}
	if _, err := s.Store.SourceSnapshotByID(r.Context(), id); err != nil {
		writeError(w, err)
		return
	}
	name := id + ".tar.zst"
	if file == "list" {
		name += ".files.tsv"
	}
	f, err := os.Open(filepath.Join(s.SourceDirectory, name))
	if errors.Is(err, os.ErrNotExist) {
		writeError(w, store.ErrNotFound)
		return
	}
	if err != nil {
		writeError(w, err)
		return
	}
	defer f.Close()
	w.Header().Set("Content-Type", "application/octet-stream")
	_, _ = io.Copy(w, f)
}

// tokenName is the name of the token a request was made with ("" for none).
func tokenName(r *http.Request) string { return principalOf(r).Name }
