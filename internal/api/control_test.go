package api

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"kairo/internal/store"
)

// An admin takes the control snapshot: the dump, the code snapshots and the
// control files, one tar; an attempt not of kairo/control-snapshot does not.
func TestControlSnapshotHoldsTheDumpSourcesAndControlFiles(t *testing.T) {
	d := newTestDaemon(t, false)
	sources := t.TempDir()
	if err := os.MkdirAll(filepath.Join(sources, "src_a"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sources, "src_a", "main.py"), []byte("print(1)\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(t.TempDir(), "kairo.toml")
	if err := os.WriteFile(config, []byte("listen = \":7474\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer((&Server{Store: d.store, SourceDirectory: sources, ControlFiles: []string{config, ""},
		DumpDatabase: func(_ context.Context, w io.Writer) error { _, err := io.WriteString(w, "PGDMP"); return err }}).Handler())
	defer server.Close()
	get := func(path, token string) (int, []byte) {
		req, _ := http.NewRequest(http.MethodGet, server.URL+path, nil)
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, body
	}
	status, body := get("/api/control/snapshot", d.admin)
	if status != http.StatusOK {
		t.Fatalf("snapshot: %d %s", status, body)
	}
	files := map[string]string{}
	tr := tar.NewReader(bytes.NewReader(body))
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		content, _ := io.ReadAll(tr)
		files[h.Name] = string(content)
	}
	if files["kairo.dump"] != "PGDMP" || files["sources/src_a/main.py"] != "print(1)\n" || files["config/kairo.toml"] == "" || files["SNAPSHOT.txt"] == "" {
		t.Fatalf("snapshot holds %v", files)
	}
	if status, body = get("/api/worker/control-snapshot", d.workerToken(t)); status != http.StatusNotFound {
		t.Fatalf("an attempt of another task took it: %d %s", status, body)
	}
	if status, _ = get("/api/control/snapshot", d.token(t, "dashboard", store.RoleRead, "")); status != http.StatusForbidden {
		t.Fatalf("a read token took it: %d", status)
	}
}

// An hourly snapshot's attempt is handed the dump and the control files, no
// code; a sources task's attempt the code snapshots it takes, each with its
// file list, and their index last; each says its part in X-Kairo-Snapshot.
func TestControlSnapshotPartsHoldWhatTheirTasksKeep(t *testing.T) {
	sources := t.TempDir()
	for _, name := range []string{"src_a.tar.zst", "src_a.tar.zst.files.tsv", "src_b.tar.zst", "src_b.tar.zst.files.tsv"} {
		if err := os.WriteFile(filepath.Join(sources, name), []byte(name), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	s := &Server{SourceDirectory: sources, DumpDatabase: func(_ context.Context, w io.Writer) error { _, err := io.WriteString(w, "PGDMP"); return err }}
	read := func(rec *httptest.ResponseRecorder) (map[string]string, []string) {
		t.Helper()
		files := map[string]string{}
		var order []string
		tr := tar.NewReader(bytes.NewReader(rec.Body.Bytes()))
		for {
			h, err := tr.Next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			content, _ := io.ReadAll(tr)
			files[h.Name] = string(content)
			order = append(order, h.Name)
		}
		return files, order
	}
	rec := httptest.NewRecorder()
	s.controlSnapshot(rec, httptest.NewRequest(http.MethodGet, "/api/worker/control-snapshot", nil), false)
	files, _ := read(rec)
	if rec.Header().Get("X-Kairo-Snapshot") != "snapshot" || files["kairo.dump"] != "PGDMP" || files["sources/src_a.tar.zst"] != "" {
		t.Fatalf("the hourly part: %s %v", rec.Header().Get("X-Kairo-Snapshot"), files)
	}
	rec = httptest.NewRecorder()
	s.sourcesSnapshot(rec, []string{"src_b"})
	files, order := read(rec)
	if rec.Header().Get("X-Kairo-Snapshot") != "sources" || files["sources/src_b.tar.zst"] != "src_b.tar.zst" || files["sources/src_b.tar.zst.files.tsv"] == "" ||
		files["sources/src_a.tar.zst"] != "" || files["kairo.dump"] != "" || files["sources/INDEX"] != "src_b\n" || order[len(order)-1] != "sources/INDEX" {
		t.Fatalf("the code's part: %s %v %v", rec.Header().Get("X-Kairo-Snapshot"), order, files)
	}
}
