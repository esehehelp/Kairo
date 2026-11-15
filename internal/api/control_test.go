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
