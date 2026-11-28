package main

import (
	"archive/tar"
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"kairo/internal/dumpdelta"
)

func controlTar(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var b strings.Builder
	tw := tar.NewWriter(&b)
	for name, content := range files {
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(content))}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write([]byte(content)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	return []byte(b.String())
}

// kairo control snapshot keeps the tar only whole and holding the dump; an
// attempt with no output directory writes nothing.
func TestControlSnapshotKeepsOnlyAWholeSnapshotWithItsDump(t *testing.T) {
	var body []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/control/snapshot" || r.Header.Get("Authorization") != "Bearer kairo_admin_t" {
			http.Error(w, "no", http.StatusForbidden)
			return
		}
		_, _ = w.Write(body)
	}))
	defer server.Close()
	t.Setenv("KAIRO_TOKEN", "kairo_admin_t")
	t.Setenv("KAIRO_ATTEMPT_TOKEN", "")
	out := t.TempDir()
	whole := controlTar(t, map[string]string{"SNAPSHOT.txt": "x", "kairo.dump": "PGDMP"})
	for _, c := range []struct {
		name string
		body []byte
		ok   bool
	}{
		{"whole", whole, true},
		{"cut short", whole[:len(whole)/2], false},
		{"no dump", controlTar(t, map[string]string{"SNAPSHOT.txt": "x"}), false},
	} {
		body = c.body
		dir := filepath.Join(out, strings.ReplaceAll(c.name, " ", "-"))
		err := run([]string{"control", "snapshot", "--api", server.URL, "--out", dir})
		kept, _ := filepath.Glob(filepath.Join(dir, "*"))
		if c.ok != (err == nil) || c.ok != (len(kept) == 1 && strings.HasSuffix(kept[0], ".tar")) {
			t.Fatalf("%s: %v, kept %v", c.name, err, kept)
		}
	}
	t.Setenv("KAIRO_ATTEMPT_TOKEN", "kairo_worker_t")
	t.Setenv("KAIRO_API_URL", server.URL)
	t.Setenv("KAIRO_OUTPUT_DIR", "")
	if err := run([]string{"control", "snapshot"}); err == nil || !strings.Contains(err.Error(), "no output directory") {
		t.Fatalf("an attempt without an output directory: %v", err)
	}
	if _, err := os.Stat("kairo.dump"); err == nil {
		t.Fatal("written here")
	}
}

// A snapshot of the code (X-Kairo-Snapshot: sources) is kept as
// kairo-sources-<time>.tar only with its index and every code snapshot the
// index names, with its file list; it holds no dump.
func TestControlSnapshotOfTheCodeKeepsWhatItsIndexNames(t *testing.T) {
	var body []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Kairo-Snapshot", "sources")
		_, _ = w.Write(body)
	}))
	defer server.Close()
	t.Setenv("KAIRO_TOKEN", "kairo_admin_t")
	t.Setenv("KAIRO_ATTEMPT_TOKEN", "")
	out := t.TempDir()
	for _, c := range []struct {
		name  string
		files map[string]string
		ok    bool
	}{
		{"whole", map[string]string{"SNAPSHOT.txt": "x", "sources/src_a.tar.zst": "p", "sources/src_a.tar.zst.files.tsv": "l", "sources/INDEX": "src_a\n"}, true},
		{"nothing to keep", map[string]string{"SNAPSHOT.txt": "x", "sources/INDEX": ""}, true},
		{"no index", map[string]string{"SNAPSHOT.txt": "x", "sources/src_a.tar.zst": "p", "sources/src_a.tar.zst.files.tsv": "l"}, false},
		{"a list missing", map[string]string{"SNAPSHOT.txt": "x", "sources/src_a.tar.zst": "p", "sources/INDEX": "src_a\n"}, false},
	} {
		body = controlTar(t, c.files)
		dir := filepath.Join(out, strings.ReplaceAll(c.name, " ", "-"))
		err := run([]string{"control", "snapshot", "--api", server.URL, "--out", dir})
		kept, _ := filepath.Glob(filepath.Join(dir, "*"))
		if c.ok != (err == nil) || c.ok != (len(kept) == 1 && strings.HasPrefix(filepath.Base(kept[0]), "kairo-sources-")) {
			t.Fatalf("%s: %v, kept %v", c.name, err, kept)
		}
	}
}

// A snapshot whose dump is the difference from a base (X-Kairo-Snapshot:
// delta) is kept only with its recipe, the new bytes it says and which base;
// kairo control rebuild-dump makes the dump again from the base and it.
func TestAControlSnapshotsDeltaIsKeptWholeAndRebuilt(t *testing.T) {
	base := bytes.Repeat([]byte("the base of a dump; "), 40000)
	next := append(append([]byte{}, base[:300000]...), append([]byte("rows added since"), base[300000:]...)...)
	chunks, sum, n, err := dumpdelta.Base(bytes.NewReader(base), io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	var fresh, recipe bytes.Buffer
	rec, err := dumpdelta.Delta(bytes.NewReader(next), chunks, sum, n, &fresh)
	if err != nil {
		t.Fatal(err)
	}
	if err := rec.Write(&recipe); err != nil {
		t.Fatal(err)
	}
	var body []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Kairo-Snapshot", "delta")
		_, _ = w.Write(body)
	}))
	defer server.Close()
	t.Setenv("KAIRO_TOKEN", "kairo_admin_t")
	t.Setenv("KAIRO_ATTEMPT_TOKEN", "")
	out := t.TempDir()
	whole := map[string]string{"SNAPSHOT.txt": "x", "kairo.dump.base": "the base", "kairo.dump.recipe": recipe.String(), "kairo.dump.new": fresh.String()}
	for _, c := range []struct {
		name string
		drop string
		ok   bool
	}{{"whole", "", true}, {"no new bytes", "kairo.dump.new", false}, {"no recipe", "kairo.dump.recipe", false}} {
		files := map[string]string{}
		for k, v := range whole {
			if k != c.drop {
				files[k] = v
			}
		}
		body = controlTar(t, files)
		dir := filepath.Join(out, strings.ReplaceAll(c.name, " ", "-"))
		err := run([]string{"control", "snapshot", "--api", server.URL, "--out", dir})
		kept, _ := filepath.Glob(filepath.Join(dir, "*"))
		if c.ok != (err == nil) || c.ok != (len(kept) == 1) {
			t.Fatalf("%s: %v, kept %v", c.name, err, kept)
		}
	}
	dir := t.TempDir()
	for name, content := range map[string][]byte{"base.dump": base, "kairo.dump.recipe": recipe.Bytes(), "kairo.dump.new": fresh.Bytes()} {
		if err := os.WriteFile(filepath.Join(dir, name), content, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	rebuilt := filepath.Join(dir, "kairo.dump")
	if err := run([]string{"control", "rebuild-dump", "--base", filepath.Join(dir, "base.dump"), "--delta", dir, "--out", rebuilt}); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(rebuilt); !bytes.Equal(got, next) {
		t.Fatal("rebuilt another dump")
	}
}
