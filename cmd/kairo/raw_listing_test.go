package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"kairo/internal/orchestration"
)

// A Hugging Face raw's files are listed at its commit, page after page:
// those matching its patterns, with each LFS file's size and sha256.
func TestHuggingFaceRawsAreListedAtTheirCommit(t *testing.T) {
	commit := strings.Repeat("ab", 20)
	sha := strings.Repeat("c", 64)
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/datasets/someone/web/tree/"+commit {
			http.NotFound(w, r)
			return
		}
		if r.URL.Query().Get("cursor") == "" {
			w.Header().Set("Link", fmt.Sprintf(`<%s/datasets/someone/web/tree/%s?recursive=true&cursor=2>; rel="next"`, srv.URL, commit))
			fmt.Fprintf(w, `[{"type":"directory","path":"data"},{"type":"file","path":"data/b.parquet","size":134,"lfs":{"oid":"%s","size":2048}},{"type":"file","path":"README.md","size":10}]`, sha)
			return
		}
		fmt.Fprint(w, `[{"type":"file","path":"data/a.parquet","size":134,"lfs":{"oid":"`+sha+`","size":1024}},{"type":"file","path":"data/x/c.parquet","size":5}]`)
	}))
	defer srv.Close()
	old := hfAPI
	hfAPI = srv.URL + "/"
	t.Cleanup(func() { hfAPI = old })
	m := orchestration.Manifest{Raws: map[string]orchestration.Raw{
		"web":  {HF: &orchestration.HFRaw{Repo: "someone/web", Revision: commit, Files: []string{"data/*.parquet"}}},
		"wiki": {URLs: []string{"https://example.org/a.xml.bz2"}},
	}}
	if err := resolveRawListings(&m); err != nil {
		t.Fatal(err)
	}
	got := m.RawFiles["web"]
	if len(got) != 2 || got[0].Name != "data/a.parquet" || got[0].Size != 1024 || got[0].SHA256 != sha || got[1].Name != "data/b.parquet" ||
		got[0].URL != "https://huggingface.co/datasets/someone/web/resolve/"+commit+"/data/a.parquet" {
		t.Fatalf("listing %+v", got)
	}
	if _, listed := m.RawFiles["wiki"]; listed {
		t.Fatal("a URL raw was listed")
	}
	m.Raws["web"].HF.Repo = "someone/none"
	if err := resolveRawListings(&m); err == nil || !strings.Contains(err.Error(), "404") {
		t.Fatalf("a repository not there: %v", err)
	}
}
