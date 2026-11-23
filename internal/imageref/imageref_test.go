package imageref

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const d64 = "5e2bdb1b0d542d5039cffb8181c65930093949194a65c365dfb1b0fe13a4fa5b"

func TestParse(t *testing.T) {
	cases := []struct{ in, want string }{
		{"python", "docker.io/library/python:latest"},
		{"python:3.12-slim", "docker.io/library/python:3.12-slim"},
		{"docker.io/library/python:3.12", "docker.io/library/python:3.12"},
		{"index.docker.io/pytorch/pytorch:2.5.1-cuda12.4-cudnn9-runtime", "docker.io/pytorch/pytorch:2.5.1-cuda12.4-cudnn9-runtime"},
		{"ghcr.io/org/team/app:1.2", "ghcr.io/org/team/app:1.2"},
		{"localhost:5000/app", "localhost:5000/app:latest"},
		{"python@sha256:" + d64, "docker.io/library/python@sha256:" + d64},
		{"python:3.12@sha256:" + d64, "docker.io/library/python:3.12@sha256:" + d64},
	}
	for _, c := range cases {
		r, err := Parse(c.in)
		if err != nil {
			t.Fatalf("Parse(%q): %v", c.in, err)
		}
		if r.String() != c.want {
			t.Errorf("Parse(%q) = %q, want %q", c.in, r.String(), c.want)
		}
	}
	for _, bad := range []string{"", "Python", "python:", "python@sha256:abc", "a//b", "python:bad tag", "-x/y"} {
		if _, err := Parse(bad); err == nil {
			t.Errorf("Parse(%q) accepted", bad)
		}
	}
	if _, err := ParsePinned("python:3.12"); err == nil {
		t.Error("ParsePinned accepted a tag")
	}
	r, _ := Parse("python:3.12@sha256:" + d64)
	if r.Pinned() != "docker.io/library/python@sha256:"+d64 {
		t.Errorf("Pinned = %q", r.Pinned())
	}
}

func TestResolveWithToken(t *testing.T) {
	manifest := []byte(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[]}`)
	sum := sha256.Sum256(manifest)
	digest := "sha256:" + hex.EncodeToString(sum[:])
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/token":
			if r.URL.Query().Get("scope") != "repository:org/app:pull" || r.URL.Query().Get("service") != "reg" {
				t.Errorf("token query %s", r.URL.RawQuery)
			}
			w.Write([]byte(`{"token":"tok"}`))
		case "/v2/org/app/manifests/1.0":
			if r.Header.Get("Authorization") != "Bearer tok" {
				w.Header().Set("WWW-Authenticate", `Bearer realm="`+srv.URL+`/token",service="reg",scope="repository:org/app:pull"`)
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			if !strings.Contains(r.Header.Get("Accept"), "image.index") {
				t.Errorf("accept %q", r.Header.Get("Accept"))
			}
			w.Header().Set("Docker-Content-Digest", digest)
			w.Write(manifest)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	host := strings.TrimPrefix(srv.URL, "http://")
	ref := Ref{Registry: "localhost" + host[strings.Index(host, ":"):], Repository: "org/app", Tag: "1.0"}
	got, err := Resolve(context.Background(), srv.Client(), ref)
	if err != nil {
		t.Fatal(err)
	}
	if got.Digest != digest || got.Tag != "1.0" {
		t.Fatalf("resolved %+v", got)
	}
}

func TestResolveRefusesAWrongDigest(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Docker-Content-Digest", "sha256:"+d64)
		w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	host := strings.TrimPrefix(srv.URL, "http://")
	ref := Ref{Registry: "localhost" + host[strings.Index(host, ":"):], Repository: "app", Tag: "x"}
	if _, err := Resolve(context.Background(), srv.Client(), ref); err == nil || !strings.Contains(err.Error(), "does not hash") {
		t.Fatalf("err = %v", err)
	}
}
