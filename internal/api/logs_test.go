package api

import (
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"kairo/internal/store"
)

func (d *testDaemon) getLog(t *testing.T, token, path string) (*http.Response, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, d.server.URL+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response, string(body)
}

func TestAttemptLogServesRecordedPaths(t *testing.T) {
	d := newTestDaemon(t, false)
	launch := d.workerLaunch(t)
	id := launch.Attempt.ID
	dir := t.TempDir()
	stderrPath := filepath.Join(dir, id+".stderr.log")
	if err := os.WriteFile(stderrPath, []byte("0123456789"), 0o600); err != nil {
		t.Fatal(err)
	}
	// stdout is recorded but was never written.
	if err := d.store.SetAttemptLogPaths(context.Background(), id, launch.Lease.CoordinationEpoch, filepath.Join(dir, id+".stdout.log"), stderrPath); err != nil {
		t.Fatal(err)
	}
	read := d.token(t, "dashboard", store.RoleRead, "")
	for _, c := range []struct {
		query, body, next string
	}{
		{"", "0123456789", "10"},
		{"?stream=stderr&offset=3&limit=4", "3456", "7"},
		{"?offset=8", "89", "10"},
		{"?offset=10", "", "10"},
		{"?offset=25", "", "25"},
		{"?limit=0", "", "0"},
	} {
		response, body := d.getLog(t, read, "/api/attempts/"+id+"/log"+c.query)
		if response.StatusCode != http.StatusOK || body != c.body {
			t.Fatalf("%q: %d %q", c.query, response.StatusCode, body)
		}
		if got := response.Header.Get("Kairo-Log-Size"); got != "10" {
			t.Fatalf("%q: size header %q", c.query, got)
		}
		if got := response.Header.Get("Kairo-Log-Next-Offset"); got != c.next {
			t.Fatalf("%q: next offset %q, want %s", c.query, got, c.next)
		}
		if got := response.Header.Get("Content-Type"); got != "text/plain; charset=utf-8" {
			t.Fatalf("%q: content type %q", c.query, got)
		}
	}
	for _, c := range []struct {
		path string
		want int
	}{
		{"/api/attempts/" + id + "/log?stream=stdout", http.StatusNotFound},
		{"/api/attempts/att_unknown/log", http.StatusNotFound},
		{"/api/attempts/" + id + "/log?stream=journal", http.StatusBadRequest},
		{"/api/attempts/" + id + "/log?offset=-1", http.StatusBadRequest},
		{"/api/attempts/" + id + "/log?limit=lots", http.StatusBadRequest},
	} {
		if response, body := d.getLog(t, read, c.path); response.StatusCode != c.want {
			t.Fatalf("%s: %d %s", c.path, response.StatusCode, body)
		}
	}
}

func TestAttemptLogWithoutRecordedPathIsNotFound(t *testing.T) {
	d := newTestDaemon(t, false)
	id := d.workerLaunch(t).Attempt.ID
	response, body := d.getLog(t, d.admin, "/api/attempts/"+id+"/log")
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("unrecorded log: %d %s", response.StatusCode, body)
	}
}
