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
	stderrPath := filepath.Join(d.logs, id+".stderr.log")
	if err := os.WriteFile(stderrPath, []byte("0123456789"), 0o600); err != nil {
		t.Fatal(err)
	}
	// stdout is recorded but was never written.
	if err := d.store.SetAttemptLogPaths(context.Background(), id, launch.Lease.CoordinationEpoch, filepath.Join(d.logs, id+".stdout.log"), stderrPath); err != nil {
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
		{"?tail=3", "789", "10"},
		{"?tail=100", "0123456789", "10"},
		{"?tail=0", "", "10"},
	} {
		response, body := d.getLog(t, read, "/api/attempts/"+id+"/log"+c.query)
		if response.StatusCode != http.StatusOK || body != c.body {
			t.Fatalf("%q: %d %q", c.query, response.StatusCode, body)
		}
		for header, want := range map[string]string{
			"Kairo-Log-Size": "10", "Kairo-Log-Next-Offset": c.next,
			"Content-Type": "text/plain; charset=utf-8",
		} {
			if got := response.Header.Get(header); got != want {
				t.Fatalf("%q: %s %q, want %q", c.query, header, got, want)
			}
		}
	}
	for _, c := range []struct {
		path string
		want int
	}{
		{"/api/attempts/att_unknown/log", http.StatusNotFound},
		{"/api/attempts/" + id + "/log?stream=journal", http.StatusBadRequest},
		{"/api/attempts/" + id + "/log?offset=-1", http.StatusBadRequest},
		{"/api/attempts/" + id + "/log?limit=lots", http.StatusBadRequest},
		{"/api/attempts/" + id + "/log?tail=lots", http.StatusBadRequest},
		{"/api/attempts/" + id + "/log?tail=3&offset=1", http.StatusBadRequest},
	} {
		if response, body := d.getLog(t, read, c.path); response.StatusCode != c.want {
			t.Fatalf("%s: %d %s", c.path, response.StatusCode, body)
		}
	}
	// A recorded log a node agent has not shipped yet reads as empty so far.
	response, body := d.getLog(t, read, "/api/attempts/"+id+"/log?stream=stdout")
	if response.StatusCode != http.StatusOK || len(body) != 0 || response.Header.Get("Kairo-Log-Size") != "0" || response.Header.Get("Kairo-Log-Next-Offset") != "0" {
		t.Fatalf("unshipped log: %d %q size=%q", response.StatusCode, body, response.Header.Get("Kairo-Log-Size"))
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

// Only a recorded path inside the daemon's log directory is ever served.
func TestAttemptLogStaysInsideTheLogDirectory(t *testing.T) {
	d := newTestDaemon(t, false)
	launch := d.workerLaunch(t)
	outside := filepath.Join(t.TempDir(), "secret.txt")
	if err := os.WriteFile(outside, []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(d.logs, launch.Attempt.ID+".stdout.log")
	paths := []string{outside, d.logs + string(filepath.Separator) + ".." + string(filepath.Separator) + filepath.Base(filepath.Dir(outside)) + string(filepath.Separator) + "secret.txt", d.logs}
	if err := os.Symlink(outside, link); err == nil {
		paths = append(paths, link)
	} else {
		t.Logf("no symbolic link test: %v", err)
	}
	for _, path := range paths {
		if err := d.store.SetAttemptLogPaths(context.Background(), launch.Attempt.ID, launch.Lease.CoordinationEpoch, path, path); err != nil {
			t.Fatal(err)
		}
		response, body := d.getLog(t, d.admin, "/api/attempts/"+launch.Attempt.ID+"/log?stream=stdout")
		if response.StatusCode != http.StatusForbidden || body == "secret" {
			t.Fatalf("%s: %d %s", path, response.StatusCode, body)
		}
	}
}
