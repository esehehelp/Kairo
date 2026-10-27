package main

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"kairo/internal/store"
)

func TestMachineApplySendsTheDeclarationAndPrintsTheChanges(t *testing.T) {
	var got map[string]any
	server := fakeAPI(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/machines" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewDecoder(r.Body).Decode(&got)
		writeJSONResponse(w, http.StatusOK, store.MachineApplyResult{DryRun: true, Unchanged: 1, Changes: []store.MachineChange{
			{UUID: "GPU-a", From: "win", To: "pve"},
			{UUID: "GPU-b", To: "win"},
			{UUID: "GPU-c", From: "pve"},
		}})
	}))
	defer server.Close()
	path := filepath.Join(t.TempDir(), "machines.toml")
	source := "[[nodes]]\nid = \"win\"\ngpus = [\"GPU-b\"]\n\n[[nodes]]\nid = \"pve\"\ngpus = [\"GPU-a\", \"GPU-d\"]\n"
	if err := os.WriteFile(path, []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}
	out, err := captureStdout(t, func() error { return run([]string{"machine", "apply", "--dry-run", "--api", server.URL, path}) })
	if err != nil {
		t.Fatal(err)
	}
	if got["dry_run"] != true || len(got["nodes"].([]any)) != 2 {
		t.Fatalf("request body %+v", got)
	}
	want := "would apply: 3 change(s), 1 unchanged\n  ~ GPU-a  win -> pve\n  + GPU-b  win\n  - GPU-c  pve\n"
	if out != want {
		t.Fatalf("output:\n%s\nwant:\n%s", out, want)
	}
}

func TestMachineApplyRejectsAnInvalidFileLocally(t *testing.T) {
	isolateClientEnv(t)
	path := filepath.Join(t.TempDir(), "machines.toml")
	// (a GPU under two nodes is a shared one; twice under one node is a mistake)
	source := "[[nodes]]\nid = \"a\"\ngpus = [\"GPU-x\", \"GPU-x\"]\n[[nodes]]\nid = \"b\"\ngpus = [\"GPU-x\"]\n"
	if err := os.WriteFile(path, []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}
	err := run([]string{"machine", "apply", "--api", "http://127.0.0.1:1", path})
	if err == nil || !strings.Contains(err.Error(), "node a lists GPU-x twice") {
		t.Fatalf("duplicate GPU: %v", err)
	}
}

func TestFormatMachines(t *testing.T) {
	declared := "win"
	out := formatMachines(store.Machines{
		GPUs:       []store.DeclaredGPU{{UUID: "GPU-a", NodeID: "pve", State: "pending"}},
		Undeclared: []store.UndeclaredGPU{{UUID: "GPU-a", NodeID: "pve", ProviderID: "pve-nvidia", DeclaredNodeID: &declared}, {UUID: "GPU-n", NodeID: "win", ProviderID: "nvidia-local"}},
	})
	for _, line := range []string{
		"NODE  GPU    STATE",
		"pve   GPU-a  pending",
		"  GPU-a  on pve (pve-nvidia), declared for win",
		"  GPU-n  on win (nvidia-local), declared for no node",
	} {
		if !strings.Contains(out, line+"\n") {
			t.Fatalf("missing %q in:\n%s", line, out)
		}
	}
}
