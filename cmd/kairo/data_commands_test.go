package main

import (
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"kairo/internal/store"
)

func TestDataListPassesOutputFilters(t *testing.T) {
	f := newContractAPI()
	f.fallback = func(w http.ResponseWriter, r *http.Request) {
		writeJSONResponse(w, http.StatusOK, map[string]any{"objects": []store.DataObject{{ID: "obj_train", Kind: "output", State: "packed"}}})
	}
	server := f.serve(t)
	for _, mode := range [][]string{nil, {"--json"}} {
		args := append([]string{"data", "list", "--api", server.URL, "--project", "p", "--task", "train", "--execution", "exe_train", "--limit", "1"}, mode...)
		out, err := captureStdout(t, func() error { return run(args) })
		if err != nil || !strings.Contains(out, "obj_train") {
			t.Fatalf("list %v: %q %v", mode, out, err)
		}
		if len(mode) > 0 && !json.Valid([]byte(out)) {
			t.Fatalf("list --json: %q", out)
		}
	}
	want := "GET /api/data/objects?execution=exe_train&limit=1&project=p&task=train"
	if got := f.seen(); !slices.Equal(got, []string{want, want}) {
		t.Fatalf("requests: %v", got)
	}
}

func TestDataFetchWaitPrintsPathsWhenReady(t *testing.T) {
	clock := useFakeClock(t, time.Now())
	f := newContractAPI()
	polls := 0
	f.fallback = func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/data/fetches":
			var req store.FetchRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Project != "p" || req.Node != "n" || !slices.Equal(req.Objects, []string{"obj_a"}) || !slices.Equal(req.Data, []string{"b"}) {
				t.Errorf("fetch request: %+v %v", req, err)
			}
			writeJSONResponse(w, http.StatusCreated, map[string]any{"objects": []store.FetchedObject{{ObjectID: "obj_b"}, {ObjectID: "obj_a", Ordered: true}}})
		case "/api/data/objects/obj_a/placement":
			polls++
			if polls == 1 {
				writeJSONResponse(w, http.StatusOK, store.PlacementStatus{State: "fetching"})
			} else {
				writeJSONResponse(w, http.StatusOK, store.PlacementStatus{State: "ready", Path: "/fast/inputs/obj_a"})
			}
		case "/api/data/objects/obj_b/placement":
			writeJSONResponse(w, http.StatusOK, store.PlacementStatus{State: "ready", Path: "D:/kairo/inputs/obj_b"})
		default:
			http.NotFound(w, r)
		}
	}
	server := f.serve(t)
	args := []string{"data", "fetch", "--api", server.URL, "--project", "p", "--node", "n", "--data", "b", "obj_a"}
	out, err := captureStdout(t, func() error { return run(args) })
	if err != nil || out != "obj_b: there or on its way already\nobj_a: ordered\n" || len(f.seen()) != 1 {
		t.Fatalf("fetch without wait: %q %v; requests %v", out, err, f.seen())
	}
	start := clock.now
	out, err = captureStdout(t, func() error { return run(append(args, "--wait")) })
	if err != nil || out != "obj_b: D:/kairo/inputs/obj_b\nobj_a: /fast/inputs/obj_a\n" || clock.now.Sub(start) != 2*time.Second {
		t.Fatalf("waiting fetch: %q %v; waited %s", out, err, clock.now.Sub(start))
	}
	if got := f.seen()[2:]; !slices.Equal(got, []string{
		"GET /api/data/objects/obj_b/placement?node=n", "GET /api/data/objects/obj_a/placement?node=n", "GET /api/data/objects/obj_a/placement?node=n",
	}) {
		t.Fatalf("placement polls: %v", got)
	}
}

func TestDataFetchWaitExitCodes(t *testing.T) {
	for _, tc := range []struct {
		state, problem string
		code           int
	}{
		{"fetching", "timed out", waitTimeout},
		{"failed", "there is no pack to fetch", 1},
		{"abandoned", "the object was abandoned", 1},
	} {
		t.Run(tc.state, func(t *testing.T) {
			f := newContractAPI()
			f.fallback = func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPost {
					writeJSONResponse(w, http.StatusCreated, map[string]any{"objects": []store.FetchedObject{{ObjectID: "obj_a", Ordered: true}}})
					return
				}
				writeJSONResponse(w, http.StatusOK, store.PlacementStatus{State: tc.state, Problem: tc.problem})
			}
			server := f.serve(t)
			args := []string{"data", "fetch", "--api", server.URL, "--project", "p", "--node", "n", "obj_a", "--wait"}
			if tc.code == waitTimeout {
				args = append(args, "--timeout", "25ms")
			}
			out, err := captureStdout(t, func() error { return run(args) })
			if err == nil || exitCode(err) != tc.code || !strings.Contains(err.Error(), tc.problem) || out != "" {
				t.Fatalf("waiting fetch: %q %v (exit %d)", out, err, exitCode(err))
			}
		})
	}
}

// A POSIX path Git Bash rewrote into its own directory is told apart from a
// Windows path the user meant.
func TestMsysRewrittenPaths(t *testing.T) {
	t.Setenv("MSYSTEM", "MINGW64")
	for p, want := range map[string]bool{
		"C:/Program Files/Git/srv/kairo-import-smoke": true,
		`C:\Program Files\Git\root\data`:              true,
		"D:/work/example-app/local/nvme-datasets":     false,
		"F:/Strata-data":                              false,
		"/srv/models":                                 false,
	} {
		if got := msysRewritten(p); got != want {
			t.Fatalf("%s: %v, want %v", p, got, want)
		}
	}
	t.Setenv("MSYSTEM", "")
	if msysRewritten("C:/Program Files/Git/srv/x") {
		t.Fatal("outside Git Bash nothing is rewritten")
	}
}
