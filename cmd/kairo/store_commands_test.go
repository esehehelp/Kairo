package main

import (
	"net/http"
	"strings"
	"testing"

	"kairo/internal/machine"
	"kairo/internal/store"
)

func TestStoreStatusPrintsStoresAndKeptScratch(t *testing.T) {
	gib := func(n int64) *int64 { v := n << 30; return &v }
	problem, kept, reason := "root E:/kairo is not on filesystem E:", "2026-11-03T01:02:03Z", "exit 1"
	stores := []store.StoreStatus{
		{Store: machine.Store{ID: "e-durable", Class: "durable", Node: "win", Root: "E:/kairo", Filesystem: "E:"}, State: "unusable", Problem: &problem, Kept: []store.KeptScratch{}},
		{Store: machine.Store{ID: "pool", Class: "volatile", Node: "pve0", Root: "/srv/kairo", Filesystem: "/"}, State: "ready",
			TotalBytes: gib(300), FreeBytes: gib(100), ScratchActiveBytes: 2 << 30, ScratchKeptBytes: 1 << 30, ExternalBytes: gib(197),
			Kept: []store.KeptScratch{{AttemptID: "att_1", ExecutionID: "exe_1", Task: "neo/train/run", Path: "/srv/kairo/scratch/att_1", Bytes: gib(1), KeptAt: &kept, Reason: &reason}}},
		{Store: machine.Store{ID: "wsl", Class: "volatile", Node: "win", Executor: "ubuntu-wsl", Root: "/var/tmp/kairo", Filesystem: "D:"}, State: "unknown", Kept: []store.KeptScratch{}},
	}
	server := fakeAPI(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/stores" {
			http.NotFound(w, r)
			return
		}
		writeJSONResponse(w, http.StatusOK, map[string]any{"stores": stores})
	}))
	defer server.Close()
	out, err := captureStdout(t, func() error { return run([]string{"store", "status", "--api", server.URL}) })
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"STORE      CLASS     NODE            STATE     ROOT            FILESYSTEM  TOTAL    FREE     ACTIVE  KEPT   PINNED  CACHE  EXTERNAL",
		"pool       volatile  pve0            ready     /srv/kairo      /           300 GiB  100 GiB  2 GiB   1 GiB  0 B     0 B    197 GiB",
		"wsl        volatile  win/ubuntu-wsl  unknown   /var/tmp/kairo  D:          -        -        0 B     0 B    0 B     0 B    -",
		"e-durable: root E:/kairo is not on filesystem E:",
		"kept scratch on pool:",
		"att_1    neo/train/run  1 GiB  2026-11-03T01:02:03Z  exit 1  /srv/kairo/scratch/att_1",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in:\n%s", want, out)
		}
	}
}

func TestStoreStatusAgainstAnOlderDaemon(t *testing.T) {
	server := fakeAPI(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.NotFound(w, r) }))
	defer server.Close()
	err := run([]string{"store", "status", "--api", server.URL})
	if err == nil || !strings.Contains(err.Error(), "the daemon does not know stores") {
		t.Fatalf("older daemon: %v", err)
	}
}
