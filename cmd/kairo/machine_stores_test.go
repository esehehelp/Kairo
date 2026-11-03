package main

import (
	"strings"
	"testing"

	"kairo/internal/machine"
	"kairo/internal/store"
)

func TestFormatApplyListsStoreChanges(t *testing.T) {
	from := machine.Store{ID: "pool", Class: "volatile", Node: "pve0", Root: "/srv/kairo", Filesystem: "/", Backend: "local-fs"}
	to := from
	to.Root = "/srv/kairo2"
	result := store.MachineApplyResult{
		Unchanged: 1,
		StoreChanges: []store.StoreChange{
			{Store: "e", To: &machine.Store{ID: "e", Class: "durable", Node: "win", Root: "E:/kairo", Filesystem: "E:", ReserveFreeBytes: 10}},
			{Store: "old", From: &from},
			{Store: "pool", From: &from, To: &to},
		},
	}
	want := "applied: 3 change(s), 1 unchanged\n" +
		"  + store e  class durable; node win; executor -; root E:/kairo; filesystem E:; backend local-fs; reserve_free_bytes 10\n" +
		"  - store old\n" +
		"  ~ store pool  root /srv/kairo -> /srv/kairo2\n"
	if got := formatApply("applied", result); got != want {
		t.Fatalf("output:\n%s\nwant:\n%s", got, want)
	}
	if got := formatApply("applied", store.MachineApplyResult{Unchanged: 1, UnchangedStores: 2}); got != "applied: no changes (1 GPUs, 2 stores declared)\n" {
		t.Fatalf("no changes: %q", got)
	}
}

func TestFormatMachinesListsStores(t *testing.T) {
	out := formatMachines(store.Machines{Stores: []machine.Store{{ID: "e", Class: "durable", Node: "win", Root: "E:/kairo", Filesystem: "E:"}}})
	for _, line := range []string{"STORE  CLASS    NODE  EXECUTOR  ROOT      FILESYSTEM", "e      durable  win   -         E:/kairo  E:"} {
		if !strings.Contains(out, line+"\n") {
			t.Fatalf("missing %q in:\n%s", line, out)
		}
	}
}
