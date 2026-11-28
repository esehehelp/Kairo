package store

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"kairo/internal/machine"
)

// Replicas go to the least protected objects first: those whose copies are
// on the fewest hosts (two nodes of one host count once), then those with
// the fewest copies, then the smallest.
func TestReplicasGoToTheObjectsOnTheFewestHostsFirst(t *testing.T) {
	store := openCoordinationStore(t)
	store.SetDataPolicy(DataPolicy{Replicas: true})
	setupSchedulerInventory(t, store)
	addTestDisk(t, store, "node", "D:")
	ctx := context.Background()
	for _, n := range []string{"node2", "remote", "fresh"} {
		if err := store.UpsertNode(ctx, Node{ID: n, Name: n, OS: "linux", Architecture: "amd64", Enabled: true}); err != nil {
			t.Fatal(err)
		}
		addTestDisk(t, store, n, "/")
	}
	if err := store.UpsertExecutor(ctx, Executor{ID: "fresh-exec", NodeID: "fresh", Kind: "linux", Attributes: json.RawMessage(`{"pack_addr":"192.0.2.40:7475"}`), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	spec := storesSpec(
		machine.Store{ID: "a-durable", Class: machine.StoreDurable, Node: "node", Root: "E:/kairo", Filesystem: "D:"},
		machine.Store{ID: "b-durable", Class: machine.StoreDurable, Node: "node2", Root: "/srv/b", Filesystem: "/"},
		machine.Store{ID: "r-durable", Class: machine.StoreDurable, Node: "remote", Root: "/srv/r", Filesystem: "/"},
		machine.Store{ID: "z-new", Class: machine.StoreDurable, Node: "fresh", Root: "/srv/z", Filesystem: "/"},
		machine.Store{ID: "d-scratch", Class: machine.StoreVolatile, Node: "node", Root: "D:/kairo", Filesystem: "D:"},
	)
	spec.Nodes = append(spec.Nodes, machine.Node{ID: "node2"}, machine.Node{ID: "remote"}, machine.Node{ID: "fresh"})
	spec.Hosts = []machine.Host{{ID: "pc", Nodes: []string{"node", "node2"}}, {ID: "srv", Nodes: []string{"remote"}}}
	if _, err := store.ApplyMachines(ctx, spec, false); err != nil {
		t.Fatal(err)
	}
	sha := strings.Repeat("ab", 32)
	roots := map[string]string{"a-durable": "E:/kairo", "b-durable": "/srv/b", "r-durable": "/srv/r"}
	const gib = int64(1) << 30
	for _, o := range []struct {
		id     string
		bytes  int64
		stores []string
	}{
		{"obj_wide", 1 * gib, []string{"a-durable", "r-durable"}}, // two hosts
		{"obj_two", 2 * gib, []string{"a-durable", "b-durable"}},  // two copies, one host
		{"obj_one", 3 * gib, []string{"a-durable"}},               // one copy
	} {
		insertPacked(t, store, o.id, "executor", "d-scratch", "D:/kairo", sha)
		if _, err := store.testExec(`UPDATE data_objects SET pack_bytes=$1 WHERE id=$2`, o.bytes, o.id); err != nil {
			t.Fatal(err)
		}
		for _, s := range o.stores {
			if _, err := store.testExec(`INSERT INTO object_copies(object_id,store_id,executor_id,root,month,state,created_at,updated_at,verified_at)
				VALUES($1,$2,'executor',$3,'202611','verified','2026-11-06T01:02:03Z','2026-11-06T01:02:03Z','2026-11-06T01:02:03Z')`, o.id, s, roots[s]); err != nil {
				t.Fatal(err)
			}
		}
	}
	// room on z-new for the margin, the next object in order and half a GiB
	replicate := func(free int64) []string {
		t.Helper()
		total := 100 * gib
		if err := store.RecordStoreObservation(ctx, StoreObservation{StoreID: "z-new", ExecutorID: "fresh-exec", Root: "/srv/z", Usable: true, FreeBytes: &free, TotalBytes: &total}); err != nil {
			t.Fatal(err)
		}
		orders, err := store.CopiesToMake(ctx, "fresh-exec")
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for _, o := range copiesOnly(orders) {
			if o.StoreID != "z-new" {
				t.Fatalf("order to %s: %+v", o.StoreID, o)
			}
			got = append(got, o.ObjectID)
		}
		return got
	}
	// the only copy goes first, though it is the biggest
	if got := replicate(shedMinShort + 3*gib + gib/2); strings.Join(got, ",") != "obj_one" {
		t.Fatalf("first replica %v", got)
	}
	if _, err := store.testExec(`UPDATE object_copies SET state='verified',verified_at='2026-11-06T01:02:03Z' WHERE object_id='obj_one' AND store_id='z-new'`); err != nil {
		t.Fatal(err)
	}
	// then two copies on one host, before two copies on two hosts
	if got := replicate(shedMinShort + 2*gib + gib/2); strings.Join(got, ",") != "obj_two" {
		t.Fatalf("second replica %v", got)
	}
}
