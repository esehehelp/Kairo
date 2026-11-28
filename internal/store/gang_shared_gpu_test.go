package store

import (
	"context"
	"encoding/json"
	"testing"
)

// Two ranks of a gang on two nodes that share their one GPU (two containers
// of one host) cannot both have it: the gang is not placed, and the lease
// guard refuses a second live lease of the device through its other row.
func TestGangRanksNeverShareOnePhysicalGPU(t *testing.T) {
	store := openCoordinationStore(t)
	setupSharedGPU(t, store)
	ctx := context.Background()
	if err := store.UpsertExecutor(ctx, Executor{ID: "exec-a", NodeID: "a", Kind: "linux", Attributes: json.RawMessage(`{"site":"a","interconnect_addr":"10.0.0.1"}`), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	spec := sharedSpec("gang-shared", "a", 10)
	spec.Gang = &GangSpec{Size: 2, Ranks: []GangRank{{Rank: 1, ExecutorSelector: json.RawMessage(`{"labels":{"site":"b"}}`)}}}
	if _, _, err := store.SubmitExecution(ctx, spec); err != nil {
		t.Fatal(err)
	}
	for _, executor := range []string{"exec-a", "exec-b"} {
		if reservation, err := store.ReserveNext(ctx, executor); err != nil || reservation != nil {
			t.Fatalf("%s reserved a rank of a gang its GPU cannot hold: %+v %v", executor, reservation, err)
		}
	}
	var live int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM leases WHERE state!='released'`).Scan(&live); err != nil || live != 0 {
		t.Fatalf("live leases: %d %v", live, err)
	}

	// The guard: a live lease of the device through one node's row refuses
	// another through the other node's.
	var rows []string
	r, err := store.db.Query(`SELECT id FROM resource_instances WHERE kind='gpu' AND stable_identity='GPU-S' ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	for r.Next() {
		var id string
		if err = r.Scan(&id); err != nil {
			t.Fatal(err)
		}
		rows = append(rows, id)
	}
	r.Close()
	if len(rows) != 2 {
		t.Fatalf("rows of the shared GPU: %v", rows)
	}
	var execution string
	if err = store.db.QueryRow(`SELECT id FROM execution_requests ORDER BY id LIMIT 1`).Scan(&execution); err != nil {
		t.Fatal(err)
	}
	for i, statement := range []string{
		`INSERT INTO leases(id,execution_id,executor_id,state,created_at) VALUES('lease-guard-a',$1,'exec-a','active','now')`,
		`INSERT INTO leases(id,execution_id,executor_id,state,created_at) VALUES('lease-guard-b',$1,'exec-b','active','now')`,
	} {
		if _, err = store.testExec(statement, execution); err != nil {
			t.Fatalf("lease %d: %v", i, err)
		}
	}
	if _, err = store.testExec(`INSERT INTO lease_items(lease_id,resource_id,kind,quantity,filesystem) VALUES('lease-guard-a',$1,'gpu',1,'')`, rows[0]); err != nil {
		t.Fatal(err)
	}
	if _, err = store.testExec(`INSERT INTO lease_items(lease_id,resource_id,kind,quantity,filesystem) VALUES('lease-guard-b',$1,'gpu',1,'')`, rows[1]); err == nil {
		t.Fatal("a second live lease of the shared GPU through its other row was accepted")
	}
}
