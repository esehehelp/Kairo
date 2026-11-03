package store

import (
	"context"
	"errors"
	"testing"

	"kairo/internal/machine"
)

func declareTestStore(t *testing.T, store *Store, stores ...machine.Store) {
	t.Helper()
	if _, err := store.ApplyMachines(context.Background(), storesSpec(stores...), false); err != nil {
		t.Fatal(err)
	}
}

// A store never reported is unknown (no agent serves it, or one older than
// stores); a report gives its filesystem's figures and what else is there.
func TestStoreStatusFromObservations(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	addTestDisk(t, store, "node", "D:")
	ctx := context.Background()
	declareTestStore(t, store,
		machine.Store{ID: "d-scratch", Class: machine.StoreVolatile, Node: "node", Root: "D:/kairo", Filesystem: "D:"},
		machine.Store{ID: "d-durable", Class: machine.StoreDurable, Node: "node", Root: "D:/durable", Filesystem: "D:", ReserveFreeBytes: 5},
	)
	status, err := store.ListStoreStatus(ctx)
	if err != nil || len(status) != 2 || status[0].ID != "d-durable" || status[0].State != "unknown" || status[1].State != "unknown" || status[0].TotalBytes != nil || status[0].ReserveFreeBytes != 5 {
		t.Fatalf("before any report: %+v %v", status, err)
	}
	if err = store.RecordStoreObservation(ctx, StoreObservation{StoreID: "elsewhere", ExecutorID: "executor", Usable: true}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("an undeclared store: %v", err)
	}
	total, free := int64(1000), int64(400)
	if err = store.RecordStoreObservation(ctx, StoreObservation{StoreID: "d-scratch", ExecutorID: "executor", Usable: true, TotalBytes: &total, FreeBytes: &free,
		Scratch: map[string]int64{"att_unrecorded": 50}}); err != nil {
		t.Fatal(err)
	}
	if err = store.RecordStoreObservation(ctx, StoreObservation{StoreID: "d-durable", ExecutorID: "executor", Usable: false, Problem: "root D:/durable is not on filesystem D:"}); err != nil {
		t.Fatal(err)
	}
	status, err = store.ListStoreStatus(ctx)
	if err != nil || len(status) != 2 {
		t.Fatalf("status: %+v %v", status, err)
	}
	durable, scratch := status[0], status[1]
	if durable.State != "unusable" || durable.Problem == nil || *durable.Problem != "root D:/durable is not on filesystem D:" {
		t.Fatalf("durable: %+v", durable)
	}
	// used 600, of which Kairo knows the 50 of a directory it made but never recorded
	if scratch.State != "ready" || *scratch.TotalBytes != 1000 || *scratch.FreeBytes != 400 || scratch.ScratchKeptBytes != 50 || scratch.ScratchActiveBytes != 0 ||
		scratch.ExternalBytes == nil || *scratch.ExternalBytes != 550 || scratch.ObservedBy == nil || *scratch.ObservedBy != "executor" {
		t.Fatalf("scratch: %+v", scratch)
	}
	// stale once its executor stops reporting
	if _, err = store.testExec(`UPDATE store_observations SET observed_at='2020-01-01T00:00:00Z' WHERE store_id='d-scratch'`); err != nil {
		t.Fatal(err)
	}
	if status, _ = store.ListStoreStatus(ctx); status[1].State != "stale" {
		t.Fatalf("stale: %+v", status[1])
	}
	// a store dropped from the declaration loses its observation
	declareTestStore(t, store, machine.Store{ID: "d-scratch", Class: machine.StoreVolatile, Node: "node", Root: "D:/kairo", Filesystem: "D:"})
	var observations int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM store_observations`).Scan(&observations); err != nil || observations != 1 {
		t.Fatalf("observations: %d %v", observations, err)
	}
}
