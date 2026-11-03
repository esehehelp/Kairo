package store

import (
	"context"
	"errors"
	"testing"
	"time"

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

// A report updates every scratch record it found in one go (bytes when
// measured, -1 keeping the last), takes a record it no longer finds after a
// whole scan as missing (bytes 0, shown, never deleted: Kairo did not delete
// it), spares records made during the scan, and brings a missing one back
// when it is found again. A missing scratch whose attempt succeeded is still
// settled.
func TestScratchMissingFromTheReport(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	addTestDisk(t, store, "node", "D:")
	declareTestStore(t, store, machine.Store{ID: "d-scratch", Class: machine.StoreVolatile, Node: "node", Root: "D:/kairo", Filesystem: "D:"})
	ctx := context.Background()
	failed := startWithScratch(t, store, "failed")
	finishLaunch(t, store, failed, 1)
	size := int64(900)
	if err := store.SettleScratch(ctx, ScratchSettled{AttemptID: failed.Attempt.ID, State: "kept", Bytes: &size}); err != nil {
		t.Fatal(err)
	}
	running := startWithScratch(t, store, "running")
	report := func(scanned bool, seconds float64, scratch map[string]int64) {
		t.Helper()
		total, free := int64(10_000), int64(5_000)
		if err := store.RecordStoreObservation(ctx, StoreObservation{StoreID: "d-scratch", ExecutorID: "executor", Usable: true, TotalBytes: &total, FreeBytes: &free,
			Scratch: scratch, ScratchScanned: scanned, ScanSeconds: seconds}); err != nil {
			t.Fatal(err)
		}
	}
	expect := func(attemptID, state string, bytes int64) {
		t.Helper()
		got, b, _ := scratchState(t, store, attemptID)
		if got != state || b == nil || *b != bytes {
			t.Fatalf("%s: %s %v, want %s %d", attemptID, got, b, state, bytes)
		}
	}

	// both found: the running one measured, the kept one not measured again
	report(true, 1, map[string]int64{running.Attempt.ID: 300, failed.Attempt.ID: -1})
	expect(running.Attempt.ID, "active", 300)
	expect(failed.Attempt.ID, "kept", 900)

	// made during the scan (as both just were): not missing although not found
	report(true, 3600, map[string]int64{})
	expect(running.Attempt.ID, "active", 300)
	if _, err := store.testExec(`UPDATE attempt_scratch SET created_at='2026-01-01T00:00:00Z'`); err != nil {
		t.Fatal(err)
	}
	// a report whose scan did not finish marks nothing missing
	report(false, 1, map[string]int64{})
	expect(running.Attempt.ID, "active", 300)
	// a whole scan that does not find them: missing, with no bytes
	report(true, 1, map[string]int64{})
	expect(running.Attempt.ID, "missing", 0)
	expect(failed.Attempt.ID, "missing", 0)
	status, err := store.ListStoreStatus(ctx)
	if err != nil || len(status[0].Missing) != 2 || len(status[0].Kept) != 0 || status[0].ScratchActiveBytes != 0 || status[0].ScratchKeptBytes != 0 ||
		status[0].Missing[0].MissingAt == nil || *status[0].ExternalBytes != 5_000 {
		t.Fatalf("status: %+v %v", status, err)
	}
	// found again: active, and kept (it was settled), again
	report(true, 1, map[string]int64{running.Attempt.ID: -1, failed.Attempt.ID: 50})
	expect(running.Attempt.ID, "active", 0)
	expect(failed.Attempt.ID, "kept", 50)
	report(true, 1, map[string]int64{running.Attempt.ID: 700, failed.Attempt.ID: -1})
	expect(running.Attempt.ID, "active", 700)
	if status, _ = store.ListStoreStatus(ctx); len(status[0].Missing) != 0 || status[0].ScratchActiveBytes != 700 || status[0].ScratchKeptBytes != 50 {
		t.Fatalf("status: %+v", status[0])
	}

	// missing when its attempt succeeds: still settled (its deletion finds
	// nothing to delete)
	report(true, 1, map[string]int64{failed.Attempt.ID: -1})
	expect(running.Attempt.ID, "missing", 0)
	finishLaunch(t, store, running, 0)
	settleExpect(t, store, map[string]ScratchSettlement{running.Attempt.ID: {Action: ScratchDelete, Reason: "exit 0"}})
	if err = store.SettleScratch(ctx, ScratchSettled{AttemptID: running.Attempt.ID, State: "deleted"}); err != nil {
		t.Fatal(err)
	}
	if state, _, _ := scratchState(t, store, running.Attempt.ID); state != "deleted" {
		t.Fatalf("settled missing scratch: %s", state)
	}
	// a deleted record found again (the report raced the deletion) stays deleted
	report(true, 1, map[string]int64{running.Attempt.ID: 10, failed.Attempt.ID: -1})
	if state, _, _ := scratchState(t, store, running.Attempt.ID); state != "deleted" {
		t.Fatalf("deleted scratch revived: %s", state)
	}
}

// A report does not wait for the decision lock: no decision reads it.
func TestStoreReportsDoNotWaitForDecisions(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	addTestDisk(t, store, "node", "D:")
	declareTestStore(t, store, machine.Store{ID: "d-scratch", Class: machine.StoreVolatile, Node: "node", Root: "D:/kairo", Filesystem: "D:"})
	ctx := context.Background()
	holder, err := store.beginWrite(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Rollback()
	done := make(chan error, 1)
	go func() {
		done <- store.RecordStoreObservation(ctx, StoreObservation{StoreID: "d-scratch", ExecutorID: "executor", Usable: true})
	}()
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a store report waited for the decision lock")
	}
}
