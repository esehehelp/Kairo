package store

import (
	"context"
	"encoding/json"
	"slices"
	"testing"

	"kairo/internal/machine"
)

// An executor its node's agent no longer declares is disabled when the agent
// registers: its live lease goes stale, it serves no store, an event says so,
// and declaring it again enables it.
func TestAnExecutorNoLongerDeclaredIsDisabled(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	addTestDisk(t, store, "node", "D:")
	scratch := machine.Store{ID: "d-scratch", Class: machine.StoreVolatile, Node: "node", Root: "D:/kairo", Filesystem: "D:"}
	declareTestStore(t, store, scratch)
	ctx := context.Background()
	other := Executor{ID: "other", NodeID: "node", Kind: "windows", Attributes: json.RawMessage(`{}`), Enabled: true}
	if err := store.UpsertExecutor(ctx, other); err != nil {
		t.Fatal(err)
	}
	launch := authorizeScratchTest(t, store, "undeclared") // on "executor"

	gone, err := store.UndeclareExecutors(ctx, "node", []string{"other"})
	if err != nil || !slices.Equal(gone, []string{"executor"}) {
		t.Fatalf("undeclared %v %v", gone, err)
	}
	if gone, err = store.UndeclareExecutors(ctx, "node", []string{"other"}); err != nil || len(gone) != 0 {
		t.Fatalf("undeclared again %v %v", gone, err)
	}
	info, err := executorInfoTx(ctx, store.db, "executor")
	if err != nil || info.enabled || info.serves(scratch, "executor") {
		t.Fatalf("executor %+v %v", info, err)
	}
	if info, err = executorInfoTx(ctx, store.db, "other"); err != nil || !info.enabled || !info.serves(scratch, "other") {
		t.Fatalf("other %+v %v", info, err)
	}
	var lease string
	if err := store.db.QueryRowContext(ctx, `SELECT state FROM leases WHERE id=$1`, launch.Lease.ID).Scan(&lease); err != nil || lease != "stale" {
		t.Fatalf("lease %q %v", lease, err)
	}
	var events int
	if err := store.db.QueryRowContext(ctx, `SELECT count(*) FROM coordination_events WHERE event_type='executor_undeclared' AND aggregate_id='executor'`).Scan(&events); err != nil || events != 1 {
		t.Fatalf("events %d %v", events, err)
	}

	// declared again
	if err := store.UpsertExecutor(ctx, Executor{ID: "executor", NodeID: "node", Kind: "local", Attributes: json.RawMessage(`{}`), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if info, err = executorInfoTx(ctx, store.db, "executor"); err != nil || !info.enabled {
		t.Fatalf("declared again %+v %v", info, err)
	}
}
