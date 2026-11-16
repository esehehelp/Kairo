package store

import (
	"context"
	"errors"
	"testing"
	"time"
)

// A node gone for good is retired: fenced, its stale leases released with
// nothing to observe, its executors and itself disabled; not while an
// executor of it still polls.
func TestARetiredNodesLeasesAreReleased(t *testing.T) {
	store, _, launch := startCooperativeExecution(t, "retire")
	ctx := context.Background()
	if _, err := store.testExec(`UPDATE executors SET last_seen_at=$1 WHERE id='executor'`, now()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RetireNode(ctx, "node", "me", "gone"); !errors.Is(err, ErrExecutorLive) {
		t.Fatalf("a polling node retired: %v", err)
	}
	if _, err := store.testExec(`UPDATE executors SET last_seen_at=$1 WHERE id='executor'`, time.Now().UTC().Add(-10*time.Minute).Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	released, err := store.RetireNode(ctx, "node", "me", "container deleted")
	if err != nil || released != 1 {
		t.Fatalf("retired: %d %v", released, err)
	}
	var lease, attempt string
	var executorEnabled, nodeEnabled bool
	if err = store.db.QueryRow(`SELECT l.state,a.state FROM leases l JOIN attempts a ON a.id=l.attempt_id WHERE l.id=$1`, launch.Lease.ID).Scan(&lease, &attempt); err != nil {
		t.Fatal(err)
	}
	if err = store.db.QueryRow(`SELECT x.enabled,n.enabled FROM executors x JOIN nodes n ON n.id=x.node_id WHERE x.id='executor'`).Scan(&executorEnabled, &nodeEnabled); err != nil {
		t.Fatal(err)
	}
	if lease != "released" || attempt != "quiesced" || executorEnabled || nodeEnabled {
		t.Fatalf("after retiring: lease %s attempt %s executor enabled %v node enabled %v", lease, attempt, executorEnabled, nodeEnabled)
	}
}
