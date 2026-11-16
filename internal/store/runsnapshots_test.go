package store

import (
	"context"
	"strings"
	"testing"
	"time"

	"kairo/internal/id"
	"kairo/internal/machine"
)

// A running run's output directory is snapshotted every interval and after
// each checkpoint it publishes, one snapshot at a time, each from the time of
// the previous one; none once the policy says never.
func TestARunningRunsOutputIsSnapshotted(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	addTestDisk(t, store, "node", "D:")
	ctx := context.Background()
	declareTestStore(t, store,
		machine.Store{ID: "d-scratch", Class: machine.StoreVolatile, Node: "node", Root: "D:/kairo", Filesystem: "D:"},
		machine.Store{ID: "d-durable", Class: machine.StoreDurable, Node: "node", Root: "D:/durable", Filesystem: "D:"},
	)
	launch := authorizeScratchTest(t, store, "snap")
	if err := store.RecordRunOutput(ctx, outputOf(launch, "d-scratch")); err != nil {
		t.Fatal(err)
	}
	if err := store.ActivateLaunch(ctx, launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch, launch.AuthorizationToken, 100, "launcher:100"); err != nil {
		t.Fatal(err)
	}
	snapshots := func() []PackOrder {
		t.Helper()
		orders, err := store.PacksToMake(ctx, "executor")
		if err != nil {
			t.Fatal(err)
		}
		var out []PackOrder
		for _, o := range orders {
			if o.Kind == ObjectRunSnapshot {
				out = append(out, o)
			}
		}
		return out
	}
	if got := snapshots(); len(got) != 0 {
		t.Fatalf("snapshots with the policy off: %+v", got)
	}
	store.SetDataPolicy(DataPolicy{RunSnapshotInterval: time.Hour})
	if got := snapshots(); len(got) != 0 {
		t.Fatalf("a run just started was snapshotted: %+v", got)
	}
	if _, err := store.testExec(`UPDATE run_outputs SET created_at='2026-01-01T00:00:00Z' WHERE run_id=$1`, launch.RunID); err != nil {
		t.Fatal(err)
	}
	got := snapshots()
	if len(got) != 1 || got[0].SourceID != launch.RunID+"~1" || got[0].Since != "" || got[0].Path != "D:/kairo/outputs/"+launch.RunID {
		t.Fatalf("first snapshot %+v", got)
	}
	first := got[0]
	if again := snapshots(); len(again) != 1 || again[0].ObjectID != first.ObjectID {
		t.Fatalf("a second snapshot while one is packing: %+v", again)
	}
	sha := strings.Repeat("ab", 32)
	if err := store.RecordPack(ctx, PackMade{ObjectID: first.ObjectID, ExecutorID: "executor", PackPath: "D:/kairo/packs/" + first.ObjectID + ".tar.zst",
		ManifestPath: "D:/kairo/packs/" + first.ObjectID + ".tar.zst.files.tsv", PackBytes: 100, ContentBytes: 50, Files: 1, PackSHA256: sha, ManifestSHA256: sha}); err != nil {
		t.Fatal(err)
	}
	if got = snapshots(); len(got) != 0 {
		t.Fatalf("snapshotted again within the interval: %+v", got)
	}
	// a checkpoint published since: snapshotted at once, from the first
	stamp := now()
	if _, err := store.testExec(`INSERT INTO commands(id,execution_id,attempt_id,lease_id,coordination_epoch,kind,origin,reason,state,created_at,updated_at)
		VALUES($1,$2,$3,$4,$5,'checkpoint','operator_checkpoint','test','checkpointed',$6,$6)`,
		id.New("cmd"), launch.Execution.ID, launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch, stamp); err != nil {
		t.Fatal(err)
	}
	got = snapshots()
	if len(got) != 1 || got[0].SourceID != launch.RunID+"~2" || got[0].Since == "" {
		t.Fatalf("after a checkpoint %+v", got)
	}
	if RunOfSnapshot(got[0].SourceID) != launch.RunID {
		t.Fatalf("run of %s", got[0].SourceID)
	}
	// its directory is the run's: never claimed for deletion
	if err := store.SettleDeletion(ctx, ObjectDeletion{ObjectID: first.ObjectID, ExecutorID: "executor", State: SourceDeleting}); err == nil {
		t.Fatal("a run snapshot's directory was claimed for deletion")
	}
}
