package store

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"kairo/internal/machine"
)

// A running checkpointable attempt that has gone long without a checkpoint is
// asked for one (one in flight at a time); a checkpoint it publishes that is
// a path of its node is imported once, as checkpoints/<task>/<name>; a
// reference that is no path is left alone. An import whose source is gone,
// or that an operator cancels, is abandoned and can be made again.
func TestRunningWorkIsAskedForCheckpointsAndWhatItPublishesIsKept(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	addTestDisk(t, store, "node", "D:")
	ctx := context.Background()
	if err := store.UpsertExecutor(ctx, Executor{ID: "win-exec", NodeID: "node", Kind: "windows", Attributes: json.RawMessage(`{"environment":"windows"}`), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	declareTestStore(t, store, machine.Store{ID: "d-scratch", Class: machine.StoreVolatile, Node: "node", Root: "D:/kairo", Filesystem: "D:"})
	if _, err := store.ApplyProject(ctx, projectSpec(t, orchestrationManifest())); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	finishLaunch(t, store, launchNext(t, store), 0) // prepare: not checkpointable
	projectTasks(t, store, "orchestrated")
	train := launchNext(t, store)
	attempt, lease, epoch := train.Attempt.ID, train.Lease.ID, train.Lease.CoordinationEpoch

	if commands, err := store.RequestDueCheckpoints(ctx, time.Hour); err != nil || len(commands) != 0 {
		t.Fatalf("asked an attempt that just started: %v %v", commands, err)
	}
	if _, err := store.testExec(`UPDATE attempts SET started_at='2026-01-01T00:00:00Z' WHERE id=$1`, attempt); err != nil {
		t.Fatal(err)
	}
	// only a project that has published a path checkpoint is asked
	if commands, err := store.RequestDueCheckpoints(ctx, time.Hour); err != nil || len(commands) != 0 {
		t.Fatalf("asked a project that never published a path: %v %v", commands, err)
	}
	operator, err := store.CheckpointTask(ctx, "orchestrated", "train", "tester")
	if err != nil || len(operator.Commands) != 1 {
		t.Fatalf("operator checkpoint: %+v %v", operator, err)
	}
	if err = store.AckCommand(ctx, attempt, lease, epoch, operator.Commands[0], "checkpointed", json.RawMessage(`{"continuation_ref":"D:\\ckpt\\train\\step_100.pt"}`)); err != nil {
		t.Fatal(err)
	}
	// These offsets reverse the order of the timestamp strings around the cutoff.
	recent := time.Now().In(time.FixedZone("", -12*3600)).Format(time.RFC3339Nano)
	if _, err = store.testExec(`UPDATE commands SET created_at=$1 WHERE id=$2`, recent, operator.Commands[0]); err != nil {
		t.Fatal(err)
	}
	if commands, err := store.RequestDueCheckpoints(ctx, time.Hour); err != nil || len(commands) != 0 {
		t.Fatalf("asked after a recent checkpoint: %v %v", commands, err)
	}
	if _, err = store.testExec(`UPDATE commands SET created_at='2026-01-01T00:00:00Z' WHERE id=$1`, operator.Commands[0]); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-2 * time.Hour).In(time.FixedZone("", 12*3600)).Format(time.RFC3339Nano)
	if _, err = store.testExec(`UPDATE attempts SET started_at=$1 WHERE id=$2`, old, attempt); err != nil {
		t.Fatal(err)
	}
	commands, err := store.RequestDueCheckpoints(ctx, time.Hour)
	if err != nil || len(commands) != 1 {
		t.Fatalf("an attempt running for hours not asked: %v %v", commands, err)
	}
	if again, err := store.RequestDueCheckpoints(ctx, time.Hour); err != nil || len(again) != 0 {
		t.Fatalf("asked again while one is in flight: %v %v", again, err)
	}
	polled, err := store.PollCommands(ctx, attempt, lease, epoch)
	if err != nil || len(polled) != 1 || polled[0].ID != commands[0] || polled[0].Kind != "checkpoint" || polled[0].Origin != "data_protection" {
		t.Fatalf("poll: %+v %v", polled, err)
	}
	ref := "D:/ckpt/train/step_200.pt"
	if err = store.AckCommand(ctx, attempt, lease, epoch, commands[0], "checkpointed", json.RawMessage(`{"continuation_ref":"`+ref+`"}`)); err != nil {
		t.Fatal(err)
	}
	if again, err := store.RequestDueCheckpoints(ctx, time.Hour); err != nil || len(again) != 0 {
		t.Fatalf("asked again right after a checkpoint: %v %v", again, err)
	}

	objectID, err := store.ProtectCheckpoint(ctx, attempt, ref)
	if err != nil || objectID == "" {
		t.Fatalf("checkpoint not kept: %q %v", objectID, err)
	}
	if again, err := store.ProtectCheckpoint(ctx, attempt, ref); err != nil || again != objectID {
		t.Fatalf("kept twice: %q %v", again, err)
	}
	if none, err := store.ProtectCheckpoint(ctx, attempt, "checkpoint://step-200"); err != nil || none != "" {
		t.Fatalf("a reference that is no path: %q %v", none, err)
	}
	objects, err := store.DataObjects(ctx, DataObjectFilter{Project: "orchestrated", Kind: ObjectImport})
	if err != nil || len(objects) != 1 {
		t.Fatalf("objects %+v %v", objects, err)
	}
	o := objects[0]
	if o.ID != objectID || o.Name == nil || *o.Name != "checkpoints/train/step_200.pt" || o.SourcePath != ref || o.StoreID != "d-scratch" || o.State != "packing" {
		t.Fatalf("checkpoint object %+v", o)
	}

	if err = store.AbandonPack(ctx, "executor", objectID, "no such file"); !errors.Is(err, ErrNotAbandonable) {
		t.Fatalf("abandoned by an executor not packing it: %v", err)
	}
	if err = store.AbandonPack(ctx, "win-exec", objectID, "no such file"); err != nil {
		t.Fatal(err)
	}
	objects, _ = store.DataObjects(ctx, DataObjectFilter{Project: "orchestrated", Kind: ObjectImport})
	if objects[0].State != "abandoned" || objects[0].SourceState != "kept" {
		t.Fatalf("abandoned object %+v", objects[0])
	}
	orders, err := store.PacksToMake(ctx, "win-exec")
	if err != nil || len(orders) != 0 {
		t.Fatalf("an abandoned import ordered packed: %+v %v", orders, err)
	}
	second, err := store.ProtectCheckpoint(ctx, attempt, ref)
	if err != nil || second == "" || second == objectID {
		t.Fatalf("the path not imported again once abandoned: %q %v", second, err)
	}
	if err = store.CancelImport(ctx, second, "tester"); err != nil {
		t.Fatal(err)
	}
	if err = store.CancelImport(ctx, second, "tester"); err != nil {
		t.Fatalf("cancelled twice: %v", err)
	}
	if err = store.CancelImport(ctx, "obj_none", "tester"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("an unknown object cancelled: %v", err)
	}
}

func TestCheckpointNamesAndPaths(t *testing.T) {
	for ref, want := range map[string]bool{`D:\ckpt\a.pt`: true, "d:/ckpt": true, "/srv/ckpt/a.pt": true, "//host/share": false, "checkpoint://x": false, "a.pt": false, "C:": false} {
		if nodePath(ref) != want {
			t.Fatalf("nodePath(%q) = %v", ref, !want)
		}
	}
	if got := checkpointName("train", `D:\ckpt\run\step_1.pt`); got != "checkpoints/train/step_1.pt" {
		t.Fatal(got)
	}
	if got := checkpointName("train", "/srv/ckpt/run/"); got != "checkpoints/train/run" {
		t.Fatal(got)
	}
}
