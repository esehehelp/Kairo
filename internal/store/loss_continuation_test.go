package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
)

// lossProject applies the orchestrated project, its train task declared
// continue_after_loss or not, runs prepare and launches train; with a
// checkpoint, train publishes ref through a checkpoint command first.
func lossProject(t *testing.T, store *Store, continueAfterLoss bool, ref string) *Launch {
	t.Helper()
	ctx := context.Background()
	manifest := orchestrationManifest()
	if continueAfterLoss {
		manifest = strings.Replace(manifest, "checkpointable = true\npreemptible = true\n", "checkpointable = true\npreemptible = true\ncontinue_after_loss = true\n", 1)
	}
	if _, err := store.ApplyProject(ctx, projectSpec(t, manifest)); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	finishLaunch(t, store, launchNext(t, store), 0)
	projectTasks(t, store, "orchestrated")
	train := launchNext(t, store)
	if ref != "" {
		asked, err := store.CheckpointTask(ctx, "orchestrated", "train", "tester")
		if err != nil || len(asked.Commands) != 1 {
			t.Fatalf("checkpoint: %+v %v", asked, err)
		}
		for _, phase := range []string{"accepted", "checkpointing"} {
			if err = store.AckCommand(ctx, train.Attempt.ID, train.Lease.ID, train.Lease.CoordinationEpoch, asked.Commands[0], phase, nil); err != nil {
				t.Fatal(err)
			}
		}
		if err = store.AckCommand(ctx, train.Attempt.ID, train.Lease.ID, train.Lease.CoordinationEpoch, asked.Commands[0], "checkpointed", json.RawMessage(`{"continuation_ref":"`+ref+`"}`)); err != nil {
			t.Fatal(err)
		}
	}
	// the node loses power: its agent comes back without the attempt
	if n, err := store.MarkExecutorUnknown(ctx, "executor"); err != nil || n != 1 {
		t.Fatalf("mark unknown: %d %v", n, err)
	}
	return train
}

// trainExecutions are train's executions, latest first: the reason of the
// decision that made each, its continuation and the node it is pinned to.
func trainExecutions(t *testing.T, store *Store) []struct{ reason, continuation, node string } {
	t.Helper()
	rows, err := store.db.Query(`SELECT d.reason,COALESCE(e.input_continuation_ref,''),e.executor_selector_json FROM orchestration_decisions d
		JOIN orchestration_tasks t ON t.task_scope_id=d.task_scope_id
		JOIN orchestration_task_executions te ON te.task_scope_id=d.task_scope_id AND te.execution_ordinal=d.execution_ordinal
		JOIN execution_requests e ON e.id=te.execution_id
		WHERE t.name='train' AND d.state!='abandoned' ORDER BY d.execution_ordinal DESC`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []struct{ reason, continuation, node string }
	for rows.Next() {
		var x struct{ reason, continuation, node string }
		var selector string
		if err := rows.Scan(&x.reason, &x.continuation, &selector); err != nil {
			t.Fatal(err)
		}
		x.node = selectorNode(selector)
		out = append(out, x)
	}
	return out
}

// A task declared continue_after_loss whose attempt is lost waits, running,
// until nothing of the attempt can run (its lease released), then goes on
// from its latest checkpoint on the node that published it (issue #18).
func TestALostTaskContinuesFromItsLatestCheckpoint(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	ctx := context.Background()
	train := lossProject(t, store, true, "/srv/ckpt/step-200")
	if task := projectTasks(t, store, "orchestrated")["train"]; task.State != "running" {
		t.Fatalf("train %s while its lost attempt's lease is stale", task.State)
	}
	if got := trainExecutions(t, store); len(got) != 1 {
		t.Fatalf("a continuation before the lost attempt's lease was released: %+v", got)
	}
	refreshSchedulerGPU(t, store)
	if err := store.ReconcileResource(ctx, "gpu-0", true); err != nil {
		t.Fatal(err)
	}
	if task := projectTasks(t, store, "orchestrated")["train"]; task.State != "running" && task.State != "pending" {
		t.Fatalf("train %s after the release", task.State)
	}
	got := trainExecutions(t, store)
	if len(got) != 2 || got[0].reason != "loss_continuation" || got[0].continuation != "/srv/ckpt/step-200" || got[0].node != "node" {
		t.Fatalf("executions %+v", got)
	}
	next := launchNext(t, store)
	if next.Execution.InputContinuationRef == nil || *next.Execution.InputContinuationRef != "/srv/ckpt/step-200" {
		t.Fatalf("continuation handed: %v", next.Execution.InputContinuationRef)
	}
	// it goes on in its run: its output directory, holding the checkpoint
	// (/kairo/output in a container), is the one its run wrote
	if next.RunID != train.RunID {
		t.Fatalf("the continuation after a loss is in run %q, want its run %s", next.RunID, train.RunID)
	}
}

// Without a published checkpoint a lost task declared continue_after_loss
// is blocked, as is one not declared so (whatever it published).
func TestALostTaskWithoutACheckpointOrTheDeclarationIsBlocked(t *testing.T) {
	for _, c := range []struct {
		name     string
		declared bool
		ref      string
		reason   string
	}{
		{"no checkpoint", true, "", "attempt identity lost; no checkpoint to continue from"},
		{"not declared", false, "/srv/ckpt/step-200", "attempt identity lost"},
	} {
		t.Run(c.name, func(t *testing.T) {
			store := openCoordinationStore(t)
			setupSchedulerInventory(t, store)
			ctx := context.Background()
			lossProject(t, store, c.declared, c.ref)
			refreshSchedulerGPU(t, store)
			if err := store.ReconcileResource(ctx, "gpu-0", true); err != nil {
				t.Fatal(err)
			}
			projectTasks(t, store, "orchestrated")
			var state string
			var result sql.NullString
			if err := store.db.QueryRow(`SELECT state,result_json FROM orchestration_tasks WHERE name='train'`).Scan(&state, &result); err != nil {
				t.Fatal(err)
			}
			var body struct {
				Reason string `json:"reason"`
			}
			_ = json.Unmarshal([]byte(result.String), &body)
			if state != "blocked" || body.Reason != c.reason {
				t.Fatalf("train %s %q", state, body.Reason)
			}
			if got := trainExecutions(t, store); len(got) != 1 {
				t.Fatalf("executions %+v", got)
			}
		})
	}
}

// kairo task rerun --from-checkpoint: the rerun goes on from the latest (or
// the named) checkpoint, on its node; a task that published none, a command
// that is not one of its checkpoints, or a task that is not checkpointable is
// refused.
func TestARerunGoesOnFromACheckpoint(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	ctx := context.Background()
	lossProject(t, store, false, "/srv/ckpt/step-300")
	projectTasks(t, store, "orchestrated")
	refreshSchedulerGPU(t, store)
	if err := store.ReconcileResource(ctx, "gpu-0", true); err != nil {
		t.Fatal(err)
	}
	_, err := store.RerunTaskWith(ctx, "orchestrated", "prepare", "tester", "", RerunOptions{FromCheckpoint: "latest"})
	taskConflictError(t, err, "task prepare is succeeded")
	_, err = store.RerunTaskWith(ctx, "orchestrated", "train", "tester", "", RerunOptions{FromCheckpoint: "cmd_not_a_checkpoint"})
	taskConflictError(t, err, "cmd_not_a_checkpoint is not a checkpoint task train published")
	out, err := store.RerunTaskWith(ctx, "orchestrated", "train", "tester", "", RerunOptions{FromCheckpoint: "latest"})
	if err != nil || out.FromCheckpoint == nil || out.FromCheckpoint.ContinuationRef != "/srv/ckpt/step-300" || out.FromCheckpoint.Node != "node" || out.FromCheckpoint.CommandID == "" {
		t.Fatalf("rerun from the latest checkpoint: %+v %v", out, err)
	}
	projectTasks(t, store, "orchestrated")
	got := trainExecutions(t, store)
	if len(got) != 2 || got[0].reason != "rerun" || got[0].continuation != "/srv/ckpt/step-300" || got[0].node != "node" {
		t.Fatalf("executions %+v", got)
	}
}
