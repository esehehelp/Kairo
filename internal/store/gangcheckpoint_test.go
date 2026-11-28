package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// publishGangPart has member r publish its part of round for command:
// accepted, checkpointing, then checkpointed with its reference.
func publishGangPart(t *testing.T, store *Store, r gangRankLaunch, rank int, commandID, round string) {
	t.Helper()
	ctx := context.Background()
	l := r.launch
	for _, phase := range []string{"accepted", "checkpointing"} {
		if err := store.AckCommand(ctx, l.Attempt.ID, l.Lease.ID, l.Lease.CoordinationEpoch, commandID, phase, nil); err != nil {
			t.Fatal(err)
		}
	}
	payload := fmt.Sprintf(`{"continuation_ref":"/kairo/output/%s-%d","gang":{"round":%q,"rank":%d,"size":2}}`, round, rank, round, rank)
	if err := store.AckCommand(ctx, l.Attempt.ID, l.Lease.ID, l.Lease.CoordinationEpoch, commandID, "checkpointed", json.RawMessage(payload)); err != nil {
		t.Fatal(err)
	}
}

// liveCommand is the live command of kind a member's attempt was given.
func liveCommand(t *testing.T, store *Store, r gangRankLaunch, kind string) string {
	t.Helper()
	commands, err := store.PollCommands(context.Background(), r.launch.Attempt.ID, r.launch.Lease.ID, r.launch.Lease.CoordinationEpoch)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range commands {
		if c.Kind == kind {
			return c.ID
		}
	}
	t.Fatalf("%s has no %s command: %+v", r.launch.Attempt.ID, kind, commands)
	return ""
}

// gangContinuationMembers are the members of the task's latest execution:
// by rank, the reference each goes on from and the node it is pinned to.
func gangContinuationMembers(t *testing.T, store *Store, task TaskStatus) map[int][2]string {
	t.Helper()
	leader := task.Executions[len(task.Executions)-1].ExecutionID
	rows, err := store.db.Query(`SELECT m.rank,COALESCE(e.input_continuation_ref,''),e.executor_selector_json FROM execution_gangs g
		JOIN gang_members m ON m.gang_id=g.id JOIN execution_requests e ON e.id=m.execution_id WHERE g.leader_execution_id=$1`, leader)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[int][2]string{}
	for rows.Next() {
		var rank int
		var ref, selector string
		if err := rows.Scan(&rank, &ref, &selector); err != nil {
			t.Fatal(err)
		}
		out[rank] = [2]string{ref, selectorNode(selector)}
	}
	return out
}

// A suspend one member got reaches every member; once every member
// published its part of the round, the gang goes on from it: each member
// from its own part, on its node, in its run.
func TestASuspendedGangGoesOnFromTheRoundEveryMemberPublished(t *testing.T) {
	store := openCoordinationStore(t)
	setupGangInventory(t, store)
	gangProject(t, store, true)
	ctx := context.Background()
	rank0, rank1 := launchGang(t, store)
	if _, err := store.EnqueueSuspend(ctx, rank1.launch.Attempt.ID, "scope_pause", "paused"); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcileGangs(ctx); err != nil {
		t.Fatal(err)
	}
	publishGangPart(t, store, rank0, 0, liveCommand(t, store, rank0, "suspend"), "s1")
	publishGangPart(t, store, rank1, 1, liveCommand(t, store, rank1, "suspend"), "s1")
	finishGangRank(t, store, rank0, 0)
	finishGangRank(t, store, rank1, 0)
	task := gangTaskState(t, store)
	if len(task.Executions) != 2 || task.Executions[1].Reason != "continuation" {
		t.Fatalf("not continued: %+v", task)
	}
	members := gangContinuationMembers(t, store, task)
	if members[0] != [2]string{"/kairo/output/s1-0", "node-a"} || members[1] != [2]string{"/kairo/output/s1-1", "node-b"} {
		t.Fatalf("members: %+v", members)
	}
	next0, next1 := launchGang(t, store)
	for _, x := range []struct {
		before, after gangRankLaunch
		ref           string
	}{{rank0, next0, "/kairo/output/s1-0"}, {rank1, next1, "/kairo/output/s1-1"}} {
		got := x.after.launch.Execution.InputContinuationRef
		if got == nil || *got != x.ref || x.after.launch.RunID != x.before.launch.RunID {
			t.Fatalf("a member goes on from %v in run %s, want %s in %s", got, x.after.launch.RunID, x.ref, x.before.launch.RunID)
		}
	}
}

// A checkpoint command one member got (data protection asks each attempt)
// reaches every member; a suspend not every member published its part of
// is not gone on from: the gang goes on from the latest round they all did.
func TestAGangGoesOnOnlyFromARoundEveryMemberPublished(t *testing.T) {
	store := openCoordinationStore(t)
	setupGangInventory(t, store)
	gangProject(t, store, true)
	ctx := context.Background()
	rank0, rank1 := launchGang(t, store)
	l := rank0.launch
	if _, err := store.testExec(`INSERT INTO commands(id,execution_id,attempt_id,lease_id,coordination_epoch,kind,origin,reason,state,created_at,updated_at)
		VALUES('cmd_dp',$1,$2,$3,$4,'checkpoint','data_protection','every 2 h','pending',$5,$5)`, l.Execution.ID, l.Attempt.ID, l.Lease.ID, l.Lease.CoordinationEpoch, now()); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcileGangs(ctx); err != nil {
		t.Fatal(err)
	}
	other := liveCommand(t, store, rank1, "checkpoint")
	var origin string
	if err := store.db.QueryRow(`SELECT origin FROM commands WHERE id=$1`, other).Scan(&origin); err != nil || origin != "data_protection" {
		t.Fatalf("propagated %q %v", origin, err)
	}
	publishGangPart(t, store, rank0, 0, "cmd_dp", "c1")
	publishGangPart(t, store, rank1, 1, other, "c1")
	// a suspend only member 1 publishes its part of (member 0 is killed)
	if _, err := store.EnqueueSuspend(ctx, rank1.launch.Attempt.ID, "scope_pause", "paused"); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcileGangs(ctx); err != nil {
		t.Fatal(err)
	}
	publishGangPart(t, store, rank1, 1, liveCommand(t, store, rank1, "suspend"), "s1")
	finishGangRank(t, store, rank1, 0)
	finishGangRank(t, store, rank0, 137)
	task := gangTaskState(t, store)
	if len(task.Executions) != 2 || task.Executions[1].Reason != "continuation" {
		t.Fatalf("not continued: %+v", task)
	}
	if members := gangContinuationMembers(t, store, task); members[0][0] != "/kairo/output/c1-0" || members[1][0] != "/kairo/output/c1-1" {
		t.Fatalf("members: %+v", members)
	}
}

// A gang declared continue_after_loss that loses a member goes on, once
// every lease is released, from its latest round every member published;
// with none, it is blocked.
func TestAGangThatLostAMemberGoesOnFromItsLatestCompleteRound(t *testing.T) {
	for _, published := range []bool{true, false} {
		t.Run(fmt.Sprint("published=", published), func(t *testing.T) {
			store := openCoordinationStore(t)
			setupGangInventory(t, store)
			gangProjectDeclared(t, store, "true", "continue_after_loss = true", "")
			ctx := context.Background()
			rank0, rank1 := launchGang(t, store)
			if published {
				asked, err := store.CheckpointTask(ctx, "gangs", "ddp", "tester")
				if err != nil || len(asked.Commands) != 2 {
					t.Fatalf("checkpoint: %+v %v", asked, err)
				}
				publishGangPart(t, store, rank0, 0, liveCommand(t, store, rank0, "checkpoint"), "c1")
				publishGangPart(t, store, rank1, 1, liveCommand(t, store, rank1, "checkpoint"), "c1")
			}
			// node-b loses power: its agent comes back without the attempt
			if n, err := store.MarkExecutorUnknown(ctx, "exec-b"); err != nil || n != 1 {
				t.Fatalf("mark unknown: %d %v", n, err)
			}
			if err := store.ReconcileGangs(ctx); err != nil {
				t.Fatal(err)
			}
			if err := store.MarkQuarantineTerminated(ctx, rank0.launch.Attempt.ID); err != nil {
				t.Fatal(err)
			}
			finishGangRank(t, store, rank0, 137)
			if task := gangTaskState(t, store); task.State != "running" || len(task.Executions) != 1 {
				t.Fatalf("before the lost member's lease is released: %+v", task)
			}
			refreshGangGPUs(t, store)
			if err := store.ReconcileResource(ctx, "gpu-b", true); err != nil {
				t.Fatal(err)
			}
			task := gangTaskState(t, store)
			if !published {
				if task.State != "blocked" || !strings.Contains(string(task.Result), "no checkpoint every member published") {
					t.Fatalf("without a gang-wide checkpoint: %+v", task)
				}
				return
			}
			if len(task.Executions) != 2 || task.Executions[1].Reason != "loss_continuation" {
				t.Fatalf("not continued: %+v", task)
			}
			if members := gangContinuationMembers(t, store, task); members[0] != [2]string{"/kairo/output/c1-0", "node-a"} || members[1] != [2]string{"/kairo/output/c1-1", "node-b"} {
				t.Fatalf("members: %+v", members)
			}
		})
	}
}
