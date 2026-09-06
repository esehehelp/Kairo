package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func startPreemptionVictim(t *testing.T, store *Store, executorID string, preemptible bool, priority int) (*ExecutionRequest, *Launch) {
	t.Helper()
	ctx := context.Background()
	spec := schedulerExecutionSpec("preemption-victim")
	spec.Priority = priority
	spec.Preemptible = preemptible
	execution, _, err := store.SubmitExecution(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	reservation, err := store.ReserveNext(ctx, executorID)
	if err != nil || reservation == nil {
		t.Fatalf("reserve victim: %+v %v", reservation, err)
	}
	if err = store.MarkLeasePrepared(ctx, reservation.Lease.ID, reservation.Lease.CoordinationEpoch); err != nil {
		t.Fatal(err)
	}
	launch, err := store.AuthorizeLaunch(ctx, reservation)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.ActivateLaunch(ctx, launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch, launch.AuthorizationToken, 4321, "win:4321:start:1"); err != nil {
		t.Fatal(err)
	}
	return &execution, launch
}

func submitPreemptionWaiter(t *testing.T, store *Store, priority int) ExecutionRequest {
	t.Helper()
	spec := schedulerExecutionSpec("preemption-waiter")
	spec.Priority = priority
	execution, _, err := store.SubmitExecution(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	return execution
}

func TestPriorityPreemptionEnqueuesOneIdempotentSuspendWithoutWorkflowTransition(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	victim, launch := startPreemptionVictim(t, store, "executor", true, 10)
	waiter := submitPreemptionWaiter(t, store, 100)

	commandIDs, err := store.EnsurePriorityPreemption(context.Background(), "executor")
	if err != nil || len(commandIDs) != 1 {
		t.Fatalf("preemption commands=%v err=%v", commandIDs, err)
	}
	second, err := store.EnsurePriorityPreemption(context.Background(), "executor")
	if err != nil || len(second) != 1 || second[0] != commandIDs[0] {
		t.Fatalf("preemption was not idempotent: first=%v second=%v err=%v", commandIDs, second, err)
	}
	var origin, attemptID, reason string
	if err = store.db.QueryRow(`SELECT origin,attempt_id,reason FROM commands WHERE id=?`, commandIDs[0]).Scan(&origin, &attemptID, &reason); err != nil {
		t.Fatal(err)
	}
	if origin != "priority_preemption" || attemptID != launch.Attempt.ID || reason == "" {
		t.Fatalf("wrong suspend command: origin=%q attempt=%q reason=%q", origin, attemptID, reason)
	}
	var commandCount int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM commands WHERE attempt_id=?`, launch.Attempt.ID).Scan(&commandCount); err != nil || commandCount != 1 {
		t.Fatalf("command count=%d err=%v", commandCount, err)
	}
	if err = store.AckCommand(context.Background(), launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch, commandIDs[0], "rejected", json.RawMessage(`{"reason":"not at safe point"}`)); err != nil {
		t.Fatal(err)
	}
	afterRejection, err := store.EnsurePriorityPreemption(context.Background(), "executor")
	if err != nil || len(afterRejection) != 0 {
		t.Fatalf("rejected suspend was automatically retried: commands=%v err=%v", afterRejection, err)
	}
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM commands WHERE attempt_id=?`, launch.Attempt.ID).Scan(&commandCount); err != nil || commandCount != 1 {
		t.Fatalf("rejection created replacement commands: count=%d err=%v", commandCount, err)
	}
	gotVictim, _ := store.GetExecution(context.Background(), victim.ID)
	gotWaiter, _ := store.GetExecution(context.Background(), waiter.ID)
	if gotVictim.State != "started" || gotWaiter.State != "waiting" {
		t.Fatalf("preemption interpreted workflow state: victim=%s waiter=%s", gotVictim.State, gotWaiter.State)
	}
}

func TestPriorityPreemptionRequiresCooperativeLowerPriorityVictim(t *testing.T) {
	tests := []struct {
		name              string
		victimPreemptible bool
		victimPriority    int
		waitingPriority   int
	}{
		{name: "not preemptible", victimPreemptible: false, victimPriority: 10, waitingPriority: 100},
		{name: "equal priority", victimPreemptible: true, victimPriority: 100, waitingPriority: 100},
		{name: "higher priority victim", victimPreemptible: true, victimPriority: 200, waitingPriority: 100},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := openCoordinationStore(t)
			setupSchedulerInventory(t, store)
			_, launch := startPreemptionVictim(t, store, "executor", test.victimPreemptible, test.victimPriority)
			submitPreemptionWaiter(t, store, test.waitingPriority)
			commands, err := store.EnsurePriorityPreemption(context.Background(), "executor")
			if err != nil || len(commands) != 0 {
				t.Fatalf("unsafe preemption commands=%v err=%v", commands, err)
			}
			var count int
			_ = store.db.QueryRow(`SELECT COUNT(*) FROM commands WHERE attempt_id=?`, launch.Attempt.ID).Scan(&count)
			if count != 0 {
				t.Fatalf("created %d suspend commands", count)
			}
		})
	}
}

func TestPriorityPreemptionRejectsUnsafeGPUEvidence(t *testing.T) {
	tests := []struct {
		name   string
		unsafe func(*testing.T, *Store)
	}{
		{name: "stale observation", unsafe: func(t *testing.T, store *Store) {
			total, free := int64(24<<30), int64(1<<30)
			stamp := time.Now().UTC()
			if err := store.RecordObservation(context.Background(), Observation{ResourceID: "gpu-0", ObservedAt: stamp.Format(time.RFC3339Nano), ValidUntil: stamp.Add(-time.Second).Format(time.RFC3339Nano), TotalBytes: &total, FreeBytes: &free}); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "external claim", unsafe: func(t *testing.T, store *Store) {
			if err := store.ObserveClaim(context.Background(), ExternalClaim{ID: "external", ResourceID: "gpu-0", ClaimKind: "external_process"}); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "unattributed activity", unsafe: func(t *testing.T, store *Store) {
			if err := store.ObserveClaim(context.Background(), ExternalClaim{ID: "unattributed", ResourceID: "gpu-0", ClaimKind: "unattributed_activity"}); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "disabled resource", unsafe: func(t *testing.T, store *Store) {
			if err := store.SetResourceAdminState(context.Background(), "gpu-0", "disabled", "test"); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := openCoordinationStore(t)
			setupSchedulerInventory(t, store)
			_, launch := startPreemptionVictim(t, store, "executor", true, 10)
			submitPreemptionWaiter(t, store, 100)
			test.unsafe(t, store)
			commands, err := store.EnsurePriorityPreemption(context.Background(), "executor")
			if err != nil || len(commands) != 0 {
				t.Fatalf("unsafe evidence allowed preemption: commands=%v err=%v", commands, err)
			}
			var count int
			_ = store.db.QueryRow(`SELECT COUNT(*) FROM commands WHERE attempt_id=?`, launch.Attempt.ID).Scan(&count)
			if count != 0 {
				t.Fatalf("created %d suspend commands", count)
			}
		})
	}
}

func TestPriorityPreemptionDoesNotCrossExecutors(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	if err := store.UpsertExecutor(context.Background(), Executor{ID: "executor-2", NodeID: "node", Kind: "local", Attributes: json.RawMessage(`{"environment":"wsl2"}`), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	_, launch := startPreemptionVictim(t, store, "executor-2", true, 10)
	submitPreemptionWaiter(t, store, 100)
	commands, err := store.EnsurePriorityPreemption(context.Background(), "executor")
	if err != nil || len(commands) != 0 {
		t.Fatalf("cross-executor preemption commands=%v err=%v", commands, err)
	}
	var count int
	_ = store.db.QueryRow(`SELECT COUNT(*) FROM commands WHERE attempt_id=?`, launch.Attempt.ID).Scan(&count)
	if count != 0 {
		t.Fatalf("created %d cross-executor suspend commands", count)
	}
}

func TestPriorityPreemptionRequiresOpenWaitingScope(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	_, launch := startPreemptionVictim(t, store, "executor", true, 10)
	waiter := submitPreemptionWaiter(t, store, 100)
	scopes, err := store.GetScopeByPath(context.Background(), ScopePath{Project: "scheduler-project", Queue: "train", Task: "model"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.PauseScope(context.Background(), scopes.Task.ID, "operator", "preemption-gate-test"); err != nil {
		t.Fatal(err)
	}
	commands, err := store.EnsurePriorityPreemption(context.Background(), "executor")
	if err != nil || len(commands) != 0 {
		t.Fatalf("closed scope allowed preemption for %s: commands=%v err=%v", waiter.ID, commands, err)
	}
	var count int
	_ = store.db.QueryRow(`SELECT COUNT(*) FROM commands WHERE attempt_id=?`, launch.Attempt.ID).Scan(&count)
	if count != 0 {
		t.Fatalf("created %d commands through closed gate", count)
	}
}
