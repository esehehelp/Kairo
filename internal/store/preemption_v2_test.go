package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func startPreemptionVictim(t *testing.T, store *Store, executorID string, preemptible bool, priority int) (*ExecutionRequest, *Launch) {
	return startPreemptionVictimWithUnattributedPolicy(t, store, executorID, preemptible, priority, "wait")
}

func startPreemptionVictimWithUnattributedPolicy(t *testing.T, store *Store, executorID string, preemptible bool, priority int, unattributedPolicy string) (*ExecutionRequest, *Launch) {
	t.Helper()
	ctx := context.Background()
	spec := schedulerExecutionSpec("preemption-victim")
	spec.Priority = priority
	spec.Preemptible = preemptible
	spec.Exclusive[0].OnUnattributedActivity = unattributedPolicy
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
	var eventPayloadJSON string
	if err = store.db.QueryRow(`SELECT payload_json FROM coordination_events WHERE event_type='suspend_requested' AND aggregate_id=?`, commandIDs[0]).Scan(&eventPayloadJSON); err != nil {
		t.Fatal(err)
	}
	var eventPayload map[string]any
	if err = json.Unmarshal([]byte(eventPayloadJSON), &eventPayload); err != nil {
		t.Fatal(err)
	}
	if eventPayload["execution_id"] != victim.ID || eventPayload["attempt_id"] != launch.Attempt.ID || eventPayload["lease_id"] != launch.Lease.ID || eventPayload["origin"] != "priority_preemption" {
		t.Fatalf("priority suspend event lacks exact execution chain: %s", eventPayloadJSON)
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

func TestPriorityPreemptionCrossesExecutorsOnTheSamePhysicalNode(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	if err := store.UpsertExecutor(context.Background(), Executor{ID: "ubuntu-wsl", NodeID: "node", Kind: "wsl2", Attributes: json.RawMessage(`{"environment":"wsl2"}`), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertExecutor(context.Background(), Executor{ID: "windows-local", NodeID: "node", Kind: "windows", Attributes: json.RawMessage(`{"environment":"windows"}`), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	_, launch := startPreemptionVictimWithUnattributedPolicy(t, store, "ubuntu-wsl", true, 10, "allow")
	if err := store.ObserveClaim(context.Background(), ExternalClaim{ID: "wsl-gpu-activity", ResourceID: "gpu-0", ClaimKind: "unattributed_activity"}); err != nil {
		t.Fatal(err)
	}
	waitingSpec := schedulerExecutionSpec("windows-preemption-waiter")
	waitingSpec.Priority = 100
	waitingSpec.ExecutorSelector = json.RawMessage(`{"labels":{"environment":"windows"}}`)
	if _, _, err := store.SubmitExecution(context.Background(), waitingSpec); err != nil {
		t.Fatal(err)
	}
	commands, err := store.EnsurePriorityPreemption(context.Background(), "windows-local")
	if err != nil || len(commands) != 1 {
		t.Fatalf("cross-executor preemption commands=%v err=%v", commands, err)
	}
	delivered, err := store.PollCommands(context.Background(), launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch)
	if err != nil || len(delivered) != 1 || delivered[0].ID != commands[0] {
		t.Fatalf("victim executor did not receive cross-executor suspend: commands=%+v err=%v", delivered, err)
	}
}

func TestPriorityPreemptionHonorsVictimUnattributedPolicy(t *testing.T) {
	for _, test := range []struct {
		name       string
		policy     string
		wantLength int
	}{
		{name: "wait remains unsafe", policy: "wait", wantLength: 0},
		{name: "allow permits suspend", policy: "allow", wantLength: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := openCoordinationStore(t)
			setupSchedulerInventory(t, store)
			_, launch := startPreemptionVictimWithUnattributedPolicy(t, store, "executor", true, 10, test.policy)
			submitPreemptionWaiter(t, store, 100)
			if err := store.ObserveClaim(context.Background(), ExternalClaim{ID: "unattributed", ResourceID: "gpu-0", ClaimKind: "unattributed_activity"}); err != nil {
				t.Fatal(err)
			}
			commands, err := store.EnsurePriorityPreemption(context.Background(), "executor")
			if err != nil || len(commands) != test.wantLength {
				t.Fatalf("policy=%s commands=%v err=%v", test.policy, commands, err)
			}
			var count int
			if err = store.db.QueryRow(`SELECT COUNT(*) FROM commands WHERE attempt_id=?`, launch.Attempt.ID).Scan(&count); err != nil || count != test.wantLength {
				t.Fatalf("policy=%s command count=%d err=%v", test.policy, count, err)
			}
		})
	}
}

func TestPriorityPreemptionProjectsVictimRAMAsReclaimable(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	ctx := context.Background()
	if err := store.UpsertProvider(ctx, "host-provider", "node", "host", nil); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertResourceInstance(ctx, ResourceInstance{ID: "host-ram", NodeID: "node", ProviderID: "host-provider", Kind: "ram", StableIdentity: "host-memory", AdminState: "enabled"}); err != nil {
		t.Fatal(err)
	}
	total, initiallyFree := int64(16<<30), int64(15<<30)
	observed := time.Now().UTC()
	if err := store.RecordObservation(ctx, Observation{ResourceID: "host-ram", ObservedAt: observed.Format(time.RFC3339Nano), ValidUntil: observed.Add(time.Minute).Format(time.RFC3339Nano), TotalBytes: &total, FreeBytes: &initiallyFree}); err != nil {
		t.Fatal(err)
	}
	victimSpec := schedulerExecutionSpec("ram-preemption-victim")
	victimSpec.Capacity = CapacityRequest{RAMBytes: 8 << 30, Strength: "admitted"}
	victim, _, err := store.SubmitExecution(ctx, victimSpec)
	if err != nil {
		t.Fatal(err)
	}
	reservation, err := store.ReserveNext(ctx, "executor")
	if err != nil || reservation == nil || reservation.Execution.ID != victim.ID {
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

	// The running victim has driven observed free RAM below the waiter's
	// request. Its admitted RAM is enough to make the waiter viable after
	// quiescence, so current usage must not suppress the suspend command.
	lowFree := int64(2 << 30)
	observed = time.Now().UTC()
	if err = store.RecordObservation(ctx, Observation{ResourceID: "host-ram", ObservedAt: observed.Format(time.RFC3339Nano), ValidUntil: observed.Add(time.Minute).Format(time.RFC3339Nano), TotalBytes: &total, FreeBytes: &lowFree}); err != nil {
		t.Fatal(err)
	}
	waiterSpec := schedulerExecutionSpec("ram-preemption-waiter")
	waiterSpec.Priority = 100
	waiterSpec.Capacity = CapacityRequest{RAMBytes: 8 << 30, Strength: "admitted"}
	if _, _, err = store.SubmitExecution(ctx, waiterSpec); err != nil {
		t.Fatal(err)
	}
	commands, err := store.EnsurePriorityPreemption(ctx, "executor")
	if err != nil || len(commands) != 1 {
		t.Fatalf("RAM-reclaiming preemption commands=%v err=%v", commands, err)
	}
}

func TestPriorityPreemptionRejectsRAMRequestAbovePhysicalTotal(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	ctx := context.Background()
	if err := store.UpsertProvider(ctx, "host-provider", "node", "host", nil); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertResourceInstance(ctx, ResourceInstance{ID: "host-ram", NodeID: "node", ProviderID: "host-provider", Kind: "ram", StableIdentity: "host-memory", AdminState: "enabled"}); err != nil {
		t.Fatal(err)
	}
	total, initiallyFree := int64(16<<30), int64(15<<30)
	observed := time.Now().UTC()
	if err := store.RecordObservation(ctx, Observation{ResourceID: "host-ram", ObservedAt: observed.Format(time.RFC3339Nano), ValidUntil: observed.Add(time.Minute).Format(time.RFC3339Nano), TotalBytes: &total, FreeBytes: &initiallyFree}); err != nil {
		t.Fatal(err)
	}
	victimSpec := schedulerExecutionSpec("impossible-ram-victim")
	victimSpec.Capacity = CapacityRequest{RAMBytes: 8 << 30, Strength: "admitted"}
	if _, _, err := store.SubmitExecution(ctx, victimSpec); err != nil {
		t.Fatal(err)
	}
	reservation, err := store.ReserveNext(ctx, "executor")
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
	widestFree := total
	observed = time.Now().UTC()
	if err = store.RecordObservation(ctx, Observation{ResourceID: "host-ram", ObservedAt: observed.Format(time.RFC3339Nano), ValidUntil: observed.Add(time.Minute).Format(time.RFC3339Nano), TotalBytes: &total, FreeBytes: &widestFree}); err != nil {
		t.Fatal(err)
	}
	waiterSpec := schedulerExecutionSpec("impossible-ram-waiter")
	waiterSpec.Priority = 100
	waiterSpec.Capacity = CapacityRequest{RAMBytes: 20 << 30, Strength: "admitted"}
	if _, _, err = store.SubmitExecution(ctx, waiterSpec); err != nil {
		t.Fatal(err)
	}
	commands, err := store.EnsurePriorityPreemption(ctx, "executor")
	if err != nil || len(commands) != 0 {
		t.Fatalf("physically impossible RAM request triggered preemption: commands=%v err=%v", commands, err)
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
