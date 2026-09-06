package store

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"
)

func schedulerExecutionSpec(requestID string) ExecutionSpec {
	return ExecutionSpec{
		ClientRequestID:  requestID,
		Scope:            ScopePath{Project: "scheduler-project", Queue: "train", Task: "model"},
		Argv:             []string{"python", "train.py"},
		CWD:              "/work",
		ExecutorSelector: json.RawMessage(`{"labels":{"environment":"wsl2"}}`),
		Priority:         10,
		Checkpointable:   true,
		Preemptible:      true,
		Exclusive: []ExclusiveRequest{{
			Kind:                   "gpu",
			Count:                  1,
			SameNode:               true,
			OnExternalClaim:        "wait",
			OnUnattributedActivity: "wait",
			OnStaleObservation:     "wait",
		}},
	}
}

func setupSchedulerInventory(t *testing.T, store *Store) {
	t.Helper()
	ctx := context.Background()
	if err := store.UpsertNode(ctx, Node{ID: "node", Name: "node", OS: "windows", Architecture: "amd64", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertExecutor(ctx, Executor{ID: "executor", NodeID: "node", Kind: "local", Attributes: json.RawMessage(`{"environment":"wsl2"}`), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertProvider(ctx, "gpu-provider", "node", "nvidia", nil); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertResourceInstance(ctx, ResourceInstance{ID: "gpu-0", NodeID: "node", ProviderID: "gpu-provider", Kind: "gpu", StableIdentity: "GPU-0", Binding: json.RawMessage(`{"index":0}`), AdminState: "enabled"}); err != nil {
		t.Fatal(err)
	}
	total, free := int64(24<<30), int64(23<<30)
	observed := time.Now().UTC()
	if err := store.RecordObservation(ctx, Observation{ResourceID: "gpu-0", ObservedAt: observed.Format(time.RFC3339Nano), ValidUntil: observed.Add(time.Minute).Format(time.RFC3339Nano), TotalBytes: &total, FreeBytes: &free}); err != nil {
		t.Fatal(err)
	}
}

func refreshSchedulerGPU(t *testing.T, store *Store) {
	t.Helper()
	// SQLite date comparison has millisecond resolution. Cross the boundary so
	// this observation is unambiguously newer than the coordination transition.
	time.Sleep(2 * time.Millisecond)
	total, free := int64(24<<30), int64(23<<30)
	observed := time.Now().UTC()
	if err := store.RecordObservation(context.Background(), Observation{ResourceID: "gpu-0", ObservedAt: observed.Format(time.RFC3339Nano), ValidUntil: observed.Add(time.Minute).Format(time.RFC3339Nano), TotalBytes: &total, FreeBytes: &free}); err != nil {
		t.Fatal(err)
	}
}

func TestSchedulerOneShotExecutionLifecycle(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	ctx := context.Background()
	execution, _, err := store.SubmitExecution(ctx, schedulerExecutionSpec("one-shot"))
	if err != nil {
		t.Fatal(err)
	}
	reservation, err := store.ReserveNext(ctx, "executor")
	if err != nil || reservation == nil {
		t.Fatalf("reserve: %+v, %v", reservation, err)
	}
	if reservation.Execution.ID != execution.ID || len(reservation.GateGenerations) != 3 || len(reservation.Resources) != 1 {
		t.Fatalf("reservation did not capture execution, gates, and gang: %+v", reservation)
	}
	if err = store.ValidateReservation(ctx, reservation.Lease.ID, reservation.Lease.CoordinationEpoch); err != nil {
		t.Fatal(err)
	}
	if err = store.MarkLeasePrepared(ctx, reservation.Lease.ID, reservation.Lease.CoordinationEpoch); err != nil {
		t.Fatal(err)
	}
	launch, err := store.AuthorizeLaunch(ctx, reservation)
	if err != nil {
		t.Fatal(err)
	}
	if launch.Execution.ID != execution.ID || launch.Attempt.ExecutionID != execution.ID || launch.AuthorizationToken == "" {
		t.Fatalf("invalid launch: %+v", launch)
	}
	if err = store.ActivateLaunch(ctx, launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch, launch.AuthorizationToken, 1234, "win:1234:start:1"); err != nil {
		t.Fatal(err)
	}
	started, err := store.GetExecution(ctx, execution.ID)
	if err != nil || started.State != "started" {
		t.Fatalf("execution state=%+v err=%v", started, err)
	}
	var role, namespace string
	var rank int
	if err = store.db.QueryRow(`SELECT role,namespace,rank FROM attempt_processes WHERE attempt_id=?`, launch.Attempt.ID).Scan(&role, &namespace, &rank); err != nil {
		t.Fatal(err)
	}
	if role != "launcher" || namespace != "host" || rank != -1 {
		t.Fatalf("launcher identity was not namespaced: %s %s %d", role, namespace, rank)
	}
	if next, err := store.ReserveNext(ctx, "executor"); err != nil || next != nil {
		t.Fatalf("one-shot execution was reserved twice: %+v %v", next, err)
	}
}

func TestSchedulerGateGenerationFencesReservationAfterPauseResume(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	ctx := context.Background()
	execution, _, err := store.SubmitExecution(ctx, schedulerExecutionSpec("gate-fence"))
	if err != nil {
		t.Fatal(err)
	}
	reservation, err := store.ReserveNext(ctx, "executor")
	if err != nil || reservation == nil {
		t.Fatalf("reserve: %+v %v", reservation, err)
	}
	scopes, _ := store.GetScopeByPath(ctx, ScopePath{Project: "scheduler-project"})
	if _, err = store.PauseScope(ctx, scopes.Project.ID, "operator", "pause-fence"); err != nil {
		t.Fatal(err)
	}
	if _, err = store.ResumeScope(ctx, scopes.Project.ID, "operator"); err != nil {
		t.Fatal(err)
	}
	if err = store.ValidateReservation(ctx, reservation.Lease.ID, reservation.Lease.CoordinationEpoch); !errors.Is(err, ErrGateClosed) {
		t.Fatalf("pause/resume generation did not fence reservation: %v", err)
	}
	if err = store.ReleaseReservation(ctx, reservation.Lease.ID, reservation.Lease.CoordinationEpoch, "gate generation changed"); err == nil {
		t.Fatal("reservation released without a post-reservation observation")
	}
	refreshSchedulerGPU(t, store)
	if err = store.ReleaseReservation(ctx, reservation.Lease.ID, reservation.Lease.CoordinationEpoch, "gate generation changed"); err != nil {
		t.Fatal(err)
	}
	waiting, _ := store.GetExecution(ctx, execution.ID)
	if waiting.State != "waiting" {
		t.Fatalf("pre-authorization release changed one-shot request: %+v", waiting)
	}
	next, err := store.ReserveNext(ctx, "executor")
	if err != nil || next == nil || next.Lease.CoordinationEpoch != reservation.Lease.CoordinationEpoch+1 {
		t.Fatalf("fresh reservation was not issued at a new epoch: %+v %v", next, err)
	}
}

func TestSchedulerGateFencesActivationWithoutConsumingAuthorization(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	ctx := context.Background()
	execution, _, err := store.SubmitExecution(ctx, schedulerExecutionSpec("activation-fence"))
	if err != nil {
		t.Fatal(err)
	}
	reservation, err := store.ReserveNext(ctx, "executor")
	if err != nil || reservation == nil {
		t.Fatalf("reserve: %+v %v", reservation, err)
	}
	if err = store.MarkLeasePrepared(ctx, reservation.Lease.ID, reservation.Lease.CoordinationEpoch); err != nil {
		t.Fatal(err)
	}
	launch, err := store.AuthorizeLaunch(ctx, reservation)
	if err != nil {
		t.Fatal(err)
	}
	scopes, _ := store.GetScopeByPath(ctx, ScopePath{Project: "scheduler-project"})
	if _, err = store.PauseScope(ctx, scopes.Project.ID, "operator", "pause-before-activation"); err != nil {
		t.Fatal(err)
	}
	if err = store.ActivateLaunch(ctx, launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch, launch.AuthorizationToken, 1234, "win:1234:start:1"); !errors.Is(err, ErrGateClosed) {
		t.Fatalf("closed gate did not fence activation: %v", err)
	}
	var authorizationState string
	if err = store.db.QueryRow(`SELECT state FROM launch_authorizations WHERE id=?`, launch.AuthorizationID).Scan(&authorizationState); err != nil {
		t.Fatal(err)
	}
	if authorizationState != "issued" {
		t.Fatalf("fenced activation consumed authorization: %s", authorizationState)
	}
	refreshSchedulerGPU(t, store)
	if err = store.ReleaseReservation(ctx, launch.Lease.ID, launch.Lease.CoordinationEpoch, "gate closed before activation"); err != nil {
		t.Fatal(err)
	}
	terminal, _ := store.GetExecution(ctx, execution.ID)
	if terminal.State != "terminal" {
		t.Fatalf("authorized one-shot execution was made reusable: %+v", terminal)
	}
}

func TestSchedulerDoesNotReserveThroughClosedScope(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	ctx := context.Background()
	if _, _, err := store.SubmitExecution(ctx, schedulerExecutionSpec("closed")); err != nil {
		t.Fatal(err)
	}
	scopes, _ := store.GetScopeByPath(ctx, ScopePath{Project: "scheduler-project", Queue: "train"})
	if _, err := store.PauseScope(ctx, scopes.Queue.ID, "operator", "close-queue"); err != nil {
		t.Fatal(err)
	}
	reservation, err := store.ReserveNext(ctx, "executor")
	if err != nil || reservation != nil {
		t.Fatalf("closed queue admitted execution: %+v %v", reservation, err)
	}
}

func TestSchedulerCreatesOnlyOneLiveReservationPerExecution(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	ctx := context.Background()
	if err := store.UpsertExecutor(ctx, Executor{ID: "executor-2", NodeID: "node", Kind: "local", Attributes: json.RawMessage(`{"environment":"wsl2"}`), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	spec := schedulerExecutionSpec("single-reservation")
	// No physical resource guard participates in this test: the execution-level
	// live-lease fence alone must prevent two reservations.
	spec.Exclusive = nil
	if _, _, err := store.SubmitExecution(ctx, spec); err != nil {
		t.Fatal(err)
	}

	var wait sync.WaitGroup
	results := make(chan *Reservation, 2)
	errorsSeen := make(chan error, 2)
	for _, executorID := range []string{"executor", "executor-2"} {
		wait.Add(1)
		go func(id string) {
			defer wait.Done()
			reservation, err := store.ReserveNext(ctx, id)
			results <- reservation
			errorsSeen <- err
		}(executorID)
	}
	wait.Wait()
	close(results)
	close(errorsSeen)
	for err := range errorsSeen {
		if err != nil {
			t.Fatal(err)
		}
	}
	count := 0
	for reservation := range results {
		if reservation != nil {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("concurrent reservation count = %d, want 1", count)
	}
	if reservation, err := store.ReserveNext(ctx, "executor"); err != nil || reservation != nil {
		t.Fatalf("sequential duplicate reservation: %+v %v", reservation, err)
	}
	var liveLeases int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM leases WHERE state!='released'`).Scan(&liveLeases); err != nil || liveLeases != 1 {
		t.Fatalf("live leases=%d err=%v", liveLeases, err)
	}
}

func TestSchedulerNeverSelectsOneResourceTwiceWithinGang(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	ctx := context.Background()
	if err := store.UpsertResourceInstance(ctx, ResourceInstance{ID: "gpu-1", NodeID: "node", ProviderID: "gpu-provider", Kind: "gpu", StableIdentity: "GPU-1", Binding: json.RawMessage(`{"index":1}`), AdminState: "enabled"}); err != nil {
		t.Fatal(err)
	}
	total, free := int64(24<<30), int64(23<<30)
	observed := time.Now().UTC()
	if err := store.RecordObservation(ctx, Observation{ResourceID: "gpu-1", ObservedAt: observed.Format(time.RFC3339Nano), ValidUntil: observed.Add(time.Minute).Format(time.RFC3339Nano), TotalBytes: &total, FreeBytes: &free}); err != nil {
		t.Fatal(err)
	}
	spec := schedulerExecutionSpec("multi-clause-gang")
	spec.Exclusive = append(spec.Exclusive, spec.Exclusive[0])
	if _, _, err := store.SubmitExecution(ctx, spec); err != nil {
		t.Fatal(err)
	}
	reservation, err := store.ReserveNext(ctx, "executor")
	if err != nil || reservation == nil {
		t.Fatalf("reserve: %+v %v", reservation, err)
	}
	if len(reservation.Resources) != 2 || reservation.Resources[0].ID == reservation.Resources[1].ID {
		t.Fatalf("gang reused a physical resource: %+v", reservation.Resources)
	}
	var itemCount int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM lease_items WHERE lease_id=? AND kind='gpu'`, reservation.Lease.ID).Scan(&itemCount); err != nil || itemCount != 2 {
		t.Fatalf("gpu lease items=%d err=%v", itemCount, err)
	}
}
