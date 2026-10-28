package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"kairo/internal/pgtest"
)

// openCoordinationStore opens a store on a fresh database of the test
// server (pgtest), dropped when the test ends. The test runs in parallel
// with the others (each has its own database).
func openCoordinationStore(t testing.TB) *Store {
	t.Helper()
	if test, ok := t.(*testing.T); ok {
		if _, already := parallelTests.LoadOrStore(test, true); !already {
			test.Parallel()
		}
	}
	return openSerialStore(t)
}

// parallelTests are the tests openCoordinationStore made parallel (a test
// may open several stores).
var parallelTests sync.Map

// openSerialStore is openCoordinationStore for a test that changes a package
// setting and so must not run alongside the others.
func openSerialStore(t testing.TB) *Store {
	t.Helper()
	store, err := Open(pgtest.DSN(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

// testExec runs a writing statement of a test (a fixture or a fault) in a
// transaction of its own, under the decision lock like every write.
func (s *Store) testExec(query string, args ...any) (sql.Result, error) {
	return s.exec(context.Background(), query, args...)
}

func testExecutionSpec(requestID string) ExecutionSpec {
	continuation := "checkpoint://opaque"
	return ExecutionSpec{
		ClientRequestID:      requestID,
		Scope:                ScopePath{Project: "llm-develop", Queue: "training", Task: "pretrain"},
		Argv:                 []string{"python", "train.py"},
		CWD:                  "/work",
		ExecutorSelector:     json.RawMessage(`{"environment":"wsl2"}`),
		Priority:             10,
		Checkpointable:       true,
		Preemptible:          true,
		InputContinuationRef: &continuation,
		Exclusive:            []ExclusiveRequest{{Kind: "gpu", Count: 2, SameNode: true}},
		Capacity:             CapacityRequest{CPUMillis: 1000, RAMBytes: 4096, Disks: []DiskRequest{{Filesystem: "D:", ReserveBytes: 1024, MinFreeAfterBytes: 2048}}},
	}
}

func TestSubmitExecutionIsImmutableAndIdempotent(t *testing.T) {
	store := openCoordinationStore(t)
	ctx := context.Background()
	spec := testExecutionSpec("train-1")
	first, duplicate, err := store.SubmitExecution(ctx, spec)
	if err != nil || duplicate {
		t.Fatalf("first submit: duplicate=%v err=%v", duplicate, err)
	}
	if first.State != "waiting" || first.ClientRequestID != "train-1" || len(first.Resources) != 4 {
		t.Fatalf("unexpected execution: %+v", first)
	}
	second, duplicate, err := store.SubmitExecution(ctx, spec)
	if err != nil || !duplicate || second.ID != first.ID {
		t.Fatalf("idempotent submit: execution=%+v duplicate=%v err=%v", second, duplicate, err)
	}
	changed := spec
	changed.Argv = []string{"python", "other.py"}
	if _, _, err = store.SubmitExecution(ctx, changed); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("changed request reused idempotency key: %v", err)
	}
	executions, err := store.ListExecutions(ctx, ExecutionFilter{ProjectScopeID: first.ProjectScopeID})
	if err != nil || len(executions) != 1 {
		t.Fatalf("list executions: %+v %v", executions, err)
	}
	events, err := store.ListEvents(ctx, EventFilter{ProjectScopeID: first.ProjectScopeID})
	if err != nil || len(events) != 4 || events[len(events)-1].EventType != "execution_submitted" {
		t.Fatalf("events: %+v %v", events, err)
	}
}

func TestScopePauseClosesOnlyItsOwnGate(t *testing.T) {
	store := openCoordinationStore(t)
	ctx := context.Background()
	execution, _, err := store.SubmitExecution(ctx, testExecutionSpec("train-1"))
	if err != nil {
		t.Fatal(err)
	}
	scopes, err := store.GetScopeByPath(ctx, ScopePath{Project: "llm-develop", Queue: "training", Task: "pretrain"})
	if err != nil {
		t.Fatal(err)
	}
	operation, err := store.PauseScope(ctx, scopes.Project.ID, "operator", "pause-1")
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := store.PauseScope(ctx, scopes.Project.ID, "operator", "pause-1")
	if err != nil || duplicate.ID != operation.ID {
		t.Fatalf("idempotent pause: %+v %v", duplicate, err)
	}
	project, _ := store.GetScope(ctx, scopes.Project.ID)
	queue, _ := store.GetScope(ctx, scopes.Queue.ID)
	if project.Admission != "closed" || queue.Admission != "open" {
		t.Fatalf("pause propagated into child state: project=%+v queue=%+v", project, queue)
	}
	if operation.ScopeGeneration != project.Generation {
		t.Fatalf("operation generation=%d gate generation=%d", operation.ScopeGeneration, project.Generation)
	}
	open, gates, err := store.ExecutionAdmission(ctx, execution.ID)
	if err != nil || open || len(gates) != 3 {
		t.Fatalf("effective admission: open=%v gates=%+v err=%v", open, gates, err)
	}
	gate, err := store.ResumeScope(ctx, scopes.Project.ID, "operator")
	if err != nil || gate.State != "open" || gate.Generation <= operation.ScopeGeneration {
		t.Fatalf("resume: %+v %v", gate, err)
	}
	latest, err := store.GetExecution(ctx, execution.ID)
	if err != nil || latest.State != "waiting" {
		t.Fatalf("resume mutated execution: %+v %v", latest, err)
	}
}

func TestPauseDiscoversLiveExecutionOnce(t *testing.T) {
	store := openCoordinationStore(t)
	ctx := context.Background()
	execution, _, err := store.SubmitExecution(ctx, testExecutionSpec("train-1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.testExec(`INSERT INTO attempts(id,execution_id,state,executor_id,coordination_epoch,authorized_at,started_at) VALUES('att-1',$1,'running','executor',1,'now','now')`, execution.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = store.testExec(`INSERT INTO leases(id,execution_id,attempt_id,executor_id,coordination_epoch,state,created_at) VALUES('lease-1',$1,'att-1','executor',1,'active','now')`, execution.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = store.testExec(`UPDATE execution_requests SET state='started',authorized_at='now',started_at='now' WHERE id=$1`, execution.ID); err != nil {
		t.Fatal(err)
	}
	scopes, err := store.GetScopeByPath(ctx, ScopePath{Project: "llm-develop"})
	if err != nil {
		t.Fatal(err)
	}
	operation, err := store.PauseScope(ctx, scopes.Project.ID, "operator", "pause-1")
	if err != nil {
		t.Fatal(err)
	}
	targets, err := store.DiscoverPauseTargets(ctx, operation.ID)
	if err != nil || len(targets) != 1 || targets[0].ExecutionID != execution.ID || targets[0].AttemptID == nil || *targets[0].AttemptID != "att-1" {
		t.Fatalf("targets: %+v %v", targets, err)
	}
	targets, err = store.DiscoverPauseTargets(ctx, operation.ID)
	if err != nil || len(targets) != 1 {
		t.Fatalf("duplicate target discovery: %+v %v", targets, err)
	}
}

func TestWithdrawExecutionIsOneShot(t *testing.T) {
	store := openCoordinationStore(t)
	ctx := context.Background()
	execution, _, err := store.SubmitExecution(ctx, testExecutionSpec("train-1"))
	if err != nil {
		t.Fatal(err)
	}
	if err = store.WithdrawExecution(ctx, execution.ID); err != nil {
		t.Fatal(err)
	}
	if err = store.WithdrawExecution(ctx, execution.ID); err != nil {
		t.Fatalf("duplicate withdraw: %v", err)
	}
	terminal, err := store.GetExecution(ctx, execution.ID)
	if err != nil || terminal.State != "terminal" || terminal.TerminalCause == nil || *terminal.TerminalCause != "withdrawn_before_start" {
		t.Fatalf("terminal execution: %+v %v", terminal, err)
	}
}

func TestAttemptIsOneShotAndProcessRolesDoNotCollide(t *testing.T) {
	store := openCoordinationStore(t)
	ctx := context.Background()
	execution, _, err := store.SubmitExecution(ctx, testExecutionSpec("train-1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.testExec(`INSERT INTO attempts(id,execution_id,state,executor_id,coordination_epoch,authorized_at) VALUES('att-1',$1,'authorized','executor',1,'now')`, execution.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = store.testExec(`INSERT INTO attempts(id,execution_id,state,executor_id,coordination_epoch,authorized_at) VALUES('att-2',$1,'authorized','executor',1,'now')`, execution.ID); err == nil {
		t.Fatal("a second authorized attempt was accepted")
	}
	for _, statement := range []string{
		`INSERT INTO attempt_processes(attempt_id,role,namespace,rank,pid,process_identity,registered_at,last_seen_at) VALUES('att-1','launcher','host',-1,10,'host:10','now','now')`,
		`INSERT INTO attempt_processes(attempt_id,role,namespace,rank,pid,process_identity,registered_at,last_seen_at) VALUES('att-1','worker','executor',0,10,'executor:10','now','now')`,
	} {
		if _, err = store.testExec(statement); err != nil {
			t.Fatal(err)
		}
	}
}
