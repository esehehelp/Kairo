package store

import (
	"context"
	"testing"
)

func insertPauseTestRunning(t *testing.T, store *Store, execution ExecutionRequest, checkpointable bool) {
	t.Helper()
	checkpoint := 0
	if checkpointable {
		checkpoint = 1
	}
	statements := []struct {
		query string
		args  []any
	}{
		{`UPDATE execution_requests SET state='started',checkpointable=?,authorized_at='now',started_at='now' WHERE id=?`, []any{checkpoint, execution.ID}},
		{`INSERT INTO attempts(id,execution_id,state,executor_id,coordination_epoch,authorized_at,started_at) VALUES('att-running',?,'running','executor',1,'now','now')`, []any{execution.ID}},
		{`INSERT INTO leases(id,execution_id,attempt_id,executor_id,coordination_epoch,state,created_at) VALUES('lease-active',?,'att-running','executor',1,'active','now')`, []any{execution.ID}},
		{`INSERT INTO attempt_processes(attempt_id,role,namespace,rank,pid,process_identity,registered_at,last_seen_at) VALUES('att-running','launcher','host',-1,42,'host:42','now','now')`, nil},
	}
	for _, statement := range statements {
		if _, err := store.db.Exec(statement.query, statement.args...); err != nil {
			t.Fatal(err)
		}
	}
}

func pauseProjectForTest(t *testing.T, store *Store, requestID string) PauseOperation {
	t.Helper()
	ctx := context.Background()
	scopes, err := store.GetScopeByPath(ctx, ScopePath{Project: "llm-develop"})
	if err != nil {
		t.Fatal(err)
	}
	operation, err := store.PauseScope(ctx, scopes.Project.ID, "operator", requestID)
	if err != nil {
		t.Fatal(err)
	}
	return operation
}

func TestPauseReconcilerCompletesEmptyScope(t *testing.T) {
	store := openCoordinationStore(t)
	ctx := context.Background()
	if _, _, err := store.SubmitExecution(ctx, testExecutionSpec("waiting")); err != nil {
		t.Fatal(err)
	}
	operation := pauseProjectForTest(t, store, "pause-empty")
	if err := store.ReconcilePauseOperations(ctx, true); err != nil {
		t.Fatal(err)
	}
	operation, err := store.GetPauseOperation(ctx, operation.ID)
	if err != nil || operation.State != "quiesced" {
		t.Fatalf("operation = %+v, err = %v", operation, err)
	}
	var state string
	if err = store.db.QueryRow(`SELECT state FROM execution_requests WHERE client_request_id='waiting'`).Scan(&state); err != nil || state != "waiting" {
		t.Fatalf("waiting request was interpreted as workflow state: state=%q err=%v", state, err)
	}
}

func TestPauseReconcilerObserveOnlyReportsRunningBlocker(t *testing.T) {
	store := openCoordinationStore(t)
	ctx := context.Background()
	execution, _, err := store.SubmitExecution(ctx, testExecutionSpec("running"))
	if err != nil {
		t.Fatal(err)
	}
	insertPauseTestRunning(t, store, execution, true)
	operation := pauseProjectForTest(t, store, "pause-observe")
	if err = store.ReconcilePauseOperations(ctx, false); err != nil {
		t.Fatal(err)
	}
	operation, _ = store.GetPauseOperation(ctx, operation.ID)
	targets, _ := store.ListPauseTargets(ctx, operation.ID)
	if operation.State != "blocked" || len(targets) != 1 || targets[0].BlockerReason == nil || *targets[0].BlockerReason != "observe_only" {
		t.Fatalf("operation=%+v targets=%+v", operation, targets)
	}
	var commands int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM commands`).Scan(&commands); err != nil || commands != 0 {
		t.Fatalf("observe-only enqueued commands: count=%d err=%v", commands, err)
	}
}

func TestPauseReconcilerEnqueuesSuspendAndResumeDoesNotRecallIt(t *testing.T) {
	store := openCoordinationStore(t)
	ctx := context.Background()
	execution, _, err := store.SubmitExecution(ctx, testExecutionSpec("running"))
	if err != nil {
		t.Fatal(err)
	}
	insertPauseTestRunning(t, store, execution, true)
	operation := pauseProjectForTest(t, store, "pause-running")
	if err = store.ReconcilePauseOperations(ctx, true); err != nil {
		t.Fatal(err)
	}
	targets, err := store.ListPauseTargets(ctx, operation.ID)
	if err != nil || len(targets) != 1 || targets[0].State != "quiescing" || targets[0].CommandID == nil {
		t.Fatalf("targets=%+v err=%v", targets, err)
	}
	commandID := *targets[0].CommandID
	scopes, _ := store.GetScopeByPath(ctx, ScopePath{Project: "llm-develop"})
	if _, err = store.ResumeScope(ctx, scopes.Project.ID, "operator"); err != nil {
		t.Fatal(err)
	}
	if err = store.ReconcilePauseOperations(ctx, true); err != nil {
		t.Fatal(err)
	}
	var commandState string
	if err = store.db.QueryRow(`SELECT state FROM commands WHERE id=?`, commandID).Scan(&commandState); err != nil || commandState != "pending" {
		t.Fatalf("resume recalled command: state=%q err=%v", commandState, err)
	}
}

func TestResumedPauseDoesNotCaptureNewExecution(t *testing.T) {
	store := openCoordinationStore(t)
	ctx := context.Background()
	if _, _, err := store.SubmitExecution(ctx, testExecutionSpec("before-resume")); err != nil {
		t.Fatal(err)
	}
	operation := pauseProjectForTest(t, store, "pause-then-resume")
	scopes, _ := store.GetScopeByPath(ctx, ScopePath{Project: "llm-develop"})
	if _, err := store.ResumeScope(ctx, scopes.Project.ID, "operator"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.SubmitExecution(ctx, testExecutionSpec("after-resume")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`INSERT INTO leases(id,execution_id,executor_id,coordination_epoch,state,created_at)
		SELECT 'lease-after-resume',id,'executor',1,'reserved','now' FROM execution_requests WHERE client_request_id='after-resume'`); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcilePauseOperations(ctx, true); err != nil {
		t.Fatal(err)
	}
	targets, err := store.ListPauseTargets(ctx, operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range targets {
		var requestID string
		if err := store.db.QueryRow(`SELECT client_request_id FROM execution_requests WHERE id=?`, target.ExecutionID).Scan(&requestID); err != nil {
			t.Fatal(err)
		}
		if requestID == "after-resume" {
			t.Fatalf("old pause captured post-resume execution: %+v", target)
		}
	}
}

func TestPauseTargetNeverSwitchesToPostResumeLease(t *testing.T) {
	store := openCoordinationStore(t)
	ctx := context.Background()
	execution, _, err := store.SubmitExecution(ctx, testExecutionSpec("lease-generation"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.db.Exec(`INSERT INTO leases(id,execution_id,executor_id,coordination_epoch,state,created_at) VALUES('lease-before',?,'executor',1,'reserved','now')`, execution.ID); err != nil {
		t.Fatal(err)
	}
	operation := pauseProjectForTest(t, store, "pause-generation")
	if _, err = store.db.Exec(`UPDATE leases SET state='released',released_at='now' WHERE id='lease-before'`); err != nil {
		t.Fatal(err)
	}
	scopes, _ := store.GetScopeByPath(ctx, ScopePath{Project: "llm-develop"})
	if _, err = store.ResumeScope(ctx, scopes.Project.ID, "operator"); err != nil {
		t.Fatal(err)
	}
	if _, err = store.db.Exec(`INSERT INTO leases(id,execution_id,executor_id,coordination_epoch,state,created_at) VALUES('lease-after',?,'executor',2,'reserved','later')`, execution.ID); err != nil {
		t.Fatal(err)
	}
	if err = store.ReconcilePauseOperations(ctx, true); err != nil {
		t.Fatal(err)
	}
	var afterState string
	if err = store.db.QueryRow(`SELECT state FROM leases WHERE id='lease-after'`).Scan(&afterState); err != nil || afterState != "reserved" {
		t.Fatalf("old pause revoked post-resume lease: state=%s err=%v", afterState, err)
	}
	targets, _ := store.ListPauseTargets(ctx, operation.ID)
	if len(targets) != 1 || targets[0].LeaseID == nil || *targets[0].LeaseID != "lease-before" || targets[0].State != "quiesced" {
		t.Fatalf("pause target changed generations: %+v", targets)
	}
}

func TestPauseReconcilerObserveOnlyDoesNotRevokeReservation(t *testing.T) {
	store := openCoordinationStore(t)
	ctx := context.Background()
	execution, _, err := store.SubmitExecution(ctx, testExecutionSpec("reserved"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.db.Exec(`INSERT INTO leases(id,execution_id,executor_id,coordination_epoch,state,created_at) VALUES('lease-reserved',?,'executor',1,'reserved','now')`, execution.ID); err != nil {
		t.Fatal(err)
	}
	operation := pauseProjectForTest(t, store, "pause-reserved")
	if err = store.ReconcilePauseOperations(ctx, false); err != nil {
		t.Fatal(err)
	}
	var leaseState, executionState string
	if err = store.db.QueryRow(`SELECT l.state,e.state FROM leases l JOIN execution_requests e ON e.id=l.execution_id WHERE l.id='lease-reserved'`).Scan(&leaseState, &executionState); err != nil {
		t.Fatal(err)
	}
	operation, _ = store.GetPauseOperation(ctx, operation.ID)
	if leaseState != "reserved" || executionState != "waiting" || operation.State != "blocked" {
		t.Fatalf("lease=%s execution=%s operation=%+v", leaseState, executionState, operation)
	}
}

func TestPauseReconcilerRequestsRevocationUntilExecutorCleansUp(t *testing.T) {
	store := openCoordinationStore(t)
	ctx := context.Background()
	execution, _, err := store.SubmitExecution(ctx, testExecutionSpec("reserved"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.db.Exec(`INSERT INTO leases(id,execution_id,executor_id,coordination_epoch,state,created_at) VALUES('lease-reserved',?,'executor',1,'prepared','now')`, execution.ID); err != nil {
		t.Fatal(err)
	}
	operation := pauseProjectForTest(t, store, "pause-reserved")
	if err = store.ReconcilePauseOperations(ctx, true); err != nil {
		t.Fatal(err)
	}
	var leaseState, executionState string
	if err = store.db.QueryRow(`SELECT l.state,e.state FROM leases l JOIN execution_requests e ON e.id=l.execution_id WHERE l.id='lease-reserved'`).Scan(&leaseState, &executionState); err != nil {
		t.Fatal(err)
	}
	targets, _ := store.ListPauseTargets(ctx, operation.ID)
	if leaseState != "revocation_requested" || executionState != "waiting" || len(targets) != 1 || targets[0].State != "revoking" {
		t.Fatalf("lease=%s execution=%s targets=%+v", leaseState, executionState, targets)
	}
	if err = store.ReleaseReservation(ctx, "lease-reserved", 1, "scope pause"); err != nil {
		t.Fatal(err)
	}
	if err = store.db.QueryRow(`SELECT state FROM execution_requests WHERE id=?`, execution.ID).Scan(&executionState); err != nil || executionState != "waiting" {
		t.Fatalf("pre-authorization pause consumed project request: state=%s err=%v", executionState, err)
	}
	if err = store.ReconcilePauseOperations(ctx, true); err != nil {
		t.Fatal(err)
	}
	operation, _ = store.GetPauseOperation(ctx, operation.ID)
	if operation.State != "quiesced" {
		t.Fatalf("pause completed before/after cleanup incorrectly: %+v", operation)
	}
}

func TestPauseReconcilerBlocksNonCheckpointableExecution(t *testing.T) {
	store := openCoordinationStore(t)
	ctx := context.Background()
	spec := testExecutionSpec("non-checkpointable")
	spec.Checkpointable = false
	spec.Preemptible = false
	execution, _, err := store.SubmitExecution(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	insertPauseTestRunning(t, store, execution, false)
	operation := pauseProjectForTest(t, store, "pause-non-checkpointable")
	if err = store.ReconcilePauseOperations(ctx, true); err != nil {
		t.Fatal(err)
	}
	targets, _ := store.ListPauseTargets(ctx, operation.ID)
	if len(targets) != 1 || targets[0].BlockerReason == nil || *targets[0].BlockerReason != "execution_not_checkpointable" {
		t.Fatalf("targets=%+v", targets)
	}
}

func TestPauseReconcilerRequiresAttemptQuiescedAndLeaseReleased(t *testing.T) {
	store := openCoordinationStore(t)
	ctx := context.Background()
	execution, _, err := store.SubmitExecution(ctx, testExecutionSpec("inconsistent-release"))
	if err != nil {
		t.Fatal(err)
	}
	insertPauseTestRunning(t, store, execution, true)
	if _, err = store.db.Exec(`UPDATE leases SET state='released',released_at='now' WHERE id='lease-active'`); err != nil {
		t.Fatal(err)
	}
	operation := pauseProjectForTest(t, store, "pause-inconsistent-release")
	if err = store.ReconcilePauseOperations(ctx, true); err != nil {
		t.Fatal(err)
	}
	targets, _ := store.ListPauseTargets(ctx, operation.ID)
	if len(targets) != 1 || targets[0].State != "blocked" || targets[0].BlockerReason == nil || *targets[0].BlockerReason != "coordination_state_inconsistent" {
		t.Fatalf("released lease incorrectly proved quiescence: %+v", targets)
	}
}

func TestPauseReconcilerDoesNotReleaseUnknownCoordination(t *testing.T) {
	store := openCoordinationStore(t)
	ctx := context.Background()
	execution, _, err := store.SubmitExecution(ctx, testExecutionSpec("lost"))
	if err != nil {
		t.Fatal(err)
	}
	insertPauseTestRunning(t, store, execution, true)
	if _, err = store.db.Exec(`UPDATE attempts SET state='lost' WHERE id='att-running'; UPDATE leases SET state='stale',stale_at='now' WHERE id='lease-active'`); err != nil {
		t.Fatal(err)
	}
	operation := pauseProjectForTest(t, store, "pause-lost")
	if err = store.ReconcilePauseOperations(ctx, true); err != nil {
		t.Fatal(err)
	}
	targets, _ := store.ListPauseTargets(ctx, operation.ID)
	if len(targets) != 1 || targets[0].State != "blocked" || targets[0].BlockerReason == nil || *targets[0].BlockerReason != "coordination_identity_unknown" {
		t.Fatalf("unknown execution was treated as free: %+v", targets)
	}
	var leaseState string
	if err = store.db.QueryRow(`SELECT state FROM leases WHERE id='lease-active'`).Scan(&leaseState); err != nil || leaseState != "stale" {
		t.Fatalf("stale lease was released: state=%q err=%v", leaseState, err)
	}
}

func TestPauseReconcilerPreservesCheckpointRejection(t *testing.T) {
	store := openCoordinationStore(t)
	ctx := context.Background()
	execution, _, err := store.SubmitExecution(ctx, testExecutionSpec("rejected"))
	if err != nil {
		t.Fatal(err)
	}
	insertPauseTestRunning(t, store, execution, true)
	operation := pauseProjectForTest(t, store, "pause-rejected")
	if err = store.ReconcilePauseOperations(ctx, true); err != nil {
		t.Fatal(err)
	}
	targets, _ := store.ListPauseTargets(ctx, operation.ID)
	if len(targets) != 1 || targets[0].CommandID == nil {
		t.Fatalf("no suspend command: %+v", targets)
	}
	if err = store.AckCommand(ctx, "att-running", "lease-active", 1, *targets[0].CommandID, "rejected", []byte(`{"reason":"unsafe safe-point"}`)); err != nil {
		t.Fatal(err)
	}
	if err = store.ReconcilePauseOperations(ctx, true); err != nil {
		t.Fatal(err)
	}
	var count int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM commands WHERE attempt_id='att-running'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	targets, _ = store.ListPauseTargets(ctx, operation.ID)
	if count != 1 || len(targets) != 1 || targets[0].State != "blocked" || targets[0].BlockerReason == nil || *targets[0].BlockerReason != "checkpoint_rejected" {
		t.Fatalf("checkpoint rejection was retried: count=%d targets=%+v", count, targets)
	}
}
