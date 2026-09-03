package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
)

func TestOpenMigratesV1WithoutDigestUniquenessAndBackfillsCapacityResources(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v1.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`
CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL);
INSERT INTO schema_migrations VALUES(1,'now');
CREATE TABLE queues(id TEXT PRIMARY KEY, project TEXT NOT NULL, name TEXT NOT NULL, current_revision INTEGER NOT NULL DEFAULT 0, desired_state TEXT NOT NULL DEFAULT 'active', created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE queue_revisions(queue_id TEXT NOT NULL REFERENCES queues(id), revision INTEGER NOT NULL, digest TEXT NOT NULL, normalized_json TEXT NOT NULL, source_text TEXT NOT NULL, actor TEXT NOT NULL, request_id TEXT NOT NULL, created_at TEXT NOT NULL, PRIMARY KEY(queue_id,revision), UNIQUE(queue_id,digest), UNIQUE(queue_id,request_id));
CREATE TABLE executors(id TEXT PRIMARY KEY, node_id TEXT NOT NULL);
CREATE TABLE resource_instances(id TEXT PRIMARY KEY, node_id TEXT NOT NULL, kind TEXT NOT NULL, stable_identity TEXT NOT NULL);
CREATE TABLE leases(id TEXT PRIMARY KEY, executor_id TEXT NOT NULL, state TEXT NOT NULL);
CREATE TABLE lease_items(lease_id TEXT NOT NULL, resource_id TEXT, kind TEXT NOT NULL, quantity INTEGER NOT NULL, filesystem TEXT NOT NULL DEFAULT '', PRIMARY KEY(lease_id,kind,resource_id,filesystem));
CREATE TRIGGER exclusive_resource_lease_guard BEFORE INSERT ON lease_items WHEN NEW.resource_id IS NOT NULL BEGIN SELECT RAISE(ABORT,'old trigger'); END;
INSERT INTO executors VALUES('e','n');
INSERT INTO resource_instances VALUES('n-cpu','n','cpu','logical-cpu');
INSERT INTO leases VALUES('lease','e','stale');
INSERT INTO lease_items VALUES('lease',NULL,'cpu',1000,'');
`)
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}

	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	var version int
	if err = st.db.QueryRow(`SELECT MAX(version) FROM schema_migrations`).Scan(&version); err != nil || version != 2 {
		t.Fatalf("migration version: %d %v", version, err)
	}
	var resourceID string
	if err = st.db.QueryRow(`SELECT resource_id FROM lease_items WHERE lease_id='lease'`).Scan(&resourceID); err != nil || resourceID != "n-cpu" {
		t.Fatalf("capacity resource backfill: %q %v", resourceID, err)
	}
	if _, err = st.db.Exec(`INSERT INTO queues(id,project,name,current_revision,desired_state,created_at,updated_at) VALUES('q','p','q',2,'active','now','now')`); err != nil {
		t.Fatal(err)
	}
	for revision, request := range []string{"one", "two"} {
		if _, err = st.db.Exec(`INSERT INTO queue_revisions(queue_id,revision,digest,normalized_json,source_text,actor,request_id,created_at) VALUES('q',?,'same','{}','source','test',?,'now')`, revision+1, request); err != nil {
			t.Fatalf("duplicate digest revision %d: %v", revision+1, err)
		}
	}
}

func openTestStore(t *testing.T) *Store {
	t.Helper()
	st, err := Open(filepath.Join(t.TempDir(), "kairo.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func submit(t *testing.T, st *Store, key string, priority int, preemptible bool) Workload {
	t.Helper()
	w, created, err := st.SubmitWorkload(context.Background(), WorkloadSpec{
		Project: "test", ExternalKey: key, Priority: priority,
		Argv: []string{"worker"}, CWD: t.TempDir(),
		ResourceKind: "gpu", ResourceCount: 1,
		CooperativeSuspend: preemptible,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Fatal("expected a new workload")
	}
	return w
}

func TestPreemptionKeepsCheckpointAndQuiescenceSeparate(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	if err := st.AddResource(ctx, Resource{ID: "gpu0", Kind: "gpu", Binding: "0"}); err != nil {
		t.Fatal(err)
	}
	low := submit(t, st, "pretrain", 10, true)
	first, err := st.AllocateNext(ctx, "local")
	if err != nil || first == nil {
		t.Fatalf("allocate first: launch=%v err=%v", first, err)
	}
	if first.Workload.ID != low.ID || first.Attempt.Ordinal != 1 {
		t.Fatalf("unexpected first launch: %+v", first)
	}
	if err := st.MarkAttemptRunning(ctx, first.Attempt.ID, 1234); err != nil {
		t.Fatal(err)
	}
	high := submit(t, st, "short-experiment", 100, false)
	commandID, created, err := st.EnsurePreemption(ctx)
	if err != nil || !created || commandID == "" {
		t.Fatalf("ensure preemption: id=%q created=%v err=%v", commandID, created, err)
	}

	commands, err := st.PollCommands(ctx, first.Attempt.ID)
	if err != nil || len(commands) != 1 || commands[0].DeliveryCount != 1 {
		t.Fatalf("first delivery: commands=%+v err=%v", commands, err)
	}
	commands, err = st.PollCommands(ctx, first.Attempt.ID)
	if err != nil || len(commands) != 1 || commands[0].ID != commandID || commands[0].DeliveryCount != 2 {
		t.Fatalf("second delivery: commands=%+v err=%v", commands, err)
	}

	for _, phase := range []string{"accepted", "checkpointing", "checkpointed", "accepted"} {
		payload := json.RawMessage(`{}`)
		if phase == "checkpointed" {
			payload = json.RawMessage(`{"continuation_ref":"checkpoint://step-12345"}`)
		}
		if err := st.AckCommand(ctx, first.Attempt.ID, commandID, phase, payload); err != nil {
			t.Fatalf("ack %s: %v", phase, err)
		}
	}
	attempt, err := st.GetAttempt(ctx, first.Attempt.ID)
	if err != nil {
		t.Fatal(err)
	}
	if attempt.CheckpointedAt == nil || attempt.QuiescedAt != nil {
		t.Fatalf("checkpoint must not imply quiescence: %+v", attempt)
	}
	if launch, err := st.AllocateNext(ctx, "local"); err != nil || launch != nil {
		t.Fatalf("lease was released before quiescence: launch=%v err=%v", launch, err)
	}

	if err := st.MarkAttemptExited(ctx, first.Attempt.ID, 0); err != nil {
		t.Fatal(err)
	}
	attempt, err = st.GetAttempt(ctx, first.Attempt.ID)
	if err != nil || attempt.QuiescedAt == nil {
		t.Fatalf("executor exit must establish quiescence: attempt=%+v err=%v", attempt, err)
	}
	second, err := st.AllocateNext(ctx, "local")
	if err != nil || second == nil || second.Workload.ID != high.ID {
		t.Fatalf("high priority workload was not next: launch=%+v err=%v", second, err)
	}
	if err := st.MarkAttemptRunning(ctx, second.Attempt.ID, 2345); err != nil {
		t.Fatal(err)
	}
	if err := st.ReportDisposition(ctx, second.Attempt.ID, "close", nil); err != nil {
		t.Fatal(err)
	}
	if err := st.MarkAttemptExited(ctx, second.Attempt.ID, 0); err != nil {
		t.Fatal(err)
	}
	third, err := st.AllocateNext(ctx, "local")
	if err != nil || third == nil || third.Workload.ID != low.ID || third.Attempt.Ordinal != 2 {
		t.Fatalf("same logical workload must receive a new attempt: launch=%+v err=%v", third, err)
	}
	if third.ContinuationRef == nil || *third.ContinuationRef != "checkpoint://step-12345" {
		t.Fatalf("acknowledged continuation was not passed to next attempt: %+v", third.ContinuationRef)
	}
}

func TestSubmissionIsIdempotentByProjectExternalKey(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	spec := WorkloadSpec{
		Project: "fpmvslm", ExternalKey: "tiny-v1", Priority: 10,
		Argv: []string{"python", "train.py"}, CWD: t.TempDir(),
		ResourceKind: "gpu", ResourceCount: 1, CooperativeSuspend: true,
	}
	first, created, err := st.SubmitWorkload(ctx, spec)
	if err != nil || !created {
		t.Fatalf("first submit: created=%v err=%v", created, err)
	}
	second, created, err := st.SubmitWorkload(ctx, spec)
	if err != nil || created || second.ID != first.ID {
		t.Fatalf("duplicate submit: first=%s second=%s created=%v err=%v", first.ID, second.ID, created, err)
	}
	spec.Priority++
	if _, _, err := st.SubmitWorkload(ctx, spec); !errors.Is(err, ErrSpecConflict) {
		t.Fatalf("mismatched duplicate must conflict, got %v", err)
	}
}

func TestRejectedSuspendRestoresResourceOwnership(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	if err := st.AddResource(ctx, Resource{ID: "gpu0", Kind: "gpu", Binding: "0"}); err != nil {
		t.Fatal(err)
	}
	w := submit(t, st, "pretrain", 10, true)
	launch, err := st.AllocateNext(ctx, "local")
	if err != nil || launch == nil {
		t.Fatalf("allocate: launch=%v err=%v", launch, err)
	}
	if err := st.MarkAttemptRunning(ctx, launch.Attempt.ID, 1234); err != nil {
		t.Fatal(err)
	}
	commandID, err := st.PauseWorkload(ctx, w.ID)
	if err != nil || commandID == "" {
		t.Fatalf("pause: command=%q err=%v", commandID, err)
	}
	if err := st.AckCommand(ctx, launch.Attempt.ID, commandID, "accepted", nil); err != nil {
		t.Fatal(err)
	}
	if err := st.AckCommand(ctx, launch.Attempt.ID, commandID, "rejected", json.RawMessage(`{"reason":"unsafe"}`)); err != nil {
		t.Fatal(err)
	}
	attempt, err := st.GetAttempt(ctx, launch.Attempt.ID)
	if err != nil || attempt.State != "running" {
		t.Fatalf("attempt was not restored: attempt=%+v err=%v", attempt, err)
	}
	var leaseState string
	if err := st.db.QueryRowContext(ctx, `SELECT state FROM leases WHERE id=?`, launch.LeaseID).Scan(&leaseState); err != nil {
		t.Fatal(err)
	}
	if leaseState != "active" {
		t.Fatalf("lease was not restored: %s", leaseState)
	}
	stored, err := st.GetWorkload(ctx, w.ID)
	if err != nil || stored.SchedulingState != "running" || stored.DesiredState != "paused" {
		t.Fatalf("workload ownership state is inconsistent: workload=%+v err=%v", stored, err)
	}
}

func TestExitReportingIsIdempotent(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	if err := st.AddResource(ctx, Resource{ID: "gpu0", Kind: "gpu", Binding: "0"}); err != nil {
		t.Fatal(err)
	}
	w := submit(t, st, "once", 10, false)
	launch, err := st.AllocateNext(ctx, "local")
	if err != nil || launch == nil {
		t.Fatalf("allocate: launch=%v err=%v", launch, err)
	}
	if err := st.MarkAttemptRunning(ctx, launch.Attempt.ID, 1234); err != nil {
		t.Fatal(err)
	}
	if err := st.ReportDisposition(ctx, launch.Attempt.ID, "close", nil); err != nil {
		t.Fatal(err)
	}
	if err := st.MarkAttemptExited(ctx, launch.Attempt.ID, 0); err != nil {
		t.Fatal(err)
	}
	if err := st.MarkAttemptExited(ctx, launch.Attempt.ID, 99); err != nil {
		t.Fatal(err)
	}
	attempt, err := st.GetAttempt(ctx, launch.Attempt.ID)
	if err != nil || attempt.ExitCode == nil || *attempt.ExitCode != 0 {
		t.Fatalf("duplicate exit report changed history: attempt=%+v err=%v", attempt, err)
	}
	stored, err := st.GetWorkload(ctx, w.ID)
	if err != nil || stored.SchedulingState != "closed" {
		t.Fatalf("duplicate exit report changed workload: workload=%+v err=%v", stored, err)
	}
}

func TestCrashAfterCheckpointPublishBeforeAckHoldsContinuation(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	if err := st.AddResource(ctx, Resource{ID: "gpu0", Kind: "gpu", Binding: "0"}); err != nil {
		t.Fatal(err)
	}
	low := submit(t, st, "pretrain", 10, true)
	first, err := st.AllocateNext(ctx, "local")
	if err != nil || first == nil {
		t.Fatalf("allocate first: launch=%v err=%v", first, err)
	}
	if err := st.MarkAttemptRunning(ctx, first.Attempt.ID, 1234); err != nil {
		t.Fatal(err)
	}
	high := submit(t, st, "short-experiment", 100, false)
	commandID, created, err := st.EnsurePreemption(ctx)
	if err != nil || !created || commandID == "" {
		t.Fatalf("preempt: id=%q created=%v err=%v", commandID, created, err)
	}
	// The project may have atomically published a checkpoint here, but because
	// its acknowledgement did not commit before process death Kairo must not
	// infer continuation safety from process exit.
	if err := st.MarkAttemptExited(ctx, first.Attempt.ID, 1); err != nil {
		t.Fatal(err)
	}
	stored, err := st.GetWorkload(ctx, low.ID)
	if err != nil || stored.SchedulingState != "held" {
		t.Fatalf("unasserted continuation was automatically requeued: workload=%+v err=%v", stored, err)
	}
	attempt, err := st.GetAttempt(ctx, first.Attempt.ID)
	if err != nil || attempt.QuiescedAt == nil || attempt.CheckpointedAt != nil {
		t.Fatalf("checkpoint and quiescence facts were conflated: attempt=%+v err=%v", attempt, err)
	}
	second, err := st.AllocateNext(ctx, "local")
	if err != nil || second == nil || second.Workload.ID != high.ID {
		t.Fatalf("released resource was not available to high priority work: launch=%+v err=%v", second, err)
	}
}

func TestRestartQuarantinesInsteadOfReleasing(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	if err := st.AddResource(ctx, Resource{ID: "gpu0", Kind: "gpu", Binding: "0"}); err != nil {
		t.Fatal(err)
	}
	submit(t, st, "pretrain", 10, true)
	launch, err := st.AllocateNext(ctx, "local")
	if err != nil || launch == nil {
		t.Fatalf("allocate: launch=%v err=%v", launch, err)
	}
	if err := st.MarkAttemptRunning(ctx, launch.Attempt.ID, 1234); err != nil {
		t.Fatal(err)
	}
	n, err := st.QuarantineUnreconciled(ctx, "local")
	if err != nil || n != 1 {
		t.Fatalf("quarantine: count=%d err=%v", n, err)
	}
	snapshot, err := st.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Resources) != 1 || snapshot.Resources[0].State != "quarantined" {
		t.Fatalf("resource was not quarantined: %+v", snapshot.Resources)
	}
	attempt, err := st.GetAttempt(ctx, launch.Attempt.ID)
	if err != nil || attempt.State != "lost" {
		t.Fatalf("attempt was not marked lost: %+v err=%v", attempt, err)
	}
}
