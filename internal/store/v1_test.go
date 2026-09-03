package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"kairo/internal/plan"
)

const testManifest = `schema_version=1
project="p"
queue="q"
[[tasks]]
key="first"
argv=["run","first"]
cwd="."
priority=10
depends_on=[]
checkpointable=true
[tasks.executor]
labels={environment="windows"}
[tasks.retry]
max_failure_attempts=2
retry_on=["exit_nonzero"]
initial_backoff_seconds=1
max_backoff_seconds=2
[[tasks.exclusive]]
kind="gpu"
count=1
min_total_memory_bytes=100
min_observed_free_memory_bytes=80
[tasks.capacity]
cpu_millis=1000
ram_bytes=1000
[[tasks.capacity.disks]]
filesystem="D:"
reserve_bytes=1000
min_free_after_bytes=1000
[[tasks]]
key="second"
argv=["run","second"]
cwd="."
priority=5
depends_on=["first"]
`

func applyManifest(t *testing.T, st *Store, source string, expected int, create bool, request string) ApplyResult {
	t.Helper()
	v, err := plan.Parse([]byte(source))
	if err != nil {
		t.Fatal(err)
	}
	result, err := st.ApplyPlan(context.Background(), v, ApplyOptions{ExpectedRevision: expected, Create: create, Actor: "test", RequestID: request})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestAtomicPlanApplyAndRevisionPropagation(t *testing.T) {
	st := openTestStore(t)
	first := applyManifest(t, st, testManifest, 0, true, "r1")
	if first.Revision != 1 {
		t.Fatal(first)
	}
	v, _ := plan.Parse([]byte(testManifest))
	same, err := st.ApplyPlan(context.Background(), v, ApplyOptions{ExpectedRevision: 0, Actor: "test", RequestID: "another"})
	if err != nil || !same.Idempotent {
		t.Fatalf("idempotent: %+v %v", same, err)
	}
	priority := string([]byte(testManifest))
	priority = replaceOnce(priority, "priority=10", "priority=20")
	applyManifest(t, st, priority, 1, false, "r2")
	tasks, _ := st.ListTasks(context.Background(), first.Queue.ID)
	if tasks[0].Priority != 20 || tasks[0].CurrentRevision != 1 {
		t.Fatalf("priority-only created revision: %+v", tasks)
	}
	changed := replaceOnce(priority, `argv=["run","first"]`, `argv=["run","first","changed"]`)
	applyManifest(t, st, changed, 2, false, "r3")
	tasks, _ = st.ListTasks(context.Background(), first.Queue.ID)
	revs := map[string]int{}
	for _, x := range tasks {
		revs[x.Key] = x.CurrentRevision
	}
	if revs["first"] != 2 || revs["second"] != 2 {
		t.Fatalf("revision did not propagate: %+v", revs)
	}
	bad, _ := plan.Parse([]byte(replaceOnce(testManifest, "priority=10", "priority=999")))
	_, err = st.ApplyPlan(context.Background(), bad, ApplyOptions{ExpectedRevision: 99, Actor: "x", RequestID: "conflict"})
	if !errors.Is(err, ErrRevisionConflict) {
		t.Fatalf("wanted conflict: %v", err)
	}
}

func replaceOnce(s, old, new string) string {
	for i := 0; i+len(old) <= len(s); i++ {
		if s[i:i+len(old)] == old {
			return s[:i] + new + s[i+len(old):]
		}
	}
	return s
}

func TestGangReservationAuthorizationAndEpochFence(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	result := applyManifest(t, st, testManifest, 0, true, "r1")
	if err := st.UpsertNode(ctx, Node{ID: "n", Name: "n", OS: "windows", Architecture: "amd64", Attributes: json.RawMessage(`{}`), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertExecutor(ctx, Executor{ID: "e", NodeID: "n", Kind: "windows", Attributes: json.RawMessage(`{"environment":"windows"}`), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertProvider(ctx, "p", "n", "compat", nil); err != nil {
		t.Fatal(err)
	}
	future := time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano)
	observed := time.Now().UTC().Format(time.RFC3339Nano)
	resources := []ResourceInstance{{ID: "g", NodeID: "n", ProviderID: "p", Kind: "gpu", StableIdentity: "GPU-1", Binding: json.RawMessage(`{"cuda":"0"}`), AdminState: "enabled"}, {ID: "cpu", NodeID: "n", ProviderID: "p", Kind: "cpu", StableIdentity: "cpu", Binding: json.RawMessage(`{}`), AdminState: "enabled"}, {ID: "ram", NodeID: "n", ProviderID: "p", Kind: "ram", StableIdentity: "ram", Binding: json.RawMessage(`{}`), AdminState: "enabled"}, {ID: "disk", NodeID: "n", ProviderID: "p", Kind: "disk", StableIdentity: "D:", Binding: json.RawMessage(`{}`), AdminState: "enabled"}}
	for _, r := range resources {
		if err := st.UpsertResourceInstance(ctx, r); err != nil {
			t.Fatal(err)
		}
		total, free := int64(10000), int64(9000)
		if err := st.RecordObservation(ctx, Observation{ResourceID: r.ID, ObservedAt: observed, ValidUntil: future, TotalBytes: &total, FreeBytes: &free}); err != nil {
			t.Fatal(err)
		}
	}
	reservation, err := st.ReserveNext(ctx, "e")
	if err != nil || reservation == nil {
		t.Fatalf("reserve: %+v %v", reservation, err)
	}
	if len(reservation.Resources) != 1 {
		t.Fatal(reservation.Resources)
	}
	if err = st.MarkLeasePrepared(ctx, reservation.Lease.ID, reservation.Lease.CoordinationEpoch); err != nil {
		t.Fatal(err)
	}
	launch, err := st.AuthorizeLaunch(ctx, reservation)
	if err != nil {
		t.Fatal(err)
	}
	if err = st.ActivateLaunch(ctx, launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch, launch.AuthorizationToken, 123, "pid:123:start:1"); err != nil {
		t.Fatal(err)
	}
	if err = st.HeartbeatV1(ctx, launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch+1, json.RawMessage(`{"unit":"step","current":1}`)); !errors.Is(err, ErrStaleEpoch) {
		t.Fatalf("stale epoch accepted: %v", err)
	}
	if err = st.TerminalV1(ctx, launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch, 0, ""); err != nil {
		t.Fatal(err)
	}
	tasks, err := st.ListTasks(ctx, result.Queue.ID)
	if err != nil || tasks[0].SchedulingState != "succeeded" {
		t.Fatalf("terminal: %+v %v", tasks, err)
	}
}

func seedV1GPUs(t *testing.T, st *Store, ids ...string) {
	t.Helper()
	ctx := context.Background()
	if err := st.UpsertNode(ctx, Node{ID: "n", Name: "n", OS: "windows", Architecture: "amd64", Attributes: json.RawMessage(`{}`), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertExecutor(ctx, Executor{ID: "e", NodeID: "n", Kind: "windows", Attributes: json.RawMessage(`{"environment":"windows"}`), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertProvider(ctx, "gpu-provider", "n", "nvidia", nil); err != nil {
		t.Fatal(err)
	}
	stamp := time.Now().UTC()
	total, free := int64(24_000), int64(23_000)
	resources := make([]ResourceInstance, 0, len(ids))
	observations := make([]Observation, 0, len(ids))
	for i, resourceID := range ids {
		resources = append(resources, ResourceInstance{ID: resourceID, NodeID: "n", ProviderID: "gpu-provider", Kind: "gpu", StableIdentity: fmt.Sprintf("GPU-%d", i), Binding: json.RawMessage(fmt.Sprintf(`{"cuda_index":"%d"}`, i)), AdminState: "enabled"})
		observations = append(observations, Observation{ResourceID: resourceID, ObservedAt: stamp.Format(time.RFC3339Nano), ValidUntil: stamp.Add(time.Minute).Format(time.RFC3339Nano), TotalBytes: &total, FreeBytes: &free})
	}
	if err := st.ApplyObservationBatch(ctx, "gpu-provider", resources, observations, nil); err != nil {
		t.Fatal(err)
	}
}

const pilotLowManifest = `schema_version=1
project="pilot"
queue="acceptance"
[[tasks]]
key="pretrain"
argv=["train"]
cwd="."
priority=10
checkpointable=true
[tasks.retry]
max_failure_attempts=2
retry_on=["lost_after_reconcile"]
initial_backoff_seconds=1
max_backoff_seconds=1
[[tasks.exclusive]]
kind="gpu"
count=1
`

const pilotHighTask = `
[[tasks]]
key="short"
argv=["evaluate"]
cwd="."
priority=100
checkpointable=false
[[tasks.exclusive]]
kind="gpu"
count=1
`

func prepareAndActivateV1(t *testing.T, st *Store) *V1Launch {
	t.Helper()
	ctx := context.Background()
	reservation, err := st.ReserveNext(ctx, "e")
	if err != nil || reservation == nil {
		t.Fatalf("reserve: %+v %v", reservation, err)
	}
	if err = st.ValidateReservation(ctx, reservation.Lease.ID, reservation.Lease.CoordinationEpoch); err != nil {
		t.Fatal(err)
	}
	if err = st.MarkLeasePrepared(ctx, reservation.Lease.ID, reservation.Lease.CoordinationEpoch); err != nil {
		t.Fatal(err)
	}
	launch, err := st.AuthorizeLaunch(ctx, reservation)
	if err != nil {
		t.Fatal(err)
	}
	pid := 1000 + launch.Attempt.Ordinal
	if err = st.ActivateLaunch(ctx, launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch, launch.AuthorizationToken, pid, fmt.Sprintf("pid:%d:start:test", pid)); err != nil {
		t.Fatal(err)
	}
	return launch
}

func TestPilotPreemptionCheckpointExitReleaseAndResume(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	seedV1GPUs(t, st, "gpu0")
	firstApply := applyManifest(t, st, pilotLowManifest, 0, true, "pilot-1")
	low := prepareAndActivateV1(t, st)
	applyManifest(t, st, pilotLowManifest+pilotHighTask, firstApply.Revision, false, "pilot-2")

	if reservation, err := st.ReserveNext(ctx, "e"); err != nil || reservation != nil {
		t.Fatalf("high-priority work bypassed the active lease: %+v %v", reservation, err)
	}
	commands, err := st.EnsureV1Preemption(ctx)
	if err != nil || len(commands) != 1 {
		t.Fatalf("preemption: %+v %v", commands, err)
	}
	commandID := commands[0]
	for delivery := 1; delivery <= 2; delivery++ {
		polled, pollErr := st.PollCommandsV1(ctx, low.Attempt.ID, low.Lease.ID, low.Lease.CoordinationEpoch)
		if pollErr != nil || len(polled) != 1 || polled[0].ID != commandID || polled[0].DeliveryCount != delivery {
			t.Fatalf("delivery %d: %+v %v", delivery, polled, pollErr)
		}
	}
	for _, phase := range []string{"accepted", "checkpointing"} {
		if err = st.AckCommandV1(ctx, low.Attempt.ID, low.Lease.ID, low.Lease.CoordinationEpoch, commandID, phase, nil); err != nil {
			t.Fatalf("ack %s: %v", phase, err)
		}
	}
	payload := json.RawMessage(`{"continuation_ref":"checkpoint://step-12345","step":12345}`)
	if err = st.AckCommandV1(ctx, low.Attempt.ID, low.Lease.ID, low.Lease.CoordinationEpoch, commandID, "checkpointed", payload); err != nil {
		t.Fatal(err)
	}
	if err = st.AckCommandV1(ctx, low.Attempt.ID, low.Lease.ID, low.Lease.CoordinationEpoch, commandID, "checkpointed", payload); err != nil {
		t.Fatalf("idempotent checkpoint ACK failed: %v", err)
	}
	if err = st.AckCommandV1(ctx, low.Attempt.ID, low.Lease.ID, low.Lease.CoordinationEpoch, commandID, "checkpointed", json.RawMessage(`{"continuation_ref":"checkpoint://wrong"}`)); err == nil {
		t.Fatal("conflicting duplicate checkpoint ACK overwrote the continuation")
	}
	if err = st.TerminalV1(ctx, low.Attempt.ID, low.Lease.ID, low.Lease.CoordinationEpoch, 0, ""); err != nil {
		t.Fatal(err)
	}

	high := prepareAndActivateV1(t, st)
	if high.Task.Key != "short" {
		t.Fatalf("high-priority task was not scheduled first: %s", high.Task.Key)
	}
	if err = st.TerminalV1(ctx, high.Attempt.ID, high.Lease.ID, high.Lease.CoordinationEpoch, 0, ""); err != nil {
		t.Fatal(err)
	}
	resumed := prepareAndActivateV1(t, st)
	if resumed.Task.Key != "pretrain" || resumed.ContinuationRef == nil || *resumed.ContinuationRef != "checkpoint://step-12345" {
		t.Fatalf("pretrain did not resume from the published continuation: %+v", resumed)
	}
}

func TestCheckpointedAckRequiresOpaqueContinuation(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	seedV1GPUs(t, st, "gpu0")
	applyManifest(t, st, pilotLowManifest, 0, true, "continuation-1")
	launch := prepareAndActivateV1(t, st)
	commandID, err := st.EnqueueCommandV1(ctx, launch.Attempt.ID, "suspend", "test")
	if err != nil {
		t.Fatal(err)
	}
	if err = st.AckCommandV1(ctx, launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch, commandID, "checkpointed", json.RawMessage(`{}`)); err == nil {
		t.Fatal("checkpointed ACK without continuation_ref was accepted")
	}
}

func TestGangReservationIsAllOrNothingAroundExternalClaims(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	seedV1GPUs(t, st, "gpu0", "gpu1")
	manifest := strings.Replace(pilotLowManifest, "count=1", "count=2", 1)
	applyManifest(t, st, manifest, 0, true, "gang-1")
	if err := st.ObserveClaim(ctx, ExternalClaim{ID: "external", ResourceID: "gpu1", ClaimKind: "external_process"}); err != nil {
		t.Fatal(err)
	}
	if reservation, err := st.ReserveNext(ctx, "e"); err != nil || reservation != nil {
		t.Fatalf("partial gang reservation escaped: %+v %v", reservation, err)
	}
	status, err := st.Status(ctx)
	if err != nil || len(status.Leases) != 0 {
		t.Fatalf("failed gang left a lease behind: %+v %v", status.Leases, err)
	}
	if err = st.ClearClaim(ctx, "external"); err != nil {
		t.Fatal(err)
	}
	reservation, err := st.ReserveNext(ctx, "e")
	if err != nil || reservation == nil || len(reservation.Resources) != 2 {
		t.Fatalf("two-GPU gang was not reserved atomically: %+v %v", reservation, err)
	}
}

func TestExecutorCrashFencesOldWorkerAndKeepsLeaseStale(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	seedV1GPUs(t, st, "gpu0")
	applyManifest(t, st, pilotLowManifest, 0, true, "crash-1")
	launch := prepareAndActivateV1(t, st)
	commandID, err := st.EnqueueCommandV1(ctx, launch.Attempt.ID, "suspend", "crash_test")
	if err != nil {
		t.Fatal(err)
	}
	if err = st.AckCommandV1(ctx, launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch, commandID, "checkpointed", json.RawMessage(`{"continuation_ref":"checkpoint://durable-before-crash"}`)); err != nil {
		t.Fatal(err)
	}
	if n, err := st.MarkExecutorUnknown(ctx, "e"); err != nil || n != 1 {
		t.Fatalf("mark unknown: %d %v", n, err)
	}
	if err := st.HeartbeatV1(ctx, launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch, nil); !errors.Is(err, ErrStaleEpoch) {
		t.Fatalf("old worker crossed epoch fence: %v", err)
	}
	if reservation, err := st.ReserveNext(ctx, "e"); err != nil || reservation != nil {
		t.Fatalf("stale lease allowed double allocation: %+v %v", reservation, err)
	}
	status, err := st.Status(ctx)
	if err != nil || len(status.Leases) != 1 || status.Leases[0].State != "stale" {
		t.Fatalf("lease was not retained as stale: %+v %v", status.Leases, err)
	}
	if err = st.ReconcileResource(ctx, "gpu0"); err == nil {
		t.Fatal("registered process was reconciled without explicit absence confirmation")
	}
	if err = st.ReconcileResource(ctx, "gpu0", true); err != nil {
		t.Fatal(err)
	}
	status, err = st.Status(ctx)
	if err != nil || len(status.Attempts) != 1 || status.Attempts[0].ContinuationRef == nil || *status.Attempts[0].ContinuationRef != "checkpoint://durable-before-crash" {
		t.Fatalf("acknowledged continuation was lost during reconciliation: %+v %v", status.Attempts, err)
	}
}

func TestObservationBatchRollsBackAsOneRealitySnapshot(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	if err := st.UpsertNode(ctx, Node{ID: "n", Name: "n", OS: "windows", Architecture: "amd64", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertProvider(ctx, "p", "n", "nvidia", nil); err != nil {
		t.Fatal(err)
	}
	stamp := time.Now().UTC()
	total, free := int64(100), int64(90)
	err := st.ApplyObservationBatch(ctx, "p",
		[]ResourceInstance{{ID: "gpu0", NodeID: "n", ProviderID: "p", Kind: "gpu", StableIdentity: "GPU-0", AdminState: "enabled"}},
		[]Observation{{ResourceID: "gpu0", ObservedAt: stamp.Format(time.RFC3339Nano), ValidUntil: stamp.Add(time.Minute).Format(time.RFC3339Nano), TotalBytes: &total, FreeBytes: &free}},
		[]ExternalClaim{{ID: "bad-claim", ResourceID: "missing", ClaimKind: "external_process"}},
	)
	if err == nil {
		t.Fatal("invalid claim unexpectedly committed")
	}
	status, statusErr := st.Status(ctx)
	if statusErr != nil {
		t.Fatal(statusErr)
	}
	if len(status.Resources) != 0 || len(status.Claims) != 0 {
		t.Fatalf("partial observation batch became visible: resources=%+v claims=%+v", status.Resources, status.Claims)
	}
}

func TestManifestRevisionWaitsForOldAttemptAndDoesNotInheritContinuation(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	seedV1GPUs(t, st, "gpu0", "gpu1")
	firstApply := applyManifest(t, st, pilotLowManifest, 0, true, "revision-1")
	old := prepareAndActivateV1(t, st)
	changed := strings.Replace(pilotLowManifest, `argv=["train"]`, `argv=["train","v2"]`, 1)
	applyManifest(t, st, changed, firstApply.Revision, false, "revision-2")

	// A second GPU is free, but one logical task must not run two revisions at once.
	if reservation, err := st.ReserveNext(ctx, "e"); err != nil || reservation != nil {
		t.Fatalf("new revision ran concurrently with old attempt: %+v %v", reservation, err)
	}
	commandID, err := st.EnqueueCommandV1(ctx, old.Attempt.ID, "suspend", "revision_update")
	if err != nil {
		t.Fatal(err)
	}
	if err = st.AckCommandV1(ctx, old.Attempt.ID, old.Lease.ID, old.Lease.CoordinationEpoch, commandID, "checkpointed", json.RawMessage(`{"continuation_ref":"checkpoint://revision-1"}`)); err != nil {
		t.Fatal(err)
	}
	if err = st.TerminalV1(ctx, old.Attempt.ID, old.Lease.ID, old.Lease.CoordinationEpoch, 0, ""); err != nil {
		t.Fatal(err)
	}

	current := prepareAndActivateV1(t, st)
	if current.TaskRevision != 2 || len(current.Argv) != 2 || current.Argv[1] != "v2" {
		t.Fatalf("current revision was not launched: %+v", current)
	}
	if current.ContinuationRef != nil {
		t.Fatalf("continuation leaked across immutable revisions: %q", *current.ContinuationRef)
	}
}

func TestManagedIdentityOnlySuppressesClaimsOnItsLease(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	seedV1GPUs(t, st, "gpu0", "gpu1")
	applyManifest(t, st, pilotLowManifest, 0, true, "claim-scope-1")
	launch := prepareAndActivateV1(t, st)
	identity := fmt.Sprintf("pid:%d:start:test", 1000+launch.Attempt.Ordinal)
	stamp := time.Now().UTC()
	total, free := int64(24_000), int64(20_000)
	if err := st.ApplyObservationBatch(ctx, "gpu-provider", nil,
		[]Observation{{ResourceID: "gpu1", ObservedAt: stamp.Format(time.RFC3339Nano), ValidUntil: stamp.Add(time.Minute).Format(time.RFC3339Nano), TotalBytes: &total, FreeBytes: &free}},
		[]ExternalClaim{{ID: "wrong-gpu", ResourceID: "gpu1", ClaimKind: "external_process", ProcessIdentity: &identity}},
	); err != nil {
		t.Fatal(err)
	}
	claims, err := st.ListClaims(ctx)
	if err != nil || len(claims) != 1 || claims[0].ResourceID != "gpu1" {
		t.Fatalf("managed identity suppressed activity outside its lease: %+v %v", claims, err)
	}
}

func TestCancelDoesNotSatisfyDependencies(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	seedV1GPUs(t, st, "gpu0")
	manifest := `schema_version=1
project="pilot"
queue="cancel"
[[tasks]]
key="first"
argv=["first"]
cwd="."
priority=10
checkpointable=true
[[tasks.exclusive]]
kind="gpu"
count=1
[[tasks]]
key="dependent"
argv=["dependent"]
cwd="."
depends_on=["first"]
[[tasks.exclusive]]
kind="gpu"
count=1
`
	result := applyManifest(t, st, manifest, 0, true, "cancel-1")
	launch := prepareAndActivateV1(t, st)
	commandID, err := st.SetTaskState(ctx, launch.Task.ID, "cancel")
	if err != nil || commandID == "" {
		t.Fatalf("cancel: %q %v", commandID, err)
	}
	if err = st.TerminalV1(ctx, launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch, 0, ""); err != nil {
		t.Fatal(err)
	}
	if reservation, err := st.ReserveNext(ctx, "e"); err != nil || reservation != nil {
		t.Fatalf("cancelled dependency unlocked downstream work: %+v %v", reservation, err)
	}
	tasks, err := st.ListTasks(ctx, result.Queue.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range tasks {
		if task.Key == "first" && task.SchedulingState != "cancelled" {
			t.Fatalf("cancelled task summary is %s", task.SchedulingState)
		}
	}
	var revisionState, commandState string
	if err = st.db.QueryRowContext(ctx, `SELECT scheduling_state FROM task_revisions WHERE task_id=? AND revision=?`, launch.Task.ID, launch.TaskRevision).Scan(&revisionState); err != nil {
		t.Fatal(err)
	}
	if err = st.db.QueryRowContext(ctx, `SELECT state FROM commands WHERE id=?`, commandID).Scan(&commandState); err != nil {
		t.Fatal(err)
	}
	if revisionState != "cancelled" || commandState != "completed" {
		t.Fatalf("cancel terminal states: revision=%s command=%s", revisionState, commandState)
	}
}

func TestHistoricalManifestAppliesAsNewRevision(t *testing.T) {
	st := openTestStore(t)
	first := `schema_version=1
project="pilot"
queue="history"
[[tasks]]
key="a"
argv=["a"]
cwd="."
[[tasks]]
key="b"
argv=["b"]
cwd="."
`
	second := `schema_version=1
project="pilot"
queue="history"
[[tasks]]
key="b"
argv=["b"]
cwd="."
`
	r1 := applyManifest(t, st, first, 0, true, "history-1")
	r2 := applyManifest(t, st, second, r1.Revision, false, "history-2")
	r3 := applyManifest(t, st, first, r2.Revision, false, "history-3")
	if r3.Idempotent || r3.Revision != 3 || r3.Queue.CurrentRevision != 3 {
		t.Fatalf("historical desired state was not applied as a new revision: %+v", r3)
	}
	var desired, revisionDesired string
	if err := st.db.QueryRow(`SELECT desired_state FROM tasks WHERE queue_id=? AND task_key='a'`, r3.Queue.ID).Scan(&desired); err != nil {
		t.Fatal(err)
	}
	if err := st.db.QueryRow(`SELECT tr.desired_state FROM task_revisions tr JOIN tasks t ON t.id=tr.task_id WHERE t.queue_id=? AND t.task_key='a' AND tr.revision=t.current_revision`, r3.Queue.ID).Scan(&revisionDesired); err != nil {
		t.Fatal(err)
	}
	if desired != "active" || revisionDesired != "active" {
		t.Fatalf("re-added task was not reactivated: task=%s revision=%s", desired, revisionDesired)
	}
}

func TestCheckpointFailureRejectRestoresActiveOwnership(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	seedV1GPUs(t, st, "gpu0")
	applyManifest(t, st, pilotLowManifest, 0, true, "reject-1")
	launch := prepareAndActivateV1(t, st)
	commandID, err := st.EnqueueCommandV1(ctx, launch.Attempt.ID, "suspend", "test")
	if err != nil {
		t.Fatal(err)
	}
	if err = st.AckCommandV1(ctx, launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch, commandID, "checkpointing", nil); err != nil {
		t.Fatal(err)
	}
	if err = st.AckCommandV1(ctx, launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch, commandID, "rejected", json.RawMessage(`{"reason":"rank failed"}`)); err != nil {
		t.Fatal(err)
	}
	status, err := st.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if status.Attempts[0].State != "running" || status.Leases[0].State != "active" {
		t.Fatalf("checkpoint rejection left ownership stuck: attempt=%s lease=%s", status.Attempts[0].State, status.Leases[0].State)
	}
	var commandState, revisionState string
	if err = st.db.QueryRowContext(ctx, `SELECT state FROM commands WHERE id=?`, commandID).Scan(&commandState); err != nil {
		t.Fatal(err)
	}
	if err = st.db.QueryRowContext(ctx, `SELECT scheduling_state FROM task_revisions WHERE task_id=? AND revision=?`, launch.Task.ID, launch.TaskRevision).Scan(&revisionState); err != nil {
		t.Fatal(err)
	}
	if commandState != "rejected" || revisionState != "running" {
		t.Fatalf("checkpoint rejection states: command=%s revision=%s", commandState, revisionState)
	}
}

func TestImpossibleGangDoesNotPreemptUsefulWork(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	seedV1GPUs(t, st, "gpu0")
	lowManifest := strings.Replace(pilotLowManifest, "priority=10", "priority=1", 1)
	r1 := applyManifest(t, st, lowManifest, 0, true, "feasible-1")
	launch := prepareAndActivateV1(t, st)
	impossible := lowManifest + strings.Replace(pilotHighTask, "count=1", "count=2", 1)
	applyManifest(t, st, impossible, r1.Revision, false, "feasible-2")
	commands, err := st.EnsureV1Preemption(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(commands) != 0 {
		t.Fatalf("impossible gang request preempted useful work: %+v", commands)
	}
	status, err := st.Status(ctx)
	if err != nil || len(status.Attempts) != 1 || status.Attempts[0].ID != launch.Attempt.ID || status.Attempts[0].State != "running" {
		t.Fatalf("useful work did not remain running: %+v %v", status.Attempts, err)
	}
}

func TestTaskControlTargetsLiveSupersededAttempt(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	seedV1GPUs(t, st, "gpu0", "gpu1")
	r1 := applyManifest(t, st, pilotLowManifest, 0, true, "control-1")
	launch := prepareAndActivateV1(t, st)
	changed := strings.Replace(pilotLowManifest, `argv=["train"]`, `argv=["train","v2"]`, 1)
	applyManifest(t, st, changed, r1.Revision, false, "control-2")
	commandID, err := st.SetTaskState(ctx, launch.Task.ID, "cancel")
	if err != nil || commandID == "" {
		t.Fatalf("could not control live superseded attempt: %q %v", commandID, err)
	}
	var commandAttempt string
	if err = st.db.QueryRowContext(ctx, `SELECT attempt_id FROM commands WHERE id=?`, commandID).Scan(&commandAttempt); err != nil {
		t.Fatal(err)
	}
	if commandAttempt != launch.Attempt.ID {
		t.Fatalf("control targeted %s, want %s", commandAttempt, launch.Attempt.ID)
	}
}

func TestCapacityOnlyStaleLeaseCanBeReconciled(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	if err := st.UpsertNode(ctx, Node{ID: "n", Name: "n", OS: "windows", Architecture: "amd64", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertExecutor(ctx, Executor{ID: "e", NodeID: "n", Kind: "windows", Attributes: json.RawMessage(`{}`), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertProvider(ctx, "host", "n", "host", nil); err != nil {
		t.Fatal(err)
	}
	stamp := time.Now().UTC()
	total, free := int64(16_000), int64(16_000)
	if err := st.ApplyObservationBatch(ctx, "host",
		[]ResourceInstance{{ID: "n-cpu", NodeID: "n", ProviderID: "host", Kind: "cpu", StableIdentity: "logical-cpu", AdminState: "enabled"}},
		[]Observation{{ResourceID: "n-cpu", ObservedAt: stamp.Format(time.RFC3339Nano), ValidUntil: stamp.Add(time.Minute).Format(time.RFC3339Nano), TotalBytes: &total, FreeBytes: &free}}, nil); err != nil {
		t.Fatal(err)
	}
	manifest := `schema_version=1
project="pilot"
queue="capacity"
[[tasks]]
key="cpu-only"
argv=["run"]
cwd="."
[tasks.capacity]
cpu_millis=1000
`
	applyManifest(t, st, manifest, 0, true, "capacity-1")
	_ = prepareAndActivateV1(t, st)
	if n, err := st.MarkExecutorUnknown(ctx, "e"); err != nil || n != 1 {
		t.Fatalf("mark unknown: %d %v", n, err)
	}
	if err := st.ReconcileResource(ctx, "n-cpu", true); err != nil {
		t.Fatalf("capacity-only reconcile: %v", err)
	}
	status, err := st.Status(ctx)
	if err != nil || status.Leases[0].State != "released" || status.Attempts[0].State != "quiesced" {
		t.Fatalf("capacity-only reconciliation incomplete: %+v %v", status, err)
	}
}
