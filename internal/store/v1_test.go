package store

import (
	"context"
	"encoding/json"
	"errors"
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
