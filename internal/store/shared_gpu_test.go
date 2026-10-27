package store

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"kairo/internal/machine"
)

// setupSharedGPU: nodes "a" and "b" (two containers of one host), each with an
// executor (labels site=a / site=b) and an nvidia provider, both declared for
// the one GPU "GPU-S", and both providers reporting it.
func setupSharedGPU(t *testing.T, store *Store) {
	t.Helper()
	ctx := context.Background()
	setupTwoGPUNodes(t, store)
	for _, node := range []string{"a", "b"} {
		if err := store.UpsertExecutor(ctx, Executor{ID: "exec-" + node, NodeID: node, Kind: "linux", Attributes: json.RawMessage(`{"site":"` + node + `"}`), Enabled: true}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.ApplyMachines(ctx, machine.Spec{Nodes: []machine.Node{{ID: "a", GPUs: []string{"GPU-S"}}, {ID: "b", GPUs: []string{"GPU-S"}}}}, false); err != nil {
		t.Fatal(err)
	}
	pollShared(t, store, false, false)
}

// pollShared: both nodes' providers report GPU-S, fresh (with an
// unattributed-activity claim where aClaim / bClaim).
func pollShared(t *testing.T, store *Store, aClaim, bClaim bool) {
	t.Helper()
	time.Sleep(2 * time.Millisecond) // (newer than the last coordination transition)
	if err := gpuPoll(t, store, "a-gpu", "a", "GPU-S", aClaim); err != nil {
		t.Fatal(err)
	}
	if err := gpuPoll(t, store, "b-gpu", "b", "GPU-S", bClaim); err != nil {
		t.Fatal(err)
	}
}

func sharedSpec(requestID, site string, priority int) ExecutionSpec {
	spec := schedulerExecutionSpec(requestID)
	spec.ExecutorSelector = json.RawMessage(`{"labels":{"site":"` + site + `"}}`)
	spec.Priority = priority
	return spec
}

func startShared(t *testing.T, store *Store, requestID, site string, priority int) *Launch {
	t.Helper()
	ctx := context.Background()
	if _, _, err := store.SubmitExecution(ctx, sharedSpec(requestID, site, priority)); err != nil {
		t.Fatal(err)
	}
	reservation, err := store.ReserveNext(ctx, "exec-"+site)
	if err != nil || reservation == nil {
		t.Fatalf("reserve on %s: %+v %v", site, reservation, err)
	}
	if err = store.MarkLeasePrepared(ctx, reservation.Lease.ID, reservation.Lease.CoordinationEpoch); err != nil {
		t.Fatal(err)
	}
	launch, err := store.AuthorizeLaunch(ctx, reservation)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.ActivateLaunch(ctx, launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch, launch.AuthorizationToken, 4321, site+":4321:start:1"); err != nil {
		t.Fatal(err)
	}
	return launch
}

func openClaims(t *testing.T, store *Store) []ExternalClaim {
	t.Helper()
	claims, err := store.ListClaims(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return claims
}

func TestSharedGPUHasARowOnEachOfItsNodes(t *testing.T) {
	store := openCoordinationStore(t)
	setupSharedGPU(t, store)
	ctx := context.Background()
	a, b := resourceByID(t, store, "gpu-GPU-S"), resourceByID(t, store, "gpu-GPU-S@b")
	if a.NodeID != "a" || a.ProviderID != "a-gpu" || a.DerivedState != "available" || a.DeclaredNodeID == nil || *a.DeclaredNodeID != "a" {
		t.Fatalf("a's row: %+v", a)
	}
	if b.NodeID != "b" || b.ProviderID != "b-gpu" || b.StableIdentity != "GPU-S" || b.DerivedState != "available" || b.DeclaredNodeID == nil || *b.DeclaredNodeID != "b" {
		t.Fatalf("b's row: %+v", b)
	}
	machines, err := store.ListMachines(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(machines.GPUs) != 2 || len(machines.Undeclared) != 0 {
		t.Fatalf("machines: %+v", machines)
	}
	for _, gpu := range machines.GPUs {
		if gpu.State != "observed" || gpu.ResourceID == nil || (gpu.NodeID == "a") != (*gpu.ResourceID == "gpu-GPU-S") {
			t.Fatalf("declaration: %+v", gpu)
		}
	}
	// b's claim is filed under its own row and name; it blocks the device on both nodes
	pollShared(t, store, false, true)
	claims := openClaims(t, store)
	if len(claims) != 1 || claims[0].ResourceID != "gpu-GPU-S@b" || !strings.HasSuffix(claims[0].ID, "@b") {
		t.Fatalf("claims: %+v", claims)
	}
	if r := resourceByID(t, store, "gpu-GPU-S"); r.DerivedState != "conflicted" {
		t.Fatalf("a's row with b's claim: %+v", r)
	}
	pollShared(t, store, false, false)
	if claims = openClaims(t, store); len(claims) != 0 {
		t.Fatalf("b's claim was not cleared: %+v", claims)
	}
	// a third poll keeps the same two rows
	pollShared(t, store, false, false)
	if all, _ := store.ListResourceStatus(ctx); len(all) != 2 {
		t.Fatalf("rows: %+v", all)
	}
}

func TestALeaseOnASharedGPUExcludesItsOtherNodes(t *testing.T) {
	store := openCoordinationStore(t)
	setupSharedGPU(t, store)
	ctx := context.Background()
	if _, _, err := store.SubmitExecution(ctx, sharedSpec("on-a", "a", 10)); err != nil {
		t.Fatal(err)
	}
	first, err := store.ReserveNext(ctx, "exec-a")
	if err != nil || first == nil || first.Resources[0].ID != "gpu-GPU-S" {
		t.Fatalf("reserve on a: %+v %v", first, err)
	}
	if _, _, err = store.SubmitExecution(ctx, sharedSpec("on-b", "b", 10)); err != nil {
		t.Fatal(err)
	}
	if second, err := store.ReserveNext(ctx, "exec-b"); err != nil || second != nil {
		t.Fatalf("b reserved a device a holds: %+v %v", second, err)
	}
	if r := resourceByID(t, store, "gpu-GPU-S@b"); r.DerivedState != "leased" {
		t.Fatalf("b's row while a holds the device: %+v", r)
	}
	pollShared(t, store, false, false)
	if err = store.ReleaseReservation(ctx, first.Lease.ID, first.Lease.CoordinationEpoch, "test"); err != nil {
		t.Fatal(err)
	}
	pollShared(t, store, false, false)
	second, err := store.ReserveNext(ctx, "exec-b")
	if err != nil || second == nil || second.Resources[0].ID != "gpu-GPU-S@b" || second.Resources[0].NodeID != "b" {
		t.Fatalf("reserve on b once a let go: %+v %v", second, err)
	}
}

func TestActivityTheOtherNodeCannotSeeIsTheLeaseHolders(t *testing.T) {
	store := openCoordinationStore(t)
	setupSharedGPU(t, store)
	ctx := context.Background()
	// unleased: activity no process explains blocks the device on both nodes
	pollShared(t, store, false, true)
	if _, _, err := store.SubmitExecution(ctx, sharedSpec("on-a", "a", 10)); err != nil {
		t.Fatal(err)
	}
	if reservation, err := store.ReserveNext(ctx, "exec-a"); err != nil || reservation != nil {
		t.Fatalf("a reserved a device with activity b reports: %+v %v", reservation, err)
	}
	pollShared(t, store, false, false)
	launch := startPrepared(t, store, "exec-a")
	// a holds it: what b sees there without a process is a's work
	pollShared(t, store, false, true)
	if claims := openClaims(t, store); len(claims) != 0 {
		t.Fatalf("a's own work was filed as b's claim: %+v", claims)
	}
	// a's own provider seeing activity without a process on the device a holds is still a claim
	pollShared(t, store, true, false)
	if claims := openClaims(t, store); len(claims) != 1 || claims[0].ResourceID != "gpu-GPU-S" {
		t.Fatalf("claims on a's own lease: %+v", claims)
	}
	_ = launch
}

// startPrepared: the waiting execution reserved on executor, prepared, authorized and active
func startPrepared(t *testing.T, store *Store, executorID string) *Launch {
	t.Helper()
	ctx := context.Background()
	reservation, err := store.ReserveNext(ctx, executorID)
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
	if err = store.ActivateLaunch(ctx, launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch, launch.AuthorizationToken, 4321, "x:4321:start:1"); err != nil {
		t.Fatal(err)
	}
	return launch
}

func TestPreemptionReachesAVictimOnAnotherNodeOfASharedGPU(t *testing.T) {
	store := openCoordinationStore(t)
	setupSharedGPU(t, store)
	ctx := context.Background()
	victim := startShared(t, store, "victim-on-b", "b", -100)
	pollShared(t, store, true, false) // (b's work, as a sees it: dropped)
	if _, _, err := store.SubmitExecution(ctx, sharedSpec("waiter-on-a", "a", 10)); err != nil {
		t.Fatal(err)
	}
	if reservation, err := store.ReserveNext(ctx, "exec-a"); err != nil || reservation != nil {
		t.Fatalf("a reserved the device b holds: %+v %v", reservation, err)
	}
	commands, err := store.EnsurePriorityPreemption(ctx, "exec-a")
	if err != nil || len(commands) != 1 {
		t.Fatalf("preemption commands=%v err=%v", commands, err)
	}
	var attemptID, origin string
	if err = store.db.QueryRow(`SELECT attempt_id,origin FROM commands WHERE id=?`, commands[0]).Scan(&attemptID, &origin); err != nil {
		t.Fatal(err)
	}
	if attemptID != victim.Attempt.ID || origin != "priority_preemption" {
		t.Fatalf("suspend for %s (%s), want %s", attemptID, origin, victim.Attempt.ID)
	}
	// a higher-priority victim stays
	store2 := openCoordinationStore(t)
	setupSharedGPU(t, store2)
	startShared(t, store2, "victim-on-b", "b", 50)
	if _, _, err := store2.SubmitExecution(ctx, sharedSpec("waiter-on-a", "a", 10)); err != nil {
		t.Fatal(err)
	}
	if commands, err = store2.EnsurePriorityPreemption(ctx, "exec-a"); err != nil || len(commands) != 0 {
		t.Fatalf("preempted a higher-priority execution: %v %v", commands, err)
	}
}

func TestMachineApplyKeepsALeasedSharedGPUOnItsNode(t *testing.T) {
	store := openCoordinationStore(t)
	setupSharedGPU(t, store)
	ctx := context.Background()
	startShared(t, store, "on-b", "b", 10)
	if _, err := store.ApplyMachines(ctx, machine.Spec{Nodes: []machine.Node{{ID: "a", GPUs: []string{"GPU-S"}}, {ID: "b"}}}, false); !errors.Is(err, ErrMachineConflict) {
		t.Fatalf("took a leased GPU from its node: %v", err)
	}
	result, err := store.ApplyMachines(ctx, machine.Spec{Nodes: []machine.Node{{ID: "a"}, {ID: "b", GPUs: []string{"GPU-S"}}}}, false)
	if err != nil || len(result.Changes) != 1 || result.Changes[0] != (MachineChange{UUID: "GPU-S", From: "a,b", To: "b"}) {
		t.Fatalf("dropping the unleased node: %+v %v", result, err)
	}
	if r := resourceByID(t, store, "gpu-GPU-S"); r.DerivedState != "leased" {
		// (a's row is no longer declared; b's lease on the device still shows)
		t.Fatalf("a's row: %+v", r)
	}
	machines, _ := store.ListMachines(ctx)
	if len(machines.GPUs) != 1 || machines.GPUs[0].NodeID != "b" || machines.GPUs[0].State != "observed" {
		t.Fatalf("machines: %+v", machines)
	}
}

func TestOpenWidensTheMachineDeclarationKey(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kairo.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	setupSchedulerInventory(t, store)
	// a table from before shared GPUs: uuid alone is the key
	for _, statement := range []string{
		`DROP TABLE machine_gpus`,
		`CREATE TABLE machine_gpus(uuid TEXT PRIMARY KEY, node_id TEXT NOT NULL REFERENCES nodes(id), declared_at TEXT NOT NULL)`,
		`INSERT INTO machine_gpus(uuid,node_id,declared_at) VALUES('GPU-0','node','2026-01-01T00:00:00Z')`,
	} {
		if _, err = store.db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	store.Close()
	if store, err = Open(path); err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var ddl string
	if err = store.db.QueryRow(`SELECT sql FROM sqlite_master WHERE type='table' AND name='machine_gpus'`).Scan(&ddl); err != nil || !strings.Contains(ddl, "PRIMARY KEY(uuid,node_id)") {
		t.Fatalf("key not widened: %q %v", ddl, err)
	}
	machines, err := store.ListMachines(context.Background())
	if err != nil || len(machines.GPUs) != 1 || machines.GPUs[0].UUID != "GPU-0" || machines.GPUs[0].NodeID != "node" {
		t.Fatalf("declaration kept: %+v %v", machines, err)
	}
	if err = store.UpsertNode(context.Background(), Node{ID: "other", Name: "other", OS: "linux", Architecture: "amd64", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if _, err = store.ApplyMachines(context.Background(), machine.Spec{Nodes: []machine.Node{{ID: "node", GPUs: []string{"GPU-0"}}, {ID: "other", GPUs: []string{"GPU-0"}}}}, false); err != nil {
		t.Fatalf("shared declaration after the widening: %v", err)
	}
}
