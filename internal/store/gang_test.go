package store

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

// setupGangInventory: two nodes with one GPU each, a Windows (WSL) one whose
// interconnect address makes it a rank-0 host and a Linux one.
func setupGangInventory(t *testing.T, store *Store) {
	t.Helper()
	ctx := context.Background()
	for _, n := range []struct{ node, executor, attrs, gpu string }{
		{"node-a", "exec-a", `{"environment":"windows","interconnect_addr":"10.77.10.2"}`, "gpu-a"},
		{"node-b", "exec-b", `{"environment":"linux","interconnect_addr":"10.77.10.1"}`, "gpu-b"},
	} {
		if err := store.UpsertNode(ctx, Node{ID: n.node, Name: n.node, OS: "linux", Architecture: "amd64", Enabled: true}); err != nil {
			t.Fatal(err)
		}
		if err := store.UpsertExecutor(ctx, Executor{ID: n.executor, NodeID: n.node, Kind: "local", Attributes: json.RawMessage(n.attrs), Enabled: true}); err != nil {
			t.Fatal(err)
		}
		if err := store.UpsertProvider(ctx, n.gpu+"-provider", n.node, "nvidia", nil); err != nil {
			t.Fatal(err)
		}
		if err := store.UpsertResourceInstance(ctx, ResourceInstance{ID: n.gpu, NodeID: n.node, ProviderID: n.gpu + "-provider", Kind: "gpu", StableIdentity: n.gpu, Binding: json.RawMessage(`{"index":0}`), AdminState: "enabled"}); err != nil {
			t.Fatal(err)
		}
	}
	refreshGangGPUs(t, store)
}

func refreshGangGPUs(t *testing.T, store *Store) {
	t.Helper()
	time.Sleep(2 * time.Millisecond)
	total, free := int64(12<<30), int64(11<<30)
	observed := time.Now().UTC()
	for _, gpu := range []string{"gpu-a", "gpu-b"} {
		if err := store.RecordObservation(context.Background(), Observation{ResourceID: gpu, ObservedAt: observed.Format(time.RFC3339Nano), ValidUntil: observed.Add(time.Minute).Format(time.RFC3339Nano), TotalBytes: &total, FreeBytes: &free}); err != nil {
			t.Fatal(err)
		}
	}
}

// gangProject declares a two-rank task: rank 0 in WSL on node-a, rank 1 on
// Linux node-b with its own argv and cwd.
func gangProject(t *testing.T, store *Store, checkpointable bool) {
	t.Helper()
	cp := "false"
	if checkpointable {
		cp = "true"
	}
	parsed := projectSpec(t, `schema_version = 1
[project]
name = "gangs"
[[tasks]]
name = "ddp"
queue = "training"
depends_on = []
policy = "RunToCompletion@v1"
[tasks.execution]
schema_version = 2
argv = ["wsl-train", "--config", "x.toml"]
cwd = "/mnt/d/Dev/neo-ime"
checkpointable = `+cp+`
preemptible = `+cp+`
[tasks.execution.executor]
labels = { environment = "windows" }
[[tasks.execution.exclusive]]
kind = "gpu"
count = 1
[tasks.execution.gang]
size = 2
[[tasks.execution.gang.ranks]]
rank = 1
argv = ["linux-train", "--config", "x.toml"]
cwd = "/srv/neo-ime"
executor = { labels = { environment = "linux" } }
`)
	ctx := context.Background()
	if _, err := store.ApplyProject(ctx, parsed); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
}

type gangRankLaunch struct {
	executor string
	launch   *Launch
}

// launchGang reserves, prepares, authorizes and activates both ranks, rank 1's
// executor asking first.
func launchGang(t *testing.T, store *Store) (rank0, rank1 gangRankLaunch) {
	t.Helper()
	ctx := context.Background()
	rb, err := store.ReserveNext(ctx, "exec-b")
	if err != nil || rb == nil || rb.Gang == nil || rb.Gang.Rank != 1 {
		t.Fatalf("exec-b reservation: %+v %v", rb, err)
	}
	if err = store.MarkLeasePrepared(ctx, rb.Lease.ID, rb.Lease.CoordinationEpoch); err != nil {
		t.Fatal(err)
	}
	if _, err = store.AuthorizeLaunch(ctx, rb); !errors.Is(err, ErrGangNotReady) {
		t.Fatalf("rank 1 launched before rank 0 was prepared: %v", err)
	}
	ra, err := store.ReserveNext(ctx, "exec-a")
	if err != nil || ra == nil || ra.Gang == nil || ra.Gang.Rank != 0 {
		t.Fatalf("exec-a reservation: %+v %v", ra, err)
	}
	if err = store.MarkLeasePrepared(ctx, ra.Lease.ID, ra.Lease.CoordinationEpoch); err != nil {
		t.Fatal(err)
	}
	for i, r := range []struct {
		executor    string
		reservation *Reservation
		pid         int
	}{{"exec-a", ra, 200}, {"exec-b", rb, 300}} {
		launch, err := store.AuthorizeLaunch(ctx, r.reservation)
		if err != nil {
			t.Fatalf("authorize rank %d: %v", i, err)
		}
		if err = store.ActivateLaunch(ctx, launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch, launch.AuthorizationToken, r.pid, "launcher"); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			rank0 = gangRankLaunch{r.executor, launch}
		} else {
			rank1 = gangRankLaunch{r.executor, launch}
		}
	}
	return rank0, rank1
}

func finishGangRank(t *testing.T, store *Store, r gangRankLaunch, exitCode int) {
	t.Helper()
	ctx := context.Background()
	epoch := r.launch.Lease.CoordinationEpoch
	if err := store.RecordTerminal(ctx, r.launch.Attempt.ID, r.launch.Lease.ID, epoch, exitCode, ""); err != nil {
		t.Fatal(err)
	}
	refreshGangGPUs(t, store)
	if err := store.FinalizeQuiescence(ctx, r.launch.Attempt.ID, r.launch.Lease.ID, epoch); err != nil {
		t.Fatal(err)
	}
}

func gangTaskState(t *testing.T, store *Store) TaskStatus {
	t.Helper()
	ctx := context.Background()
	if err := store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	project, err := store.GetProjectStatus(ctx, "gangs")
	if err != nil {
		t.Fatal(err)
	}
	return project.Tasks[0]
}

func TestGangPlacesRanksOnDistinctNodesAndLaunchesThemTogether(t *testing.T) {
	store := openCoordinationStore(t)
	setupGangInventory(t, store)
	gangProject(t, store, false)
	ctx := context.Background()
	rank0, rank1 := launchGang(t, store)
	a, b := rank0.launch, rank1.launch
	if a.Gang.MasterAddr != "10.77.10.2" || a.Gang.MasterPort != gangPortFirst || b.Gang.MasterAddr != a.Gang.MasterAddr || a.Gang.Size != 2 {
		t.Fatalf("rendezvous: %+v / %+v", a.Gang, b.Gang)
	}
	if a.Argv[0] != "wsl-train" || b.Argv[0] != "linux-train" || b.CWD != "/srv/neo-ime" {
		t.Fatalf("rank overrides: %v %s / %v %s", a.Argv, a.CWD, b.Argv, b.CWD)
	}
	if a.Resources[0].ID != "gpu-a" || b.Resources[0].ID != "gpu-b" {
		t.Fatalf("resources: %+v / %+v", a.Resources, b.Resources)
	}
	// nothing more to hand out, and the running gang stays as it is
	if r, err := store.ReserveNext(ctx, "exec-b"); err != nil || r != nil {
		t.Fatalf("second delivery: %+v %v", r, err)
	}
	if err := store.ReconcileGangs(ctx); err != nil {
		t.Fatal(err)
	}
	if terms, _ := store.ListQuarantineTerminations(ctx, "exec-a"); len(terms) != 0 {
		t.Fatalf("healthy gang terminated: %+v", terms)
	}
	if task := gangTaskState(t, store); task.State != "running" {
		t.Fatalf("task: %+v", task)
	}
	finishGangRank(t, store, rank1, 0)
	if task := gangTaskState(t, store); task.State != "running" {
		t.Fatalf("task finished with one rank still running: %+v", task)
	}
	finishGangRank(t, store, rank0, 0)
	if task := gangTaskState(t, store); task.State != "succeeded" {
		t.Fatalf("task after both ranks exited 0: %+v", task)
	}
}

func TestGangRankFailureTerminatesTheOthersAndFailsTheTask(t *testing.T) {
	store := openCoordinationStore(t)
	setupGangInventory(t, store)
	gangProject(t, store, false)
	ctx := context.Background()
	rank0, rank1 := launchGang(t, store)
	finishGangRank(t, store, rank1, 1)
	if err := store.ReconcileGangs(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcileGangs(ctx); err != nil { // idempotent
		t.Fatal(err)
	}
	terms, err := store.ListQuarantineTerminations(ctx, "exec-a")
	if err != nil || len(terms) != 1 || terms[0].AttemptID != rank0.launch.Attempt.ID {
		t.Fatalf("rank 0 not terminated: %+v %v", terms, err)
	}
	if err = store.MarkQuarantineTerminated(ctx, rank0.launch.Attempt.ID); err != nil {
		t.Fatal(err)
	}
	finishGangRank(t, store, rank0, 137)
	if task := gangTaskState(t, store); task.State != "failed" {
		t.Fatalf("task: %+v", task)
	}
}

func TestGangPlacementNotPreparedInTimeIsAbortedAndPlacedAgain(t *testing.T) {
	store := openCoordinationStore(t)
	setupGangInventory(t, store)
	gangProject(t, store, false)
	ctx := context.Background()
	rb, err := store.ReserveNext(ctx, "exec-b")
	if err != nil || rb == nil {
		t.Fatalf("reserve: %+v %v", rb, err)
	}
	if err = store.MarkLeasePrepared(ctx, rb.Lease.ID, rb.Lease.CoordinationEpoch); err != nil {
		t.Fatal(err)
	}
	// exec-a never shows up
	if _, err = store.db.Exec(`UPDATE gang_placements SET created_at=?`, time.Now().UTC().Add(-2*GangPrepareTimeout).Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	if err = store.ReconcileGangs(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = store.AuthorizeLaunch(ctx, rb); !errors.Is(err, ErrGangAborted) {
		t.Fatalf("aborted placement launched: %v", err)
	}
	refreshGangGPUs(t, store)
	if err = store.ReleaseReservation(ctx, rb.Lease.ID, rb.Lease.CoordinationEpoch, "gang aborted"); err != nil {
		t.Fatal(err)
	}
	var waiting int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM execution_requests WHERE state='waiting'`).Scan(&waiting); err != nil || waiting != 2 {
		t.Fatalf("ranks not waiting again: %d %v", waiting, err)
	}
	if r, err := store.ReserveNext(ctx, "exec-a"); err != nil || r == nil || r.Gang == nil || r.Gang.Rank != 0 {
		t.Fatalf("gang not placed again: %+v %v", r, err)
	}
}

func TestGangSuspendOfOneRankReachesEveryRank(t *testing.T) {
	store := openCoordinationStore(t)
	setupGangInventory(t, store)
	gangProject(t, store, true)
	ctx := context.Background()
	rank0, rank1 := launchGang(t, store)
	if _, err := store.EnqueueSuspend(ctx, rank1.launch.Attempt.ID, "node_quarantine", "node node-b quarantined"); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcileGangs(ctx); err != nil {
		t.Fatal(err)
	}
	commands, err := store.PollCommands(ctx, rank0.launch.Attempt.ID, rank0.launch.Lease.ID, rank0.launch.Lease.CoordinationEpoch)
	if err != nil || len(commands) != 1 || commands[0].Origin != "node_quarantine" {
		t.Fatalf("rank 0 commands: %+v %v", commands, err)
	}
	if terms, _ := store.ListQuarantineTerminations(ctx, "exec-a"); len(terms) != 0 {
		t.Fatalf("suspending gang terminated: %+v", terms)
	}
}

func TestGangSpecIsValidatedAndDigestOnlyChangesWithAGang(t *testing.T) {
	base := schedulerExecutionSpec("digest")
	_, _, plain, err := normalizeExecutionSpec(base)
	if err != nil {
		t.Fatal(err)
	}
	withGang := base
	withGang.Gang = &GangSpec{Size: 2}
	_, _, ganged, err := normalizeExecutionSpec(withGang)
	if err != nil || ganged == plain {
		t.Fatalf("gang digest: %s %s %v", plain, ganged, err)
	}
	for _, bad := range []GangSpec{{Size: 1}, {Size: 2, Ranks: []GangRank{{Rank: 2}}}, {Size: 2, Ranks: []GangRank{{Rank: 1}, {Rank: 1}}}, {Size: 2, Port: 80}} {
		spec := base
		spec.Gang = &bad
		if _, _, _, err := normalizeExecutionSpec(spec); err == nil {
			t.Fatalf("accepted invalid gang %+v", bad)
		}
	}
}
