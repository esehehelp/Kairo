package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// gangSegmentPrefix is where the tests' gangs' segments take their subnets.
const gangSegmentPrefix = "10.67.0.0/16"

// setupGangInventory: two nodes with one GPU each, a Windows one and a Linux
// one, both on the fabric and routing (where a gang's members may go), the
// daemon giving gangs segments of gangSegmentPrefix.
func setupGangInventory(t *testing.T, store *Store) {
	t.Helper()
	ctx := context.Background()
	store.policy.GangSegmentPrefix = gangSegmentPrefix
	for i, n := range []struct{ node, executor, attrs, gpu string }{
		{"node-a", "exec-a", `{"environment":"windows"}`, "gpu-a"},
		{"node-b", "exec-b", `{"environment":"linux"}`, "gpu-b"},
	} {
		if err := store.UpsertNode(ctx, Node{ID: n.node, Name: n.node, OS: "linux", Architecture: "amd64", Enabled: true}); err != nil {
			t.Fatal(err)
		}
		if _, err := store.SyncFabric(ctx, n.node, FabricPlan{Prefix: "10.66.0.0/16"}, FabricSelf{PublicKey: fabricKey(byte('a' + i)), ListenPort: 51820, Routing: true}); err != nil {
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
		declareTestGPU(t, store, n.gpu, n.node)
	}
	reachGangNodes(t, store, true)
	refreshGangGPUs(t, store)
}

// reachGangNodes has node-a and node-b report a handshake with each other
// now (they reach each other on the fabric), or their latest ten minutes
// ago (a partition).
func reachGangNodes(t *testing.T, store *Store, reach bool) {
	t.Helper()
	at := time.Now().UTC()
	if !reach {
		at = at.Add(-10 * time.Minute)
	}
	for _, pair := range [][2]string{{"node-a", "node-b"}, {"node-b", "node-a"}} {
		if err := store.RecordFabricLinks(context.Background(), pair[0], FabricLinksReport{Peers: []FabricPeerSeen{{Node: pair[1], Path: FabricPathDirect, HandshakeAt: at.Format(time.RFC3339Nano)}}}); err != nil {
			t.Fatal(err)
		}
	}
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
cwd = "/mnt/d/Dev/example-app"
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
cwd = "/srv/example-app"
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
	if a.Gang.MasterAddr != "10.67.0.1" || a.Gang.MasterPort != GangDefaultPort || b.Gang.MasterAddr != a.Gang.MasterAddr || a.Gang.Size != 2 {
		t.Fatalf("rendezvous: %+v / %+v", a.Gang, b.Gang)
	}
	// the generation's segment: a VNI of its own, an address each, on both
	// nodes
	if a.Gang.VNI != firstGangVNI || b.Gang.VNI != a.Gang.VNI || a.Gang.Subnet != "10.67.0.0/24" || a.Gang.MTU != DefaultSegmentMTU ||
		a.Gang.Address != "10.67.0.1/24" || b.Gang.Address != "10.67.0.2/24" || strings.Join(b.Gang.Addresses, ",") != "10.67.0.1/24,10.67.0.2/24" {
		t.Fatalf("segment: %+v / %+v", a.Gang, b.Gang)
	}
	segments, err := store.FabricSegments(ctx)
	if err != nil || len(segments) != 1 || segments[0].VNI != a.Gang.VNI || segments[0].Purpose != "gang" || strings.Join(segments[0].Nodes, ",") != "node-a,node-b" {
		t.Fatalf("segments: %+v %v", segments, err)
	}
	if err := store.DeleteFabricSegment(ctx, a.Gang.VNI); err == nil {
		t.Fatal("a gang's segment removed by hand")
	}
	if a.Argv[0] != "wsl-train" || b.Argv[0] != "linux-train" || b.CWD != "/srv/example-app" {
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
	if err := store.ReconcileGangs(ctx); err != nil {
		t.Fatal(err)
	}
	if segments, _ := store.FabricSegments(ctx); len(segments) != 1 {
		t.Fatalf("the segment went while a member runs: %+v", segments)
	}
	finishGangRank(t, store, rank0, 0)
	if task := gangTaskState(t, store); task.State != "succeeded" {
		t.Fatalf("task after both ranks exited 0: %+v", task)
	}
	// the generation ended: its segment goes
	if err := store.ReconcileGangs(ctx); err != nil {
		t.Fatal(err)
	}
	if segments, err := store.FabricSegments(ctx); err != nil || len(segments) != 0 {
		t.Fatalf("the segment of an ended generation: %+v %v", segments, err)
	}
}

// A gang's members go only to nodes that route on the fabric, and none
// without the daemon's segment prefix.
func TestGangWaitsForTheFabric(t *testing.T) {
	store := openCoordinationStore(t)
	setupGangInventory(t, store)
	gangProject(t, store, false)
	ctx := context.Background()
	wait := func(reason string) {
		t.Helper()
		if r, err := store.ReserveNext(ctx, "exec-a"); err != nil || r != nil {
			t.Fatalf("placed: %+v %v", r, err)
		}
		var got, message string
		if err := store.db.QueryRow(`SELECT w.reason_code,w.detail_json FROM admission_waits w JOIN gang_members m ON m.execution_id=w.execution_id WHERE m.rank=0`).Scan(&got, &message); err != nil || got != reason {
			t.Fatalf("wait %q %q (%v), want %s", got, message, err, reason)
		}
	}
	store.policy.GangSegmentPrefix = ""
	wait("no_fabric")
	store.policy.GangSegmentPrefix = gangSegmentPrefix
	if _, err := store.testExec(`UPDATE fabric_nodes SET routing=false WHERE node_id='node-b'`); err != nil {
		t.Fatal(err)
	}
	wait("no_executor")
	if _, err := store.testExec(`UPDATE fabric_nodes SET routing=true WHERE node_id='node-b'`); err != nil {
		t.Fatal(err)
	}
	// a partition: the nodes do not reach each other
	reachGangNodes(t, store, false)
	wait("fabric_partitioned")
	reachGangNodes(t, store, true)
	if r, err := store.ReserveNext(ctx, "exec-a"); err != nil || r == nil || r.Gang == nil {
		t.Fatalf("not placed on the fabric: %+v %v", r, err)
	}
}

// Each generation's segment has a VNI no earlier one had, and a subnet
// further on.
func TestEachGangGenerationHasANewSegment(t *testing.T) {
	store := openCoordinationStore(t)
	setupGangInventory(t, store)
	gangProject(t, store, false)
	ctx := context.Background()
	rb, err := store.ReserveNext(ctx, "exec-b")
	if err != nil || rb == nil || rb.Gang == nil {
		t.Fatalf("reserve: %+v %v", rb, err)
	}
	first, _, err := gangLaunchTx(ctx, store.db, rb.Lease.ID)
	if err != nil {
		t.Fatal(err)
	}
	// it is never prepared: aborted, and placed again
	if _, err = store.testExec(`UPDATE gang_placements SET created_at=$1`, time.Now().UTC().Add(-2*GangPrepareTimeout).Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	if err = store.ReconcileGangs(ctx); err != nil {
		t.Fatal(err)
	}
	if segments, _ := store.FabricSegments(ctx); len(segments) != 0 {
		t.Fatalf("an aborted generation's segment: %+v", segments)
	}
	refreshGangGPUs(t, store)
	if err = store.ReleaseReservation(ctx, rb.Lease.ID, rb.Lease.CoordinationEpoch, "gang aborted"); err != nil {
		t.Fatal(err)
	}
	ra, err := store.ReserveNext(ctx, "exec-a")
	if err != nil || ra == nil || ra.Gang == nil {
		t.Fatalf("placed again: %+v %v", ra, err)
	}
	second, _, err := gangLaunchTx(ctx, store.db, ra.Lease.ID)
	if err != nil {
		t.Fatal(err)
	}
	if second.VNI <= first.VNI || second.Subnet == first.Subnet || second.PlacementID == first.PlacementID {
		t.Fatalf("generations: %+v then %+v", first, second)
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

// An abort for a rank that was never prepared says, for each rank, where it
// waits, whether its lease was delivered and when its executor last polled; the
// daemon logs the same reason.
func TestGangAbortNamesEachRankExecutorDeliveryAndLastPoll(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
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
	// exec-b polls; exec-a never received its rank, so it has not polled.
	if _, err = store.testExec(`UPDATE executors SET last_seen_at=$1 WHERE id='exec-b'`, "2026-10-06T19:23:00Z"); err != nil {
		t.Fatal(err)
	}
	if _, err = store.testExec(`UPDATE gang_placements SET created_at=$1`, time.Now().UTC().Add(-2*GangPrepareTimeout).Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	if err = store.ReconcileGangs(ctx); err != nil {
		t.Fatal(err)
	}
	want := "not every rank was prepared within 2m0s: " +
		"rank 0 on executor exec-a: lease reserved, delivered=false, executor last_seen_at=none; " +
		"rank 1 on executor exec-b: lease prepared, delivered=true, executor last_seen_at=2026-10-06T19:23:00Z"
	var reason string
	if err = store.db.QueryRow(`SELECT abort_reason FROM gang_placements WHERE state='aborted'`).Scan(&reason); err != nil || reason != want {
		t.Fatalf("abort reason %q (%v), want %q", reason, err, want)
	}
	if !strings.Contains(logs.String(), "gang placement aborted") || !strings.Contains(logs.String(), want) {
		t.Fatalf("abort not logged with its reason: %s", logs.String())
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
	if _, err = store.testExec(`UPDATE gang_placements SET created_at=$1`, time.Now().UTC().Add(-2*GangPrepareTimeout).Format(time.RFC3339Nano)); err != nil {
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

// A gang runs in an image: every member's execution has it, and every
// member's selector (its own one too) asks for an executor that runs
// containers.
func TestAGangInAnImageRunsEveryMemberInAContainer(t *testing.T) {
	store := openCoordinationStore(t)
	ctx := context.Background()
	spec := schedulerExecutionSpec("gang-image")
	spec.Checkpointable, spec.Preemptible = false, false
	spec.Image = testImagePin
	spec.Gang = &GangSpec{Size: 2, Ranks: []GangRank{{Rank: 1, ExecutorSelector: json.RawMessage(`{"node":"node-b","container":false}`)}}}
	leader, _, err := store.SubmitExecution(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := store.db.Query(`SELECT m.rank,e.executor_selector_json,COALESCE(e.image_ref,'') FROM gang_members m JOIN execution_requests e ON e.id=m.execution_id
		JOIN execution_gangs g ON g.id=m.gang_id WHERE g.leader_execution_id=$1 ORDER BY m.rank`, leader.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	members := 0
	for rows.Next() {
		var rank int
		var selector, image string
		if err := rows.Scan(&rank, &selector, &image); err != nil {
			t.Fatal(err)
		}
		members++
		if image != testImagePin || !strings.Contains(selector, `"container":true`) || (rank == 1) != strings.Contains(selector, `"node":"node-b"`) {
			t.Fatalf("member %d: selector %s, image %q", rank, selector, image)
		}
	}
	if members != 2 {
		t.Fatalf("%d members", members)
	}
}
