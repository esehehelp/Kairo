package store

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"kairo/internal/machine"
)

// landingFixture is two nodes running the project moves' work: node (its
// executor, GPU-0, the volatile store a-scratch at D:/kairo) and node2
// (exec2, the volatile store b-scratch at /srv/kairo-store; no GPU until
// addNode2GPU), each agent with a pack server. moves has one task, train,
// on a GPU, checkpointable.
func landingFixture(t *testing.T) *Store {
	t.Helper()
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	ctx := context.Background()
	if err := store.UpsertExecutor(ctx, Executor{ID: "executor", NodeID: "node", Kind: "local", Attributes: json.RawMessage(`{"environment":"wsl2","pack_addr":"192.0.2.1:7475"}`), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertNode(ctx, Node{ID: "node2", Name: "node2", OS: "linux", Architecture: "amd64", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertExecutor(ctx, Executor{ID: "exec2", NodeID: "node2", Kind: "local", Attributes: json.RawMessage(`{"environment":"wsl2","pack_addr":"192.0.2.2:7475"}`), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	addTestDisk(t, store, "node", "D:")
	addTestDisk(t, store, "node2", "/")
	applyLandingMachines(t, store, false)
	if _, err := store.ApplyProject(ctx, projectSpec(t, `schema_version = 1
[project]
name = "moves"
[[tasks]]
name = "train"
queue = "training"
depends_on = []
policy = "RunToCompletion@v1"
[tasks.execution]
schema_version = 2
argv = ["python", "train.py"]
cwd = "/work"
checkpointable = true
preemptible = true
[tasks.execution.executor]
labels = { environment = "wsl2" }
[[tasks.execution.exclusive]]
kind = "gpu"
count = 1
`)); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	return store
}

// applyLandingMachines declares the fixture's stores, and GPU-1 on node2
// with node2GPU.
func applyLandingMachines(t *testing.T, store *Store, node2GPU bool) {
	t.Helper()
	spec := storesSpec(
		machine.Store{ID: "a-scratch", Class: machine.StoreVolatile, Node: "node", Root: "D:/kairo", Filesystem: "D:"},
		machine.Store{ID: "b-scratch", Class: machine.StoreVolatile, Node: "node2", Root: "/srv/kairo-store", Filesystem: "/"},
	)
	node2 := machine.Node{ID: "node2"}
	if node2GPU {
		node2.GPUs = []string{"GPU-1"}
	}
	spec.Nodes = append(spec.Nodes, node2)
	if _, err := store.ApplyMachines(context.Background(), spec, false); err != nil {
		t.Fatal(err)
	}
}

// addNode2GPU gives node2 GPU-1, free and observed now.
func addNode2GPU(t *testing.T, store *Store) {
	t.Helper()
	ctx := context.Background()
	if err := store.UpsertProvider(ctx, "gpu-provider-2", "node2", "nvidia", nil); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertResourceInstance(ctx, ResourceInstance{ID: "gpu-1", NodeID: "node2", ProviderID: "gpu-provider-2", Kind: "gpu", StableIdentity: "GPU-1", Binding: json.RawMessage(`{"index":0}`), AdminState: "enabled"}); err != nil {
		t.Fatal(err)
	}
	applyLandingMachines(t, store, true)
	observeGPU(t, store, "gpu-1")
}

func observeGPU(t *testing.T, store *Store, resourceID string) {
	t.Helper()
	time.Sleep(2 * time.Millisecond)
	total, free := int64(24<<30), int64(23<<30)
	observed := time.Now().UTC()
	if err := store.RecordObservation(context.Background(), Observation{ResourceID: resourceID, ObservedAt: observed.Format(time.RFC3339Nano), ValidUntil: observed.Add(time.Minute).Format(time.RFC3339Nano), TotalBytes: &total, FreeBytes: &free}); err != nil {
		t.Fatal(err)
	}
}

// reserveOn reserves on executorID the execution the pass places there and
// authorizes its launch (nil when the pass places nothing there).
func reserveOn(t *testing.T, store *Store, executorID string) *Launch {
	t.Helper()
	ctx := context.Background()
	reservation, err := store.ReserveNext(ctx, executorID)
	if err != nil {
		t.Fatal(err)
	}
	if reservation == nil {
		return nil
	}
	if err = store.MarkLeasePrepared(ctx, reservation.Lease.ID, reservation.Lease.CoordinationEpoch); err != nil {
		t.Fatal(err)
	}
	launch, err := store.AuthorizeLaunch(ctx, reservation)
	if err != nil {
		t.Fatal(err)
	}
	return launch
}

// suspendWith runs launch, has it suspend publishing ref, and quiesces it.
func suspendWith(t *testing.T, store *Store, launch *Launch, ref string) {
	t.Helper()
	ctx := context.Background()
	epoch := launch.Lease.CoordinationEpoch
	if err := store.ActivateLaunch(ctx, launch.Attempt.ID, launch.Lease.ID, epoch, launch.AuthorizationToken, 100, "launcher:100"); err != nil {
		t.Fatal(err)
	}
	commandID, err := store.EnqueueSuspend(ctx, launch.Attempt.ID, "priority_preemption", "test")
	if err != nil {
		t.Fatal(err)
	}
	for _, phase := range []string{"accepted", "checkpointing"} {
		if err = store.AckCommand(ctx, launch.Attempt.ID, launch.Lease.ID, epoch, commandID, phase, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err = store.AckCommand(ctx, launch.Attempt.ID, launch.Lease.ID, epoch, commandID, "checkpointed", json.RawMessage(`{"continuation_ref":"`+ref+`"}`)); err != nil {
		t.Fatal(err)
	}
	if err = store.RecordTerminal(ctx, launch.Attempt.ID, launch.Lease.ID, epoch, 75, ""); err != nil {
		t.Fatal(err)
	}
	observeGPU(t, store, "gpu-0")
	if err = store.FinalizeQuiescence(ctx, launch.Attempt.ID, launch.Lease.ID, epoch); err != nil {
		t.Fatal(err)
	}
}

func waitOf(t *testing.T, store *Store, executionID string) (string, string) {
	t.Helper()
	var reason, detail string
	if err := store.db.QueryRow(`SELECT reason_code,detail_json FROM admission_waits WHERE execution_id=$1`, executionID).Scan(&reason, &detail); err != nil {
		t.Fatalf("wait of %s: %v", executionID, err)
	}
	return reason, detail
}

func landingState(t *testing.T, store *Store, executionID string) (state, node, storeID, objectID string) {
	t.Helper()
	var s, o *string
	if err := store.db.QueryRow(`SELECT state,node_id,store_id,object_id FROM output_landings WHERE execution_id=$1`, executionID).Scan(&state, &node, &s, &o); err != nil {
		t.Fatalf("landing of %s: %v", executionID, err)
	}
	if s != nil {
		storeID = *s
	}
	if o != nil {
		objectID = *o
	}
	return state, node, storeID, objectID
}

func runOutputRow(t *testing.T, store *Store, runID, storeID string) (state string, attempt *string) {
	t.Helper()
	if err := store.db.QueryRow(`SELECT state,last_attempt_id FROM run_outputs WHERE run_id=$1 AND store_id=$2`, runID, storeID).Scan(&state, &attempt); err != nil {
		t.Fatalf("run output %s on %s: %v", runID, storeID, err)
	}
	return state, attempt
}

// A continuation whose run's node is taken (by work no estimate says the end
// of) and that fits now on another node is sent there: its run's output
// directory is packed where it is, brought by the other node's agent from
// that agent's pack server and put in place as the run's directory there;
// the continuation runs there with it, and the directory it left is
// removed with the pack.
func TestAContinuationMovesWithItsRunsOutputToWhereItEndsSooner(t *testing.T) {
	store := landingFixture(t)
	ctx := context.Background()
	first := reserveOn(t, store, "executor")
	if first == nil {
		t.Fatal("train not placed on node")
	}
	runID := first.RunID
	if err := store.RecordRunOutput(ctx, RunOutput{AttemptID: first.Attempt.ID, LeaseID: first.Lease.ID, Epoch: first.Lease.CoordinationEpoch, RunID: runID,
		StoreID: "a-scratch", ExecutorID: "executor", Root: "D:/kairo", Path: "D:/kairo/outputs/" + runID}); err != nil {
		t.Fatal(err)
	}
	suspendWith(t, store, first, "/kairo/output/ckpt/step-100")
	// other work takes node's GPU before the continuation is planned
	hog := schedulerExecutionSpec("hog")
	hog.Scope = ScopePath{Project: "hogs", Queue: "q", Task: "hog"}
	hogExecution, _, err := store.SubmitExecution(ctx, hog)
	if err != nil {
		t.Fatal(err)
	}
	if held := reserveOn(t, store, "executor"); held == nil || held.Execution.ID != hogExecution.ID {
		t.Fatalf("hog not placed on node: %+v", held)
	}
	addNode2GPU(t, store)
	if err := store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	if got := reserveOn(t, store, "exec2"); got != nil {
		t.Fatalf("the continuation launched on node2 before its run's directory: %+v", got.Execution.ID)
	}
	var continuation string
	if err := store.db.QueryRow(`SELECT id FROM execution_requests WHERE state='waiting' AND input_continuation_ref='/kairo/output/ckpt/step-100'`).Scan(&continuation); err != nil {
		t.Fatal(err)
	}
	if reason, detail := waitOf(t, store, continuation); reason != "output_moving" || !strings.Contains(detail, "from node to node2") {
		t.Fatalf("wait %s %s", reason, detail)
	}
	state, node, storeID, objectID := landingState(t, store, continuation)
	if state != landingPacking || node != "node2" || storeID != "b-scratch" || objectID == "" {
		t.Fatalf("landing %s %s %s %s", state, node, storeID, objectID)
	}
	var estimate string
	if err := store.db.QueryRow(`SELECT estimate_json FROM output_landings WHERE execution_id=$1`, continuation).Scan(&estimate); err != nil || !strings.Contains(estimate, `"to":"node2"`) {
		t.Fatalf("estimate %s %v", estimate, err)
	}
	// the planner does not order it twice
	if got := reserveOn(t, store, "exec2"); got != nil {
		t.Fatal("launched while its directory is packed")
	}
	var moves int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM data_objects WHERE kind='output-move'`).Scan(&moves); err != nil || moves != 1 {
		t.Fatalf("%d moves %v", moves, err)
	}

	// packed where it is, whole
	orders, err := store.PacksToMake(ctx, "executor")
	if err != nil {
		t.Fatal(err)
	}
	var move *PackOrder
	for i := range orders {
		if orders[i].Kind == ObjectOutputMove {
			move = &orders[i]
		}
	}
	if move == nil || move.ObjectID != objectID || move.SourceID != runID || move.Identity || move.Path != "D:/kairo/outputs/"+runID {
		t.Fatalf("pack orders %+v", orders)
	}
	packSHA, listSHA := strings.Repeat("ab", 32), strings.Repeat("cd", 32)
	if err = store.RecordPack(ctx, PackMade{ObjectID: objectID, ExecutorID: "executor", PackPath: "D:/kairo/packs/" + objectID + ".tar.zst", ManifestPath: "D:/kairo/packs/" + objectID + ".tar.zst.files.tsv",
		PackBytes: 10, ContentBytes: 20, Files: 2, PackSHA256: packSHA, ManifestSHA256: listSHA}); err != nil {
		t.Fatal(err)
	}
	if copies, err := store.CopiesToMake(ctx, "executor"); err != nil || len(copies) != 0 {
		t.Fatalf("a move copied to a durable store: %+v %v", copies, err)
	}
	// brought by node2's agent from node's
	fetches, err := store.FetchesToMake(ctx, "exec2")
	if err != nil || len(fetches) != 1 {
		t.Fatalf("fetches %+v %v", fetches, err)
	}
	f := fetches[0]
	if f.RunID != runID || f.ObjectID != objectID || f.StoreID != "b-scratch" || f.Source.Addr != "192.0.2.1:7475" || f.Source.Token == "" || f.PackSHA256 != packSHA {
		t.Fatalf("fetch %+v", f)
	}
	if read, err := store.AuthorizePackRead(ctx, "node", objectID, f.Source.Token); err != nil || read.StoreID != "a-scratch" || read.Root != "D:/kairo" {
		t.Fatalf("read %+v %v", read, err)
	}
	if _, err := store.AuthorizePackRead(ctx, "node", objectID, "wrong"); err == nil {
		t.Fatal("a wrong token read the pack")
	}
	if err = store.RecordFetch(ctx, FetchMade{ObjectID: objectID, StoreID: "b-scratch", ExecutorID: "exec2", Path: "/srv/kairo-store/inputs/" + objectID, Bytes: 20, RunID: runID}); err == nil {
		t.Fatal("a run's directory recorded outside <root>/outputs")
	}
	if err = store.RecordFetch(ctx, FetchMade{ObjectID: objectID, StoreID: "b-scratch", ExecutorID: "exec2", Path: "/srv/kairo-store/outputs/" + runID, Bytes: 20, RunID: runID}); err != nil {
		t.Fatal(err)
	}
	if state, attempt := runOutputRow(t, store, runID, "b-scratch"); state != "active" || attempt != nil {
		t.Fatalf("landed: %s %v", state, attempt)
	}
	if state, _ := runOutputRow(t, store, runID, "a-scratch"); state != "moved" {
		t.Fatalf("left behind: %s", state)
	}
	if state, _, _, _ := landingState(t, store, continuation); state != landingPlaced {
		t.Fatalf("landing %s", state)
	}

	// it runs on node2, on the store its directory is on
	if got := reserveOn(t, store, "executor"); got != nil {
		t.Fatalf("placed on node: %s", got.Execution.ID)
	}
	observeGPU(t, store, "gpu-1")
	next := reserveOn(t, store, "exec2")
	if next == nil || next.Execution.ID != continuation || next.RunID != runID || next.Store != "b-scratch" {
		t.Fatalf("continuation launch %+v", next)
	}
	if err = store.RecordRunOutput(ctx, RunOutput{AttemptID: next.Attempt.ID, LeaseID: next.Lease.ID, Epoch: next.Lease.CoordinationEpoch, RunID: runID,
		StoreID: "b-scratch", ExecutorID: "exec2", Root: "/srv/kairo-store", Path: "/srv/kairo-store/outputs/" + runID}); err != nil {
		t.Fatal(err)
	}
	if _, attempt := runOutputRow(t, store, runID, "b-scratch"); attempt == nil || *attempt != next.Attempt.ID {
		t.Fatalf("its attempt not noted on the directory: %v", attempt)
	}

	// the pack, and the directory it left, removed
	if err = store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	orders, err = store.PacksToMake(ctx, "executor")
	if err != nil {
		t.Fatal(err)
	}
	move = nil
	for i := range orders {
		if orders[i].ObjectID == objectID {
			move = &orders[i]
		}
	}
	if move == nil || !move.Drop || !move.DropSource || move.SourceID != runID {
		t.Fatalf("drop orders %+v", orders)
	}
	if err = store.RecordPack(ctx, PackMade{ObjectID: objectID, ExecutorID: "executor", Dropped: true, SourceRemoved: true}); err != nil {
		t.Fatal(err)
	}
	if state, _ := runOutputRow(t, store, runID, "a-scratch"); state != "deleted" {
		t.Fatalf("left behind after its removal: %s", state)
	}
	if state, _, _, _ := landingState(t, store, continuation); state != landingDone {
		t.Fatalf("landing %s", state)
	}
}

// A continuation whose run's node is free runs there: nothing is moved.
func TestAContinuationStaysWhereItsRunsOutputIsWhenItFits(t *testing.T) {
	store := landingFixture(t)
	ctx := context.Background()
	first := reserveOn(t, store, "executor")
	runID := first.RunID
	if err := store.RecordRunOutput(ctx, RunOutput{AttemptID: first.Attempt.ID, LeaseID: first.Lease.ID, Epoch: first.Lease.CoordinationEpoch, RunID: runID,
		StoreID: "a-scratch", ExecutorID: "executor", Root: "D:/kairo", Path: "D:/kairo/outputs/" + runID}); err != nil {
		t.Fatal(err)
	}
	suspendWith(t, store, first, "/kairo/output/ckpt/step-1")
	addNode2GPU(t, store)
	if err := store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	if got := reserveOn(t, store, "exec2"); got != nil {
		t.Fatalf("placed on node2, away from its run's directory: %s", got.Execution.ID)
	}
	next := reserveOn(t, store, "executor")
	if next == nil || next.RunID != runID || next.Store != "a-scratch" {
		t.Fatalf("continuation %+v", next)
	}
	var landings int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM output_landings`).Scan(&landings); err != nil || landings != 0 {
		t.Fatalf("%d landings %v", landings, err)
	}
}

// A rerun from a checkpoint is a run of its own: it starts with the output
// of the run that published the checkpoint, brought (from its pack, on the
// same node here) as its own output directory before it launches.
func TestARerunFromACheckpointStartsWithTheOutputHoldingIt(t *testing.T) {
	store := landingFixture(t)
	ctx := context.Background()
	first := reserveOn(t, store, "executor")
	runID := first.RunID
	epoch := first.Lease.CoordinationEpoch
	if err := store.RecordRunOutput(ctx, RunOutput{AttemptID: first.Attempt.ID, LeaseID: first.Lease.ID, Epoch: epoch, RunID: runID,
		StoreID: "a-scratch", ExecutorID: "executor", Root: "D:/kairo", Path: "D:/kairo/outputs/" + runID}); err != nil {
		t.Fatal(err)
	}
	if err := store.ActivateLaunch(ctx, first.Attempt.ID, first.Lease.ID, epoch, first.AuthorizationToken, 100, "launcher:100"); err != nil {
		t.Fatal(err)
	}
	asked, err := store.CheckpointTask(ctx, "moves", "train", "tester")
	if err != nil || len(asked.Commands) != 1 {
		t.Fatalf("checkpoint: %+v %v", asked, err)
	}
	for _, phase := range []string{"accepted", "checkpointing"} {
		if err = store.AckCommand(ctx, first.Attempt.ID, first.Lease.ID, epoch, asked.Commands[0], phase, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err = store.AckCommand(ctx, first.Attempt.ID, first.Lease.ID, epoch, asked.Commands[0], "checkpointed", json.RawMessage(`{"continuation_ref":"/kairo/output/ckpt/step-7"}`)); err != nil {
		t.Fatal(err)
	}
	finishLaunch(t, store, first, 1)
	if err = store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	// the failed run's output is packed
	orders, err := store.PacksToMake(ctx, "executor")
	if err != nil {
		t.Fatal(err)
	}
	var output string
	for _, o := range orders {
		if o.Kind == ObjectOutput && o.SourceID == runID {
			output = o.ObjectID
		}
	}
	if output == "" {
		t.Fatalf("orders %+v", orders)
	}
	if err = store.RecordPack(ctx, PackMade{ObjectID: output, ExecutorID: "executor", PackPath: "D:/kairo/packs/" + output + ".tar.zst", ManifestPath: "D:/kairo/packs/" + output + ".tar.zst.files.tsv",
		PackBytes: 10, ContentBytes: 20, Files: 2, PackSHA256: strings.Repeat("ab", 32), ManifestSHA256: strings.Repeat("cd", 32)}); err != nil {
		t.Fatal(err)
	}
	if _, err = store.RerunTaskWith(ctx, "moves", "train", "tester", "from-7", RerunOptions{FromCheckpoint: "latest"}); err != nil {
		t.Fatal(err)
	}
	if err = store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	var rerun string
	if err = store.db.QueryRow(`SELECT id FROM execution_requests WHERE state='waiting' AND input_continuation_ref='/kairo/output/ckpt/step-7'`).Scan(&rerun); err != nil {
		t.Fatal(err)
	}
	if state, node, storeID, objectID := landingState(t, store, rerun); state != landingBringing || node != "node" || storeID != "a-scratch" || objectID != output {
		t.Fatalf("landing %s %s %s %s", state, node, storeID, objectID)
	}
	if got := reserveOn(t, store, "executor"); got != nil {
		t.Fatal("the rerun launched before the output holding its checkpoint")
	}
	if reason, detail := waitOf(t, store, rerun); reason != "output_moving" || !strings.Contains(detail, "holds its checkpoint") {
		t.Fatalf("wait %s %s", reason, detail)
	}
	fetches, err := store.FetchesToMake(ctx, "executor")
	if err != nil || len(fetches) != 1 || fetches[0].RunID != rerun || !fetches[0].Source.Local || fetches[0].Source.StoreID != "a-scratch" {
		t.Fatalf("fetches %+v %v", fetches, err)
	}
	if err = store.RecordFetch(ctx, FetchMade{ObjectID: output, StoreID: "a-scratch", ExecutorID: "executor", Path: "D:/kairo/outputs/" + rerun, Bytes: 20, RunID: rerun}); err != nil {
		t.Fatal(err)
	}
	if state, _, _, _ := landingState(t, store, rerun); state != landingDone {
		t.Fatalf("landing %s", state)
	}
	observeGPU(t, store, "gpu-0")
	next := reserveOn(t, store, "executor")
	if next == nil || next.Execution.ID != rerun || next.RunID != rerun || next.Store != "a-scratch" {
		t.Fatalf("rerun launch %+v", next)
	}
}

// Of the nodes a continuation could run on, it moves to the one it is
// expected to end on soonest, only when that is enough sooner than where its
// run's directory is: by moveMinGain, and a tenth of the time it would take
// there; a node whose end is not known is never chosen, and where the
// directory is not known any node known will do.
func TestBestMoveNeedsAGain(t *testing.T) {
	now := time.Date(2026, 11, 26, 12, 0, 0, 0, time.UTC)
	node := func(name string, after time.Duration, known bool) nodeEstimate {
		return nodeEstimate{Node: name, finish: now.Add(after), known: known}
	}
	for _, c := range []struct {
		name  string
		nodes []nodeEstimate
		want  string
	}{
		{"home soonest", []nodeEstimate{node("home", time.Hour, true), node("b", 2*time.Hour, true)}, ""},
		{"a little sooner", []nodeEstimate{node("home", time.Hour, true), node("b", 55*time.Minute, true)}, ""},
		{"less than a tenth", []nodeEstimate{node("home", 10*time.Hour, true), node("b", 9*time.Hour+30*time.Minute, true)}, ""},
		{"enough sooner", []nodeEstimate{node("home", 5*time.Hour, true), node("b", 4*time.Hour, true), node("c", 4*time.Hour+30*time.Minute, true)}, "b"},
		{"home not known", []nodeEstimate{node("home", 0, false), node("b", 3*time.Hour, true)}, "b"},
		{"home cannot run it", []nodeEstimate{node("b", 3*time.Hour, true)}, "b"},
		{"none known", []nodeEstimate{node("home", 0, false), node("b", 0, false)}, ""},
	} {
		got, ok := bestMove(c.nodes, "home", now)
		if got != c.want || ok != (c.want != "") {
			t.Errorf("%s: %q %v, want %q", c.name, got, ok, c.want)
		}
	}
	// twice as fast: half the time left, once the directory is there
	if got := expectedEnd(now, now, 2*time.Minute, 4*time.Hour, 2); !got.Equal(now.Add(2*time.Minute + 2*time.Hour)) {
		t.Fatalf("end %s", got)
	}
	// starting later than the directory arrives
	if got := expectedEnd(now, now.Add(time.Hour), 2*time.Minute, time.Hour, 0); !got.Equal(now.Add(2 * time.Hour)) {
		t.Fatalf("end %s", got)
	}
}

// The path between two nodes is the lesser of what was measured either
// way; one never measured is taken as 1 GbE.
func TestTransferTimeUsesTheMeasuredPath(t *testing.T) {
	m := &planModel{perf: &nodePerf{links: map[[2]string]float64{linkKey("b", "a"): 50e6}}}
	if got := m.transferTime(1e9, "a", "b"); got != time.Duration((1e9/50e6*1.25+30)*float64(time.Second)) {
		t.Fatalf("measured: %s", got)
	}
	if got := m.transferTime(1e9, "a", "c"); got != time.Duration((1e9/defaultLinkBytesPerSecond*1.25+30)*float64(time.Second)) {
		t.Fatalf("unmeasured: %s", got)
	}
}
