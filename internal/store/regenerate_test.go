package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"kairo/internal/datainput"
	"kairo/internal/machine"
)

// An execution waiting on a node for temporal data that is not there has it
// made again: its task, once succeeded, is rerun on that node, and the
// output it leaves, holding the same files (mtimes aside), stands for what
// was evicted (and takes its name). One made again that differs leaves a
// problem its readers wait with; nothing is rerun for it.
func TestTemporalDataIsMadeAgainWhereItIsRead(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	addTestDisk(t, store, "node", "/")
	ctx := context.Background()
	declareTestStore(t, store,
		machine.Store{ID: "w-scratch", Class: machine.StoreVolatile, Node: "node", Executor: "executor", Root: "/var/kairo", Filesystem: "/"},
	)
	if _, err := store.ApplyProject(ctx, projectSpec(t, `schema_version = 1
[project]
name = "sumire"
[[tasks]]
name = "build"
queue = "q"
depends_on = []
policy = "RunToCompletion@v1"
[tasks.execution]
schema_version = 2
argv = ["build-dataset"]
cwd = "/work"
regenerable = true
[tasks.execution.executor]
labels = { environment = "wsl2" }
`)); err != nil {
		t.Fatal(err)
	}
	listOf := func(mtime, sha string) string {
		return "path\tbytes\tsha256\tmtime_ns\nshard-0.bin\t4\t" + sha + "\t" + mtime + "\n"
	}
	same := strings.Repeat("ab", 32)
	// run runs the task's next execution to exit 0 and records its output's
	// identity with list.
	run := func(list string) (string, string, *Launch) {
		t.Helper()
		if err := store.ReconcileProjectTasks(ctx); err != nil {
			t.Fatal(err)
		}
		build := authorizeNext(t, store)
		path := "/var/kairo/outputs/" + build.RunID
		if err := store.RecordRunOutput(ctx, RunOutput{AttemptID: build.Attempt.ID, LeaseID: build.Lease.ID, Epoch: build.Lease.CoordinationEpoch, RunID: build.RunID,
			StoreID: "w-scratch", ExecutorID: "executor", Root: "/var/kairo", Path: path}); err != nil {
			t.Fatal(err)
		}
		epoch := build.Lease.CoordinationEpoch
		if err := store.ActivateLaunch(ctx, build.Attempt.ID, build.Lease.ID, epoch, build.AuthorizationToken, 100, "launcher:100"); err != nil {
			t.Fatal(err)
		}
		if err := store.RecordTerminal(ctx, build.Attempt.ID, build.Lease.ID, epoch, 0, ""); err != nil {
			t.Fatal(err)
		}
		refreshSchedulerGPU(t, store)
		if err := store.FinalizeQuiescence(ctx, build.Attempt.ID, build.Lease.ID, epoch); err != nil {
			t.Fatal(err)
		}
		if err := store.ReconcileProjectTasks(ctx); err != nil {
			t.Fatal(err)
		}
		orders, err := store.PacksToMake(ctx, "executor")
		if err != nil || len(orders) != 1 || !orders[0].Identity || orders[0].SourceID != build.RunID {
			t.Fatalf("orders %+v %v", orders, err)
		}
		sum := sha256.Sum256([]byte(list))
		if err := store.RecordPack(ctx, PackMade{ObjectID: orders[0].ObjectID, ExecutorID: "executor", ManifestPath: "/var/kairo/packs/" + orders[0].ObjectID + ".tar.zst.files.tsv",
			ContentBytes: 4, Files: 1, ManifestSHA256: hex.EncodeToString(sum[:]), Identity: true, Manifest: list}); err != nil {
			t.Fatal(err)
		}
		return build.RunID, orders[0].ObjectID, build
	}
	evict := func(objectID, runID string) {
		t.Helper()
		if _, err := store.testExec(`UPDATE data_objects SET source_state='evicted' WHERE id=$1`, objectID); err != nil {
			t.Fatal(err)
		}
		if _, err := store.testExec(`UPDATE run_outputs SET state='deleted' WHERE run_id=$1`, runID); err != nil {
			t.Fatal(err)
		}
	}
	firstRun, first, _ := run(listOf("1", same))
	if _, err := store.testExec(`UPDATE data_objects SET name='v8' WHERE id=$1`, first); err != nil {
		t.Fatal(err)
	}
	evict(first, firstRun)
	declared, err := declaredStores(ctx, store.db)
	if err != nil {
		t.Fatal(err)
	}
	// a reader of it by id, on its node
	submitReader := func(id string, in datainput.Input) ExecutionRequest {
		t.Helper()
		reader := schedulerExecutionSpec(id)
		reader.Scope.Project = "sumire"
		reader.Exclusive, reader.Checkpointable, reader.Preemptible = []ExclusiveRequest{}, false, false
		reader.Inputs = []datainput.Input{in}
		execution, _, err := store.SubmitExecution(ctx, reader)
		if err != nil {
			t.Fatal(err)
		}
		return execution
	}
	reader := submitReader("reader", datainput.Input{Name: "data", Object: first})
	if node := selectorNode(string(reader.ExecutorSelector)); node != "node" {
		t.Fatalf("reader planned onto %q", node)
	}
	inputs := func(in datainput.Input) string {
		body, _ := json.Marshal([]datainput.Input{in})
		return string(body)
	}
	wait := func(in datainput.Input) string {
		t.Helper()
		why, err := inputsWaitTx(ctx, store.db, declared, reader.ProjectScopeID, string(reader.ExecutorSelector), inputs(in))
		if err != nil {
			t.Fatal(err)
		}
		return why
	}
	byID := datainput.Input{Name: "data", Object: first}
	if why := wait(byID); !strings.Contains(why, "to be made again on node by task build") {
		t.Fatalf("wait: %q", why)
	}
	// ordered once: the task is rerun, planned onto the node
	if err := store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	var ordinal int
	if err := store.db.QueryRowContext(ctx, `SELECT regeneration_ordinal FROM data_objects WHERE id=$1`, first).Scan(&ordinal); err != nil || ordinal != 1 {
		t.Fatalf("ordered %d %v", ordinal, err)
	}
	if why := wait(byID); !strings.Contains(why, "being made again on node by task build") {
		t.Fatalf("wait: %q", why)
	}
	var selector string
	if err := store.db.QueryRowContext(ctx, `SELECT e.executor_selector_json FROM execution_requests e JOIN orchestration_task_executions o ON o.execution_id=e.id WHERE o.execution_ordinal=1`).Scan(&selector); err != nil || selectorNode(selector) != "node" {
		t.Fatalf("rerun %s %v", selector, err)
	}
	// made again with the same files (another mtime): read in its place
	secondRun, second, _ := run(listOf("2", same))
	for _, in := range []datainput.Input{byID, {Name: "data", Data: "v8"}} {
		if why := wait(in); why != "" {
			t.Fatalf("%+v made again, still waiting: %q", in, why)
		}
		src, err := inputSourceTx(ctx, store.db, declared, reader.ProjectScopeID, in)
		if err != nil || len(src.places) != 1 || src.places[0].path != "/var/kairo/outputs/"+secondRun || src.places[0].object != second {
			t.Fatalf("%+v read at %+v %v", in, src.places, err)
		}
	}
	if why, err := pinnedSourceTx(ctx, store.db, second); err != nil || !strings.Contains(why, "reads it") {
		t.Fatalf("what stands for an object read is not pinned: %q %v", why, err)
	}
	// evicted again, made again differing: a problem, not rerun again
	evict(second, secondRun)
	if why := wait(byID); !strings.Contains(why, "to be made again") {
		t.Fatalf("wait: %q", why)
	}
	if err := store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	run(listOf("3", strings.Repeat("ef", 32)))
	why := wait(byID)
	if !strings.HasPrefix(why, "input data: made again by run") || !strings.Contains(why, "0 files missing, 0 added, 1 changed (shard-0.bin)") {
		t.Fatalf("wait: %q", why)
	}
	for i := 0; i < 2; i++ {
		if err := store.ReconcileProjectTasks(ctx); err != nil {
			t.Fatal(err)
		}
	}
	var state string
	if err := store.db.QueryRowContext(ctx, `SELECT state FROM orchestration_tasks WHERE name='build'`).Scan(&state); err != nil || state != "succeeded" {
		t.Fatalf("rerun again for a difference: %s %v", state, err)
	}
}

func TestFileListsCompareWithoutMtimes(t *testing.T) {
	head := "path\tbytes\tsha256\tmtime_ns\n"
	a := head + "a\t1\tx\t1\nb\t2\ty\t1\n"
	for list, want := range map[string]string{
		head + "b\t2\ty\t9\na\t1\tx\t9\n": "",
		head + "a\t1\tx\t1\n":             "1 files missing, 0 added, 0 changed (b)",
		head + "a\t1\tz\t1\nb\t2\ty\t1\nc\t0\tw\t1\n": "0 files missing, 1 added, 1 changed (a)",
	} {
		if got := compareFileLists(a, list); got != want {
			t.Fatalf("%q: %q, want %q", list, got, want)
		}
	}
}
