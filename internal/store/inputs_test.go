package store

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"kairo/internal/datainput"
	"kairo/internal/machine"
)

// A task input is the output directory of the latest run of the task it
// names: the reading task is planned onto that node, handed the directory
// as it launches, and while it is pending or running the directory is not
// deleted once copied; afterwards it is.
func TestATaskInputIsReadInPlaceAndKeptUntilRead(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	addTestDisk(t, store, "node", "/")
	ctx := context.Background()
	declareTestStore(t, store,
		machine.Store{ID: "w-scratch", Class: machine.StoreVolatile, Node: "node", Executor: "executor", Root: "/var/kairo", Filesystem: "/"},
		machine.Store{ID: "e-durable", Class: machine.StoreDurable, Node: "node", Root: "E:/kairo", Filesystem: "/"},
	)
	if _, err := store.ApplyProject(ctx, projectSpec(t, `schema_version = 1
[project]
name = "chain"
[[tasks]]
name = "train"
queue = "q"
depends_on = []
policy = "RunToCompletion@v1"
[tasks.execution]
schema_version = 2
argv = ["python", "train.py"]
cwd = "/work"
[tasks.execution.executor]
labels = { environment = "wsl2" }
[[tasks]]
name = "decay"
queue = "q"
depends_on = ["train"]
policy = "RunToCompletion@v1"
[tasks.execution]
schema_version = 2
argv = ["python", "decay.py"]
cwd = "/work"
[tasks.execution.executor]
labels = { environment = "wsl2" }
[[tasks.execution.inputs]]
name = "init"
task = "train"
`)); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	finish := func(launch *Launch) {
		t.Helper()
		epoch := launch.Lease.CoordinationEpoch
		if err := store.ActivateLaunch(ctx, launch.Attempt.ID, launch.Lease.ID, epoch, launch.AuthorizationToken, 100, "launcher:100"); err != nil {
			t.Fatal(err)
		}
		if err := store.RecordTerminal(ctx, launch.Attempt.ID, launch.Lease.ID, epoch, 0, ""); err != nil {
			t.Fatal(err)
		}
		refreshSchedulerGPU(t, store)
		if err := store.FinalizeQuiescence(ctx, launch.Attempt.ID, launch.Lease.ID, epoch); err != nil {
			t.Fatal(err)
		}
		if err := store.ReconcileProjectTasks(ctx); err != nil {
			t.Fatal(err)
		}
	}
	train := authorizeNext(t, store)
	if len(train.Inputs) != 0 {
		t.Fatalf("train has no inputs: %+v", train.Inputs)
	}
	out := RunOutput{AttemptID: train.Attempt.ID, LeaseID: train.Lease.ID, Epoch: train.Lease.CoordinationEpoch, RunID: train.RunID,
		StoreID: "w-scratch", ExecutorID: "executor", Root: "/var/kairo", Path: "/var/kairo/outputs/" + train.RunID}
	if err := store.RecordRunOutput(ctx, out); err != nil {
		t.Fatal(err)
	}
	finish(train)

	// planned once train succeeded, submitted at the next reconcile
	if err := store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	decay := authorizeNext(t, store)
	if len(decay.Inputs) != 1 || decay.Inputs[0].Name != "init" || decay.Inputs[0].Path != out.Path || decay.Inputs[0].Problem != "" {
		t.Fatalf("decay's inputs: %+v", decay.Inputs)
	}
	var selector map[string]any
	if err := json.Unmarshal(decay.Execution.ExecutorSelector, &selector); err != nil || selector["node"] != "node" {
		t.Fatalf("decay is planned onto the node its input is on: %s %v", decay.Execution.ExecutorSelector, err)
	}
	// train's output is packed and copied meanwhile, but not deleted while
	// decay reads it
	orders, err := store.PacksToMake(ctx, "executor")
	if err != nil || len(orders) != 1 || orders[0].SourceID != train.RunID {
		t.Fatalf("pack orders %+v %v", orders, err)
	}
	objectID := orders[0].ObjectID
	sha := strings.Repeat("ab", 32)
	if err := store.RecordPack(ctx, PackMade{ObjectID: objectID, ExecutorID: "executor", PackPath: "/var/kairo/packs/" + objectID + ".tar.zst",
		ManifestPath: "/var/kairo/packs/" + objectID + ".tar.zst.files.tsv", PackBytes: 100, ContentBytes: 900, Files: 3, PackSHA256: sha, ManifestSHA256: sha}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.testExec(`INSERT INTO object_copies(object_id,store_id,executor_id,root,month,state,path,manifest_path,created_at,updated_at,verified_at)
		VALUES($1,'e-durable','executor','E:/kairo','202611','verified','E:/kairo/objects/202611/x.tar.zst','E:/kairo/objects/202611/x.tar.zst.files.tsv',$2,$2,$2)`, objectID, now()); err != nil {
		t.Fatal(err)
	}
	if deletions, err := store.DeletionsToMake(ctx, "executor"); err != nil || len(deletions) != 0 {
		t.Fatalf("a source decay reads is not deleted: %+v %v", deletions, err)
	}
	if why, err := pinnedSourceTx(ctx, store.db, objectID); err != nil || !strings.Contains(why, "decay") {
		t.Fatalf("pinned: %q %v", why, err)
	}
	finish(decay)
	if deletions, err := store.DeletionsToMake(ctx, "executor"); err != nil || len(deletions) != 1 || deletions[0].ObjectID != objectID {
		t.Fatalf("once decay is done, the source is deleted: %+v %v", deletions, err)
	}
}

// An input that cannot be read in place on the attempt's node is handed as
// a problem (the launch is aborted saying why), and a data input names the
// newest object of the project with that name.
func TestAnInputNotHereIsAProblem(t *testing.T) {
	store := importFixture(t)
	ctx := context.Background()
	imported, err := store.ImportData(ctx, ImportRequest{Project: "imp", Node: "node", Paths: []string{"D:/data/sumire"}})
	if err != nil {
		t.Fatal(err)
	}
	spec := schedulerExecutionSpec("reader")
	spec.Scope.Project = "imp"
	spec.ExecutorSelector = json.RawMessage(`{"labels":{"environment":"windows"}}`)
	spec.Exclusive, spec.Checkpointable, spec.Preemptible = []ExclusiveRequest{}, false, false
	spec.Inputs = []datainput.Input{{Name: "data", Data: "sumire"}, {Name: "gone", Data: "nothing"}}
	if _, _, err := store.SubmitExecution(ctx, spec); err != nil {
		t.Fatal(err)
	}
	reservation, err := store.ReserveNext(ctx, "win-exec")
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
	if len(launch.Inputs) != 2 || launch.Inputs[0].Path != "D:/data/sumire" || launch.Inputs[1].Problem == "" || launch.Inputs[1].Path != "" {
		t.Fatalf("inputs: %+v (imported %+v)", launch.Inputs, imported)
	}
	if !strings.Contains(launch.Inputs[1].Problem, "no data nothing") {
		t.Fatalf("problem: %q", launch.Inputs[1].Problem)
	}
}
