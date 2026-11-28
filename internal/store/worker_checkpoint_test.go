package store

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"
	"time"

	"kairo/internal/orchestration"
)

// A run that ran to its end goes on from its last state (issue #49): the
// worker asks for a checkpoint of its own attempt after its last step (a
// checkpoint command of origin worker, delivered in the answer, the same
// one while in flight), publishes it with its streams' positions and ends;
// the succeeded task is rerun from it, in newer code too, once no task
// depending on it is on its way. The program runs in the new snapshot, the
// streams' stages where the checkpoint's ran.
func TestARunThatRanToItsEndGoesOnFromItsLastCheckpoint(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	ctx := context.Background()
	if err := store.UpsertExecutor(ctx, Executor{ID: "ctr", NodeID: "node", Kind: "incus", Attributes: json.RawMessage(`{"environment":"wsl2","runtime":"incus"}`), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertResourceInstance(ctx, ResourceInstance{ID: "ram", NodeID: "node", ProviderID: "gpu-provider", Kind: "ram", StableIdentity: "physical-memory", Binding: json.RawMessage(`{}`), AdminState: "enabled"}); err != nil {
		t.Fatal(err)
	}
	total, free := int64(64<<30), int64(40<<30)
	observed := time.Now().UTC()
	if err := store.RecordObservation(ctx, Observation{ResourceID: "ram", ObservedAt: observed.Format(time.RFC3339Nano), ValidUntil: observed.Add(time.Minute).Format(time.RFC3339Nano), TotalBytes: &total, FreeBytes: &free}); err != nil {
		t.Fatal(err)
	}
	first, second := testSnapshot(t, store, "a"), testSnapshot(t, store, "b")
	apply := func(snapshot string) {
		t.Helper()
		body := "schema_version = 1\n[project]\nname = \"sumire\"\n[sources.code]\npaths = [\"src\"]\n[images.py]\nref = \"python:3.12\"\n" +
			"[raws.corpus]\nurls = [\"https://example.org/corpus/part-0.jsonl.zst\"]\n" +
			"[streams.mix-1]\nsources = [ { raw = \"corpus\", format = \"jsonl.zst\", field = \"text\" } ]\nshuffle = { seed = 9, block = 64 }\n" +
			"[[tasks]]\nname = \"train\"\nqueue = \"q\"\ndepends_on = []\npolicy = \"RunToCompletion@v1\"\n[tasks.execution]\nschema_version = 2\nargv = [\"python\", \"-m\", \"train\"]\nsource = \"code\"\nimage = \"py\"\nstreams = [\"mix-1\"]\ncheckpointable = true\n" +
			"[tasks.execution.executor]\nlabels = { environment = \"wsl2\" }\n" +
			"[[tasks]]\nname = \"eval\"\nqueue = \"q\"\ndepends_on = [\"train\"]\npolicy = \"RunToCompletion@v1\"\n[tasks.execution]\nschema_version = 2\nargv = [\"python\", \"-V\"]\ncwd = \"/\"\n" +
			"[tasks.execution.executor]\nlabels = { environment = \"wsl2\" }\n"
		v := projectSpec(t, body)
		v.Manifest.ImagePins = map[string]string{"py": testImagePin}
		v.Manifest.Snapshots = map[string]string{"code": snapshot}
		v, err := orchestration.Normalize(v.Manifest)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.ApplyProject(ctx, v); err != nil {
			t.Fatal(err)
		}
	}
	apply(first)
	if err := store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	train := launchOn(t, store, "ctr")
	l := train
	// after its last step: a checkpoint command of its own, the same one
	// again while it is in flight
	asked, err := store.RequestCheckpoint(ctx, l.Attempt.ID, l.Lease.ID, l.Lease.CoordinationEpoch, "after the last step")
	if err != nil || asked.Kind != "checkpoint" || asked.Origin != "worker" || asked.Reason != "after the last step" || asked.State != "pending" || asked.DeliveryCount != 1 {
		t.Fatalf("asked %+v %v", asked, err)
	}
	again, err := store.RequestCheckpoint(ctx, l.Attempt.ID, l.Lease.ID, l.Lease.CoordinationEpoch, "")
	if err != nil || again.ID != asked.ID || again.DeliveryCount != 2 {
		t.Fatalf("asked again %+v %v", again, err)
	}
	positions := json.RawMessage(`{"continuation_ref":"/kairo/output/step_10000.pt","streams":{"mix-1":{"world":1,"order":1,"positions":[{"rank":0,"record":6981064,"sub":6}]}}}`)
	for _, phase := range []string{"accepted", "checkpointing", "checkpointed"} {
		var payload json.RawMessage
		if phase == "checkpointed" {
			payload = positions
		}
		if err := store.AckCommand(ctx, l.Attempt.ID, l.Lease.ID, l.Lease.CoordinationEpoch, asked.ID, phase, payload); err != nil {
			t.Fatalf("%s: %v", phase, err)
		}
	}
	finishLaunch(t, store, train, 0)
	if _, err := store.RequestCheckpoint(ctx, l.Attempt.ID, l.Lease.ID, l.Lease.CoordinationEpoch, ""); !errors.Is(err, ErrStaleEpoch) {
		t.Fatalf("asked by an ended attempt: %v", err)
	}
	if task := projectTasks(t, store, "sumire")["train"]; task.State != "succeeded" {
		t.Fatalf("train %s", task.State)
	}
	// a succeeded task is not run again, only continued from a checkpoint;
	// not while a task depending on it is on its way
	_, err = store.RerunTaskWith(ctx, "sumire", "train", "tester", "", RerunOptions{})
	taskConflictError(t, err, "continued from a checkpoint it published")
	projectTasks(t, store, "sumire")
	eval := launchNext(t, store)
	_, err = store.RerunTaskWith(ctx, "sumire", "train", "tester", "", RerunOptions{FromCheckpoint: "latest"})
	taskConflictError(t, err, "task eval depends on train and its execution")
	if _, err := store.RequestCheckpoint(ctx, eval.Attempt.ID, eval.Lease.ID, eval.Lease.CoordinationEpoch, ""); !errors.Is(err, ErrTaskConflict) {
		t.Fatalf("asked by an execution that is not checkpointable: %v", err)
	}
	finishLaunch(t, store, eval, 0)

	// with the fix applied: the program in it, the stages where they ran
	apply(second)
	out, err := store.RerunTaskWith(ctx, "sumire", "train", "tester", "", RerunOptions{FromCheckpoint: "latest", Source: "current"})
	if err != nil || out.FromCheckpoint == nil || out.FromCheckpoint.CommandID != asked.ID || out.SourceSnapshot != second {
		t.Fatalf("rerun %+v %v", out, err)
	}
	if task := projectTasks(t, store, "sumire")["train"]; task.State != "running" {
		t.Fatalf("train after the rerun %s", task.State)
	}
	var executionID, program, stages, command string
	if err := store.db.QueryRowContext(ctx, `SELECT e.id,e.source_snapshot_id,COALESCE(e.stream_snapshot_id,''),COALESCE(e.input_checkpoint_command_id,'') FROM execution_requests e
		JOIN orchestration_task_executions te ON te.execution_id=e.id JOIN orchestration_tasks t ON t.task_scope_id=te.task_scope_id
		WHERE t.name='train' ORDER BY te.execution_ordinal DESC LIMIT 1`).Scan(&executionID, &program, &stages, &command); err != nil {
		t.Fatal(err)
	}
	if program != second || stages != first || command != asked.ID {
		t.Fatalf("the rerun runs %s, its stages %s, from %s; want %s, %s, %s", program, stages, command, second, first, asked.ID)
	}
	if src, err := launchStreamSourceTx(ctx, store.db, executionID); err != nil || src == nil || src.ID != first {
		t.Fatalf("the launch's stream source %+v %v", src, err)
	}
	if src, err := launchSourceTx(ctx, store.db, executionID); err != nil || src == nil || src.ID != second {
		t.Fatalf("the launch's source %+v %v", src, err)
	}
	inUse, err := store.SourcesInUse(ctx, "ctr")
	if err != nil || !slices.Contains(inUse, first) || !slices.Contains(inUse, second) {
		t.Fatalf("sources in use %v %v", inUse, err)
	}
}
