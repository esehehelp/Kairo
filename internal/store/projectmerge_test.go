package store

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
)

func mergeManifest(project string, tasks ...[2]string) string {
	m := "schema_version = 1\n[project]\nname = \"" + project + "\"\n"
	for _, t := range tasks {
		m += `[[tasks]]
name = "` + t[0] + `"
queue = "` + t[1] + `"
depends_on = []
policy = "RunToCompletion@v1"
[tasks.execution]
schema_version = 2
argv = ["true"]
cwd = "/work"
[tasks.execution.executor]
labels = { environment = "wsl2" }
`
	}
	return m
}

// A project merged into another takes all it has there, ids unchanged:
// its tasks (a queue of the same name merged, another moved), executions
// and data objects. A task name both have must be given a new name; the
// merge is refused while anything of it runs or waits.
func TestAProjectMergesIntoAnother(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	ctx := context.Background()
	for _, m := range []string{
		mergeManifest("from", [2]string{"shared", "data"}, [2]string{"mine", "extra"}),
		mergeManifest("into", [2]string{"shared", "data"}, [2]string{"theirs", "data"}),
	} {
		if _, err := store.ApplyProject(ctx, projectSpec(t, m)); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	// still waiting: refused
	if _, err := store.MergeProject(ctx, MergeRequest{From: "from", Into: "into", Renames: map[string]string{"shared": "shared-from"}}); !errors.Is(err, ErrMergeRefused) || !strings.Contains(err.Error(), "live executions") {
		t.Fatalf("merged while waiting: %v", err)
	}
	for _, p := range []string{"from", "into"} {
		for _, task := range []string{"shared", "mine", "theirs"} {
			if _, err := store.CancelTask(ctx, p, task, "tester", false); err != nil && !errors.Is(err, ErrNotFound) {
				t.Fatal(err)
			}
		}
	}
	for _, o := range []struct{ id, project, name string }{{"obj_f1", "from", "same"}, {"obj_f2", "from", "only"}, {"obj_i1", "into", "same"}} {
		if _, err := store.testExec(`INSERT INTO data_objects(id,kind,source_id,store_id,executor_id,root,source_path,project_scope_id,state,name,created_at,updated_at)
			SELECT $1,'import',$1,'s','executor','/r','/p/'||$1,id,'packed',$3,'2026-11-26','2026-11-26' FROM coordination_scopes WHERE kind='project' AND external_key=$2`, o.id, o.project, o.name); err != nil {
			t.Fatal(err)
		}
	}
	// a task name both have
	if _, err := store.MergeProject(ctx, MergeRequest{From: "from", Into: "into"}); !errors.Is(err, ErrMergeRefused) || !strings.Contains(err.Error(), "shared") {
		t.Fatalf("unnamed collision: %v", err)
	}
	if _, err := store.MergeProject(ctx, MergeRequest{From: "from", Into: "into", Renames: map[string]string{"shared": "theirs"}}); !errors.Is(err, ErrMergeRefused) {
		t.Fatalf("renamed onto a name in use: %v", err)
	}
	// dry run: told, nothing moved
	dry, err := store.MergeProject(ctx, MergeRequest{From: "from", Into: "into", Renames: map[string]string{"shared": "shared-from"}, DryRun: true})
	if err != nil || dry.Tasks != 2 || dry.Objects != 2 || dry.Renamed["shared"] != "shared-from" || dry.Queues["data"] != "merged" || dry.Queues["extra"] != "moved" ||
		!slices.Equal(dry.SharedDataNames, []string{"same"}) {
		t.Fatalf("dry run %+v %v", dry, err)
	}
	if _, err := store.GetTaskDetail(ctx, "from", "mine"); err != nil {
		t.Fatalf("a dry run moved: %v", err)
	}
	got, err := store.MergeProject(ctx, MergeRequest{From: "from", Into: "into", Renames: map[string]string{"shared": "shared-from"}, Actor: "tester"})
	if err != nil || got.Tasks != 2 || got.Executions != 2 || got.Objects != 2 {
		t.Fatalf("merge %+v %v", got, err)
	}
	status, err := store.GetProjectStatus(ctx, "into")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, task := range status.Tasks {
		names = append(names, task.Name+"@"+task.Queue)
	}
	slices.Sort(names)
	if strings.Join(names, ",") != "mine@extra,shared-from@data,shared@data,theirs@data" {
		t.Fatalf("tasks of into: %v", names)
	}
	if detail, err := store.GetTaskDetail(ctx, "into", "shared-from"); err != nil || len(detail.Executions) != 1 {
		t.Fatalf("its history: %+v %v", detail.Executions, err)
	}
	var objects int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM data_objects d JOIN coordination_scopes p ON p.id=d.project_scope_id WHERE p.external_key='into'`).Scan(&objects); err != nil || objects != 3 {
		t.Fatalf("%d objects of into, %v", objects, err)
	}
	if left, err := store.GetProjectStatus(ctx, "from"); err == nil && len(left.Tasks) != 0 {
		t.Fatalf("left in from: %+v", left.Tasks)
	}
	// declared again in into, under their names there, they are the same
	// tasks (the renamed one under its new name)
	again, err := store.ApplyProject(ctx, projectSpec(t, mergeManifest("into", [2]string{"shared-from", "data"}, [2]string{"mine", "extra"})))
	if err != nil || len(again.Created) != 0 || again.Unchanged != 2 {
		t.Fatalf("declared again: %+v %v", again, err)
	}
}

// A task renamed by a merge is renamed where the other tasks of its project
// name it too: their dependencies and task inputs (an input names a task's
// latest run by name: under the old name it would read INTO's task), in
// their declarations and in what their runs are made from: a rerun reads the
// renamed task.
func TestAMergeRenamesWhatNamesARenamedTask(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	ctx := context.Background()
	uses := mergeManifest("from", [2]string{"shared", "data"}) + `[[tasks]]
name = "uses"
queue = "data"
depends_on = ["shared"]
policy = "RunToCompletion@v1"
[tasks.execution]
schema_version = 2
argv = ["true"]
cwd = "/work"
[tasks.execution.executor]
labels = { environment = "wsl2" }
[[tasks.execution.inputs]]
name = "made"
task = "shared"
`
	for _, m := range []string{uses, mergeManifest("into", [2]string{"shared", "data"})} {
		if _, err := store.ApplyProject(ctx, projectSpec(t, m)); err != nil {
			t.Fatal(err)
		}
	}
	for _, p := range []string{"from", "into"} {
		for _, task := range []string{"shared", "uses"} {
			if _, err := store.CancelTask(ctx, p, task, "tester", false); err != nil && !errors.Is(err, ErrNotFound) {
				t.Fatal(err)
			}
		}
	}
	if _, err := store.MergeProject(ctx, MergeRequest{From: "from", Into: "into", Renames: map[string]string{"shared": "shared-from"}, Actor: "tester"}); err != nil {
		t.Fatal(err)
	}
	var body string
	if err := store.db.QueryRow(`SELECT t.spec_json FROM orchestration_tasks t JOIN coordination_scopes p ON p.id=t.project_scope_id WHERE p.external_key='into' AND t.name='uses'`).Scan(&body); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(body, `"depends_on":["shared-from"]`) || !strings.Contains(body, `"task":"shared-from"`) || strings.Contains(body, `"shared"`) {
		t.Fatalf("the declaration of uses: %s", body)
	}
	// run again, uses reads shared-from, not into's shared
	if _, err := store.RerunTask(ctx, "into", "shared-from", "tester", "rerun-shared-from"); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	completeNextOrchestrationExecution(t, store, 0)
	if _, err := store.RerunTask(ctx, "into", "uses", "tester", "rerun-uses"); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	detail, err := store.GetTaskDetail(ctx, "into", "uses")
	if err != nil || len(detail.Executions) == 0 {
		t.Fatalf("uses was not run again: %+v %v", detail.Executions, err)
	}
	var inputs string
	if err := store.db.QueryRow(`SELECT inputs_json FROM execution_requests WHERE id=$1`, detail.Executions[len(detail.Executions)-1].ExecutionID).Scan(&inputs); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(inputs, `"task":"shared-from"`) || strings.Contains(inputs, `"shared"`) {
		t.Fatalf("the rerun of uses reads %s", inputs)
	}
}
