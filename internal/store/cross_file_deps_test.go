package store

import (
	"context"
	"strings"
	"testing"
)

// crossFileManifest is project "experiments" with the tasks given, each a
// GPU task as orchestrationManifest's prepare.
func crossFileManifest(tasks string) string {
	return `schema_version = 1
[project]
name = "experiments"
[defaults]
queue = "q"
policy = "RunToCompletion@v1"
[defaults.execution]
schema_version = 2
argv = ["python", "run.py"]
cwd = "/work"
[defaults.execution.executor]
labels = { environment = "wsl2" }
[[defaults.execution.exclusive]]
kind = "gpu"
count = 1
` + tasks
}

// A task of a later file may depend on a task an earlier file declared: the
// apply resolves it against the project's tasks, and the dependant waits for
// it as for one of its own file; a name the project does not have is refused
// (issue #12).
func TestATaskDependsOnATaskAnEarlierFileDeclared(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	ctx := context.Background()
	if _, err := store.ApplyProject(ctx, projectSpec(t, crossFileManifest("\n[[tasks]]\nname = \"control\"\n"))); err != nil {
		t.Fatal(err)
	}
	applied, err := store.ApplyProject(ctx, projectSpec(t, crossFileManifest("\n[[tasks]]\nname = \"judge\"\ndepends_on = [\"control\"]\n")))
	if err != nil || len(applied.Created) != 1 || applied.Created[0] != "judge" {
		t.Fatalf("apply of the later file: %+v %v", applied, err)
	}
	var deps int
	if err = store.db.QueryRowContext(ctx, `SELECT count(*) FROM orchestration_task_dependencies d JOIN orchestration_tasks t ON t.task_scope_id=d.task_scope_id
		JOIN orchestration_tasks c ON c.task_scope_id=d.dependency_task_scope_id WHERE t.name='judge' AND c.name='control'`).Scan(&deps); err != nil || deps != 1 {
		t.Fatalf("dependency rows %d %v", deps, err)
	}
	tasks := projectTasks(t, store, "experiments")
	if tasks["judge"].State != "pending" {
		t.Fatalf("judge %s before control succeeded", tasks["judge"].State)
	}
	control := launchNext(t, store)
	finishLaunch(t, store, control, 0)
	if tasks = projectTasks(t, store, "experiments"); tasks["control"].State != "succeeded" || len(tasks["judge"].Executions) == 0 {
		t.Fatalf("after control succeeded: control %s, judge %+v", tasks["control"].State, tasks["judge"])
	}
	_, err = store.ApplyProject(ctx, projectSpec(t, crossFileManifest("\n[[tasks]]\nname = \"orphan\"\ndepends_on = [\"nowhere\"]\n")))
	if err == nil || !strings.Contains(err.Error(), "nowhere") {
		t.Fatalf("a dependency the project does not have: %v", err)
	}
}
