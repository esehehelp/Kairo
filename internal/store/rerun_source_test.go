package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"kairo/internal/orchestration"
	"kairo/internal/sourcesnap"
)

// kairo task rerun --source current runs a failed task in the snapshot the
// project's latest apply sent for its source (applying the same file again
// with fixed code leaves the task as declared), and the execution records
// it; a snapshot the daemon does not keep, or one of another source, is
// refused (issue #15).
func TestARerunRunsInTheLatestAppliedSnapshot(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	ctx := context.Background()
	first, second := testSnapshot(t, store, "a"), testSnapshot(t, store, "b")
	apply := func(snapshot string) ApplyResult {
		t.Helper()
		body := "schema_version = 1\n[project]\nname = \"sumire\"\n[sources.code]\npaths = [\"src\"]\n" +
			"[[tasks]]\nname = \"train\"\nqueue = \"q\"\ndepends_on = []\npolicy = \"RunToCompletion@v1\"\n[tasks.execution]\nschema_version = 2\nargv = [\"python\", \"-m\", \"train\"]\nsource = \"code\"\n[tasks.execution.executor]\nlabels = { environment = \"wsl2\" }\n"
		v := projectSpec(t, body)
		v.Manifest.Snapshots = map[string]string{"code": snapshot}
		v, err := orchestration.Normalize(v.Manifest)
		if err != nil {
			t.Fatal(err)
		}
		applied, err := store.ApplyProject(ctx, v)
		if err != nil {
			t.Fatal(err)
		}
		return applied
	}
	apply(first)
	if err := store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	finishLaunch(t, store, launchNext(t, store), 1)
	if task := projectTasks(t, store, "sumire")["train"]; task.State != "failed" {
		t.Fatalf("train %s", task.State)
	}
	// the fix, applied: the task is unchanged, still bound to the first
	if applied := apply(second); applied.Unchanged != 1 || len(applied.Created) != 0 {
		t.Fatalf("second apply %+v", applied)
	}
	// a snapshot of another source of the project is not the task's
	otherList := sourcesnap.ListHeader + "\nlib/b.py\t1\t" + strings.Repeat("cd", 32) + "\t1\n"
	other, err := sourcesnap.ID(otherList)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(otherList))
	if err := store.RecordSourceSnapshot(ctx, SourceSnapshot{ID: other, PackBytes: 10, PackSHA256: strings.Repeat("ab", 32), ListSHA256: hex.EncodeToString(sum[:]),
		List: otherList, Files: 1, ContentBytes: 1, Project: "sumire", SourceName: "lib"}); err != nil {
		t.Fatal(err)
	}
	_, err = store.RerunTaskWith(ctx, "sumire", "train", "tester", "", RerunOptions{Source: other})
	taskConflictError(t, err, `not of sumire source "code"`)
	_, err = store.RerunTaskWith(ctx, "sumire", "train", "tester", "", RerunOptions{Source: "src_" + strings.Repeat("7", 32)})
	taskConflictError(t, err, "is not one the daemon keeps")

	out, err := store.RerunTaskWith(ctx, "sumire", "train", "tester", "", RerunOptions{Source: "current"})
	if err != nil || out.SourceSnapshot != second {
		t.Fatalf("rerun in the current snapshot: %+v %v", out, err)
	}
	projectTasks(t, store, "sumire")
	var ran, bound string
	if err := store.db.QueryRowContext(ctx, `SELECT e.source_snapshot_id,t.source_snapshot_id FROM execution_requests e JOIN orchestration_task_executions te ON te.execution_id=e.id
		JOIN orchestration_tasks t ON t.task_scope_id=te.task_scope_id WHERE t.name='train' ORDER BY te.execution_ordinal DESC LIMIT 1`).Scan(&ran, &bound); err != nil {
		t.Fatal(err)
	}
	if ran != second || bound != first {
		t.Fatalf("the rerun ran in %s, the task is bound to %s; want %s and %s", ran, bound, second, first)
	}
}
