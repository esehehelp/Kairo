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

func testSnapshot(t *testing.T, store *Store, body string) string {
	t.Helper()
	list := sourcesnap.ListHeader + "\nsrc/a.py\t" + string(rune('0'+len(body))) + "\t" + strings.Repeat(body, 64)[:64] + "\t1\n"
	id, err := sourcesnap.ID(list)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(list))
	if err := store.RecordSourceSnapshot(context.Background(), SourceSnapshot{ID: id, PackBytes: 10, PackSHA256: strings.Repeat("ab", 32), ListSHA256: hex.EncodeToString(sum[:]),
		List: list, Files: 1, ContentBytes: 1, Project: "sumire", SourceName: "code"}); err != nil {
		t.Fatal(err)
	}
	return id
}

// A task declared with a source is bound to the snapshot the apply made of
// it, for good: a later apply with other code binds only the tasks it
// declares, the executions of each run in its own snapshot, and an apply
// that sends no snapshot (or one the daemon lacks) for a new task is
// refused.
func TestATaskIsBoundToTheSnapshotItWasDeclaredWith(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	ctx := context.Background()
	first, second := testSnapshot(t, store, "a"), testSnapshot(t, store, "b")
	if first == second {
		t.Fatal("two lists, one id")
	}
	spec := func(snapshot string, tasks ...string) orchestration.Validated {
		t.Helper()
		body := "schema_version = 1\n[project]\nname = \"sumire\"\n[sources.code]\npaths = [\"src\"]\n"
		for _, name := range tasks {
			body += "[[tasks]]\nname = \"" + name + "\"\nqueue = \"q\"\ndepends_on = []\npolicy = \"RunToCompletion@v1\"\n[tasks.execution]\nschema_version = 2\nargv = [\"python\", \"-m\", \"train\"]\nsource = \"code\"\n[tasks.execution.executor]\nlabels = { environment = \"wsl2\" }\n"
		}
		v := projectSpec(t, body)
		if snapshot != "" {
			v.Manifest.Snapshots = map[string]string{"code": snapshot}
		}
		v, err := orchestration.Normalize(v.Manifest)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	if _, err := store.ApplyProject(ctx, spec("", "train")); err == nil || !strings.Contains(err.Error(), "sent no snapshot") {
		t.Fatalf("applied with no snapshot: %v", err)
	}
	if _, err := store.ApplyProject(ctx, spec("src_"+strings.Repeat("9", 32), "train")); err == nil || !strings.Contains(err.Error(), "was not sent") {
		t.Fatalf("applied with a snapshot the daemon lacks: %v", err)
	}
	if _, err := store.ApplyProject(ctx, spec(first, "train")); err != nil {
		t.Fatal(err)
	}
	applied, err := store.ApplyProject(ctx, spec(second, "train", "eval"))
	if err != nil || len(applied.Created) != 1 || applied.Unchanged != 1 {
		t.Fatalf("second apply %+v %v", applied, err)
	}
	bound := func(task string) string {
		t.Helper()
		var id string
		if err := store.db.QueryRowContext(ctx, `SELECT source_snapshot_id FROM orchestration_tasks WHERE name=$1`, task).Scan(&id); err != nil {
			t.Fatal(err)
		}
		return id
	}
	if bound("train") != first || bound("eval") != second {
		t.Fatalf("train %s, eval %s", bound("train"), bound("eval"))
	}
	if err := store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	launch := authorizeNext(t, store)
	var task string
	if err := store.db.QueryRowContext(ctx, `SELECT t.name FROM orchestration_task_executions o JOIN orchestration_tasks t ON t.task_scope_id=o.task_scope_id WHERE o.execution_id=$1`, launch.Execution.ID).Scan(&task); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"train": first, "eval": second}[task]
	if launch.Source == nil || launch.Source.ID != want || launch.Source.PackSHA256 != strings.Repeat("ab", 32) || launch.CWD != "." {
		t.Fatalf("launch of %s: %+v cwd %q", task, launch.Source, launch.CWD)
	}
	snap, err := store.SourceSnapshotByID(ctx, first)
	if err != nil || len(snap.Tasks) != 1 || snap.Tasks[0] != "sumire/train" || snap.List == "" {
		t.Fatalf("snapshot %+v %v", snap, err)
	}
	listed, err := store.ListSourceSnapshots(ctx, "sumire", 10)
	if err != nil || len(listed) != 2 {
		t.Fatalf("listed %+v %v", listed, err)
	}
	if err = store.RecordSourceSnapshot(ctx, SourceSnapshot{ID: first, List: snap.List + "src/b.py\t1\t" + strings.Repeat("cd", 32) + "\t1\n"}); err == nil {
		t.Fatal("a list that makes another id was recorded")
	}
}
