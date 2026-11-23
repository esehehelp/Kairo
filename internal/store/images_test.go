package store

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"kairo/internal/orchestration"
)

const testImagePin = "docker.io/library/python@sha256:5e2bdb1b0d542d5039cffb8181c65930093949194a65c365dfb1b0fe13a4fa5b"

// An execution in an image goes only to an executor with a container
// runtime, and such an executor takes nothing else.
func TestSelectorMatchesContainers(t *testing.T) {
	host, ctr := `{"environment":"linux"}`, `{"environment":"linux","runtime":"incus"}`
	for _, c := range []struct {
		selector, executor string
		want               bool
	}{
		{`{"labels":{"environment":"linux"}}`, host, true},
		{`{"labels":{"environment":"linux"}}`, ctr, false},
		{`{"labels":{"environment":"linux"},"container":true}`, ctr, true},
		{`{"labels":{"environment":"linux"},"container":true}`, host, false},
		{`{"labels":{},"container":true,"node":"other"}`, ctr, false},
	} {
		if got := selectorMatches(c.selector, "node", c.executor); got != c.want {
			t.Errorf("selectorMatches(%s, %s) = %v", c.selector, c.executor, got)
		}
	}
}

// An execution's image must be pinned to a digest; it sets the selector's
// "container", which a caller cannot set (nor clear) itself, and it is in the
// digest only when set.
func TestAnExecutionImageIsPinnedAndSelectsContainers(t *testing.T) {
	spec := schedulerExecutionSpec("img")
	spec.ExecutorSelector = json.RawMessage(`{"labels":{},"container":true}`)
	plain, _, plainDigest, err := normalizeExecutionSpec(spec)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(plain.ExecutorSelector), "container") {
		t.Fatalf("a caller's container key kept: %s", plain.ExecutorSelector)
	}
	spec.Image = "python:3.12"
	if _, _, _, err := normalizeExecutionSpec(spec); err == nil || !strings.Contains(err.Error(), "not pinned") {
		t.Fatalf("an unpinned image: %v", err)
	}
	spec.Image = "python@sha256:5e2bdb1b0d542d5039cffb8181c65930093949194a65c365dfb1b0fe13a4fa5b"
	in, _, inDigest, err := normalizeExecutionSpec(spec)
	if err != nil {
		t.Fatal(err)
	}
	if in.Image != testImagePin || !strings.Contains(string(in.ExecutorSelector), `"container":true`) || inDigest == plainDigest {
		t.Fatalf("image %q selector %s digest same %v", in.Image, in.ExecutorSelector, inDigest == plainDigest)
	}
}

// A task declared with an image is bound to the digest its apply resolved,
// its executions run in it on the container executor of the node (the host
// executor beside it does not take them) and the launch names it.
func TestATaskRunsInItsImage(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	ctx := context.Background()
	if err := store.UpsertExecutor(ctx, Executor{ID: "ctr", NodeID: "node", Kind: "incus", Attributes: json.RawMessage(`{"environment":"wsl2","runtime":"incus"}`), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	body := "schema_version = 1\n[project]\nname = \"boxed\"\n[images.py]\nref = \"python:3.12\"\n" +
		"[[tasks]]\nname = \"train\"\nqueue = \"q\"\ndepends_on = []\npolicy = \"RunToCompletion@v1\"\n[tasks.execution]\nschema_version = 2\nargv = [\"python\", \"-V\"]\ncwd = \"/\"\nimage = \"py\"\n[tasks.execution.executor]\nlabels = { environment = \"wsl2\" }\n"
	v := projectSpec(t, body)
	if _, err := store.ApplyProject(ctx, v); err == nil || !strings.Contains(err.Error(), "no digest") {
		t.Fatalf("an apply without pins: %v", err)
	}
	v.Manifest.ImagePins = map[string]string{"py": testImagePin}
	v, err := orchestration.Normalize(v.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ApplyProject(ctx, v); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	if r, err := store.ReserveNext(ctx, "executor"); err != nil || r != nil {
		t.Fatalf("the host executor took the image's execution: %+v %v", r, err)
	}
	reservation, err := store.ReserveNext(ctx, "ctr")
	if err != nil || reservation == nil {
		t.Fatalf("reserve on the container executor: %+v %v", reservation, err)
	}
	if err = store.MarkLeasePrepared(ctx, reservation.Lease.ID, reservation.Lease.CoordinationEpoch); err != nil {
		t.Fatal(err)
	}
	launch, err := store.AuthorizeLaunch(ctx, reservation)
	if err != nil {
		t.Fatal(err)
	}
	if launch.Image != testImagePin {
		t.Fatalf("launch image %q", launch.Image)
	}
	var bound, ran string
	if err := store.db.QueryRowContext(ctx, `SELECT t.image_ref,e.image_ref FROM orchestration_tasks t JOIN orchestration_task_executions te ON te.task_scope_id=t.task_scope_id
		JOIN execution_requests e ON e.id=te.execution_id WHERE t.name='train'`).Scan(&bound, &ran); err != nil {
		t.Fatal(err)
	}
	if bound != testImagePin || ran != testImagePin {
		t.Fatalf("bound %q ran %q", bound, ran)
	}
}
