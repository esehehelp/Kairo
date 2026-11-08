package store

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestListAttemptsCarriesProgressAndLogPaths(t *testing.T) {
	store := openCoordinationStore(t)
	ctx := context.Background()
	execution, _, err := store.SubmitExecution(ctx, testExecutionSpec("train-1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.testExec(`INSERT INTO attempts(id,execution_id,state,executor_id,coordination_epoch,authorized_at,started_at,progress_json,stderr_path) VALUES('att-1',$1,'running','executor',1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z','{"unit":"step","current":5}','/logs/att-1.stderr.log')`, execution.ID); err != nil {
		t.Fatal(err)
	}
	all, err := store.ListAttempts(ctx, AttemptFilter{ProjectScopeID: execution.ProjectScopeID})
	if err != nil || len(all) != 1 {
		t.Fatalf("list: %+v %v", all, err)
	}
	got := all[0]
	if got.ID != "att-1" || string(got.Progress) != `{"unit":"step","current":5}` || got.StderrPath == nil || *got.StderrPath != "/logs/att-1.stderr.log" {
		t.Fatalf("attempt view: %+v", got)
	}
	none, err := store.ListAttempts(ctx, AttemptFilter{State: "exited"})
	if err != nil || len(none) != 0 {
		t.Fatalf("state filter: %+v %v", none, err)
	}
}

func TestAttemptLogPaths(t *testing.T) {
	store := openCoordinationStore(t)
	ctx := context.Background()
	execution, _, err := store.SubmitExecution(ctx, testExecutionSpec("train-1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.testExec(`INSERT INTO attempts(id,execution_id,state,executor_id,coordination_epoch,authorized_at,started_at) VALUES('att-1',$1,'running','executor',1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')`, execution.ID); err != nil {
		t.Fatal(err)
	}
	if stdout, stderr, err := store.AttemptLogPaths(ctx, "att-1"); err != nil || stdout != "" || stderr != "" {
		t.Fatalf("unrecorded paths: %q %q %v", stdout, stderr, err)
	}
	if err = store.SetAttemptLogPaths(ctx, "att-1", 1, "/logs/att-1.stdout.log", "/logs/att-1.stderr.log"); err != nil {
		t.Fatal(err)
	}
	if stdout, stderr, err := store.AttemptLogPaths(ctx, "att-1"); err != nil || stdout != "/logs/att-1.stdout.log" || stderr != "/logs/att-1.stderr.log" {
		t.Fatalf("recorded paths: %q %q %v", stdout, stderr, err)
	}
	if _, _, err := store.AttemptLogPaths(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown attempt: %v", err)
	}
}

// Lists longer than a page are read on with the cursor each page ends at:
// every row once, in the list's order, ties of the sort key broken by id
// and attempts with no time last.
func TestListsArePagedWithACursor(t *testing.T) {
	store := openCoordinationStore(t)
	ctx := context.Background()
	var project string
	for i, name := range []string{"e1", "e2", "e3", "e4", "e5"} {
		execution, _, err := store.SubmitExecution(ctx, testExecutionSpec(name))
		if err != nil {
			t.Fatal(err)
		}
		project = execution.ProjectScopeID
		at := []string{"2026-01-01T00:00:01Z", "2026-01-01T00:00:02Z", "2026-01-01T00:00:02Z", "2026-01-01T00:00:03Z", ""}[i]
		if at == "" {
			_, err = store.testExec(`INSERT INTO attempts(id,execution_id,state,executor_id,coordination_epoch) VALUES($1,$2,'running','executor',1)`, "att-"+name, execution.ID)
		} else {
			_, err = store.testExec(`INSERT INTO attempts(id,execution_id,state,executor_id,coordination_epoch,authorized_at,started_at) VALUES($1,$2,'running','executor',1,$3,$3)`, "att-"+name, execution.ID, at)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.testExec(`UPDATE execution_requests SET submitted_at='2026-01-01T00:00:00Z'`); err != nil {
		t.Fatal(err)
	}
	executions, err := store.ListExecutions(ctx, ExecutionFilter{ProjectScopeID: project, Limit: 1000})
	if err != nil || len(executions) != 5 {
		t.Fatalf("all executions: %d %v", len(executions), err)
	}
	var paged []string
	for after, pages := "", 0; ; pages++ {
		page, next, err := store.ListExecutionsPage(ctx, ExecutionFilter{ProjectScopeID: project, Limit: 2, After: after})
		if err != nil || pages > 3 {
			t.Fatalf("page %d: %v", pages, err)
		}
		for _, e := range page {
			paged = append(paged, e.ID)
		}
		if next == "" {
			break
		}
		after = next
	}
	for i := range executions {
		if i >= len(paged) || paged[i] != executions[i].ID {
			t.Fatalf("executions paged %v", paged)
		}
	}
	if len(paged) != 5 {
		t.Fatalf("executions paged %v", paged)
	}
	var attempts []string
	for after, pages := "", 0; ; pages++ {
		page, next, err := store.ListAttemptsPage(ctx, AttemptFilter{ProjectScopeID: project, Limit: 2, After: after})
		if err != nil || pages > 3 {
			t.Fatalf("page %d: %v", pages, err)
		}
		for _, a := range page {
			attempts = append(attempts, a.ID)
		}
		if next == "" {
			break
		}
		after = next
	}
	if got := strings.Join(attempts, ","); got != "att-e4,att-e2,att-e3,att-e1,att-e5" {
		t.Fatalf("attempts paged %s", got)
	}
	if _, _, err := store.ListAttemptsPage(ctx, AttemptFilter{After: "not-a-cursor"}); !errors.Is(err, ErrBadCursor) {
		t.Fatalf("a bad cursor: %v", err)
	}
}
