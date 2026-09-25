package store

import (
	"context"
	"testing"
)

func TestListAttemptsCarriesProgressAndLogPaths(t *testing.T) {
	store := openCoordinationStore(t)
	ctx := context.Background()
	execution, _, err := store.SubmitExecution(ctx, testExecutionSpec("train-1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.db.Exec(`INSERT INTO attempts(id,execution_id,state,executor_id,coordination_epoch,authorized_at,started_at,progress_json,stderr_path) VALUES('att-1',?,'running','executor',1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z','{"unit":"step","current":5}','/logs/att-1.stderr.log')`, execution.ID); err != nil {
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
