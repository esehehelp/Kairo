package store

import (
	"context"
	"errors"
	"maps"
	"testing"
)

// The digests were computed before env existed: an execution without env must
// keep its digest, or resubmitting it would be an idempotency conflict.
func TestExecutionSpecWithoutEnvKeepsItsDigest(t *testing.T) {
	_, _, plain, err := normalizeExecutionSpec(testExecutionSpec("pin"))
	if err != nil {
		t.Fatal(err)
	}
	if plain != "5ba34f7519c722dff5f40c160567a9de6b3717cd942bf7419b93f4db664861f6" {
		t.Fatalf("digest changed: %s", plain)
	}
	withEnv := testExecutionSpec("pin")
	withEnv.Env = map[string]string{}
	if _, _, digest, err := normalizeExecutionSpec(withEnv); err != nil || digest != plain {
		t.Fatalf("empty env changed the digest: %s %v", digest, err)
	}
	withEnv.Env = map[string]string{"PYTHONUTF8": "1"}
	if _, _, digest, err := normalizeExecutionSpec(withEnv); err != nil || digest == plain {
		t.Fatalf("env left out of the digest: %s %v", digest, err)
	}
}

func TestSubmitExecutionEnvReachesTheLaunch(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	ctx := context.Background()
	spec := schedulerExecutionSpec("env")
	spec.Env = map[string]string{"PYTHONUTF8": "1", "Path": `C:\tools`}
	for _, bad := range []map[string]string{{"CUDA_VISIBLE_DEVICES": "-1"}, {"KAIRO_TOKEN": "x"}} {
		invalid := schedulerExecutionSpec("bad-env")
		invalid.Env = bad
		if _, _, err := store.SubmitExecution(ctx, invalid); err == nil {
			t.Fatalf("accepted env %v", bad)
		}
	}
	execution, _, err := store.SubmitExecution(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	if !maps.Equal(execution.Env, spec.Env) {
		t.Fatalf("submitted env: %v", execution.Env)
	}
	if again, idempotent, err := store.SubmitExecution(ctx, spec); err != nil || !idempotent || again.ID != execution.ID {
		t.Fatalf("resubmit: %+v %v %v", again, idempotent, err)
	}
	changed := spec
	changed.Env = map[string]string{"PYTHONUTF8": "0"}
	if _, _, err := store.SubmitExecution(ctx, changed); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("changed env reused the request ID: %v", err)
	}
	listed, err := store.ListExecutions(ctx, ExecutionFilter{ProjectScopeID: execution.ProjectScopeID})
	if err != nil || len(listed) != 1 || !maps.Equal(listed[0].Env, spec.Env) {
		t.Fatalf("listed: %+v %v", listed, err)
	}
	reservation, err := store.ReserveNext(ctx, "executor")
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
	if !maps.Equal(launch.Env, spec.Env) {
		t.Fatalf("launch env: %v", launch.Env)
	}
}
