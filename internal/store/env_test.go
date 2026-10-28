package store

import (
	"context"
	"encoding/json"
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
	ganged := testExecutionSpec("pin")
	ganged.Gang = &GangSpec{Size: 2, Ranks: []GangRank{{Rank: 1, Argv: []string{"linux-train"}, CWD: "/srv", ExecutorSelector: json.RawMessage(`{"labels":{"environment":"linux"}}`)}}}
	_, _, gang, err := normalizeExecutionSpec(ganged)
	if err != nil {
		t.Fatal(err)
	}
	if plain != "72c97292b3c56092c5a009e4607501505d05f11f51b52b427dd046bfd3b92b67" || gang != "18952961c32447398e9d1245ad2ad50492649f36d2a2c0392bfb102ec58fc271" {
		t.Fatalf("digests changed: %s %s", plain, gang)
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

// A ProjectSpec task's env is copied into its execution; a gang rank's env is
// merged over the task's for that rank only.
func TestGangRankEnvMergesOverTaskEnv(t *testing.T) {
	store := openCoordinationStore(t)
	setupGangInventory(t, store)
	parsed := projectSpec(t, `schema_version = 1
[project]
name = "gangs"
[[tasks]]
name = "ddp"
queue = "training"
depends_on = []
policy = "RunToCompletion@v1"
[tasks.execution]
schema_version = 2
argv = ["wsl-train"]
cwd = "/mnt/d/work"
[tasks.execution.executor]
labels = { environment = "windows" }
[tasks.execution.env]
PYTHONUTF8 = "1"
NCCL_DEBUG = "WARN"
[[tasks.execution.exclusive]]
kind = "gpu"
count = 1
[tasks.execution.gang]
size = 2
[[tasks.execution.gang.ranks]]
rank = 1
argv = ["linux-train"]
cwd = "/srv/work"
executor = { labels = { environment = "linux" } }
env = { NCCL_DEBUG = "INFO", GLOO_LOG = "1" }
`)
	ctx := context.Background()
	if _, err := store.ApplyProject(ctx, parsed); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	rank0, rank1 := launchGang(t, store)
	if want := map[string]string{"PYTHONUTF8": "1", "NCCL_DEBUG": "WARN"}; !maps.Equal(rank0.launch.Env, want) {
		t.Fatalf("rank 0 env: %v", rank0.launch.Env)
	}
	if want := map[string]string{"PYTHONUTF8": "1", "NCCL_DEBUG": "INFO", "GLOO_LOG": "1"}; !maps.Equal(rank1.launch.Env, want) {
		t.Fatalf("rank 1 env: %v", rank1.launch.Env)
	}
	member, err := store.GetExecution(ctx, rank1.launch.Execution.ID)
	if err != nil || member.Env["NCCL_DEBUG"] != "INFO" {
		t.Fatalf("rank 1 execution: %+v %v", member, err)
	}
}
