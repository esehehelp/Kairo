package store

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	"kairo/internal/orchestration"
)

// addGangGPU puts a second GPU, gpu-b2, on node-b and observes it free.
func addGangGPU(t *testing.T, store *Store) {
	t.Helper()
	ctx := context.Background()
	if err := store.UpsertResourceInstance(ctx, ResourceInstance{ID: "gpu-b2", NodeID: "node-b", ProviderID: "gpu-b-provider", Kind: "gpu", StableIdentity: "gpu-b2", Binding: json.RawMessage(`{"index":1}`), AdminState: "enabled"}); err != nil {
		t.Fatal(err)
	}
	declareTestGPU(t, store, "gpu-b2", "node-b")
	total, free := int64(12<<30), int64(11<<30)
	observed := time.Now().UTC()
	if err := store.RecordObservation(ctx, Observation{ResourceID: "gpu-b2", ObservedAt: observed.Format(time.RFC3339Nano), ValidUntil: observed.Add(time.Minute).Format(time.RFC3339Nano), TotalBytes: &total, FreeBytes: &free}); err != nil {
		t.Fatal(err)
	}
}

// gangResourceProject declares the "gangs/ddp" task of gangProject with extra
// TOML appended to rank 1's override.
func gangResourceProject(t *testing.T, store *Store, taskExtra, rank1Extra string) {
	t.Helper()
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
cwd = "/mnt/d/Dev/neo-ime"
[tasks.execution.executor]
labels = { environment = "windows" }
[[tasks.execution.exclusive]]
kind = "gpu"
count = 1
`+taskExtra+`
[tasks.execution.gang]
size = 2
[[tasks.execution.gang.ranks]]
rank = 1
argv = ["linux-train"]
cwd = "/srv/neo-ime"
executor = { labels = { environment = "linux" } }
`+rank1Extra)
	ctx := context.Background()
	if _, err := store.ApplyProject(ctx, parsed); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
}

// rankRequests lists "type:kind=quantity" for each rank's resource requests.
func rankRequests(t *testing.T, store *Store) map[int][]string {
	t.Helper()
	rows, err := store.db.Query(`SELECT gm.rank,r.request_type,r.kind,r.quantity FROM gang_members gm JOIN resource_requests r ON r.execution_id=gm.execution_id ORDER BY gm.rank,r.request_type,r.kind`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[int][]string{}
	for rows.Next() {
		var rank int
		var requestType, kind string
		var quantity int64
		if err := rows.Scan(&rank, &requestType, &kind, &quantity); err != nil {
			t.Fatal(err)
		}
		out[rank] = append(out[rank], requestType+":"+kind+"="+strconv.FormatInt(quantity, 10))
	}
	return out
}

func TestGangRankExclusiveReplacesTheTasksForThatRank(t *testing.T) {
	store := openCoordinationStore(t)
	setupGangInventory(t, store)
	addGangGPU(t, store)
	gangResourceProject(t, store, "", `[[tasks.execution.gang.ranks.exclusive]]
kind = "gpu"
count = 2
same_node = true
`)
	rank0, rank1 := launchGang(t, store)
	if len(rank0.launch.Resources) != 1 || rank0.launch.Resources[0].ID != "gpu-a" {
		t.Fatalf("rank 0 resources: %+v", rank0.launch.Resources)
	}
	var ids []string
	for _, r := range rank1.launch.Resources {
		ids = append(ids, r.ID)
	}
	if strings.Join(ids, ",") != "gpu-b,gpu-b2" && strings.Join(ids, ",") != "gpu-b2,gpu-b" {
		t.Fatalf("rank 1 resources: %v", ids)
	}
}

func TestGangWithoutRankExclusiveAsksEveryNodeForTheSameGPUs(t *testing.T) {
	store := openCoordinationStore(t)
	setupGangInventory(t, store)
	addGangGPU(t, store)
	// count = 2 for every rank: node-a has one GPU, so the gang is never placed
	parsed := projectSpec(t, strings.Replace(`schema_version = 1
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
cwd = "/mnt/d/Dev/neo-ime"
[tasks.execution.executor]
labels = { environment = "windows" }
[[tasks.execution.exclusive]]
kind = "gpu"
count = COUNT
[tasks.execution.gang]
size = 2
[[tasks.execution.gang.ranks]]
rank = 1
executor = { labels = { environment = "linux" } }
`, "COUNT", "2", 1))
	ctx := context.Background()
	if _, err := store.ApplyProject(ctx, parsed); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	for _, executor := range []string{"exec-b", "exec-a"} {
		if r, err := store.ReserveNext(ctx, executor); err != nil || r != nil {
			t.Fatalf("%s reserved a gang no node-a GPU set can hold: %+v %v", executor, r, err)
		}
	}
}

func TestGangRankCapacityAndEmptyExclusiveAreStoredPerRank(t *testing.T) {
	store := openCoordinationStore(t)
	setupGangInventory(t, store)
	gangResourceProject(t, store, `[tasks.execution.capacity]
cpu_millis = 16000
ram_bytes = 50
`, `exclusive = []
[tasks.execution.gang.ranks.capacity]
cpu_millis = 8000
ram_bytes = 18
`)
	got := rankRequests(t, store)
	if strings.Join(got[0], " ") != "capacity:cpu=16000 capacity:ram=50 exclusive:gpu=1" {
		t.Fatalf("rank 0 requests: %v", got[0])
	}
	if strings.Join(got[1], " ") != "capacity:cpu=8000 capacity:ram=18" {
		t.Fatalf("rank 1 requests: %v", got[1])
	}
}

func TestGangRankResourcesAreValidatedAndLeaveOldDigestsAlone(t *testing.T) {
	base := schedulerExecutionSpec("digest")
	base.Gang = &GangSpec{Size: 2, Ranks: []GangRank{{Rank: 1, Argv: []string{"x"}}}}
	_, _, plain, err := normalizeExecutionSpec(base)
	if err != nil {
		t.Fatal(err)
	}
	// unset rank resources stay out of the digested JSON, so existing gangs keep their digests
	if body, _ := json.Marshal(base.Gang); strings.Contains(string(body), "exclusive") || strings.Contains(string(body), "capacity") {
		t.Fatalf("unset rank resources appear in the gang JSON: %s", body)
	}
	empty := []ExclusiveRequest{}
	withRank := base
	withRank.Gang = &GangSpec{Size: 2, Ranks: []GangRank{{Rank: 1, Argv: []string{"x"}, Exclusive: &empty}}}
	if _, _, digest, err := normalizeExecutionSpec(withRank); err != nil || digest == plain {
		t.Fatalf("rank exclusive did not change the digest: %v", err)
	}
	for _, bad := range []GangRank{
		{Rank: 1, Exclusive: &[]ExclusiveRequest{{Kind: "gpu", Count: 0}}},
		{Rank: 1, Exclusive: &[]ExclusiveRequest{{Kind: "gpu", Count: 1, OnExternalClaim: "maybe"}}},
		{Rank: 1, Capacity: &CapacityRequest{RAMBytes: -1}},
		{Rank: 1, Capacity: &CapacityRequest{Disks: []DiskRequest{{Filesystem: " "}}}},
	} {
		spec := base
		spec.Gang = &GangSpec{Size: 2, Ranks: []GangRank{bad}}
		if _, _, _, err := normalizeExecutionSpec(spec); err == nil {
			t.Fatalf("accepted invalid rank %+v", bad)
		}
	}
}

func TestGangRankGPUForbidsCUDAVisibleDevicesInTheTaskEnv(t *testing.T) {
	_, err := orchestration.Parse([]byte(`schema_version = 1
[project]
name = "gangs"
[[tasks]]
name = "ddp"
queue = "training"
depends_on = []
policy = "RunToCompletion@v1"
[tasks.execution]
schema_version = 2
argv = ["x"]
cwd = "/srv"
[tasks.execution.env]
CUDA_VISIBLE_DEVICES = "0"
[tasks.execution.gang]
size = 2
[[tasks.execution.gang.ranks]]
rank = 1
[[tasks.execution.gang.ranks.exclusive]]
kind = "gpu"
count = 1
`))
	if err == nil || !strings.Contains(err.Error(), "CUDA_VISIBLE_DEVICES") {
		t.Fatalf("a task env CUDA_VISIBLE_DEVICES reaching a GPU rank was accepted: %v", err)
	}
}
