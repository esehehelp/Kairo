package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

// processClaim is a provider's external_process claim on resourceID of the
// process identity, with the lineage the provider found.
func processClaim(resourceID, identity, attemptID string, ancestors ...string) ExternalClaim {
	return ExternalClaim{ID: "claim-" + resourceID + "-process-" + identity, ResourceID: resourceID, ClaimKind: "external_process", ProcessIdentity: &identity, AttemptID: attemptID, Ancestors: ancestors}
}

// processPoll is a provider poll that reports GPU uuid as resourceID, fresh,
// with claims.
func processPoll(t *testing.T, store *Store, providerID, nodeID, resourceID, uuid string, claims ...ExternalClaim) {
	t.Helper()
	time.Sleep(2 * time.Millisecond) // (newer than the last coordination transition)
	stamp := time.Now().UTC()
	total, free := int64(24<<30), int64(20<<30)
	resources := []ResourceInstance{{ID: resourceID, NodeID: nodeID, ProviderID: providerID, Kind: "gpu", StableIdentity: uuid, Binding: json.RawMessage(`{}`)}}
	observations := []Observation{{ResourceID: resourceID, ObservedAt: stamp.Format(time.RFC3339Nano), ValidUntil: stamp.Add(time.Minute).Format(time.RFC3339Nano), TotalBytes: &total, FreeBytes: &free}}
	if err := store.ApplyObservationBatch(context.Background(), providerID, resources, observations, claims); err != nil {
		t.Fatal(err)
	}
}

func claimIdentities(t *testing.T, store *Store) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, c := range openClaims(t, store) {
		out[*c.ProcessIdentity] = c.ResourceID
	}
	return out
}

func TestTheLeaseHoldersProcessTreeIsNotAClaim(t *testing.T) {
	store, _, launch := startCooperativeExecution(t, "process-tree")
	processPoll(t, store, "gpu-provider", "node", "gpu-0", "GPU-0",
		processClaim("gpu-0", "launcher:100", ""),                           // the registered launcher itself
		processClaim("gpu-0", "maya:101", "", "launcher:100"),               // its child
		processClaim("gpu-0", "render:102", "", "maya:101", "launcher:100"), // a grandchild
		processClaim("gpu-0", "daemon:103", launch.Attempt.ID, "init:1"),    // reparented, carrying the attempt's id
		processClaim("gpu-0", "foreign:104", ""),
		processClaim("gpu-0", "foreign:105", "", "shell:9"),
	)
	got := claimIdentities(t, store)
	if len(got) != 2 || got["foreign:104"] != "gpu-0" || got["foreign:105"] != "gpu-0" {
		t.Fatalf("claims: %+v", got)
	}
	// a claim recorded before attribution is cleared once the poll attributes it
	processPoll(t, store, "gpu-provider", "node", "gpu-0", "GPU-0", processClaim("gpu-0", "maya:101", "", "launcher:100"))
	if got = claimIdentities(t, store); len(got) != 0 {
		t.Fatalf("claims: %+v", got)
	}
	// a quiescing attempt still owns its tree
	if _, err := store.testExec(`UPDATE attempts SET state='quiescing' WHERE id=$1`, launch.Attempt.ID); err != nil {
		t.Fatal(err)
	}
	processPoll(t, store, "gpu-provider", "node", "gpu-0", "GPU-0", processClaim("gpu-0", "maya:101", "", "launcher:100"))
	if got = claimIdentities(t, store); len(got) != 0 {
		t.Fatalf("claims of a quiescing attempt: %+v", got)
	}
}

func TestAProcessOfAnotherAttemptStaysAClaim(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	ctx := context.Background()
	if err := store.UpsertResourceInstance(ctx, ResourceInstance{ID: "gpu-1", NodeID: "node", ProviderID: "gpu-provider", Kind: "gpu", StableIdentity: "GPU-1", Binding: json.RawMessage(`{"index":1}`), AdminState: "enabled"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.testExec(`INSERT INTO machine_gpus(uuid,node_id,declared_at) VALUES('GPU-1','node',$1)`, now()); err != nil {
		t.Fatal(err)
	}
	processPoll(t, store, "gpu-provider", "node", "gpu-1", "GPU-1")
	// two attempts, one GPU each; the launcher of the one holding gpu-id is gpu-id:launcher
	launches := map[string]*Launch{}
	for _, request := range []string{"first", "second"} {
		if _, _, err := store.SubmitExecution(ctx, schedulerExecutionSpec(request)); err != nil {
			t.Fatal(err)
		}
		reservation, err := store.ReserveNext(ctx, "executor")
		if err != nil || reservation == nil || len(reservation.Resources) != 1 {
			t.Fatalf("reserve: %+v %v", reservation, err)
		}
		if err = store.MarkLeasePrepared(ctx, reservation.Lease.ID, reservation.Lease.CoordinationEpoch); err != nil {
			t.Fatal(err)
		}
		launch, err := store.AuthorizeLaunch(ctx, reservation)
		if err != nil {
			t.Fatal(err)
		}
		gpu := reservation.Resources[0].ID
		if err = store.ActivateLaunch(ctx, launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch, launch.AuthorizationToken, 100, gpu+":launcher"); err != nil {
			t.Fatal(err)
		}
		launches[gpu] = launch
	}
	other := launches["gpu-1"]
	if launches["gpu-0"] == nil || other == nil {
		t.Fatalf("leases: %+v", launches)
	}
	// the other attempt's processes, by ancestry and by attempt id, on gpu-0 and on its own gpu-1
	for _, gpu := range []string{"gpu-0", "gpu-1"} {
		processPoll(t, store, "gpu-provider", "node", gpu, "GPU-"+gpu[len("gpu-"):],
			processClaim(gpu, "child:10", "", "gpu-1:launcher"),
			processClaim(gpu, "env:11", other.Attempt.ID),
		)
	}
	got := claimIdentities(t, store)
	if len(got) != 2 || got["child:10"] != "gpu-0" || got["env:11"] != "gpu-0" {
		t.Fatalf("claims: %+v", got)
	}
}

func TestADescendantOutlivingItsAttemptKeepsTheLease(t *testing.T) {
	store, _, launch := startCooperativeExecution(t, "outliving-descendant")
	ctx := context.Background()
	epoch := launch.Lease.CoordinationEpoch
	descendants := func() []ExternalClaim {
		return []ExternalClaim{processClaim("gpu-0", "maya:101", "", "launcher:100"), processClaim("gpu-0", "orphan:102", launch.Attempt.ID)}
	}
	processPoll(t, store, "gpu-provider", "node", "gpu-0", "GPU-0", descendants()...)
	if got := claimIdentities(t, store); len(got) != 0 {
		t.Fatalf("claims while the attempt runs: %+v", got)
	}
	if err := store.RecordTerminal(ctx, launch.Attempt.ID, launch.Lease.ID, epoch, 0, ""); err != nil {
		t.Fatal(err)
	}
	// the launcher is gone, its descendants are not: they are claims again
	processPoll(t, store, "gpu-provider", "node", "gpu-0", "GPU-0", descendants()...)
	if got := claimIdentities(t, store); len(got) != 2 {
		t.Fatalf("claims after the attempt exited: %+v", got)
	}
	if err := store.FinalizeQuiescence(ctx, launch.Attempt.ID, launch.Lease.ID, epoch); err == nil {
		t.Fatal("lease released while a descendant of its attempt still uses the GPU")
	}
	var leaseState string
	if err := store.db.QueryRow(`SELECT state FROM leases WHERE id=$1`, launch.Lease.ID).Scan(&leaseState); err != nil || leaseState != "releasing" {
		t.Fatalf("lease state %q, %v", leaseState, err)
	}
	processPoll(t, store, "gpu-provider", "node", "gpu-0", "GPU-0")
	if err := store.FinalizeQuiescence(ctx, launch.Attempt.ID, launch.Lease.ID, epoch); err != nil {
		t.Fatal(err)
	}
}

func TestTheLeaseHoldersProcessSeenOnAnotherNodesRowIsNotAClaim(t *testing.T) {
	store := openCoordinationStore(t)
	setupSharedGPU(t, store)
	launch := startShared(t, store, "on-a", "a", 10)
	// b's provider sees a's processes on its row of the device
	processPoll(t, store, "b-gpu", "b", "gpu-GPU-S", "GPU-S",
		processClaim("gpu-GPU-S", "a:4321:start:1", ""),
		processClaim("gpu-GPU-S", "child:4322", "", "a:4321:start:1"),
		processClaim("gpu-GPU-S", "env:4323", launch.Attempt.ID),
		processClaim("gpu-GPU-S", "foreign:77", ""),
	)
	got := claimIdentities(t, store)
	if len(got) != 1 || got["foreign:77"] != "gpu-GPU-S@b" {
		t.Fatalf("claims: %+v", got)
	}
}
