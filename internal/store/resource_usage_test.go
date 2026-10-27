package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestResourceUsageShowsObservationReservationAndHolder(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	ctx := context.Background()
	if err := store.UpsertResourceInstance(ctx, ResourceInstance{ID: "ram", NodeID: "node", ProviderID: "gpu-provider", Kind: "ram", StableIdentity: "physical-memory", Binding: json.RawMessage(`{}`), AdminState: "enabled"}); err != nil {
		t.Fatal(err)
	}
	total, free := int64(64<<30), int64(40<<30)
	observed := time.Now().UTC()
	if err := store.RecordObservation(ctx, Observation{ResourceID: "ram", ObservedAt: observed.Format(time.RFC3339Nano), ValidUntil: observed.Add(time.Minute).Format(time.RFC3339Nano), TotalBytes: &total, FreeBytes: &free}); err != nil {
		t.Fatal(err)
	}
	usage, err := store.ListResourceUsage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if u := usage["gpu-0"]; u.TotalBytes == nil || *u.TotalBytes != 24<<30 || *u.FreeBytes != 23<<30 || u.Holders != nil {
		t.Fatalf("idle gpu: %+v", u)
	}
	if _, _, err = store.SubmitExecution(ctx, ExecutionSpec{
		ClientRequestID: "train", Scope: ScopePath{Project: "p", Queue: "q", Task: "train"}, Argv: []string{"train"}, CWD: ".",
		ExecutorSelector: json.RawMessage(`{"environment":"wsl2"}`),
		Exclusive:        []ExclusiveRequest{{Kind: "gpu", Count: 1}},
		Capacity:         CapacityRequest{RAMBytes: 16 << 30},
	}); err != nil {
		t.Fatal(err)
	}
	launch := launchNext(t, store)
	if usage, err = store.ListResourceUsage(ctx); err != nil {
		t.Fatal(err)
	}
	want := ResourceHolder{ExecutionID: launch.Execution.ID, LeaseState: "active", Quantity: 1, Project: "p", Queue: "q", Task: "train"}
	if u := usage["gpu-0"]; len(u.Holders) != 1 || u.Holders[0] != want || u.Reserved != 0 {
		t.Fatalf("leased gpu: %+v", u)
	}
	want.Quantity = 16 << 30
	if u := usage["ram"]; u.Reserved != 16<<30 || len(u.Holders) != 1 || u.Holders[0] != want || *u.FreeBytes != 40<<30 {
		t.Fatalf("reserved ram: %+v", u)
	}
	finishLaunch(t, store, launch, 0)
	if usage, err = store.ListResourceUsage(ctx); err != nil {
		t.Fatal(err)
	}
	if u := usage["ram"]; u.Reserved != 0 || u.Holders != nil {
		t.Fatalf("released ram: %+v", u)
	}
}
