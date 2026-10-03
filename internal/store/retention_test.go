package store

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestPruneObservationsKeepsEachResourcesLatest(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	ctx := context.Background()
	if err := store.UpsertResourceInstance(ctx, ResourceInstance{ID: "gpu-1", NodeID: "node", ProviderID: "gpu-provider", Kind: "gpu", StableIdentity: "GPU-1", AdminState: "enabled"}); err != nil {
		t.Fatal(err)
	}
	old := time.Now().UTC().Add(-3 * time.Hour)
	observe := func(resource string, at time.Time) {
		t.Helper()
		if err := store.RecordObservation(ctx, Observation{ResourceID: resource, ObservedAt: at.Format(time.RFC3339Nano), ValidUntil: at.Add(15 * time.Second).Format(time.RFC3339Nano)}); err != nil {
			t.Fatal(err)
		}
	}
	// gpu-0: many old rows and fresh ones; gpu-1: only old rows (its latest must survive).
	for i := 0; i < 12000; i++ {
		observe("gpu-0", old.Add(time.Duration(i)*time.Millisecond))
	}
	for i := 0; i < 3; i++ {
		observe("gpu-1", old.Add(time.Duration(i)*time.Second))
	}
	fresh := time.Now().UTC()
	observe("gpu-0", fresh.Add(-time.Minute))
	observe("gpu-0", fresh)

	count := func(resource string) int {
		var n int
		if err := store.db.QueryRow(`SELECT COUNT(*) FROM resource_observations WHERE resource_id=?`, resource).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	before0 := count("gpu-0")
	removed, err := store.PruneObservations(ctx, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	// gpu-0 keeps its two fresh rows (setupSchedulerInventory's row is fresh too);
	// gpu-1 keeps only its latest.
	if got := count("gpu-1"); got != 1 {
		t.Fatalf("gpu-1 kept %d rows, want its latest only", got)
	}
	if got := count("gpu-0"); got != before0-12000 {
		t.Fatalf("gpu-0 kept %d of %d rows (removed %d)", got, before0, removed)
	}
	if removed != 12000+2 {
		t.Fatalf("removed %d rows", removed)
	}
	var latestGPU1 string
	if err := store.db.QueryRow(`SELECT observed_at FROM resource_observations WHERE resource_id='gpu-1'`).Scan(&latestGPU1); err != nil {
		t.Fatal(err)
	}
	if latestGPU1 != old.Add(2*time.Second).Format(time.RFC3339Nano) {
		t.Fatalf("gpu-1 kept %s, not its latest", latestGPU1)
	}
	// Nothing left to prune: a second pass removes nothing.
	if removed, err = store.PruneObservations(ctx, time.Hour); err != nil || removed != 0 {
		t.Fatalf("second pass removed %d (%v)", removed, err)
	}
	// Resource status still reads the latest observation of each resource.
	resources, err := store.ListResourceStatus(ctx)
	if err != nil || len(resources) != 2 {
		t.Fatalf("status after pruning: %+v %v", resources, err)
	}
}

func TestCompactShrinksTheFileAndKeepsTheLatest(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	ctx := context.Background()
	old := time.Now().UTC().Add(-3 * time.Hour)
	for i := 0; i < 20000; i++ {
		at := old.Add(time.Duration(i) * time.Millisecond)
		if err := store.RecordObservation(ctx, Observation{ResourceID: "gpu-0", ObservedAt: at.Format(time.RFC3339Nano), ValidUntil: at.Add(15 * time.Second).Format(time.RFC3339Nano), Evidence: []byte(`{"padding":"` + strings.Repeat("x", 200) + `"}`)}); err != nil {
			t.Fatal(err)
		}
	}
	pages := func() int {
		var n int
		if err := store.db.QueryRow(`PRAGMA page_count`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	before := pages()
	if _, err := store.Compact(ctx); err != nil {
		t.Fatal(err)
	}
	if after := pages(); after*2 > before {
		t.Fatalf("pages %d -> %d: not compacted", before, after)
	}
	var n int
	// The fresh row from setupSchedulerInventory and the latest (highest id) old one.
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM resource_observations WHERE resource_id='gpu-0'`).Scan(&n); err != nil || n != 2 {
		t.Fatalf("gpu-0 rows after compact: %d %v", n, err)
	}
}
