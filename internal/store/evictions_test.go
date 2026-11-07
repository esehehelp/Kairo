package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"kairo/internal/datainput"
)

// A volatile store whose free space is below its floor (a tenth of its total
// here) has things evicted until it would be above it: what was brought
// there first, then temporal data, each least recently used (read, made or
// brought) first, never one an execution not yet ended reads. Each is claimed (and no longer read where it
// is) until settled: evicted (a placement forgotten, a temporal object
// evicted, its node to be trimmed) or kept (present again, not ordered again
// until room runs short again, after the others).
func TestAStoreShortOfRoomEvictsWhatCanBeHadAgain(t *testing.T) {
	store := importFixture(t)
	ctx := context.Background()
	if _, err := store.ApplyProject(ctx, projectSpec(t, `schema_version = 1
[project]
name = "imp"
[[tasks]]
name = "build"
queue = "q"
depends_on = []
policy = "RunToCompletion@v1"
[tasks.execution]
schema_version = 2
argv = ["build-dataset"]
cwd = "/work"
regenerable = true
`)); err != nil {
		t.Fatal(err)
	}
	ids := map[string]string{}
	for _, name := range []string{"t1", "t2", "t3"} {
		imported, err := store.ImportData(ctx, ImportRequest{Project: "imp", Node: "node", Paths: []string{"D:/data/" + name}, Name: name, RegenerableBy: "build"})
		if err != nil {
			t.Fatal(err)
		}
		ids[name] = imported[0].ObjectID
	}
	imported, err := store.ImportData(ctx, ImportRequest{Project: "imp", Node: "node", Paths: []string{"D:/data/dur"}, Name: "dur"})
	if err != nil {
		t.Fatal(err)
	}
	ids["dur"] = imported[0].ObjectID
	orders, err := store.PacksToMake(ctx, "win-exec")
	if err != nil || len(orders) != 4 {
		t.Fatalf("packs %+v %v", orders, err)
	}
	sizes := map[string]int64{"t1": 100, "t2": 200, "t3": 300}
	for _, o := range orders {
		list := "path\tbytes\tsha256\tmtime_ns\nf\t1\t" + strings.Repeat("ab", 32) + "\t1\n"
		sum := sha256.Sum256([]byte(list))
		made := PackMade{ObjectID: o.ObjectID, ExecutorID: "win-exec", ManifestPath: "D:/kairo/packs/" + o.ObjectID + ".tar.zst.files.tsv", Files: 1, ManifestSHA256: hex.EncodeToString(sum[:])}
		if o.Identity {
			for name, id := range ids {
				if id == o.ObjectID {
					made.ContentBytes = sizes[name]
				}
			}
			made.Identity, made.Manifest = true, list
		} else {
			made.PackPath, made.PackBytes, made.ContentBytes, made.PackSHA256 = "D:/kairo/packs/"+o.ObjectID+".tar.zst", 40, 50, strings.Repeat("cd", 32)
		}
		if err := store.RecordPack(ctx, made); err != nil {
			t.Fatal(err)
		}
	}
	// the durable one brought to the store (as a fetch would), t1 read lately
	stamp := now()
	if _, err := store.testExec(`INSERT INTO object_placements(object_id,store_id,executor_id,root,path,state,bytes,created_at,updated_at,ready_at) VALUES($1,'d-scratch','win-exec','D:/kairo',$2,'ready',50,$3,$3,$3)`,
		ids["dur"], "D:/kairo/inputs/"+ids["dur"], stamp); err != nil {
		t.Fatal(err)
	}
	if _, err := store.testExec(`UPDATE data_objects SET last_used_at=$1 WHERE id=$2`, stamp, ids["t1"]); err != nil {
		t.Fatal(err)
	}
	// t2 read by an execution waiting
	reader := schedulerExecutionSpec("reader")
	reader.Scope.Project = "imp"
	reader.Exclusive, reader.Checkpointable, reader.Preemptible = []ExclusiveRequest{}, false, false
	reader.Inputs = []datainput.Input{{Name: "data", Data: "t2"}}
	if _, _, err := store.SubmitExecution(ctx, reader); err != nil {
		t.Fatal(err)
	}
	evictions := func() []string {
		t.Helper()
		orders, err := store.EvictionsToMake(ctx, "win-exec")
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for _, o := range orders {
			name := ""
			for n, id := range ids {
				if id == o.ObjectID {
					name = n
				}
			}
			if o.StoreID != "d-scratch" || o.Root != "D:/kairo" {
				t.Fatalf("order %+v", o)
			}
			got = append(got, o.Kind+":"+name)
		}
		return got
	}
	observe := func(free int64) {
		t.Helper()
		total := int64(10000)
		if err := store.RecordStoreObservation(ctx, StoreObservation{StoreID: "d-scratch", ExecutorID: "win-exec", Root: "D:/kairo", Usable: true, TotalBytes: &total, FreeBytes: &free}); err != nil {
			t.Fatal(err)
		}
	}
	if got := evictions(); len(got) != 0 {
		t.Fatalf("evicted with no report of the store: %v", got)
	}
	observe(1500)
	if got := evictions(); len(got) != 0 {
		t.Fatalf("evicted above the floor: %v", got)
	}
	// 200 short: the placement (50), then t3 (300; t2 is read, t1 read lately)
	observe(800)
	want := "placement:dur,temporal:t3"
	if got := strings.Join(evictions(), ","); got != want {
		t.Fatalf("evictions %s, want %s", got, want)
	}
	if got := strings.Join(evictions(), ","); got != want {
		t.Fatalf("asked again: %s, want %s (nothing more while they are claimed)", got, want)
	}
	declared, err := declaredStores(ctx, store.db)
	if err != nil {
		t.Fatal(err)
	}
	var projectID string
	if err := store.db.QueryRowContext(ctx, `SELECT project_scope_id FROM data_objects WHERE id=$1`, ids["t3"]).Scan(&projectID); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"t3", "dur"} {
		src, err := inputSourceTx(ctx, store.db, declared, projectID, datainput.Input{Name: "x", Data: name})
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range src.places {
			if p.storeID == "d-scratch" || name == "t3" {
				t.Fatalf("%s read where it is being evicted: %+v", name, src.places)
			}
		}
	}
	if err := store.SettleEviction(ctx, EvictionSettled{ObjectID: ids["t3"], Kind: EvictTemporal, StoreID: "d-scratch", ExecutorID: "executor", Evicted: true}); err == nil {
		t.Fatal("settled by an executor not its own")
	}
	for _, s := range []EvictionSettled{
		{ObjectID: ids["dur"], Kind: EvictPlacement, StoreID: "d-scratch", ExecutorID: "win-exec", Evicted: true},
		{ObjectID: ids["t3"], Kind: EvictTemporal, StoreID: "d-scratch", ExecutorID: "win-exec", Evicted: true},
		{ObjectID: ids["t3"], Kind: EvictTemporal, StoreID: "d-scratch", ExecutorID: "win-exec", Evicted: true},
	} {
		if err := store.SettleEviction(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	var placements int
	var sourceState string
	var trim int64
	if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM object_placements`).Scan(&placements); err != nil || placements != 0 {
		t.Fatalf("placements %d %v", placements, err)
	}
	if err := store.db.QueryRowContext(ctx, `SELECT source_state FROM data_objects WHERE id=$1`, ids["t3"]).Scan(&sourceState); err != nil || sourceState != "evicted" {
		t.Fatalf("t3 %s %v", sourceState, err)
	}
	if err := store.db.QueryRowContext(ctx, `SELECT requested_bytes FROM node_trims WHERE node_id='node'`).Scan(&trim); err != nil || trim != 350 {
		t.Fatalf("trim %d %v", trim, err)
	}
	// still short as last reported: t1 next (t2 is read); kept, it is not
	// ordered again for a while
	if got := strings.Join(evictions(), ","); got != "temporal:t1" {
		t.Fatalf("evictions %s", got)
	}
	if err := store.SettleEviction(ctx, EvictionSettled{ObjectID: ids["t1"], Kind: EvictTemporal, StoreID: "d-scratch", ExecutorID: "win-exec", Reason: "not evicted: changed"}); err != nil {
		t.Fatal(err)
	}
	var reason string
	if err := store.db.QueryRowContext(ctx, `SELECT source_state,COALESCE(source_reason,'') FROM data_objects WHERE id=$1`, ids["t1"]).Scan(&sourceState, &reason); err != nil || sourceState != "present" || reason != "not evicted: changed" {
		t.Fatalf("t1 %s %q %v", sourceState, reason, err)
	}
	if got := evictions(); len(got) != 0 {
		t.Fatalf("kept, ordered again at once: %v", got)
	}
	if _, err := store.testExec(`UPDATE data_objects SET updated_at='2026-01-01T00:00:00Z' WHERE id=$1`, ids["t1"]); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(evictions(), ","); got != "temporal:t1" {
		t.Fatalf("an hour on: %s", got)
	}
	if err := store.SettleEviction(ctx, EvictionSettled{ObjectID: "obj_x", Kind: "other", StoreID: "d-scratch", ExecutorID: "win-exec"}); err == nil {
		t.Fatal("an eviction of no kind settled")
	}
}
