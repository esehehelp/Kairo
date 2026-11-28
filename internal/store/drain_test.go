package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"kairo/internal/machine"
)

// Without replicas, an object keeps one durable copy. A drained store takes
// no copy and sheds, whatever its room, each copy whose object has a good
// copy on a store not drained, verified or read back within a scrub
// interval; an object whose copies are all on drained stores is first copied
// onto one that is not (read from the drained copy), and only then shed
// there. Drained of every copy, the store says so.
func TestADrainedStoreHandsItsCopiesOverAndEmpties(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	addTestDisk(t, store, "node", "D:")
	ctx := context.Background()
	if err := store.UpsertExecutor(ctx, Executor{ID: "durable-exec", NodeID: "node", Kind: "windows", Attributes: json.RawMessage(`{"environment":"windows"}`), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	stores := []machine.Store{
		{ID: "a-old", Class: machine.StoreDurable, Node: "node", Executor: "durable-exec", Root: "C:/_kairo-durable", Filesystem: "D:"},
		{ID: "e-durable", Class: machine.StoreDurable, Node: "node", Executor: "durable-exec", Root: "E:/kairo", Filesystem: "D:"},
		{ID: "f-durable", Class: machine.StoreDurable, Node: "node", Executor: "durable-exec", Root: "F:/kairo", Filesystem: "D:"},
		{ID: "d-scratch", Class: machine.StoreVolatile, Node: "node", Executor: "durable-exec", Root: "D:/kairo", Filesystem: "D:"},
	}
	declareTestStore(t, store, stores...)
	sha := strings.Repeat("aa", 32)
	verifiedAt := func(id, storeID, root, at string) {
		t.Helper()
		if _, err := store.testExec(`INSERT INTO object_copies(object_id,store_id,executor_id,root,month,state,path,manifest_path,created_at,updated_at,verified_at)
			VALUES($1,$2,'durable-exec',$3,'202611','verified',$4,$5,$6,$6,$6)`,
			id, storeID, root, root+"/objects/202611/"+id+".tar.zst", root+"/objects/202611/"+id+".tar.zst.files.tsv", at); err != nil {
			t.Fatal(err)
		}
	}
	verified := func(id, storeID, root string) { verifiedAt(id, storeID, root, now()) }
	// obj_both on a-old and e-durable, obj_old on a-old and on f-durable at a
	// root f no longer declares, obj_e on e-durable only, obj_stale on a-old
	// and on e-durable, there neither verified nor read back for a month
	for _, id := range []string{"obj_both", "obj_old", "obj_e", "obj_stale"} {
		insertPacked(t, store, id, "executor", "d-scratch", "D:/kairo", sha)
	}
	verified("obj_both", "a-old", "C:/_kairo-durable")
	verified("obj_both", "e-durable", "E:/kairo")
	verified("obj_old", "a-old", "C:/_kairo-durable")
	verified("obj_old", "f-durable", "F:/before")
	verified("obj_e", "e-durable", "E:/kairo")
	verified("obj_stale", "a-old", "C:/_kairo-durable")
	monthAgo := time.Now().UTC().Add(-30 * 24 * time.Hour).Format(time.RFC3339Nano)
	verifiedAt("obj_stale", "e-durable", "E:/kairo", monthAgo)
	orders := func() (copies, shed []CopyOrder) {
		t.Helper()
		out, err := store.CopiesToMake(ctx, "durable-exec")
		if err != nil {
			t.Fatal(err)
		}
		for _, o := range out {
			switch {
			case o.Shed:
				shed = append(shed, o)
			case !o.DescribeOnly:
				copies = append(copies, o)
			}
		}
		return copies, shed
	}
	// not drained and no replicas: nothing is copied again, nothing shed
	if copies, shed := orders(); len(copies) != 0 || len(shed) != 0 {
		t.Fatalf("one copy each, without replicas: copies %+v, shed %+v", copies, shed)
	}

	stores[0].Drain = true
	declareTestStore(t, store, stores...)
	free, total := int64(1)<<40, int64(1)<<41
	if err := store.RecordStoreObservation(ctx, StoreObservation{StoreID: "a-old", ExecutorID: "durable-exec", Root: "C:/_kairo-durable", Usable: true, FreeBytes: &free, TotalBytes: &total}); err != nil {
		t.Fatal(err)
	}
	// a new object goes past the drained store (the first by id)
	insertPacked(t, store, "obj_new", "executor", "d-scratch", "D:/kairo", sha)
	copies, shed := orders()
	got := map[string]string{}
	for _, o := range copies {
		got[o.ObjectID] = o.StoreID + " from " + o.Source.StoreID
	}
	// obj_old's only good copy at a declared root is on a-old: copied onto
	// one store not drained (the first), read from a-old
	if len(copies) != 2 || got["obj_new"] != "e-durable from d-scratch" || got["obj_old"] != "e-durable from a-old" {
		t.Fatalf("copies %v, want obj_new and obj_old (from a-old) onto e-durable", got)
	}
	if len(shed) != 1 || shed[0].ObjectID != "obj_both" || shed[0].StoreID != "a-old" {
		t.Fatalf("shed %+v, want obj_both off a-old (not obj_stale: its other copy was not read lately)", shed)
	}
	// obj_old is not shed before its copy elsewhere is verified, nor
	// obj_stale before its other copy is read back
	for _, id := range []string{"obj_old", "obj_stale"} {
		if err := store.ClaimShed(ctx, "durable-exec", id, "a-old"); !errors.Is(err, ErrShedRefused) {
			t.Fatalf("claim of %s: %v", id, err)
		}
	}
	if _, err := store.testExec(`UPDATE object_copies SET checked_at=$1 WHERE object_id='obj_stale' AND store_id='e-durable'`, now()); err != nil {
		t.Fatal(err)
	}
	if err := store.ClaimShed(ctx, "durable-exec", "obj_stale", "a-old"); err != nil {
		t.Fatalf("obj_stale, its other copy read back: %v", err)
	}
	if err := store.ClaimShed(ctx, "durable-exec", "obj_both", "a-old"); err != nil {
		t.Fatal(err)
	}
	status := func() string {
		t.Helper()
		list, err := store.ListStoreStatus(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, st := range list {
			if st.ID == "a-old" {
				return st.State
			}
		}
		return "missing"
	}
	if s := status(); s != "draining" {
		t.Fatalf("a-old with obj_old still on it is %s", s)
	}
	if err := store.RecordCopy(ctx, CopyMade{ObjectID: "obj_old", StoreID: "e-durable", ExecutorID: "durable-exec", Path: "E:/kairo/objects/202611/obj_old.tar.zst",
		ManifestPath: "E:/kairo/objects/202611/obj_old.tar.zst.files.tsv", Bytes: 100, SHA256: sha, ManifestSHA256: sha}); err != nil {
		t.Fatal(err)
	}
	if _, shed = orders(); len(shed) != 1 || shed[0].ObjectID != "obj_old" || shed[0].StoreID != "a-old" {
		t.Fatalf("shed %+v, want obj_old off a-old once on e-durable", shed)
	}
	if err := store.ClaimShed(ctx, "durable-exec", "obj_old", "a-old"); err != nil {
		t.Fatal(err)
	}
	if s := status(); s != "drained" {
		t.Fatalf("a-old without copies is %s", s)
	}
	if copies, shed = orders(); len(shed) != 0 {
		t.Fatalf("shed from a store not drained, without replicas: %+v", shed)
	}

	// an executor that serves no durable store but a drained one sheds too
	if err := store.UpsertExecutor(ctx, Executor{ID: "old-exec", NodeID: "node", Kind: "windows", Attributes: json.RawMessage(`{"environment":"windows"}`), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	stores = append(stores, machine.Store{ID: "b-old", Class: machine.StoreDurable, Node: "node", Executor: "old-exec", Root: "D:/_kairo-durable", Filesystem: "D:", Drain: true})
	declareTestStore(t, store, stores...)
	verified("obj_e", "b-old", "D:/_kairo-durable")
	out, err := store.CopiesToMake(ctx, "old-exec")
	if err != nil {
		t.Fatal(err)
	}
	var shedOld []CopyOrder
	for _, o := range out {
		if o.Shed {
			shedOld = append(shedOld, o)
		}
	}
	if len(shedOld) != 1 || shedOld[0].ObjectID != "obj_e" || shedOld[0].StoreID != "b-old" {
		t.Fatalf("orders of an executor serving only a drained store %+v, want obj_e shed off b-old", out)
	}

	// only a durable store is drained, and not one being moved
	for _, bad := range []machine.Store{
		{ID: "d-scratch", Class: machine.StoreVolatile, Node: "node", Root: "D:/kairo", Filesystem: "D:", Drain: true},
		{ID: "e-durable", Class: machine.StoreDurable, Node: "node", Root: "E:/kairo2", MovedFrom: "E:/kairo", Filesystem: "D:", Drain: true},
	} {
		if _, err := store.ApplyMachines(ctx, storesSpec(bad), true); err == nil || !strings.Contains(err.Error(), "drained") {
			t.Fatalf("%s drained: %v", bad.ID, err)
		}
	}
}
