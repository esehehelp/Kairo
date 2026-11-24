package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"kairo/internal/machine"
)

// Dropping data keeps only its file list: nothing changes without Confirm;
// with it, the object is temporal, every copy on a durable store is removed
// whatever room is there (the file list beside one sent and kept first, none
// removed before), and what was brought to a volatile store is evicted
// whatever room is there; only packed durable data is dropped.
func TestDroppedDataKeepsOnlyItsFileList(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	addTestDisk(t, store, "node", "D:")
	ctx := context.Background()
	if err := store.UpsertExecutor(ctx, Executor{ID: "durable-exec", NodeID: "node", Kind: "windows", Attributes: json.RawMessage(`{"environment":"windows"}`), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	declareTestStore(t, store,
		machine.Store{ID: "c-durable", Class: machine.StoreDurable, Node: "node", Executor: "durable-exec", Root: "C:/_kairo-durable", Filesystem: "D:"},
		machine.Store{ID: "e-durable", Class: machine.StoreDurable, Node: "node", Executor: "durable-exec", Root: "E:/kairo", Filesystem: "D:"},
		machine.Store{ID: "d-scratch", Class: machine.StoreVolatile, Node: "node", Executor: "durable-exec", Root: "D:/kairo", Filesystem: "D:"},
	)
	if _, err := store.testExec(`INSERT INTO coordination_scopes(id,kind,external_key,created_at,updated_at) VALUES('scp_corpus','project','corpus','2026-11-06T01:02:03Z','2026-11-06T01:02:03Z')`); err != nil {
		t.Fatal(err)
	}
	list := "data/part-0.bin\t10\t" + strings.Repeat("ab", 32) + "\t2026-11-06T01:02:03Z\n"
	sum := sha256.Sum256([]byte(list))
	listSHA := hex.EncodeToString(sum[:])
	sha := strings.Repeat("cd", 32)
	for _, id := range []string{"obj_mix", "obj_scratch"} {
		insertPacked(t, store, id, "executor", "d-scratch", "D:/kairo", sha)
		if _, err := store.testExec(`UPDATE data_objects SET project_scope_id='scp_corpus',name=$1,kind=$2,manifest_sha256=$3 WHERE id=$4`,
			"datasets/"+id, map[string]string{"obj_mix": ObjectImport, "obj_scratch": ObjectScratch}[id], listSHA, id); err != nil {
			t.Fatal(err)
		}
		for _, s := range []struct{ id, root string }{{"c-durable", "C:/_kairo-durable"}, {"e-durable", "E:/kairo"}} {
			if _, err := store.testExec(`INSERT INTO object_copies(object_id,store_id,executor_id,root,month,state,created_at,updated_at,verified_at)
				VALUES($1,$2,'durable-exec',$3,'202611','verified','2026-11-06T01:02:03Z','2026-11-06T01:02:03Z','2026-11-06T01:02:03Z')`, id, s.id, s.root); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := store.testExec(`INSERT INTO object_placements(object_id,store_id,executor_id,root,path,state,bytes,created_at,updated_at,ready_at)
		VALUES('obj_mix','d-scratch','durable-exec','D:/kairo','D:/kairo/inputs/obj_mix','ready',50,'2026-11-06T01:02:03Z','2026-11-06T01:02:03Z','2026-11-06T01:02:03Z')`); err != nil {
		t.Fatal(err)
	}
	lifecycle := func() (string, bool) {
		t.Helper()
		var l string
		var dropped bool
		if err := store.db.QueryRowContext(ctx, `SELECT lifecycle,dropped_at IS NOT NULL FROM data_objects WHERE id='obj_mix'`).Scan(&l, &dropped); err != nil {
			t.Fatal(err)
		}
		return l, dropped
	}

	if _, err := store.DropData(ctx, DropRequest{Project: "corpus", Objects: []string{"obj_none"}}, "tester"); !errors.Is(err, ErrDropRefused) {
		t.Fatalf("an object not there: %v", err)
	}
	// without Confirm, only what would go
	got, err := store.DropData(ctx, DropRequest{Project: "corpus", NamePrefix: "datasets/"}, "tester")
	if err != nil || len(got) != 2 {
		t.Fatalf("dry run %+v %v", got, err)
	}
	if got[0].ObjectID != "obj_mix" || got[0].Refused != "" || got[0].Dropped || got[0].Copies != 2 || got[0].Placements != 1 || got[1].Refused == "" {
		t.Fatalf("dry run %+v", got)
	}
	if l, dropped := lifecycle(); l != LifecycleDurable || dropped {
		t.Fatalf("a dry run changed the object: %s %v", l, dropped)
	}
	got, err = store.DropData(ctx, DropRequest{Project: "corpus", NamePrefix: "datasets/", Reason: "a mix: made again from raw", Confirm: true}, "tester")
	if err != nil || !got[0].Dropped || got[1].Dropped {
		t.Fatalf("drop %+v %v", got, err)
	}
	if l, dropped := lifecycle(); l != LifecycleTemporal || !dropped {
		t.Fatalf("dropped object %s %v", l, dropped)
	}

	// each copy is to go, its list sent first; none goes before it is kept
	orders, err := store.CopiesToMake(ctx, "durable-exec")
	if err != nil {
		t.Fatal(err)
	}
	var drops []CopyOrder
	for _, o := range orders {
		if o.Drop {
			drops = append(drops, o)
		} else if !o.DescribeOnly {
			t.Fatalf("an order other than a drop: %+v", o)
		}
	}
	if len(drops) != 2 || !drops[0].Shed || !drops[0].SendList || drops[0].ObjectID != "obj_mix" {
		t.Fatalf("drop orders %+v", drops)
	}
	if err := store.ClaimShed(ctx, "durable-exec", "obj_mix", "c-durable"); !errors.Is(err, ErrShedRefused) {
		t.Fatalf("a copy removed before the list is kept: %v", err)
	}
	if err := store.KeepIdentity(ctx, "durable-exec", "obj_mix", "c-durable", list+"extra\n"); !errors.Is(err, ErrIdentityRefused) {
		t.Fatalf("another list kept: %v", err)
	}
	if err := store.KeepIdentity(ctx, "durable-exec", "obj_scratch", "c-durable", list); !errors.Is(err, ErrIdentityRefused) {
		t.Fatalf("the list of an object not dropped kept: %v", err)
	}
	if err := store.KeepIdentity(ctx, "durable-exec", "obj_mix", "c-durable", list); err != nil {
		t.Fatal(err)
	}
	var kept string
	if err := store.db.QueryRowContext(ctx, `SELECT manifest FROM data_objects WHERE id='obj_mix'`).Scan(&kept); err != nil || kept != list {
		t.Fatalf("identity %q %v", kept, err)
	}
	if orders, err = store.CopiesToMake(ctx, "durable-exec"); err != nil {
		t.Fatal(err)
	}
	for _, o := range orders {
		if o.Drop && o.SendList {
			t.Fatalf("the list asked for again: %+v", o)
		}
	}
	// both copies go, though no other copy is left
	for _, s := range []string{"c-durable", "e-durable"} {
		if err := store.ClaimShed(ctx, "durable-exec", "obj_mix", s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	if orders, err = store.CopiesToMake(ctx, "durable-exec"); err != nil {
		t.Fatal(err)
	}
	for _, o := range orders {
		if o.ObjectID == "obj_mix" {
			t.Fatalf("an order for the dropped object once its copies are gone: %+v", o)
		}
	}

	// what was brought to the volatile store goes, though it has room
	evictions, err := store.EvictionsToMake(ctx, "durable-exec")
	if err != nil || len(evictions) != 1 || evictions[0].ObjectID != "obj_mix" || evictions[0].Kind != EvictPlacement {
		t.Fatalf("evictions %+v %v", evictions, err)
	}
	if err := store.SettleEviction(ctx, EvictionSettled{ObjectID: "obj_mix", Kind: EvictPlacement, StoreID: "d-scratch", ExecutorID: "durable-exec", Evicted: true}); err != nil {
		t.Fatal(err)
	}

	// a reader of it waits for the project to make it again and import it
	why, hard, err := regenerationWaitTx(ctx, store.db, "obj_mix", "node")
	if err != nil || !hard || !strings.Contains(why, "dropped data") {
		t.Fatalf("reader's wait %q %v %v", why, hard, err)
	}
	// dropping it again is no change
	if got, err = store.DropData(ctx, DropRequest{Project: "corpus", Objects: []string{"obj_mix"}, Confirm: true}, "tester"); err != nil || len(got) != 1 || !got[0].Dropped {
		t.Fatalf("again %+v %v", got, err)
	}
}
