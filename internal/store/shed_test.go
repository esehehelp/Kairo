package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"kairo/internal/machine"
)

// A durable store below its reserve sheds copies of objects that have two
// other good copies, newest first, until the shortfall is covered; a copy
// that would leave fewer is kept, and its claim refused.
func TestADurableStoreBelowItsReserveShedsCopiesKeptElsewhere(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	addTestDisk(t, store, "node", "D:")
	ctx := context.Background()
	if err := store.UpsertExecutor(ctx, Executor{ID: "durable-exec", NodeID: "node", Kind: "windows", Attributes: json.RawMessage(`{"environment":"windows"}`), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	declareTestStore(t, store,
		machine.Store{ID: "c-durable", Class: machine.StoreDurable, Node: "node", Root: "C:/_kairo-durable", Filesystem: "D:", ReserveFreeBytes: 1000},
		machine.Store{ID: "e-durable", Class: machine.StoreDurable, Node: "node", Root: "E:/kairo", Filesystem: "D:"},
		machine.Store{ID: "f-durable", Class: machine.StoreDurable, Node: "node", Root: "F:/kairo", Filesystem: "D:"},
		machine.Store{ID: "d-scratch", Class: machine.StoreVolatile, Node: "node", Root: "D:/kairo", Filesystem: "D:"},
	)
	sha := strings.Repeat("aa", 32)
	verified := func(id, storeID, root string) {
		t.Helper()
		if _, err := store.testExec(`INSERT INTO object_copies(object_id,store_id,executor_id,root,month,state,path,manifest_path,created_at,updated_at,verified_at)
			VALUES($1,$2,'durable-exec',$3,'202611','verified',$4,$5,$6,$6,$6)`, id, storeID, root, root+"/objects/202611/"+id+".tar.zst", root+"/objects/202611/"+id+".tar.zst.files.tsv",
			"2026-11-0"+id[len(id)-1:]+"T00:00:00Z"); err != nil {
			t.Fatal(err)
		}
	}
	// obj_1 and obj_2 everywhere (obj_2 the newer on c), obj_3 on c and e only
	for _, id := range []string{"obj_1", "obj_2", "obj_3"} {
		insertPacked(t, store, id, "executor", "d-scratch", "D:/kairo", sha)
		verified(id, "c-durable", "C:/_kairo-durable")
		verified(id, "e-durable", "E:/kairo")
	}
	verified("obj_1", "f-durable", "F:/kairo")
	verified("obj_2", "f-durable", "F:/kairo")
	shedOrders := func() []CopyOrder {
		t.Helper()
		orders, err := store.CopiesToMake(ctx, "durable-exec")
		if err != nil {
			t.Fatal(err)
		}
		var out []CopyOrder
		for _, o := range orders {
			if o.Shed {
				out = append(out, o)
			}
		}
		return out
	}
	if shed := shedOrders(); len(shed) != 0 {
		t.Fatalf("shed with room: %+v", shed)
	}
	// 950 free, 1000 kept: 50 short, one pack of 100 covers it
	free, total := int64(950), int64(10000)
	if err := store.RecordStoreObservation(ctx, StoreObservation{StoreID: "c-durable", ExecutorID: "durable-exec", Root: "C:/_kairo-durable", Usable: true, FreeBytes: &free, TotalBytes: &total}); err != nil {
		t.Fatal(err)
	}
	shed := shedOrders()
	if len(shed) != 1 || shed[0].ObjectID != "obj_2" || shed[0].StoreID != "c-durable" || shed[0].Month != "202611" || shed[0].Root != "C:/_kairo-durable" {
		t.Fatalf("shed %+v", shed)
	}
	if err := store.ClaimShed(ctx, "executor", "obj_2", "c-durable"); !errors.Is(err, ErrShedRefused) {
		t.Fatalf("claimed by an executor not serving the store: %v", err)
	}
	if err := store.ClaimShed(ctx, "durable-exec", "obj_2", "c-durable"); err != nil {
		t.Fatal(err)
	}
	if err := store.ClaimShed(ctx, "durable-exec", "obj_2", "c-durable"); err != nil {
		t.Fatalf("claimed again: %v", err)
	}
	var left int
	if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM object_copies WHERE object_id='obj_2'`).Scan(&left); err != nil || left != 2 {
		t.Fatalf("obj_2 copies left %d %v", left, err)
	}
	// obj_3 has one other copy: never shed
	if err := store.ClaimShed(ctx, "durable-exec", "obj_3", "c-durable"); !errors.Is(err, ErrShedRefused) {
		t.Fatalf("a copy leaving one other was claimed: %v", err)
	}
}
