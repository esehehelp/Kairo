package store

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"testing"

	"kairo/internal/machine"
)

// A durable store below its reserve sheds copies of objects that have two
// other good copies, the biggest first, until the shortfall is covered; what
// it shed counts until the next report (no round sheds the same shortfall
// again); a copy that would leave fewer is kept, and its claim refused.
func TestADurableStoreBelowItsReserveShedsCopiesKeptElsewhere(t *testing.T) {
	reserve, packBytes := 2*shedMinShort+1000, 2*shedMinShort+200
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	addTestDisk(t, store, "node", "D:")
	ctx := context.Background()
	if err := store.UpsertExecutor(ctx, Executor{ID: "durable-exec", NodeID: "node", Kind: "windows", Attributes: json.RawMessage(`{"environment":"windows"}`), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	declareTestStore(t, store,
		machine.Store{ID: "c-durable", Class: machine.StoreDurable, Node: "node", Root: "C:/_kairo-durable", Filesystem: "D:", ReserveFreeBytes: reserve},
		machine.Store{ID: "e-durable", Class: machine.StoreDurable, Node: "node", Root: "E:/kairo", Filesystem: "D:"},
		machine.Store{ID: "f-durable", Class: machine.StoreDurable, Node: "node", Root: "F:/kairo", Filesystem: "D:"},
		machine.Store{ID: "d-scratch", Class: machine.StoreVolatile, Node: "node", Root: "D:/kairo", Filesystem: "D:"},
	)
	sha := strings.Repeat("aa", 32)
	verified := func(id, storeID, root string) {
		t.Helper()
		if _, err := store.testExec(`INSERT INTO object_copies(object_id,store_id,executor_id,root,month,state,path,manifest_path,created_at,updated_at,verified_at)
			VALUES($1,$2,'durable-exec',$3,'202611','verified',$4,$5,$6,$6,$6)`, id, storeID, root, root+"/objects/202611/"+id+".tar.zst", root+"/objects/202611/"+id+".tar.zst.files.tsv",
			"2000-11-0"+id[len(id)-1:]+"T00:00:00Z"); err != nil {
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
	if _, err := store.testExec(`UPDATE data_objects SET pack_bytes=$1 WHERE id='obj_1'`, packBytes); err != nil {
		t.Fatal(err)
	}
	orders := func() []CopyOrder {
		t.Helper()
		out, err := store.CopiesToMake(ctx, "durable-exec")
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	shedOrders := func() []CopyOrder {
		t.Helper()
		var out []CopyOrder
		for _, o := range orders() {
			if o.Shed {
				out = append(out, o)
			}
		}
		return out
	}
	if shed := shedOrders(); len(shed) != 0 {
		t.Fatalf("shed with room: %+v", shed)
	}
	observe := func(free int64) {
		t.Helper()
		total := 10 * reserve
		if err := store.RecordStoreObservation(ctx, StoreObservation{StoreID: "c-durable", ExecutorID: "durable-exec", Root: "C:/_kairo-durable", Usable: true, FreeBytes: &free, TotalBytes: &total}); err != nil {
			t.Fatal(err)
		}
	}
	// Dropping an obsolete order credits its bytes. Shedding still uses the
	// observation read before it, even if a report changes the room mid-pass.
	insertPacked(t, store, "obj_old", "executor", "d-scratch", "D:/kairo", sha)
	for _, query := range []string{
		`INSERT INTO object_copies(object_id,store_id,executor_id,root,month,state,created_at,updated_at)
			VALUES('obj_old','c-durable','durable-exec','C:/old','202611','copying','2026-11-01T00:00:00Z','2026-11-01T00:00:00Z')`,
		`CREATE FUNCTION change_room() RETURNS trigger LANGUAGE plpgsql AS $$
			BEGIN UPDATE store_observations SET free_bytes=0 WHERE store_id=OLD.store_id; RETURN OLD; END; $$`,
		`CREATE TRIGGER change_room AFTER DELETE ON object_copies FOR EACH ROW WHEN (OLD.object_id='obj_old') EXECUTE FUNCTION change_room()`,
	} {
		if _, err := store.testExec(query); err != nil {
			t.Fatal(err)
		}
	}
	observe(reserve - shedMinShort + 50)
	if shed := shedOrders(); len(shed) != 0 {
		t.Fatalf("shed after dropping an obsolete copy order: %+v", shed)
	}
	if _, err := store.testExec(`DELETE FROM data_objects WHERE id='obj_old'`); err != nil {
		t.Fatal(err)
	}
	// The shortfall exceeds the threshold. Obj_1 is the biggest that may go.
	observe(reserve - shedMinShort - 50)
	shed := shedOrders()
	if len(shed) != 1 || shed[0].ObjectID != "obj_1" || shed[0].StoreID != "c-durable" || shed[0].Month != "202611" || shed[0].Root != "C:/_kairo-durable" {
		t.Fatalf("shed %+v", shed)
	}
	if err := store.ClaimShed(ctx, "executor", "obj_1", "c-durable"); !errors.Is(err, ErrShedRefused) {
		t.Fatalf("claimed by an executor not serving the store: %v", err)
	}
	if err := store.ClaimShed(ctx, "durable-exec", "obj_1", "c-durable"); err != nil {
		t.Fatal(err)
	}
	if err := store.ClaimShed(ctx, "durable-exec", "obj_1", "c-durable"); err != nil {
		t.Fatalf("claimed again: %v", err)
	}
	var left int
	if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM object_copies WHERE object_id='obj_1'`).Scan(&left); err != nil || left != 2 {
		t.Fatalf("obj_1 copies left %d %v", left, err)
	}
	for _, id := range []string{"obj_4", "obj_5"} {
		insertPacked(t, store, id, "executor", "d-scratch", "D:/kairo", sha)
		verified(id, "e-durable", "E:/kairo")
		verified(id, "f-durable", "F:/kairo")
	}
	// No new report: what was shed counts. Nothing more goes, and there
	// are 150 bytes above the margin, enough for one 100-byte replica.
	if shed = shedOrders(); len(shed) != 0 {
		t.Fatalf("shed again on the same report: %+v", shed)
	}
	// obj_3 has one other copy: never shed
	if err := store.ClaimShed(ctx, "durable-exec", "obj_3", "c-durable"); !errors.Is(err, ErrShedRefused) {
		t.Fatalf("a copy leaving one other was claimed: %v", err)
	}
	checkReplicas := func(want string) {
		t.Helper()
		var ids []string
		for _, o := range copiesOnly(orders()) {
			if o.StoreID == "c-durable" {
				ids = append(ids, o.ObjectID)
			}
		}
		sort.Strings(ids)
		if got := strings.Join(ids, ","); got != want {
			t.Fatalf("replicas on c-durable: %s, want %s", got, want)
		}
	}
	checkReplicas("obj_4")
	// Even with room for all three, the store does not take back obj_1 yet.
	observe(reserve + shedMinShort + packBytes + 200)
	checkReplicas("obj_4,obj_5")
	if _, err := store.testExec(`UPDATE coordination_events SET created_at='2000-01-01T00:00:00Z' WHERE event_type='copy_shed' AND aggregate_id='obj_1'`); err != nil {
		t.Fatal(err)
	}
	checkReplicas("obj_1,obj_4,obj_5")
}
