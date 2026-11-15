package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"kairo/internal/machine"
)

// storeOf is the store id of a node's stores, as an agent is told them.
func storeOf(t *testing.T, store *Store, id string) machine.Store {
	t.Helper()
	stores, err := store.NodeStores(context.Background(), "node")
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range stores {
		if s.ID == id {
			return s
		}
	}
	t.Fatalf("store %s is not among %+v", id, stores)
	return machine.Store{}
}

// recordedRoot is the root (and path) a row of table is recorded at.
func recordedRoot(t *testing.T, store *Store, query string) (root, path string) {
	t.Helper()
	if err := store.db.QueryRowContext(context.Background(), query).Scan(&root, &path); err != nil {
		t.Fatal(err)
	}
	return root, path
}

// A store declared again at another root with moved_from naming where it is
// is being moved: it must name where the store is and change nothing else,
// and its root cannot change again meanwhile. Its node is told what still
// uses it there; its records stay at the old root until its executor says it
// moved it, then follow it there, in its paths' own spelling; the
// declaration applied again changes nothing.
func TestAStoreIsMovedAndItsRecordsFollow(t *testing.T) {
	store := importFixture(t)
	ctx := context.Background()
	sha := strings.Repeat("ab", 32)
	insertPacked(t, store, "obj_1", "win-exec", "d-scratch", "D:/kairo", sha)
	insertPacked(t, store, "obj_2", "win-exec", "d-scratch", "D:/kairo", sha)
	if _, err := store.testExec(`UPDATE data_objects SET source_path='D:\kairo\outputs\run_1',pack_path='D:\kairo\packs\obj_1.tar.zst',manifest_path='D:\kairo\packs\obj_1.tar.zst.files.tsv' WHERE id='obj_1'`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.testExec(`INSERT INTO object_copies(object_id,store_id,executor_id,root,month,state,path,manifest_path,created_at,updated_at,verified_at)
		VALUES('obj_1','e-durable','win-exec','E:/kairo','202611','verified','E:/kairo/objects/202611/obj_1.tar.zst','E:/kairo/objects/202611/obj_1.tar.zst.files.tsv','2026-11-06T01:02:03Z','2026-11-06T01:02:03Z','2026-11-06T01:02:03Z')`); err != nil {
		t.Fatal(err)
	}
	scratch := machine.Store{ID: "d-scratch", Class: machine.StoreVolatile, Node: "node", Root: "D:/kairo", Filesystem: "D:"}
	durable := machine.Store{ID: "e-durable", Class: machine.StoreDurable, Node: "node", Root: "E:/kairo", Filesystem: "D:"}
	moving := durable
	moving.Root, moving.MovedFrom = "E:/_kairo-durable", "E:/kairo"

	for _, c := range []struct {
		name   string
		stores []machine.Store
		want   string
	}{
		{"not where it is", []machine.Store{scratch, func() machine.Store { s := moving; s.MovedFrom = "E:/elsewhere"; return s }()}, "is not where the store is (E:/kairo)"},
		{"something else changed", []machine.Store{scratch, func() machine.Store { s := moving; s.ReserveFreeBytes = 1; return s }()}, "changes nothing but its root"},
		{"a new store", []machine.Store{scratch, durable, {ID: "f-new", Class: machine.StoreVolatile, Node: "node", Root: "D:/_new", Filesystem: "D:", MovedFrom: "D:/old"}}, "is not declared yet"},
	} {
		if _, err := store.ApplyMachines(ctx, storesSpec(c.stores...), false); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("%s: %v", c.name, err)
		}
	}
	if _, err := store.ApplyMachines(ctx, storesSpec(scratch, moving), false); err != nil {
		t.Fatal(err)
	}
	if s := storeOf(t, store, "e-durable"); s.Root != "E:/_kairo-durable" || s.MovedFrom != "E:/kairo" || s.MoveWait != "" {
		t.Fatalf("being moved: %+v", s)
	}
	// what still uses it where it is holds the move
	if _, err := store.testExec(`INSERT INTO object_copies(object_id,store_id,executor_id,root,month,state,created_at,updated_at)
		VALUES('obj_2','e-durable','win-exec','E:/kairo','202611','copying','2026-11-06T01:02:03Z','2026-11-06T01:02:03Z')`); err != nil {
		t.Fatal(err)
	}
	if s := storeOf(t, store, "e-durable"); s.MoveWait != "1 copies being made there" {
		t.Fatalf("a copy being made: %+v", s)
	}
	if _, err := store.testExec(`DELETE FROM object_copies WHERE object_id='obj_2'`); err != nil {
		t.Fatal(err)
	}
	// its root cannot change again meanwhile; the same declaration is no change
	third := durable
	third.Root = "E:/third"
	if _, err := store.ApplyMachines(ctx, storesSpec(scratch, third), false); !errors.Is(err, ErrMachineConflict) || !strings.Contains(err.Error(), "is being moved from E:/kairo to E:/_kairo-durable") {
		t.Fatalf("another root while moved: %v", err)
	}
	if again, err := store.ApplyMachines(ctx, storesSpec(scratch, moving), false); err != nil || len(again.StoreChanges) != 0 || again.UnchangedStores != 2 {
		t.Fatalf("the same declaration again: %+v %v", again, err)
	}
	statuses, err := store.ListStoreStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, st := range statuses {
		if st.ID == "e-durable" && st.State != "moving" {
			t.Fatalf("status %+v", st)
		}
	}

	// not moved yet: the records stay
	if err = store.RecordStoreObservation(ctx, StoreObservation{StoreID: "e-durable", ExecutorID: "win-exec", Root: "E:/_kairo-durable", Problem: "being moved"}); err != nil {
		t.Fatal(err)
	}
	if root, _ := recordedRoot(t, store, `SELECT root,path FROM object_copies WHERE object_id='obj_1'`); root != "E:/kairo" {
		t.Fatalf("a copy followed a store not moved yet: %s", root)
	}
	// moved: they follow
	if err = store.RecordStoreObservation(ctx, StoreObservation{StoreID: "e-durable", ExecutorID: "win-exec", Root: "E:/_kairo-durable", Usable: true, MovedFrom: "E:/kairo"}); err != nil {
		t.Fatal(err)
	}
	if root, path := recordedRoot(t, store, `SELECT root,path FROM object_copies WHERE object_id='obj_1'`); root != "E:/_kairo-durable" || path != "E:/_kairo-durable/objects/202611/obj_1.tar.zst" {
		t.Fatalf("the copy is recorded at %s %s", root, path)
	}
	if s := storeOf(t, store, "e-durable"); s.MovedFrom != "" || s.Root != "E:/_kairo-durable" {
		t.Fatalf("moved: %+v", s)
	}
	if again, err := store.ApplyMachines(ctx, storesSpec(scratch, moving), false); err != nil || len(again.StoreChanges) != 0 {
		t.Fatalf("the declaration that moved it, again: %+v %v", again, err)
	}
	var events int
	if err = store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM coordination_events WHERE event_type='store_moved'`).Scan(&events); err != nil || events != 1 {
		t.Fatalf("store_moved events %d %v", events, err)
	}

	// A volatile store: an execution naming it, reserved before the move, is
	// not launched and waits (store_moving) until it has moved.
	spec := schedulerExecutionSpec("on-d")
	spec.Scope.Project = "imp"
	spec.ExecutorSelector = json.RawMessage(`{"labels":{"environment":"windows"}}`)
	spec.Exclusive, spec.Checkpointable, spec.Preemptible = []ExclusiveRequest{}, false, false
	spec.Store = "d-scratch"
	execution, _, err := store.SubmitExecution(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	reservation, err := store.ReserveNext(ctx, "win-exec")
	if err != nil || reservation == nil {
		t.Fatalf("reserve: %+v %v", reservation, err)
	}
	if err = store.MarkLeasePrepared(ctx, reservation.Lease.ID, reservation.Lease.CoordinationEpoch); err != nil {
		t.Fatal(err)
	}
	movingScratch := scratch
	movingScratch.Root, movingScratch.MovedFrom = "D:/_kairo-volatile", "D:/kairo"
	moved := moving
	moved.MovedFrom = ""
	if _, err = store.ApplyMachines(ctx, storesSpec(movingScratch, moved), false); err != nil {
		t.Fatal(err)
	}
	if s := storeOf(t, store, "d-scratch"); !strings.Contains(s.MoveWait, "1 execution(s) using it hold a lease") {
		t.Fatalf("a reserved execution: %+v", s)
	}
	if _, err = store.AuthorizeLaunch(ctx, reservation); !errors.Is(err, ErrGateClosed) || !strings.Contains(err.Error(), "store d-scratch is being moved") {
		t.Fatalf("launched while its store is moved: %v", err)
	}
	if err = store.ReleaseReservation(ctx, reservation.Lease.ID, reservation.Lease.CoordinationEpoch, "its store is being moved"); err != nil {
		t.Fatal(err)
	}
	if again, err := store.ReserveNext(ctx, "win-exec"); err != nil || again != nil {
		t.Fatalf("reserved while its store is moved: %+v %v", again, err)
	}
	waits, err := store.AdmissionWaits(ctx, []string{execution.ID})
	if err != nil || waits[execution.ID].Reason != "store_moving" {
		t.Fatalf("waits %+v %v", waits, err)
	}
	if s := storeOf(t, store, "d-scratch"); s.MoveWait != "" {
		t.Fatalf("nothing uses it any more: %+v", s)
	}
	if err = store.RecordStoreObservation(ctx, StoreObservation{StoreID: "d-scratch", ExecutorID: "win-exec", Root: "D:/_kairo-volatile", Usable: true, MovedFrom: "D:/kairo"}); err != nil {
		t.Fatal(err)
	}
	var root, source, pack string
	if err = store.db.QueryRowContext(ctx, `SELECT root,source_path,pack_path FROM data_objects WHERE id='obj_1'`).Scan(&root, &source, &pack); err != nil {
		t.Fatal(err)
	}
	if root != "D:/_kairo-volatile" || source != `D:\_kairo-volatile\outputs\run_1` || pack != `D:\_kairo-volatile\packs\obj_1.tar.zst` {
		t.Fatalf("the object is recorded at %s %s %s", root, source, pack)
	}
	if again, err := store.ReserveNext(ctx, "win-exec"); err != nil || again == nil || again.Execution.ID != execution.ID {
		t.Fatalf("not reserved once its store moved: %+v %v", again, err)
	}
}
