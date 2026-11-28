package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"kairo/internal/machine"
)

// A source Kairo kept once copied stays until an operator releases it:
// released, it goes back to its store's executor to be deleted without being
// checked against its pack (work of an executor disabled since goes to the
// one serving the store now); one on a store no longer Kairo's, on a retired
// node, is settled deleted at once. Without a good copy, kept for nothing
// but a pack, of an import, or not kept, a source is not released; nothing
// changes without confirm.
func TestAKeptSourceIsDeletedOnceReleased(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	addTestDisk(t, store, "node", "D:")
	ctx := context.Background()
	// the store's executor now, with a pack server
	if err := store.UpsertExecutor(ctx, Executor{ID: "executor", NodeID: "node", Kind: "local", Attributes: json.RawMessage(`{"pack_addr":"192.0.2.10:7475"}`), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertExecutor(ctx, Executor{ID: "old-exec", NodeID: "node", Kind: "local", Attributes: json.RawMessage(`{}`), Enabled: false}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertNode(ctx, Node{ID: "gone", Name: "gone", OS: "linux", Architecture: "amd64", Enabled: false}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertExecutor(ctx, Executor{ID: "gone-exec", NodeID: "gone", Kind: "linux", Attributes: json.RawMessage(`{}`), Enabled: false}); err != nil {
		t.Fatal(err)
	}
	declareTestStore(t, store,
		machine.Store{ID: "e-durable", Class: machine.StoreDurable, Node: "node", Executor: "executor", Root: "E:/kairo", Filesystem: "D:"},
		machine.Store{ID: "d-scratch", Class: machine.StoreVolatile, Node: "node", Executor: "executor", Root: "D:/kairo", Filesystem: "D:"},
	)
	sha := strings.Repeat("ab", 32)
	kept := func(id, executorID, storeID, root, kind string, copied bool) {
		t.Helper()
		insertPacked(t, store, id, executorID, storeID, root, sha)
		if _, err := store.testExec(`UPDATE data_objects SET kind=$2,source_path=$3,source_state='kept',source_reason='not deleted: the source holds what its pack does not: tmp/.s.PGSQL.55432 is a special file (not packed)' WHERE id=$1`,
			id, kind, root+"/outputs/"+id); err != nil {
			t.Fatal(err)
		}
		if copied {
			if _, err := store.testExec(`INSERT INTO object_copies(object_id,store_id,executor_id,root,month,state,created_at,updated_at,verified_at)
				VALUES($1,'e-durable','executor','E:/kairo','202611','verified',$2,$2,$2)`, id, now()); err != nil {
				t.Fatal(err)
			}
		}
	}
	kept("obj_here", "executor", "d-scratch", "D:/kairo", ObjectOutput, true)
	kept("obj_handed", "old-exec", "d-scratch", "D:/kairo", ObjectOutput, true)
	kept("obj_gone", "gone-exec", "gone-scratch", "/srv/kairo", ObjectOutput, true)
	kept("obj_nocopy", "executor", "d-scratch", "D:/kairo", ObjectOutput, false)
	kept("obj_import", "executor", "d-scratch", "D:/kairo", ObjectImport, true)

	release := func(confirm bool, objects ...string) map[string]ReleasedSource {
		t.Helper()
		out, err := store.ReleaseSources(ctx, ReleaseRequest{Objects: objects, All: len(objects) == 0, Confirm: confirm}, "operator-test")
		if err != nil {
			t.Fatal(err)
		}
		by := map[string]ReleasedSource{}
		for _, r := range out {
			by[r.ObjectID] = r
		}
		return by
	}
	sourceState := func(id string) (state, reason, executor string) {
		t.Helper()
		if err := store.db.QueryRowContext(ctx, `SELECT source_state,COALESCE(source_reason,''),executor_id FROM data_objects WHERE id=$1`, id).Scan(&state, &reason, &executor); err != nil {
			t.Fatal(err)
		}
		return state, reason, executor
	}
	// all selects the kept outputs and scratch; said, nothing changes
	got := release(false)
	if len(got) != 4 || !got["obj_here"].Released || !got["obj_handed"].Released || !got["obj_gone"].Gone || got["obj_nocopy"].Refused == "" {
		t.Fatalf("dry run %+v", got)
	}
	if s, _, _ := sourceState("obj_here"); s != SourceKept {
		t.Fatalf("a dry run changed obj_here to %s", s)
	}
	if got = release(false, "obj_import"); got["obj_import"].Refused == "" {
		t.Fatalf("an import's directory released: %+v", got)
	}
	if _, err := store.ReleaseSources(ctx, ReleaseRequest{Objects: []string{"obj_none"}}, "operator-test"); !errors.Is(err, ErrReleaseRefused) {
		t.Fatalf("an unknown object: %v", err)
	}

	got = release(true)
	if s, reason, _ := sourceState("obj_here"); s != SourcePresent || !strings.HasPrefix(reason, releasedPrefix+"operator-test") {
		t.Fatalf("obj_here released: %s %q", s, reason)
	}
	if s, _, executor := sourceState("obj_handed"); s != SourcePresent || executor != "executor" {
		t.Fatalf("obj_handed released: %s, executor %s (want the store's executor now)", s, executor)
	}
	if s, reason, _ := sourceState("obj_gone"); s != SourceDeleted || !strings.Contains(reason, "retired") {
		t.Fatalf("obj_gone: %s %q", s, reason)
	}
	if s, _, _ := sourceState("obj_nocopy"); s != SourceKept {
		t.Fatalf("obj_nocopy, without a copy: %s", s)
	}
	orders, err := store.DeletionsToMake(ctx, "executor")
	if err != nil {
		t.Fatal(err)
	}
	released := map[string]bool{}
	for _, o := range orders {
		released[o.ObjectID] = o.Released
	}
	if len(orders) != 2 || !released["obj_here"] || !released["obj_handed"] {
		t.Fatalf("deletion orders %+v, want obj_here and obj_handed released", orders)
	}
	// the claim keeps the source released; deleted, it is settled
	if err := store.SettleDeletion(ctx, ObjectDeletion{ObjectID: "obj_here", ExecutorID: "executor", State: SourceDeleting}); err != nil {
		t.Fatal(err)
	}
	if orders, err = store.DeletionsToMake(ctx, "executor"); err != nil || len(orders) != 2 || !orders[0].Released || !orders[1].Released {
		t.Fatalf("orders once claimed %+v %v", orders, err)
	}
	if err := store.SettleDeletion(ctx, ObjectDeletion{ObjectID: "obj_here", ExecutorID: "executor", State: SourceDeleted}); err != nil {
		t.Fatal(err)
	}
	if s, _, _ := sourceState("obj_here"); s != SourceDeleted {
		t.Fatalf("obj_here once deleted: %s", s)
	}
}
