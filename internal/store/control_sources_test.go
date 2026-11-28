package store

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"kairo/internal/machine"
)

// The hourly control snapshots kept: every one of the last day, the last of
// each day for two weeks, the last of each week before, and the latest of
// all whatever its age.
func TestControlSnapshotsAreThinnedByAge(t *testing.T) {
	now := time.Date(2026, 11, 28, 12, 0, 0, 0, time.UTC)
	created := map[string]time.Time{}
	for h := 0; h < 24*30; h++ {
		created[fmt.Sprintf("h%03d", h)] = now.Add(-time.Duration(h)*time.Hour - time.Minute)
	}
	keep := controlKept(created, now)
	var kept []string
	for id := range keep {
		kept = append(kept, id)
	}
	sort.Strings(kept)
	hourly, daily, weekly := 0, 0, 0
	for _, id := range kept {
		age := now.Sub(created[id])
		switch {
		case age < controlKeepHourly:
			hourly++
		case age < controlKeepDaily:
			daily++
		default:
			weekly++
		}
	}
	// 24 of the last day; 13 days more (the last of each); the weeks of the
	// 16 days before
	if hourly != 24 || daily < 13 || daily > 14 || weekly < 2 || weekly > 4 {
		t.Fatalf("kept %d hourly, %d daily, %d weekly: %v", hourly, daily, weekly, kept)
	}
	// a day's kept one is its latest
	for _, id := range kept {
		age := now.Sub(created[id])
		if age >= controlKeepHourly && age < controlKeepDaily {
			for other, at := range created {
				if at.Format("2006-01-02") == created[id].Format("2006-01-02") && at.After(created[id]) && now.Sub(at) >= controlKeepHourly && !keep[other] {
					t.Fatalf("%s kept for its day, %s later that day not", id, other)
				}
			}
		}
	}
	// the latest of all, however old
	if got := controlKept(map[string]time.Time{"old": now.Add(-100 * 24 * time.Hour)}, now); !got["old"] {
		t.Fatal("the only snapshot was not kept")
	}
}

// controlStore is a store with a durable store on a node that runs
// containers, where control tasks are declared.
func controlStore(t *testing.T) *Store {
	t.Helper()
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	addTestDisk(t, store, "node", "D:")
	declareTestStore(t, store,
		machine.Store{ID: "d-scratch", Class: machine.StoreVolatile, Node: "node", Root: "D:/kairo", Filesystem: "D:"},
		machine.Store{ID: "e-durable", Class: machine.StoreDurable, Node: "node", Root: "D:/durable", Filesystem: "D:"},
	)
	if err := store.UpsertExecutor(context.Background(), Executor{ID: "node-ctr", NodeID: "node", Kind: "pct", Attributes: json.RawMessage(`{"runtime":"pct"}`), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	return store
}

// controlExecution is the execution of the control task name (made by the
// orchestration's reconcile).
func controlExecution(t *testing.T, store *Store, name string) string {
	t.Helper()
	if err := store.ReconcileProjectTasks(context.Background()); err != nil {
		t.Fatal(err)
	}
	var id string
	if err := store.db.QueryRow(`SELECT e.id FROM execution_requests e JOIN coordination_scopes t ON t.id=e.task_scope_id WHERE t.external_key=$1 ORDER BY e.submitted_at DESC LIMIT 1`, name).Scan(&id); err != nil {
		t.Fatalf("execution of %s: %v", name, err)
	}
	return id
}

func addSourceSnapshot(t *testing.T, store *Store, id, at string) {
	t.Helper()
	if _, err := store.testExec(`INSERT INTO source_snapshots(id,pack_bytes,pack_sha256,list_sha256,list,files,content_bytes,project,source_name,created_at)
		VALUES($1,10,'aa','bb','',1,10,'p','code',$2)`, id, at); err != nil {
		t.Fatal(err)
	}
}

// Code snapshots are kept once: a sources task is declared while one is kept
// by no run (and none runs), its attempt is handed those, and a run that
// succeeded with them (exit 0, its output packed) keeps them: none is
// declared again until a new one comes. A run that failed keeps nothing,
// and the next takes them again. The hourly snapshot's attempt is handed
// the database's part.
func TestCodeSnapshotsAreKeptOnceBySourcesRuns(t *testing.T) {
	store := controlStore(t)
	ctx := context.Background()
	at := time.Date(2026, 11, 28, 3, 0, 0, 0, time.UTC)
	if name, err := store.ControlSourcesTask(ctx, at, time.Hour); err != nil || name != "" {
		t.Fatalf("with no code snapshot: %q %v", name, err)
	}
	addSourceSnapshot(t, store, "src_a", "2026-11-28T01:00:00Z")
	addSourceSnapshot(t, store, "src_b", "2026-11-28T02:00:00Z")
	name, err := store.ControlSourcesTask(ctx, at, time.Hour)
	if err != nil || name != "sources-20261128T0300Z" {
		t.Fatalf("declared %q %v", name, err)
	}
	// one at a time: none while it is pending
	if again, err := store.ControlSourcesTask(ctx, at.Add(time.Hour), time.Hour); err != nil || again != "" {
		t.Fatalf("while one is pending: %q %v", again, err)
	}
	run := controlExecution(t, store, name)
	ids, err := store.ControlSourcesToCarry(ctx, run)
	if err != nil || strings.Join(ids, ",") != "src_a,src_b" {
		t.Fatalf("handed %v %v", ids, err)
	}
	if again, err := store.ControlSourcesToCarry(ctx, run); err != nil || strings.Join(again, ",") != "src_a,src_b" {
		t.Fatalf("handed again %v %v", again, err)
	}
	// a run that failed (its execution ended, no exit 0) keeps nothing
	if _, err := store.testExec(`UPDATE execution_requests SET state='terminal',terminal_cause='process_exit',terminal_at=$2 WHERE id=$1`, run, now()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.testExec(`UPDATE orchestration_tasks SET state='failed' WHERE name=$1`, name); err != nil {
		t.Fatal(err)
	}
	unkept, err := unkeptSourcesTx(ctx, store.db)
	if err != nil || len(unkept) != 2 {
		t.Fatalf("after a failed run: %v %v", unkept, err)
	}
	next, err := store.ControlSourcesTask(ctx, at.Add(time.Hour), time.Hour)
	if err != nil || next != "sources-20261128T0400Z" {
		t.Fatalf("after a failed run: %q %v", next, err)
	}
	run2 := controlExecution(t, store, next)
	if ids, err := store.ControlSourcesToCarry(ctx, run2); err != nil || len(ids) != 2 {
		t.Fatalf("taken again %v %v", ids, err)
	}
	// it succeeds: its attempt exited 0, its output packed
	if _, err := store.testExec(`UPDATE execution_requests SET state='terminal',terminal_cause='process_exit',terminal_at=$2 WHERE id=$1`, run2, now()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.testExec(`UPDATE orchestration_tasks SET state='succeeded' WHERE name=$1`, next); err != nil {
		t.Fatal(err)
	}
	if _, err := store.testExec(`INSERT INTO attempts(id,execution_id,state,executor_id,exit_code,exited_at) VALUES('att_src2',$1,'quiesced','node-ctr',0,$2)`, run2, now()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.testExec(`INSERT INTO data_objects(id,kind,source_id,store_id,executor_id,root,source_path,state,created_at,updated_at)
		VALUES('obj_src2','output',$1,'d-scratch','node-ctr','D:/kairo','D:/kairo/out','packed',$2,$2)`, run2, now()); err != nil {
		t.Fatal(err)
	}
	if unkept, err := unkeptSourcesTx(ctx, store.db); err != nil || len(unkept) != 0 {
		t.Fatalf("after a run that succeeded: %v %v", unkept, err)
	}
	if again, err := store.ControlSourcesTask(ctx, at.Add(2*time.Hour), time.Hour); err != nil || again != "" {
		t.Fatalf("nothing to keep: %q %v", again, err)
	}
	// a new one comes: only it is taken
	addSourceSnapshot(t, store, "src_c", "2026-11-28T05:30:00Z")
	third, err := store.ControlSourcesTask(ctx, at.Add(3*time.Hour), time.Hour)
	if err != nil || third == "" {
		t.Fatalf("for a new one: %q %v", third, err)
	}
	if ids, err := store.ControlSourcesToCarry(ctx, controlExecution(t, store, third)); err != nil || strings.Join(ids, ",") != "src_c" {
		t.Fatalf("handed %v %v", ids, err)
	}
}

// Thinning drops the hourly snapshots' outputs controlKept does not keep,
// and only once every code snapshot is kept by a sources run (an older
// snapshot held them all); a sources run's output is never thinned.
func TestThinningKeepsWhatTheCodeNeeds(t *testing.T) {
	store := controlStore(t)
	ctx := context.Background()
	now := time.Date(2026, 11, 28, 12, 0, 0, 0, time.UTC)
	var objects []string
	for i, age := range []time.Duration{time.Hour, 3 * time.Hour, 50 * time.Hour, 52 * time.Hour, 30 * 24 * time.Hour, 31 * 24 * time.Hour} {
		slot := now.Add(-age)
		name, err := store.ControlSnapshotTask(ctx, slot, time.Hour)
		if err != nil || name == "" {
			t.Fatalf("declared %q %v", name, err)
		}
		run := controlExecution(t, store, name)
		id := fmt.Sprintf("obj_snap%d", i)
		if _, err := store.testExec(`INSERT INTO data_objects(id,kind,source_id,store_id,executor_id,root,source_path,project_scope_id,state,lifecycle,created_at,updated_at)
			SELECT $1,'output',$2,'d-scratch','node-ctr','D:/kairo','D:/kairo/out',e.project_scope_id,'packed','durable',$3,$3 FROM execution_requests e WHERE e.id=$2`,
			id, run, slot.Format(time.RFC3339Nano)); err != nil {
			t.Fatal(err)
		}
		if _, err := store.testExec(`INSERT INTO object_copies(object_id,store_id,executor_id,root,month,state,path,manifest_path,created_at,updated_at,verified_at)
			VALUES($1,'e-durable','node-ctr','D:/durable','202611','verified','p','p.files.tsv',$2,$2,$2)`, id, slot.Format(time.RFC3339Nano)); err != nil {
			t.Fatal(err)
		}
		objects = append(objects, id)
	}
	// a code snapshot no run keeps: nothing thinned
	addSourceSnapshot(t, store, "src_a", "2026-11-01T00:00:00Z")
	if dropped, err := store.ThinControlSnapshots(ctx, now); err != nil || len(dropped) != 0 {
		t.Fatalf("thinned while the code is kept by none: %+v %v", dropped, err)
	}
	if _, err := store.testExec(`DELETE FROM source_snapshots WHERE id='src_a'`); err != nil {
		t.Fatal(err)
	}
	// one made after the last snapshot that held the code is in none of them:
	// no reason to wait (CI and applies make them all the time)
	addSourceSnapshot(t, store, "src_late", now.Add(-30*time.Minute).Format(time.RFC3339Nano))
	dropped, err := store.ThinControlSnapshots(ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	var gone []string
	for _, o := range dropped {
		if o.Dropped {
			gone = append(gone, o.ObjectID)
		}
	}
	sort.Strings(gone)
	// kept: the two of the last day; the later of the day before
	// yesterday's two; the later of the two a month old (one week)
	if strings.Join(gone, ",") != "obj_snap3,obj_snap5" {
		t.Fatalf("dropped %v of %v (%+v)", gone, objects, dropped)
	}
}

// keepRun makes the control run's output kept: its attempt exited 0, its
// output packed (as object), at created.
func keepRun(t *testing.T, store *Store, run, object string, created time.Time) {
	t.Helper()
	if _, err := store.testExec(`INSERT INTO attempts(id,execution_id,state,executor_id,exit_code,exited_at) VALUES($1,$2,'quiesced','node-ctr',0,$3)`, "att_"+object, run, now()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.testExec(`INSERT INTO data_objects(id,kind,source_id,store_id,executor_id,root,source_path,project_scope_id,state,lifecycle,created_at,updated_at)
		SELECT $1,'output',$2,'d-scratch','node-ctr','D:/kairo','D:/kairo/out',e.project_scope_id,'packed','durable',$3,$3 FROM execution_requests e WHERE e.id=$2`,
		object, run, created.Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.testExec(`INSERT INTO object_copies(object_id,store_id,executor_id,root,month,state,path,manifest_path,created_at,updated_at,verified_at)
		VALUES($1,'e-durable','node-ctr','D:/durable','202611','verified','p','p.files.tsv',$2,$2,$2)`, object, created.Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
}

// An hourly snapshot's dump is the difference from the current base: the
// latest whose run kept it, less than a day old, whose latest delta was no
// more than half of it; else whole. Thinning keeps the base of every
// snapshot it keeps.
func TestAnHourlyDumpIsTheDifferenceFromItsBase(t *testing.T) {
	store := controlStore(t)
	ctx := context.Background()
	now := time.Date(2026, 11, 28, 12, 0, 0, 0, time.UTC)
	run := func(age time.Duration) (string, time.Time) {
		t.Helper()
		slot := now.Add(-age)
		name, err := store.ControlSnapshotTask(ctx, slot, time.Hour)
		if err != nil || name == "" {
			t.Fatalf("declared %q %v", name, err)
		}
		return controlExecution(t, store, name), slot
	}
	if b, err := store.CurrentDumpBase(ctx, now); err != nil || b != nil {
		t.Fatalf("with no base: %+v %v", b, err)
	}
	base, baseAt := run(50 * time.Hour)
	if err := store.RecordControlDump(ctx, ControlDump{ExecutionID: base, Kind: ControlDumpBase, SHA256: "bb", Bytes: 1000, Chunks: []byte{}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.testExec(`UPDATE control_dumps SET created_at=$1 WHERE execution_id=$2`, now.Add(-time.Hour).Format(time.RFC3339Nano), base); err != nil {
		t.Fatal(err)
	}
	if b, err := store.CurrentDumpBase(ctx, now); err != nil || b != nil {
		t.Fatalf("a base no run kept yet: %+v %v", b, err)
	}
	keepRun(t, store, base, "obj_base", baseAt)
	b, err := store.CurrentDumpBase(ctx, now)
	if err != nil || b == nil || b.ExecutionID != base || b.SHA256 != "bb" || !strings.HasPrefix(b.Task, "snapshot-") {
		t.Fatalf("the base: %+v %v", b, err)
	}
	if b, err := store.CurrentDumpBase(ctx, now.Add(24*time.Hour)); err != nil || b != nil {
		t.Fatalf("a base a day old: %+v %v", b, err)
	}
	// a delta of more than half its base: the next dump whole
	big, _ := run(30 * time.Minute)
	if err := store.RecordControlDump(ctx, ControlDump{ExecutionID: big, Kind: ControlDumpDelta, BaseExecutionID: base, SHA256: "cc", Bytes: 1000, NewBytes: 600}); err != nil {
		t.Fatal(err)
	}
	if b, err := store.CurrentDumpBase(ctx, now); err != nil || b != nil {
		t.Fatalf("after a delta of 60%%: %+v %v", b, err)
	}
	if err := store.RecordControlDump(ctx, ControlDump{ExecutionID: big, Kind: ControlDumpDelta, BaseExecutionID: base, SHA256: "cc", Bytes: 1000, NewBytes: 100}); err != nil {
		t.Fatal(err)
	}
	if b, err := store.CurrentDumpBase(ctx, now); err != nil || b == nil {
		t.Fatalf("after a delta of 10%%: %+v %v", b, err)
	}
	if err := store.RecordControlDump(ctx, ControlDump{ExecutionID: big, Kind: ControlDumpDelta, SHA256: "cc", Bytes: 1000}); err == nil {
		t.Fatal("a delta of no base was recorded")
	}
	// thinning: a delta of the last day keeps its base, though a later
	// snapshot of the base's day is that day's
	keepRun(t, store, big, "obj_delta", now.Add(-30*time.Minute))
	later, laterAt := run(49 * time.Hour)
	keepRun(t, store, later, "obj_later", laterAt)
	earlier, earlierAt := run(51 * time.Hour)
	keepRun(t, store, earlier, "obj_earlier", earlierAt)
	dropped, err := store.ThinControlSnapshots(ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	var gone []string
	for _, o := range dropped {
		if o.Dropped {
			gone = append(gone, o.ObjectID)
		}
	}
	if strings.Join(gone, ",") != "obj_earlier" {
		t.Fatalf("dropped %v (%+v)", gone, dropped)
	}
}
