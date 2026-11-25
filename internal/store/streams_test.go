package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"kairo/internal/loader"
	"kairo/internal/machine"
	"kairo/internal/orchestration"
)

// A task reading a stream is launched with it: its loader's spec (the
// raw's files, not identified until a loader first reads them), its
// socket's variable and the RAM its loader holds; what a loader learns of
// the files is kept (the first identity of a URL stands) and handed to the
// next launch; a continuation starts where its checkpoint said each rank
// stood.
func TestATaskIsLaunchedWithItsStreams(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	ctx := context.Background()
	if err := store.UpsertExecutor(ctx, Executor{ID: "ctr", NodeID: "node", Kind: "incus", Attributes: json.RawMessage(`{"environment":"wsl2","runtime":"incus"}`), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertResourceInstance(ctx, ResourceInstance{ID: "ram", NodeID: "node", ProviderID: "gpu-provider", Kind: "ram", StableIdentity: "physical-memory", Binding: json.RawMessage(`{}`), AdminState: "enabled"}); err != nil {
		t.Fatal(err)
	}
	total, free := int64(64<<30), int64(40<<30)
	observed := time.Now().UTC()
	if err := store.RecordObservation(ctx, Observation{ResourceID: "ram", ObservedAt: observed.Format(time.RFC3339Nano), ValidUntil: observed.Add(time.Minute).Format(time.RFC3339Nano), TotalBytes: &total, FreeBytes: &free}); err != nil {
		t.Fatal(err)
	}
	const url = "https://example.org/corpus/part-0.jsonl.zst"
	body := "schema_version = 1\n[project]\nname = \"streamed\"\n[images.py]\nref = \"python:3.12\"\n" +
		"[raws.corpus]\nurls = [\"" + url + "\"]\n" +
		"[streams.mix-1]\nsources = [ { raw = \"corpus\", format = \"jsonl.zst\", field = \"text\" } ]\nshuffle = { seed = 9, block = 64 }\nmemory_bytes = 268435456\n" +
		"[[tasks]]\nname = \"train\"\nqueue = \"q\"\ndepends_on = []\npolicy = \"RunToCompletion@v1\"\n[tasks.execution]\nschema_version = 2\nargv = [\"python\", \"-V\"]\ncwd = \"/\"\nimage = \"py\"\nstreams = [\"mix-1\"]\n" +
		"[tasks.execution.capacity]\nram_bytes = 1073741824\n[tasks.execution.executor]\nlabels = { environment = \"wsl2\" }\n"
	v := projectSpec(t, body)
	v.Manifest.ImagePins = map[string]string{"py": testImagePin}
	v, err := orchestration.Normalize(v.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ApplyProject(ctx, v); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	reservation, err := store.ReserveNext(ctx, "ctr")
	if err != nil || reservation == nil {
		t.Fatalf("reserve: %+v %v", reservation, err)
	}
	if err = store.MarkLeasePrepared(ctx, reservation.Lease.ID, reservation.Lease.CoordinationEpoch); err != nil {
		t.Fatal(err)
	}
	launch, err := store.AuthorizeLaunch(ctx, reservation)
	if err != nil {
		t.Fatal(err)
	}
	if len(launch.Streams) != 1 {
		t.Fatalf("streams %+v", launch.Streams)
	}
	s := launch.Streams[0]
	src := s.Spec.Sources[0]
	if s.Name != "mix-1" || s.Env != "KAIRO_STREAM_MIX_1" || s.Spec.Seed != 9 || s.Spec.Block != 64 || s.Spec.Epochs != 1 || len(s.Positions) != 0 ||
		src.Name != "corpus" || src.Format != "jsonl.zst" || src.Field != "text" || len(src.Files) != 1 || src.Files[0].URL != url || src.Files[0].Identified() {
		t.Fatalf("stream %+v", s)
	}
	var ram int64
	if err := store.db.QueryRowContext(ctx, `SELECT quantity FROM resource_requests WHERE execution_id=$1 AND request_type='capacity' AND kind='ram'`, launch.Execution.ID).Scan(&ram); err != nil || ram != 1073741824+268435456 {
		t.Fatalf("RAM %d (the task's and the loader's): %v", ram, err)
	}

	// what the agent learned: an identity (the first of a URL stands), a
	// record count it counted (the first stands), for an executor whose
	// leased executions' streams read the file
	f, err := loader.Identify(strings.NewReader("whatever the file holds"), "part-0.jsonl.zst", loader.DefaultChunkBytes)
	if err != nil {
		t.Fatal(err)
	}
	f.URL = url
	if _, found, err := store.RawIdentity(ctx, "ctr", url); err != nil || found {
		t.Fatalf("an identity before any: %v %v", found, err)
	}
	if err := store.RawPlaced(ctx, "ctr", "", "", f); err != nil {
		t.Fatal(err)
	}
	if kept, found, err := store.RawIdentity(ctx, "ctr", url); err != nil || !found || kept.SHA256 != f.SHA256 || len(kept.Chunks) != 1 {
		t.Fatalf("identity %+v %v %v", kept, found, err)
	}
	changed := f
	changed.SHA256 = strings.Repeat("e", 64)
	if err := store.RawPlaced(ctx, "ctr", "", "", changed); !errors.Is(err, ErrRawChanged) {
		t.Fatalf("another identity of the URL: %v", err)
	}
	count := RecordsCount{URL: url, SHA256: f.SHA256, Format: "jsonl.zst", Field: "text", Records: 42}
	if err := store.RawRecords(ctx, "ctr", count); err != nil {
		t.Fatal(err)
	}
	wrong := count
	wrong.Records = 41
	if err := store.RawRecords(ctx, "ctr", wrong); !errors.Is(err, ErrRawChanged) {
		t.Fatalf("another record count: %v", err)
	}
	asText := count
	asText.Format, asText.Field = "text", ""
	if err := store.RawRecords(ctx, "ctr", asText); !errors.Is(err, ErrStreamReportRefused) {
		t.Fatalf("a count as no source reads it: %v", err)
	}
	other := f
	other.URL = "https://example.org/elsewhere"
	if err := store.RawPlaced(ctx, "ctr", "", "", other); !errors.Is(err, ErrStreamReportRefused) {
		t.Fatalf("a file of no stream leased there: %v", err)
	}
	if _, _, err := store.RawIdentity(ctx, "executor", url); !errors.Is(err, ErrStreamReportRefused) {
		t.Fatalf("asked by an executor leasing nothing reading it: %v", err)
	}
	streams, err := launchStreamsTx(ctx, store.db, launch.Execution.ID)
	if err != nil {
		t.Fatal(err)
	}
	got := streams[0].Spec.Sources[0]
	recordsKey, _ := loader.RecordsKey(f.SHA256, got.Format, got.Field, got.Files[0].Name)
	if !got.Files[0].Identified() || got.Files[0].SHA256 != f.SHA256 || len(got.Files[0].Chunks) != 1 || got.Records[recordsKey] != 42 {
		t.Fatalf("the next launch's source %+v", got)
	}

	// a checkpoint says where every rank of every stream stood, or it is
	// refused; a continuation starts where its checkpoint (its command) says
	decls, err := executionStreamsTx(ctx, store.db, launch.Execution.ID)
	if err != nil || len(decls) != 1 {
		t.Fatalf("decls %+v %v", decls, err)
	}
	for _, bad := range []string{
		`{"continuation_ref":"ckpt-1"}`,
		`{"continuation_ref":"ckpt-1","streams":{"other":{"world":1,"order":1,"positions":[{"rank":0,"record":5,"sub":0}]}}}`,
		`{"continuation_ref":"ckpt-1","streams":{"mix-1":{"world":1,"order":1,"positions":[{"rank":1,"record":5}]}}}`,
		`{"continuation_ref":"ckpt-1","streams":{"mix-1":{"world":2,"order":1,"positions":[{"rank":0,"record":5},{"rank":0,"record":6}]}}}`,
		`{"continuation_ref":"ckpt-1","streams":{"mix-1":{"world":2,"order":1,"positions":[{"rank":0,"record":5}]}}}`,
		`{"continuation_ref":"ckpt-1","streams":{"mix-1":{"world":1,"order":1,"positions":[{"rank":0,"record":-1}]}}}`,
		`{"continuation_ref":"ckpt-1","streams":{"mix-1":{"world":1,"order":2,"positions":[{"rank":0,"record":5}]}}}`,
		`{"continuation_ref":"ckpt-1","streams":{"mix-1":{"world":1,"order":1,"positions":[{"rank":0,"record":5,"sub":4294967296}]}}}`,
		`{"continuation_ref":"ckpt-1","streams":{"mix-1":[{"rank":0,"record":5}]}}`,
		`{"continuation_ref":"ckpt-1","streams":{"mix-1":{"world":1,"order":1,"positions":[{}]}}}`,
		`{"continuation_ref":"ckpt-1","streams":{"mix-1":{"world":1,"order":1,"positions":[{"rank":0,"record":5}]}}}`,
		`{"continuation_ref":"ckpt-1","streams":{"mix-1":{"order":1,"positions":[{"rank":0,"record":5,"sub":0}]}}}`,
	} {
		if err := checkStreamPositionsTx(ctx, store.db, launch.Execution.ID, []byte(bad)); !errors.Is(err, ErrStreamPositions) {
			t.Fatalf("%s: %v", bad, err)
		}
	}
	good := `{"continuation_ref":"ckpt-1","streams":{"mix-1":{"world":2,"order":1,"positions":[{"rank":1,"record":4,"sub":0},{"rank":0,"record":5,"sub":1}]}}}`
	if err := checkStreamPositionsTx(ctx, store.db, launch.Execution.ID, []byte(good)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.testExec(`INSERT INTO commands(id,execution_id,attempt_id,lease_id,coordination_epoch,kind,origin,reason,state,payload_json,created_at,updated_at)
		VALUES('cmd_1',$1,$2,$3,$4,'suspend','scope_pause','test','checkpointed',$5,'2026-11-25T00:00:00Z','2026-11-25T00:00:00Z')`,
		launch.Execution.ID, launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch, good); err != nil {
		t.Fatal(err)
	}
	// another publication of the same reference does not stand for it
	if _, err := store.testExec(`INSERT INTO command_acks(command_id,attempt_id,coordination_epoch,phase,payload_json,created_at)
		VALUES('cmd_1',$1,$2,'checkpointed',$3,'2026-11-25T00:00:01Z')`, launch.Attempt.ID, launch.Lease.CoordinationEpoch,
		`{"continuation_ref":"ckpt-1","streams":{"mix-1":{"world":2,"order":1,"positions":[{"rank":0,"record":99},{"rank":1,"record":99}]}}}`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.testExec(`UPDATE execution_requests SET input_continuation_ref='ckpt-1' WHERE id=$1`, launch.Execution.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := launchStreamsTx(ctx, store.db, launch.Execution.ID); !errors.Is(err, ErrStreamPositions) {
		t.Fatalf("a continuation without its checkpoint's command: %v", err)
	}
	if _, err := store.testExec(`UPDATE execution_requests SET input_checkpoint_command_id='cmd_1' WHERE id=$1`, launch.Execution.ID); err != nil {
		t.Fatal(err)
	}
	streams, err = launchStreamsTx(ctx, store.db, launch.Execution.ID)
	if err != nil || len(streams[0].Positions) != 2 || streams[0].World != 2 {
		t.Fatalf("continuation streams %+v %v", streams, err)
	}
	got0 := streams[0].Positions
	if !(got0[0] == (loader.Position{Rank: 1, Record: 4}) && got0[1] == (loader.Position{Rank: 0, Record: 5, Sub: 1})) {
		t.Fatalf("positions %+v", got0)
	}

	// the raw cache: a file placed on a volatile store the executor serves
	// is recorded (its identity checked as a loader's), and evicted first
	// among what can be had again when the store runs short
	addTestDisk(t, store, "node", "D:")
	declareTestStore(t, store, machine.Store{ID: "vol", Class: machine.StoreVolatile, Node: "node", Executor: "ctr", Root: "D:/kairo-vol", Filesystem: "D:"})
	if err := store.RawPlaced(ctx, "ctr", "vol", "D:/kairo-vol", f); err != nil {
		t.Fatal(err)
	}
	if err := store.RawPlaced(ctx, "ctr", "", "", f); err != nil {
		t.Fatalf("identity only: %v", err)
	}
	if err := store.RawPlaced(ctx, "ctr", "vol", "D:/kairo-vol", changed); !errors.Is(err, ErrRawChanged) {
		t.Fatalf("another identity placed: %v", err)
	}
	if err := store.RawPlaced(ctx, "ctr", "vol", "D:/kairo-vol", other); !errors.Is(err, ErrStreamReportRefused) {
		t.Fatalf("a file of no leased stream placed: %v", err)
	}
	if err := store.RawPlaced(ctx, "ctr", "none", "D:/x", f); !errors.Is(err, ErrStreamReportRefused) {
		t.Fatalf("placed on a store not served: %v", err)
	}
	totalBytes, freeBytes := int64(100<<30), int64(1<<20)
	if err := store.RecordStoreObservation(ctx, StoreObservation{StoreID: "vol", ExecutorID: "ctr", Root: "D:/kairo-vol", Usable: true, FreeBytes: &freeBytes, TotalBytes: &totalBytes}); err != nil {
		t.Fatal(err)
	}
	evictions, err := store.EvictionsToMake(ctx, "ctr")
	if err != nil || len(evictions) != 1 || evictions[0].Kind != EvictRaw || evictions[0].ObjectID != f.SHA256 {
		t.Fatalf("evictions %+v %v", evictions, err)
	}
	if err := store.SettleEviction(ctx, EvictionSettled{ObjectID: f.SHA256, Kind: EvictRaw, StoreID: "vol", ExecutorID: "ctr", Evicted: true}); err != nil {
		t.Fatal(err)
	}
	var left int
	if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM raw_placements`).Scan(&left); err != nil || left != 0 {
		t.Fatalf("placements left %d %v", left, err)
	}
}
