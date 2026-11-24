package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"kairo/internal/loader"
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

	// what the loader learned
	f, err := loader.Identify(strings.NewReader("whatever the file holds"), "part-0.jsonl.zst", loader.DefaultChunkBytes)
	if err != nil {
		t.Fatal(err)
	}
	f.URL = url
	report := StreamReport{Identities: []loader.File{f}, Records: []RecordsCount{{SHA256: f.SHA256, Format: "jsonl.zst", Field: "text", Records: 42}}}
	if err := store.RecordStreamReport(ctx, launch.Attempt.ID, report); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordStreamReport(ctx, launch.Attempt.ID, report); err != nil {
		t.Fatalf("the same again: %v", err)
	}
	changed := f
	changed.SHA256 = strings.Repeat("e", 64)
	if err := store.RecordStreamReport(ctx, launch.Attempt.ID, StreamReport{Identities: []loader.File{changed}}); !errors.Is(err, ErrRawChanged) {
		t.Fatalf("another identity of the URL: %v", err)
	}
	other := f
	other.URL = "https://example.org/elsewhere"
	if err := store.RecordStreamReport(ctx, launch.Attempt.ID, StreamReport{Identities: []loader.File{other}}); !errors.Is(err, ErrStreamReportRefused) {
		t.Fatalf("a file of no stream of the attempt: %v", err)
	}
	streams, err := launchStreamsTx(ctx, store.db, launch.Execution.ID)
	if err != nil {
		t.Fatal(err)
	}
	got := streams[0].Spec.Sources[0]
	if !got.Files[0].Identified() || got.Files[0].SHA256 != f.SHA256 || len(got.Files[0].Chunks) != 1 || got.Records[f.SHA256] != 42 {
		t.Fatalf("the next launch's source %+v", got)
	}

	// a checkpoint said where rank 0 stood
	var task string
	if err := store.db.QueryRowContext(ctx, `SELECT task_scope_id FROM execution_requests WHERE id=$1`, launch.Execution.ID).Scan(&task); err != nil {
		t.Fatal(err)
	}
	if _, err := store.testExec(`INSERT INTO commands(id,execution_id,attempt_id,lease_id,coordination_epoch,kind,origin,reason,state,created_at,updated_at)
		VALUES('cmd_1',$1,$2,$3,$4,'suspend','scope_pause','test','checkpointed','2026-11-25T00:00:00Z','2026-11-25T00:00:00Z')`,
		launch.Execution.ID, launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch); err != nil {
		t.Fatal(err)
	}
	if _, err := store.testExec(`INSERT INTO command_acks(command_id,attempt_id,coordination_epoch,phase,payload_json,created_at)
		VALUES('cmd_1',$1,$2,'checkpointed',$3,'2026-11-25T00:00:00Z')`, launch.Attempt.ID, launch.Lease.CoordinationEpoch,
		`{"continuation_ref":"ckpt-1","streams":{"mix-1":[{"rank":0,"record":5,"sub":1}]}}`); err != nil {
		t.Fatal(err)
	}
	positions, err := continuationPositionsTx(ctx, store.db, task, "ckpt-1")
	if err != nil || len(positions["mix-1"]) != 1 || positions["mix-1"][0] != (loader.Position{Rank: 0, Record: 5, Sub: 1}) {
		t.Fatalf("positions %+v %v", positions, err)
	}
	if positions, err = continuationPositionsTx(ctx, store.db, task, "ckpt-none"); err != nil || len(positions) != 0 {
		t.Fatalf("no such checkpoint: %+v %v", positions, err)
	}
}
