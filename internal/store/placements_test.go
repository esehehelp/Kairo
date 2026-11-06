package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"

	"kairo/internal/datainput"
	"kairo/internal/machine"
)

// An input not on the node an execution is planned onto is brought there:
// the execution waits (inputs_not_ready) while a fetch is ordered for the
// node's volatile store, read from the object's durable copy through the
// agent serving it (with a token only that fetch's node may use), and once
// recorded ready the execution is handed the unpacked directory.
func TestAnInputIsBroughtToTheNodeItIsReadOn(t *testing.T) {
	store := importFixture(t)
	ctx := context.Background()
	if err := store.UpsertNode(ctx, Node{ID: "node2", Name: "node2", OS: "linux", Architecture: "amd64", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertExecutor(ctx, Executor{ID: "exec2", NodeID: "node2", Kind: "linux", Attributes: json.RawMessage(`{"environment":"linux"}`), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertExecutor(ctx, Executor{ID: "win-exec", NodeID: "node", Kind: "windows", Attributes: json.RawMessage(`{"environment":"windows","pack_addr":"192.168.1.12:7475"}`), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	addTestDisk(t, store, "node2", "/")
	spec := storesSpec(
		machine.Store{ID: "d-scratch", Class: machine.StoreVolatile, Node: "node", Root: "D:/kairo", Filesystem: "D:"},
		machine.Store{ID: "e-durable", Class: machine.StoreDurable, Node: "node", Root: "E:/kairo", Filesystem: "D:"},
		machine.Store{ID: "n2-scratch", Class: machine.StoreVolatile, Node: "node2", Root: "/srv/kairo-store", Filesystem: "/"},
	)
	spec.Nodes = append(spec.Nodes, machine.Node{ID: "node2"})
	if _, err := store.ApplyMachines(ctx, spec, false); err != nil {
		t.Fatal(err)
	}
	imported, err := store.ImportData(ctx, ImportRequest{Project: "imp", Node: "node", Paths: []string{"D:/data/sumire"}, Name: "sumire", DeleteSource: true})
	if err != nil {
		t.Fatal(err)
	}
	objectID := imported[0].ObjectID
	sha := strings.Repeat("ab", 32)
	if err := store.RecordPack(ctx, PackMade{ObjectID: objectID, ExecutorID: "win-exec", PackPath: "D:/kairo/packs/" + objectID + ".tar.zst",
		ManifestPath: "D:/kairo/packs/" + objectID + ".tar.zst.files.tsv", PackBytes: 100, ContentBytes: 900, Files: 3, PackSHA256: sha, ManifestSHA256: sha}); err != nil {
		t.Fatal(err)
	}
	copies, err := store.CopiesToMake(ctx, "win-exec")
	if err != nil || len(copies) != 1 {
		t.Fatalf("copies %+v %v", copies, err)
	}
	c := copies[0]
	copyPath := "E:/kairo/objects/" + c.Month + "/" + objectID + ".tar.zst"
	if err := store.RecordCopy(ctx, CopyMade{ObjectID: objectID, StoreID: "e-durable", ExecutorID: "win-exec", Path: copyPath, ManifestPath: copyPath + ".files.tsv",
		Bytes: c.PackBytes, SHA256: c.PackSHA256, ManifestSHA256: c.ManifestSHA256}); err != nil {
		t.Fatal(err)
	}
	// an execution on node2 that reads it
	reader := schedulerExecutionSpec("reader")
	reader.Scope.Project = "imp"
	reader.ExecutorSelector = json.RawMessage(`{"labels":{"environment":"linux"},"node":"node2"}`)
	reader.Exclusive, reader.Checkpointable, reader.Preemptible = []ExclusiveRequest{}, false, false
	reader.Inputs = []datainput.Input{{Name: "data", Data: "sumire"}}
	execution, _, err := store.SubmitExecution(ctx, reader)
	if err != nil {
		t.Fatal(err)
	}
	declared, err := declaredStores(ctx, store.db)
	if err != nil {
		t.Fatal(err)
	}
	inputs := `[{"name":"data","data":"sumire"}]`
	wait := func() string {
		t.Helper()
		why, err := inputsWaitTx(ctx, store.db, declared, execution.ProjectScopeID, string(reader.ExecutorSelector), inputs)
		if err != nil {
			t.Fatal(err)
		}
		return why
	}
	if why := wait(); !strings.Contains(why, "being brought to node2") {
		t.Fatalf("wait: %q", why)
	}
	if r, err := store.ReserveNext(ctx, "exec2"); err != nil || r != nil {
		t.Fatalf("admitted before its input is there: %+v %v", r, err)
	}
	if err := store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	fetches, err := store.FetchesToMake(ctx, "exec2")
	if err != nil || len(fetches) != 1 {
		t.Fatalf("fetches %+v %v", fetches, err)
	}
	f := fetches[0]
	if f.ObjectID != objectID || f.StoreID != "n2-scratch" || f.Source.Local || f.Source.Addr != "192.168.1.12:7475" || f.Source.Month != c.Month || f.Source.StoreID != "e-durable" || f.Source.Token == "" {
		t.Fatalf("fetch order %+v", f)
	}
	read, err := store.AuthorizePackRead(ctx, "node", objectID, f.Source.Token)
	if err != nil || read.StoreID != "e-durable" || read.Root != "E:/kairo" || read.Month != c.Month {
		t.Fatalf("the durable copy's agent hands it over: %+v %v", read, err)
	}
	if _, err := store.AuthorizePackRead(ctx, "node2", objectID, f.Source.Token); err == nil {
		t.Fatal("another node's agent hands the copy over")
	}
	if err := store.RecordFetch(ctx, FetchMade{ObjectID: objectID, StoreID: "n2-scratch", ExecutorID: "exec2", Path: "/srv/kairo-store/elsewhere", Bytes: 900}); err == nil {
		t.Fatal("recorded somewhere else than <root>/inputs/<object id>")
	}
	if err := store.RecordFetch(ctx, FetchMade{ObjectID: objectID, StoreID: "n2-scratch", ExecutorID: "exec2", Path: "/srv/kairo-store/inputs/" + objectID, Bytes: 900}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AuthorizePackRead(ctx, "node", objectID, f.Source.Token); err == nil {
		t.Fatal("a fetch's token works once it is done")
	}
	if why := wait(); why != "" {
		t.Fatalf("the input is there now: %q", why)
	}
	launch := func() *Launch {
		t.Helper()
		reservation, err := store.ReserveNext(ctx, "exec2")
		if err != nil || reservation == nil {
			t.Fatalf("reserve: %+v %v", reservation, err)
		}
		if err = store.MarkLeasePrepared(ctx, reservation.Lease.ID, reservation.Lease.CoordinationEpoch); err != nil {
			t.Fatal(err)
		}
		l, err := store.AuthorizeLaunch(ctx, reservation)
		if err != nil {
			t.Fatal(err)
		}
		return l
	}()
	if len(launch.Inputs) != 1 || launch.Inputs[0].Path != "/srv/kairo-store/inputs/"+objectID {
		t.Fatalf("inputs: %+v", launch.Inputs)
	}
	var state string
	if err := store.db.QueryRowContext(ctx, `SELECT state FROM object_placements WHERE object_id=$1`, objectID).Scan(&state); err != nil || state != "ready" {
		t.Fatalf("placement %s %v", state, err)
	}
	_ = sql.ErrNoRows
}
