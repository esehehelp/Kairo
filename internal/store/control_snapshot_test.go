package store

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"kairo/internal/machine"
)

// The daemon declares one control-snapshot task per interval, in a container
// on the node of the first durable store, running the agent's kairo there and
// reserving nothing (so it never suspends anything to fit); none while no
// durable store or no container executor is there to run it.
func TestControlSnapshotTaskOnceASlotOnTheFirstDurableStoresNode(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	addTestDisk(t, store, "node", "D:")
	ctx := context.Background()
	at := time.Date(2026, 11, 16, 1, 23, 0, 0, time.UTC)

	if name, err := store.ControlSnapshotTask(ctx, at, time.Hour); err != nil || name != "" {
		t.Fatalf("without a durable store: %q %v", name, err)
	}
	declareTestStore(t, store,
		machine.Store{ID: "d-scratch", Class: machine.StoreVolatile, Node: "node", Root: "D:/kairo", Filesystem: "D:"},
		machine.Store{ID: "e-durable", Class: machine.StoreDurable, Node: "node", Root: "D:/durable", Filesystem: "D:"},
	)
	if name, err := store.ControlSnapshotTask(ctx, at, time.Hour); err != nil || name != "" {
		t.Fatalf("without a container executor on the node: %q %v", name, err)
	}
	if err := store.UpsertExecutor(ctx, Executor{ID: "node-ctr", NodeID: "node", Kind: "pct", Attributes: json.RawMessage(`{"runtime":"pct"}`), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	name, err := store.ControlSnapshotTask(ctx, at, time.Hour)
	if err != nil || name != "snapshot-20261116T0100Z" {
		t.Fatalf("declared %q %v", name, err)
	}
	holder, err := store.beginWrite(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Rollback()
	readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if again, err := store.ControlSnapshotTask(readCtx, at.Add(30*time.Minute), time.Hour); err != nil || again != "" {
		t.Fatalf("checking the existing slot while a decision holds the lock: %q %v", again, err)
	}
	if err = holder.Rollback(); err != nil {
		t.Fatal(err)
	}
	var queue, template, image string
	if err = store.db.QueryRowContext(ctx, `SELECT queue_name,execution_template_json,image_ref FROM orchestration_tasks WHERE name=$1`, name).Scan(&queue, &template, &image); err != nil {
		t.Fatal(err)
	}
	if image != "docker.io/library/python@sha256:191083cdf6c7bda8c76158983163ead5d5cf7b77cf3033e805bb0f5296f51b4b" {
		t.Fatalf("the task's image: %s", image)
	}
	var execution struct {
		Argv     []string `json:"argv"`
		CWD      string   `json:"cwd"`
		Priority int      `json:"priority"`
		Capacity struct {
			CPUMillis int64 `json:"cpu_millis"`
			RAMBytes  int64 `json:"ram_bytes"`
		} `json:"capacity"`
		Executor struct {
			Node   string            `json:"node"`
			Labels map[string]string `json:"labels"`
		} `json:"executor"`
	}
	if err = json.Unmarshal([]byte(template), &execution); err != nil {
		t.Fatal(err)
	}
	if queue != ControlQueue || len(execution.Argv) != 3 || execution.Argv[0] != "/kairo/bin/kairo" || execution.Argv[1] != "control" || execution.Argv[2] != "snapshot" ||
		execution.CWD != "/" || execution.Priority != ControlPriority || execution.Capacity.CPUMillis != 0 || execution.Capacity.RAMBytes != 0 || execution.Executor.Node != "node" {
		t.Fatalf("task %s in %s: %s", name, queue, template)
	}
	if next, err := store.ControlSnapshotTask(ctx, at.Add(time.Hour), time.Hour); err != nil || next != "snapshot-20261116T0200Z" {
		t.Fatalf("the next slot: %q %v", next, err)
	}
	if _, err = store.ControlSnapshotTask(ctx, at, 0); err == nil {
		t.Fatal("a zero interval was taken")
	}
	if _, _, err = store.AttemptQueue(ctx, "att_none"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("an unknown attempt's queue: %v", err)
	}
}

// The control snapshot runs in a container: a durable store's node that
// runs no containers is passed over for the next one's. (The bench driver
// still runs on a host: a containers-only node reports no bench.)
func TestControlSnapshotRunsOnANodeThatRunsContainers(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store) // node "node": a host executor only
	addTestDisk(t, store, "node", "D:")
	ctx := context.Background()
	if err := store.UpsertNode(ctx, Node{ID: "ctr", Name: "ctr", OS: "linux", Architecture: "amd64", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertExecutor(ctx, Executor{ID: "ctr-pct", NodeID: "ctr", Kind: "pct", Attributes: json.RawMessage(`{"runtime":"pct"}`), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	addTestDisk(t, store, "ctr", "/")
	declareTestStore(t, store,
		machine.Store{ID: "a-durable", Class: machine.StoreDurable, Node: "node", Root: "D:/durable", Filesystem: "D:"},
		machine.Store{ID: "e-durable", Class: machine.StoreDurable, Node: "ctr", Root: "/srv/durable", Filesystem: "/"},
	)
	if b, err := nodeBenchTx(ctx, store.db, "ctr"); err != nil || b.Kairo != containerKairo {
		t.Fatalf("a node that runs containers runs benches in them: %+v %v", b, err)
	}
	name, err := store.ControlSnapshotTask(ctx, time.Date(2026, 11, 24, 7, 0, 0, 0, time.UTC), time.Hour)
	if err != nil || name == "" {
		t.Fatalf("declared %q %v", name, err)
	}
	var node string
	if err := store.db.QueryRowContext(ctx, `SELECT (execution_template_json::jsonb)->'executor'->>'node' FROM orchestration_tasks WHERE name=$1`, name).Scan(&node); err != nil || node != "ctr" {
		t.Fatalf("on node %q %v", node, err)
	}
}
