package store

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"kairo/internal/machine"
)

// The daemon declares one control-snapshot task per interval, on the node of
// the first durable store, running that node's kairo and reserving nothing (so
// it never suspends anything to fit); none while no durable store or no kairo
// is there to run it.
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
		t.Fatalf("without kairo on the node: %q %v", name, err)
	}
	attributes, _ := json.Marshal(map[string]any{"bench": map[string]any{"kairo": "C:/kairo/kairo.exe", "executor_labels": map[string]string{"environment": "windows"}}})
	if err := store.UpsertNode(ctx, Node{ID: "node", Name: "node", OS: "windows", Architecture: "amd64", Attributes: attributes, Enabled: true}); err != nil {
		t.Fatal(err)
	}
	name, err := store.ControlSnapshotTask(ctx, at, time.Hour)
	if err != nil || name != "snapshot-20261116T0100Z" {
		t.Fatalf("declared %q %v", name, err)
	}
	if again, err := store.ControlSnapshotTask(ctx, at.Add(30*time.Minute), time.Hour); err != nil || again != "" {
		t.Fatalf("the slot's task declared again: %q %v", again, err)
	}
	var queue, template string
	if err = store.db.QueryRowContext(ctx, `SELECT queue_name,execution_template_json FROM orchestration_tasks WHERE name=$1`, name).Scan(&queue, &template); err != nil {
		t.Fatal(err)
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
	if queue != ControlQueue || len(execution.Argv) != 3 || execution.Argv[0] != "C:/kairo/kairo.exe" || execution.Argv[1] != "control" || execution.Argv[2] != "snapshot" ||
		execution.CWD != "C:/kairo" || execution.Priority != ControlPriority || execution.Capacity.CPUMillis != 0 || execution.Capacity.RAMBytes != 0 || execution.Executor.Node != "node" || execution.Executor.Labels["environment"] != "windows" {
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
