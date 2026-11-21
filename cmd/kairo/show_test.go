package main

import (
	"strings"
	"testing"

	"kairo/internal/store"
)

func TestTaskShowPrintsExecutionOutputs(t *testing.T) {
	f := newContractAPI()
	f.details["train"] = map[string]any{
		"name": "train", "queue": "training", "state": "succeeded",
		"executions": []map[string]any{{
			"execution_id": "exe_train", "ordinal": 1, "state": "terminal",
			"outputs": []store.TaskOutput{{ID: "obj_train", ExecutionID: "exe_train", State: "packed", Lifecycle: "durable", Copies: 1,
				Locations: []store.OutputCopy{{StoreID: "backup", Path: "/backup/obj_train.tar.zst"}}}},
		}},
	}
	server := f.serve(t)
	args := []string{"task", "show", "--api", server.URL, "p", "training", "train"}
	out, err := captureStdout(t, func() error { return run(args) })
	if err != nil || !strings.Contains(out, "obj_train  packed  durable  verified copies: 1") || !strings.Contains(out, "backup: /backup/obj_train.tar.zst") {
		t.Fatalf("task show: %q %v", out, err)
	}
	out, err = captureStdout(t, func() error { return run(append(args, "--json")) })
	if err != nil || !strings.Contains(out, `"outputs":[{"id":"obj_train"`) {
		t.Fatalf("task show --json: %q %v", out, err)
	}
}

func TestEnergyTextScalesAndSaysWhatIsMissing(t *testing.T) {
	for _, tc := range []struct {
		energy store.AttemptEnergy
		want   string
	}{
		{store.AttemptEnergy{GPUJoules: 4428000, GPUSecondsCovered: 3600}, "GPU 1.23 kWh"},
		{store.AttemptEnergy{GPUJoules: 135000, GPUSecondsCovered: 900, Partial: true}, "GPU 37.5 Wh (partial)"},
		{store.AttemptEnergy{Partial: true}, "GPU not measured"},
	} {
		if got := energyText(tc.energy); got != tc.want {
			t.Fatalf("energyText(%+v) = %q, want %q", tc.energy, got, tc.want)
		}
	}
}
