package main

import (
	"encoding/json"
	"strings"
	"testing"

	"kairo/internal/store"
)

// task show prints a checkpoint's reference, its command, and where each
// stream it reads stood: each rank's record and output within it.
func TestCheckpointTextShowsWhereStreamsStood(t *testing.T) {
	cp := store.CheckpointView{CommandID: "cmd_1", Kind: "suspend", Origin: "scope_pause", ContinuationRef: "/kairo/output/step_833.pt", At: "2026-11-26T06:00:00Z",
		Streams: json.RawMessage(`{"wiki":{"world":2,"order":1,"positions":[{"rank":0,"record":15667,"sub":66},{"rank":1,"record":15666,"sub":0}]}}`)}
	got := checkpointText(cp)
	for _, want := range []string{"/kairo/output/step_833.pt", "suspend cmd_1 scope_pause", "stream wiki (world 2): rank 0 at record 15667 sub 66, rank 1 at record 15666 sub 0"} {
		if !strings.Contains(got, want) {
			t.Fatalf("%q lacks %q", got, want)
		}
	}
	if got := checkpointText(store.CheckpointView{CommandID: "cmd_2", Kind: "checkpoint", Origin: "data_protection"}); !strings.HasPrefix(got, "(no reference)  checkpoint cmd_2") {
		t.Fatalf("no reference: %q", got)
	}
}
