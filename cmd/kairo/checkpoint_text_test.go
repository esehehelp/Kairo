package main

import (
	"encoding/json"
	"strings"
	"testing"

	"kairo/internal/loader"
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

// task show prints what a stream's loader said last: each rank's position,
// rates, what is made ahead, the share of the time its taker waited for data
// and where its sources are read.
func TestStreamTextShowsEachRank(t *testing.T) {
	st := store.AttemptStream{Stream: "wiki", ObservedAt: "2026-11-26T10:00:00Z", Status: loader.Status{World: 2, WindowSeconds: 30, Ranks: []loader.RankStatus{
		{Rank: 0, Position: loader.Position{Record: 120, Sub: 3}, Taker: true, Queued: 4096, QueuedBytes: 3 << 20, ItemsPerSecond: 850, BytesPerSecond: 2 << 20, StarvedShare: 0.125,
			Reading: []loader.SourceAt{{Source: "jawiki", Epoch: 1, File: "part-3.jsonl.zst"}}},
		{Rank: 1, Position: loader.Position{Rank: 1, Record: 119}},
	}}}
	got := strings.Join(streamText(st), "\n")
	for _, want := range []string{"wiki: 2 ranks", "(rates over 30 s)", "rank 0 at record 120 sub 3; 850 items/s 2.0 MiB/s; made ahead 4096 items (3.0 MiB); waited for data 12%",
		"reading jawiki epoch 1 part-3.jsonl.zst", "rank 1 at record 119 sub 0, no taker"} {
		if !strings.Contains(got, want) {
			t.Fatalf("%q lacks %q", got, want)
		}
	}
	if got := streamText(store.AttemptStream{Stream: "wiki", Status: loader.Status{Error: "raw changed"}}); len(got) != 1 || !strings.Contains(got[0], "no rank came yet; failed: raw changed") {
		t.Fatalf("failed before a rank: %q", got)
	}
}
