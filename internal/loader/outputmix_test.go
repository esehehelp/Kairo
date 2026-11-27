package loader

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// outputData is two sources for a stream weighted by output: a, 3 files of
// 100 records (one epoch of a is the stream), and b, 2 files of 30, put
// through the test stage with b's records giving four outputs each.
func outputData(t *testing.T) (Spec, string) {
	t.Helper()
	dir := t.TempDir()
	var a, b []File
	write := func(name string, lines []string) File {
		body := []byte(strings.Join(lines, "\n") + "\n")
		if err := os.WriteFile(filepath.Join(dir, name), body, 0o644); err != nil {
			t.Fatal(err)
		}
		f, err := Identify(bytes.NewReader(body), name, 64)
		if err != nil {
			t.Fatal(err)
		}
		return f
	}
	for i := 0; i < 3; i++ {
		var lines []string
		for j := 0; j < 100; j++ {
			lines = append(lines, fmt.Sprintf("a%d-%d", i, j))
		}
		a = append(a, write(fmt.Sprintf("a-%d.txt", i), lines))
	}
	for i := 0; i < 2; i++ {
		var lines []string
		for j := 0; j < 30; j++ {
			lines = append(lines, fmt.Sprintf("b%d-%d", i, j))
		}
		b = append(b, write(fmt.Sprintf("b-%d.txt", i), lines))
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	return Spec{
		Sources: []SourceSpec{
			{Name: "a", Weight: 1, Format: "text", Files: a},
			{Name: "b", Weight: 1, Format: "text", Files: b},
		},
		Seed: 7, ShuffleFiles: true, Epochs: 1, WeightBy: WeightByOutput,
		Stages: []Stage{{Argv: []string{exe}, Env: []string{"KAIRO_LOADER_TEST_STAGE=1", "KAIRO_LOADER_TEST_B_GIVES=4"}, Workers: 2}},
	}, dir
}

// shareOfB is b's share of a rank's outputs.
func shareOfB(items []string) float64 {
	b := 0
	for _, it := range items {
		if strings.HasPrefix(it[strings.Index(it, ":")+1:], "B") {
			b++
		}
	}
	return float64(b) / float64(len(items))
}

// The sources of a chunk's records are chosen by what they are expected to
// give: alike at first (1 a record), then by what their records gave.
func TestAChunkIsChosenByWhatItsSourcesGive(t *testing.T) {
	st := newMixState(2)
	if got := st.picks([]int64{1, 1}, 6); !reflect.DeepEqual(got, []int{0, 1, 0, 1, 0, 1}) {
		t.Fatalf("nothing known: %v", got)
	}
	// a's records gave 1 each, b's 4: one of b for four of a
	st = mixState{Taken: []int64{40, 10}, Prev: []int64{40, 10}, Outputs: []int64{40, 40}}
	got := st.picks([]int64{1, 1}, 10)
	n := 0
	for _, s := range got {
		n += s
	}
	if n != 2 {
		t.Fatalf("b chosen %d times in 10: %v", n, got)
	}
	// weights 3:1 of what is given
	st = mixState{Taken: []int64{30, 10}, Prev: []int64{30, 10}, Outputs: []int64{30, 10}}
	got = st.picks([]int64{3, 1}, 8)
	if n := len(got) - strings.Count(fmt.Sprint(got), "0"); n != 2 {
		t.Fatalf("3:1 of outputs alike: %v", got)
	}
}

// Weighted by output, each source's share of what the stages give is its
// weight's (b's records give four outputs: one of b is read for about four
// of a); weighted by input, of the records read.
func TestAStreamWeightedByOutputMixesWhatItsStagesGive(t *testing.T) {
	defer func(c int) { outputChunk = c }(outputChunk)
	outputChunk = 16
	spec, dir := outputData(t)
	got := collectKeeping(t, spec, dir, 1, nil, t.TempDir())[0]
	if share := shareOfB(got); share < 0.45 || share > 0.55 {
		t.Fatalf("b gave %.2f of the outputs weighted by output", share)
	}
	byInput := spec
	byInput.WeightBy = ""
	if share := shareOfB(collect(t, byInput, dir, 1, nil)[0]); share < 0.7 {
		t.Fatalf("b gave %.2f of the outputs weighted by input", share)
	}
	// a's records each once: the stream is one epoch of a
	seen := map[string]int{}
	for _, it := range got {
		if v := it[strings.Index(it, ":")+1:]; strings.HasPrefix(v, "A") {
			seen[v]++
		}
	}
	for v, n := range seen {
		if n > 2 { // a record ending in 7 gives itself twice
			t.Fatalf("%s given %d times", v, n)
		}
	}
}

// A stream weighted by output is one fixed order: the same twice, and from
// any position (a chunk's start, its middle, mid-record) the same as what
// followed it from the start, the chunks' states read from the order state;
// without them a rank cannot start mid-way.
func TestAStreamWeightedByOutputIsTheSameFromAnyPosition(t *testing.T) {
	defer func(c int) { outputChunk = c }(outputChunk)
	outputChunk = 16
	spec, dir := outputData(t)
	spec.Block = 5
	state := t.TempDir()
	full := collectKeeping(t, spec, dir, 2, nil, state)
	if again := collectKeeping(t, spec, dir, 2, nil, t.TempDir()); !reflect.DeepEqual(full, again) {
		t.Fatal("two runs gave two orders")
	}
	after := func(it string) Position {
		var p Position
		fmt.Sscanf(it[:strings.Index(it, ":")], "%d.%d", &p.Record, &p.Sub)
		return p
	}
	for _, at := range [][2]int{{1, 1}, {16, 40}, {17, 33}, {50, 3}, {len(full[0]) - 2, len(full[1]) - 1}} {
		var positions []Position
		for r := range full {
			p := after(full[r][at[r]-1])
			p.Rank = r
			positions = append(positions, p)
		}
		got := collectKeeping(t, spec, dir, 2, positions, state)
		for r := range got {
			if !reflect.DeepEqual(got[r], full[r][at[r]:]) {
				t.Fatalf("rank %d from %+v: %v\nfrom the start: %v", r, positions[r], got[r], full[r][at[r]:])
			}
		}
	}
	l := &Loader{Spec: spec, OrderState: t.TempDir()}
	if _, err := l.mixStateAt(0, 40, 2); err == nil {
		t.Fatal("a rank started mid-way with no order state kept")
	}
}
