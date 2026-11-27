package loader

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// A stream weighted by output (Spec.WeightBy "output"): a source's weight is
// its share of the records the stages give (a document read gives none or
// many, as many as its stages make of it), not of the records read. Each
// rank mixes on its own, from its own share of each source (the source's
// records i with i mod W = rank), outputChunk records at a time:
//
//   - the sources of a chunk's records are chosen one by one, each time the
//     source furthest under its share of what is expected to have been given
//     (weight*all - its own, the lowest index on a tie), a record of source s
//     expected to give what its records gave so far on average (1 before any
//     is known);
//   - what chunk c is chosen from (mixState) is what the chunks before it
//     took of each source and what those before c-1 gave, so that chunk c is
//     chosen while chunk c-1 still goes through the stages: the order is
//     fixed by the declaration (stages give the same outputs for the same
//     input), and the stages never wait for it;
//   - with a shuffle block, a chunk's records are permuted by (seed, rank,
//     chunk) before they go through the stages.
//
// A position names a rank's record only; where its chunk began is kept in
// the run's output (OrderState: a line per chunk, written before any of its
// records is handed over), so that a continuation, on any node, starts from
// it: the chunk before is put through the stages again to count what it gave
// (nothing of it is handed over), then the position's chunk from its start.

// outputChunk is how many records a rank's chunk has (a variable: tests
// make it small).
var outputChunk = 1024

// maxOutputWeight bounds a weight of a stream weighted by output (the
// arithmetic of the choice stays far from overflowing).
const maxOutputWeight = 1_000_000

// mixState is what a rank's chunk is chosen from: the rank's record it
// starts at, per source the records the rank took before it (Taken) and
// before the chunk before it (Prev), and what the records before Prev gave
// (Outputs).
type mixState struct {
	Record  int64   `json:"record"`
	Taken   []int64 `json:"taken"`
	Prev    []int64 `json:"prev"`
	Outputs []int64 `json:"outputs"`
}

func newMixState(sources int) mixState {
	return mixState{Taken: make([]int64, sources), Prev: make([]int64, sources), Outputs: make([]int64, sources)}
}

// picks chooses the sources of a chunk's n records (see above). Whole
// numbers only (thousandths of an output): the same choice on every machine.
func (st mixState) picks(weights []int64, n int) []int {
	var sum int64
	for _, w := range weights {
		sum += w
	}
	rate := make([]int64, len(weights)) // thousandths of an output a record gives
	expected := make([]int64, len(weights))
	var total int64
	for s := range weights {
		rate[s] = 1000
		if st.Prev[s] > 0 {
			rate[s] = st.Outputs[s] * 1000 / st.Prev[s]
		}
		expected[s] = st.Outputs[s]*1000 + rate[s]*(st.Taken[s]-st.Prev[s])
		total += expected[s]
	}
	out := make([]int, n)
	for i := range out {
		best, bestBy := 0, int64(0)
		for s, w := range weights {
			if by := w*total - expected[s]*sum; s == 0 || by > bestBy {
				best, bestBy = s, by
			}
		}
		out[i] = best
		expected[best] += rate[best]
		total += rate[best]
	}
	return out
}

// rankSource is a rank's share of a source: its records i with i mod W =
// rank, the n-th being the source's record n*W + rank.
type rankSource struct {
	*sourceStream
	rank, world int64
	started     bool // a record of it was given: the next is W on
}

func (rs *rankSource) seekTo(ctx context.Context, n int64) error {
	rs.started = false
	return rs.seek(ctx, n*rs.world+rs.rank)
}

// nextOwn is the rank's next record of the source.
func (rs *rankSource) nextOwn(ctx context.Context) ([]byte, error) {
	if rs.started {
		for i := int64(1); i < rs.world; i++ {
			if _, _, err := rs.next(ctx); err != nil {
				return nil, err
			}
		}
	}
	rec, _, err := rs.next(ctx)
	if err != nil {
		return nil, err
	}
	rs.started = true
	return rec, nil
}

// orderStateFile is where rank r's chunk states are kept.
func (l *Loader) orderStateFile(r int) string {
	return filepath.Join(l.OrderState, fmt.Sprintf("rank-%d.jsonl", r))
}

// keepMixState appends a chunk's state to rank r's file, synced: it is there
// before any of the chunk's records is handed over.
func (l *Loader) keepMixState(r int, st mixState) error {
	if l.OrderState == "" {
		return nil
	}
	if err := os.MkdirAll(l.OrderState, 0o755); err != nil {
		return fmt.Errorf("the stream's order state: %w", err)
	}
	body, err := json.Marshal(st)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(l.orderStateFile(r), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o644)
	if err != nil {
		return fmt.Errorf("the stream's order state: %w", err)
	}
	_, err = f.Write(append(body, '\n'))
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return fmt.Errorf("the stream's order state: %w", err)
	}
	return nil
}

// mixStateAt is the state of the chunk rank r's record lies in: a new one at
// the start, else the last kept that begins at or before it.
func (l *Loader) mixStateAt(r int, record int64, sources int) (mixState, error) {
	if record == 0 {
		return newMixState(sources), nil
	}
	if l.OrderState == "" {
		return mixState{}, fmt.Errorf("rank %d starts at its record %d of a stream weighted by output, and no order state is kept (the loader's --order-state)", r, record)
	}
	f, err := os.Open(l.orderStateFile(r))
	if err != nil {
		return mixState{}, fmt.Errorf("rank %d starts at its record %d of a stream weighted by output: its order state: %w", r, record, err)
	}
	defer f.Close()
	var best *mixState
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<16), 1<<24)
	for sc.Scan() {
		var st mixState
		if json.Unmarshal(sc.Bytes(), &st) != nil || len(st.Taken) != sources || len(st.Prev) != sources || len(st.Outputs) != sources {
			continue // a line cut short when the loader stopped
		}
		if st.Record <= record && (best == nil || st.Record > best.Record) {
			best = &st
		}
	}
	if err := sc.Err(); err != nil {
		return mixState{}, fmt.Errorf("rank %d's order state: %w", r, err)
	}
	if best == nil {
		return mixState{}, fmt.Errorf("rank %d starts at its record %d of a stream weighted by output, and its order state has no chunk starting at or before it", r, record)
	}
	return *best, nil
}

// outputMeta is what the consumer of a rank's stages is told of each record
// put through them, in their order: the rank's record (-1: counted only),
// its source and chunk, whether its outputs are handed over.
type outputMeta struct {
	record int64
	source int
	chunk  int64
	hand   bool
}

// chunkGave is what a chunk's records gave, per source.
type chunkGave struct {
	chunk   int64
	outputs []int64
}

// produceRankByOutput is produceRank for a stream weighted by output.
func (l *Loader) produceRankByOutput(ctx context.Context, gen *generation, r int, positions []Position) error {
	world := int64(len(positions))
	pos := positions[r]
	n := len(l.Spec.Sources)
	weights := make([]int64, n)
	sources := make([]*rankSource, n)
	defer func() {
		for _, s := range sources {
			if s != nil {
				s.Close()
			}
		}
	}()
	for i, spec := range l.Spec.Sources {
		weights[i] = int64(max(spec.Weight, 1))
		s, err := newSourceStream(spec, i, l.Fetch, l.Spec.ShuffleFiles, l.Spec.Seed, l.Learned, l.Identified)
		if err != nil {
			return err
		}
		sources[i] = &rankSource{sourceStream: s, rank: int64(r), world: world}
	}
	if l.Spec.Epochs > 0 {
		sources[0].endEpoch = l.Spec.Epochs
	}
	start, err := l.mixStateAt(r, pos.Record, n)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	orderDone := make(chan struct{})
	started := false
	defer func() {
		cancel()
		if started {
			<-orderDone
		}
	}()
	errs := make(chan error, len(l.Spec.Stages)+1)
	groups := make(chan group, 64)
	var out <-chan group = groups
	for _, st := range l.Spec.Stages {
		st.Workers = max(1, max(st.Workers, 1)/int(world))
		out = runStage(ctx, st, out, errs, l.Stderr)
	}
	metas := make(chan outputMeta, 4096)
	gave := make(chan chunkGave, 64)
	orderErr := make(chan error, 1)
	started = true
	go func() {
		defer close(orderDone)
		defer close(groups)
		orderErr <- l.chooseByOutput(ctx, gen, r, sources, weights, start, pos, groups, metas, gave)
	}()
	// the consumer: the stages' outputs in order, handed over (or counted
	// only), what each chunk gave told back to the order
	current, counts := int64(-1), make([]int64, n)
	for grp := range out {
		var m outputMeta
		select {
		case m = <-metas:
		case <-ctx.Done():
			return ctx.Err()
		}
		if m.chunk != current {
			if current >= 0 {
				select {
				case gave <- chunkGave{chunk: current, outputs: counts}:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			current, counts = m.chunk, make([]int64, n)
		}
		counts[m.source] += int64(len(grp.recs))
		if !m.hand {
			continue
		}
		outs, sub := grp.recs, int64(0)
		if m.record == pos.Record && pos.Sub > 0 {
			sub = pos.Sub
			if sub >= int64(len(outs)) {
				return fmt.Errorf("rank %d stands at output %d of its record %d, which gives %d outputs now: not the stream it was", r, sub, m.record, len(outs))
			}
			outs = outs[sub:]
		}
		if len(grp.recs) == 0 {
			if err := l.put(ctx, gen, r, item{record: m.record, skip: true}); err != nil {
				return err
			}
			continue
		}
		for i, data := range outs {
			if err := l.put(ctx, gen, r, item{record: m.record, sub: sub + int64(i), last: i == len(outs)-1, data: data}); err != nil {
				return err
			}
			l.outputs.Add(1)
			l.bytes.Add(int64(len(data)))
		}
	}
	select {
	case err := <-errs:
		return err
	default:
	}
	if err := <-orderErr; err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return nil
}

// chooseByOutput is the order of rank r of a stream weighted by output: the
// chunk before start's counted again (unless start's is the first), then
// chunk after chunk from start's, each record put through the stages with
// what the consumer is told of it, until the first source ends.
func (l *Loader) chooseByOutput(ctx context.Context, gen *generation, r int, sources []*rankSource, weights []int64, start mixState, pos Position,
	groups chan<- group, metas chan<- outputMeta, gave <-chan chunkGave) error {
	first := start.Record / int64(outputChunk)
	send := func(m outputMeta, rec []byte) error {
		select {
		case metas <- m:
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case groups <- group{g: m.record, recs: [][]byte{rec}}:
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	}
	waitGave := func(chunk int64) ([]int64, error) {
		select {
		case g := <-gave:
			if g.chunk != chunk {
				return nil, fmt.Errorf("rank %d: chunk %d's outputs counted where chunk %d's were due", r, g.chunk, chunk)
			}
			return g.outputs, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	var at []SourceAt
	status := func() {
		if now := readingOf(sources, at); now != nil {
			at = now
			gen.reading[r].Store(&now)
		}
	}
	// the chunk before: what it gave is counted again (none of it handed)
	if first > 0 {
		for s, src := range sources {
			if err := src.seekTo(ctx, start.Prev[s]); err != nil {
				return err
			}
			for i := start.Prev[s]; i < start.Taken[s]; i++ {
				rec, err := src.nextOwn(ctx)
				if err != nil {
					return fmt.Errorf("rank %d: the chunk before its record %d: %w", r, start.Record, err)
				}
				status()
				if err := send(outputMeta{record: -1, source: s, chunk: first - 1}, rec); err != nil {
					return err
				}
			}
		}
	} else {
		for s, src := range sources {
			if err := src.seekTo(ctx, start.Taken[s]); err != nil {
				return err
			}
		}
	}
	st := start
	outputs := append([]int64(nil), start.Outputs...) // what the records before st.Prev gave
	for c := first; ; c++ {
		if c > first {
			// chunk c is chosen from what chunk c-2 gave
			if c >= 2 {
				g, err := waitGave(c - 2)
				if err != nil {
					return err
				}
				for s := range outputs {
					outputs[s] += g[s]
				}
			}
			st.Outputs = append([]int64(nil), outputs...)
		}
		if err := l.keepMixState(r, st); err != nil {
			return err
		}
		picks := st.picks(weights, outputChunk)
		recs, from := make([][]byte, 0, len(picks)), make([]int, 0, len(picks))
		taken := append([]int64(nil), st.Taken...)
		ended := false
		for _, s := range picks {
			rec, err := sources[s].nextOwn(ctx)
			if errors.Is(err, errSourceEnded) {
				ended = true
				break
			}
			if err != nil {
				return err
			}
			status()
			recs, from = append(recs, rec), append(from, s)
			taken[s]++
		}
		var perm []int
		if l.Spec.Block > 1 && len(recs) > 1 {
			perm = permutation(len(recs), seedOf(l.Spec.Seed, "output-chunk", uint64(r), uint64(c)))
		}
		for t := range recs {
			i := t
			if perm != nil {
				i = perm[t]
			}
			k := st.Record + int64(t)
			if err := send(outputMeta{record: k, source: from[i], chunk: c, hand: k >= pos.Record}, recs[i]); err != nil {
				return err
			}
			recs[i] = nil
		}
		end := st.Record + int64(len(from))
		if ended {
			// a rank that was to start past the end stood where this stream never was
			if pos.Record > end || pos.Record == end && pos.Sub > 0 {
				return fmt.Errorf("rank %d was to start at %+v, past the stream's end (its %d records)", r, pos, end)
			}
			return nil
		}
		st = mixState{Record: end, Taken: taken, Prev: st.Taken}
	}
}

// readingOf is where a rank's sources are read now, when that changed since
// prev (nil when not).
func readingOf(sources []*rankSource, prev []SourceAt) []SourceAt {
	changed := len(prev) != len(sources)
	for i, s := range sources {
		if changed || prev[i].Epoch != s.epoch || prev[i].File != s.fileName() {
			changed = true
			break
		}
	}
	if !changed {
		return nil
	}
	at := make([]SourceAt, len(sources))
	for i, s := range sources {
		at[i] = SourceAt{Source: s.spec.Name, Epoch: s.epoch, File: s.fileName()}
	}
	return at
}
