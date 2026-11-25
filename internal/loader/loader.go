package loader

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/semaphore"
)

// Position is where a rank stands in a stream: Record of its records taken
// in full (the rank's records are outputs g of the order with g mod W =
// rank, the k-th being g = k*W + rank) and Sub outputs of the next one.
type Position struct {
	Rank   int   `json:"rank"`
	Record int64 `json:"record"`
	Sub    int64 `json:"sub"`
}

// item is what a rank is handed: an output of its record Record (the Sub-th;
// Last when no more of that record follow), or that the record gave none
// (Skip), or the stream's end or failure.
type item struct {
	record int64
	sub    int64
	last   bool
	skip   bool
	data   []byte
	end    bool
	err    error
}

// rankQueue is what is made for one rank and not yet handed over.
const rankQueue = 4096

// Loader makes a stream for the ranks of one attempt: the order (Spec), its
// records fetched by Fetch, split between World ranks and put through the
// stages, from the ranks' positions.
type Loader struct {
	Spec   Spec
	Fetch  *Fetcher
	Stderr io.Writer
	// Learned hears a file's record count once a source read it to its end
	// (for Kairo to keep: a later start mid-way passes the file over).
	Learned func(source int, f File, records int64)
	// Identified hears a file's identity once a source read it the first
	// time (for Kairo to keep: later reads are checked against it).
	Identified func(source int, f File)

	mu        sync.Mutex
	world     int
	delivered []Position // per rank: its next position not yet handed over
	floor     []Position // per rank: where the stream was started (a checkpoint's)
	gen       *generation
	stopped   bool
	hard      error

	outputs, bytes atomic.Int64
}

// generation is the stream made from one set of positions; a rank that
// comes back from elsewhere than where it was handed makes another.
type generation struct {
	cancel context.CancelFunc
	queues []chan item
	// budgets bound the bytes made ahead for each rank
	budgets []*semaphore.Weighted
	budget  int64
	done    chan struct{}
	// last is what each rank is handed once the stream ended or failed and
	// its queue is empty (handed again to one that comes back)
	last *item
}

// Start makes the stream for world ranks from their positions (nil: from
// the start); a later Hello may make it again from others.
func (l *Loader) Start(ctx context.Context, world int, positions []Position) error {
	if err := l.Spec.check(); err != nil {
		return err
	}
	if world < 1 {
		return fmt.Errorf("a stream is for 1 or more ranks, not %d", world)
	}
	if l.Stderr == nil {
		l.Stderr = os.Stderr
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(positions) > 0 && len(positions) != world {
		return fmt.Errorf("positions of %d ranks, for %d ranks (a run keeps its ranks)", len(positions), world)
	}
	l.world = world
	l.delivered = make([]Position, world)
	seen := make([]bool, world)
	for r := range l.delivered {
		l.delivered[r] = Position{Rank: r}
	}
	for _, p := range positions {
		if p.Rank < 0 || p.Rank >= world || seen[p.Rank] || p.Record < 0 || p.Record > maxRecord || p.Sub < 0 || p.Sub > maxSub {
			return fmt.Errorf("positions %v are not one each of ranks 0..%d", positions, world-1)
		}
		seen[p.Rank] = true
		l.delivered[p.Rank] = p
	}
	l.floor = append([]Position(nil), l.delivered...)
	l.startLocked(ctx)
	return nil
}

// Stop ends the stream.
func (l *Loader) Stop() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.stopped = true
	if l.gen != nil {
		l.gen.cancel()
	}
}

// World is the ranks the stream is for (0 before Start).
func (l *Loader) World() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.world
}

// Delivered is each rank's next position not handed over yet.
func (l *Loader) Delivered() []Position {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]Position(nil), l.delivered...)
}

// startLocked makes a generation from the delivered positions.
func (l *Loader) startLocked(parent context.Context) {
	if l.gen != nil {
		l.gen.cancel()
	}
	if l.hard != nil {
		// a stream that failed is not made again
		gen := &generation{cancel: func() {}, queues: make([]chan item, l.world), done: make(chan struct{}), last: &item{err: l.hard}}
		for r := range gen.queues {
			gen.queues[r] = make(chan item)
		}
		close(gen.done)
		l.gen = gen
		return
	}
	ctx, cancel := context.WithCancel(parent)
	gen := &generation{cancel: cancel, queues: make([]chan item, l.world), done: make(chan struct{}), budget: l.rankBudget()}
	for r := range gen.queues {
		gen.queues[r] = make(chan item, rankQueue)
		gen.budgets = append(gen.budgets, semaphore.NewWeighted(gen.budget))
	}
	l.gen = gen
	positions := append([]Position(nil), l.delivered...)
	go func() {
		defer close(gen.done)
		err := l.produce(ctx, gen, positions)
		if ctx.Err() != nil {
			return // made again from other positions, or stopped
		}
		end := item{end: true}
		l.mu.Lock()
		if err != nil {
			end = item{err: err}
			l.hard = err
		}
		gen.last = &end
		l.mu.Unlock()
	}()
}

// produce makes the stream of one generation into its queues.
func (l *Loader) produce(ctx context.Context, gen *generation, positions []Position) error {
	world := int64(len(positions))
	start := int64(-1)
	for _, p := range positions {
		if g := p.Record*world + int64(p.Rank); start < 0 || g < start {
			start = g
		}
	}
	in, err := newInputs(l.Spec, l.Fetch, l.Learned, l.Identified)
	if err != nil {
		return err
	}
	// the inputs are closed only once the goroutine reading them is done:
	// on every way out, the goroutines of this generation are told to end
	// and waited for
	ctx, cancel := context.WithCancel(ctx)
	orderDone := make(chan struct{})
	started := false
	defer func() {
		cancel()
		if started {
			<-orderDone
		}
		in.Close()
	}()
	order := newOrderer(in, l.Spec.Block, l.Spec.Seed)
	if err := order.seek(ctx, start); err != nil {
		return err
	}
	errs := make(chan error, len(l.Spec.Stages)+1)
	groups := make(chan group, 64)
	var out <-chan group = groups
	for _, st := range l.Spec.Stages {
		out = runStage(ctx, st, out, errs, l.Stderr)
	}
	orderErr := make(chan error, 1)
	started = true
	go func() {
		defer close(orderDone)
		defer close(groups)
		for {
			g, rec, ok, err := order.next(ctx)
			if err != nil || !ok {
				orderErr <- err
				return
			}
			r, k := int(g%world), g/world
			if k < positions[r].Record {
				continue // taken before
			}
			select {
			case groups <- group{g: g, recs: [][]byte{rec}}:
			case <-ctx.Done():
				orderErr <- ctx.Err()
				return
			}
		}
	}()
	for grp := range out {
		r, k := int(grp.g%world), grp.g/world
		outs, sub := grp.recs, int64(0)
		if k == positions[r].Record && positions[r].Sub > 0 {
			// a position within a record names one of its outputs: a record
			// giving fewer is not what it gave before (its stages changed)
			sub = positions[r].Sub
			if sub >= int64(len(outs)) {
				return fmt.Errorf("rank %d stands at output %d of its record %d, which gives %d outputs now: not the stream it was", r, sub, k, len(outs))
			}
			outs = outs[sub:]
		}
		if len(grp.recs) == 0 {
			if err := l.put(ctx, gen, r, item{record: k, skip: true}); err != nil {
				return err
			}
			continue
		}
		for i, data := range outs {
			it := item{record: k, sub: sub + int64(i), last: i == len(outs)-1, data: data}
			if err := l.put(ctx, gen, r, it); err != nil {
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
	// the stream ended: a rank that was to start past its end stood
	// somewhere this stream never was
	total := order.g
	for _, p := range positions {
		records := int64(0)
		if r := int64(p.Rank); total > r {
			records = (total - r + world - 1) / world
		}
		if p.Record > records || p.Record == records && p.Sub > 0 {
			return fmt.Errorf("rank %d was to start at %+v, past the stream's end (its %d records)", p.Rank, p, records)
		}
	}
	return nil
}

func (l *Loader) put(ctx context.Context, gen *generation, r int, it item) error {
	if err := gen.budgets[r].Acquire(ctx, gen.cost(it)); err != nil {
		return err
	}
	select {
	case gen.queues[r] <- it:
		return nil
	case <-ctx.Done():
		gen.budgets[r].Release(gen.cost(it))
		return ctx.Err()
	}
}

// cost is what an item takes of its rank's budget (all of it at most).
func (gen *generation) cost(it item) int64 {
	return min(int64(len(it.data))+64, gen.budget)
}

// rankBudget is the bytes made ahead for each rank.
func (l *Loader) rankBudget() int64 {
	if l.Spec.MemoryBytes <= 0 {
		return 64 << 20
	}
	return max(l.Spec.MemoryBytes/4/int64(l.world), 1<<20)
}

// ErrRankTaken: a rank already has a process taking its records.
var ErrRankTaken = errors.New("rank taken")

// take gives rank r's next item (waiting up to wait), from the generation
// current now; ok false when none came in time.
func (l *Loader) take(r int, wait time.Duration) (item, bool) {
	l.mu.Lock()
	gen := l.gen
	l.mu.Unlock()
	if gen == nil {
		return item{}, false
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case it := <-gen.queues[r]:
		return l.took(gen, r, it)
	case <-gen.done:
		// what was made is handed over first, then the end, again and again
		select {
		case it := <-gen.queues[r]:
			return l.took(gen, r, it)
		default:
		}
		l.mu.Lock()
		last, current := gen.last, l.gen == gen
		l.mu.Unlock()
		if last == nil || !current {
			return item{}, false
		}
		return *last, true
	case <-timer.C:
		return item{}, false
	}
}

// took records what rank r was handed from gen: where it stands now.
func (l *Loader) took(gen *generation, r int, it item) (item, bool) {
	if gen.budgets != nil && !it.end && it.err == nil {
		gen.budgets[r].Release(gen.cost(it))
	}
	l.mu.Lock()
	current := l.gen == gen
	if current && !it.end && it.err == nil {
		switch {
		case it.skip || it.last:
			l.delivered[r] = Position{Rank: r, Record: it.record + 1}
		default:
			l.delivered[r] = Position{Rank: r, Record: it.record, Sub: it.sub + 1}
		}
	}
	l.mu.Unlock()
	if !current {
		return item{}, false // of a generation made again since
	}
	return it, true
}

// resume makes the stream again when rank r comes back at p, not where it
// was handed to: every other rank from where it was handed to.
func (l *Loader) resume(ctx context.Context, r int, p Position) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if f := l.floor[r]; p.Record < f.Record || p.Record == f.Record && p.Sub < f.Sub {
		return fmt.Errorf("rank %d comes back at %+v, before where its stream started (%+v)", r, p, f)
	}
	if l.delivered[r] == p || l.stopped {
		return nil
	}
	l.delivered[r] = p
	l.startLocked(ctx)
	return nil
}
