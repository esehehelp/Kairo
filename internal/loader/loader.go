package loader

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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
	// StateDir, when set, keeps which ranks were served across the
	// loader's restarts (a file served-<rank> each).
	StateDir string

	mu        sync.Mutex
	world     int
	delivered []Position // per rank: its next position not yet handed over
	floor     []Position // per rank: where the stream was started (a checkpoint's)
	present   []bool     // per rank: a process takes it now
	served    []bool     // per rank: it was handed an item (since the stream started: StateDir)
	gen       *generation
	stopped   bool
	hard      error
	counters  []rankCounters // per rank, over the loader's life (Status)

	outputs, bytes atomic.Int64
}

// generation is the stream made from one set of positions; a rank that
// comes back from elsewhere than where it was handed makes another. Each
// rank's records are made by a producer of its own (produceRank), so that
// no rank's waiting holds another up: two ranks taking the same number of
// outputs need different numbers of records (a record gives none or many),
// a gap that grows without bound as a run goes on, which one producer
// making every rank's records in turn would have to hold.
type generation struct {
	cancel context.CancelFunc
	ctx    context.Context
	queues []chan item
	// budgets bound the bytes made ahead for each rank; queuedBytes is
	// what each has taken of its budget now
	budgets     []*semaphore.Weighted
	budget      int64
	queuedBytes []atomic.Int64
	// reading is where each rank's producer reads its sources now (Status)
	reading []atomic.Pointer[[]SourceAt]
	// done[r] closes once rank r's producer is done; last[r] is what rank r
	// is handed then, once its queue is empty (handed again to one that
	// comes back): the end, or the stream's failure (of any rank's)
	done []chan struct{}
	last []*item
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
	l.present, l.served = make([]bool, world), make([]bool, world)
	l.counters = make([]rankCounters, world)
	if l.StateDir != "" {
		// the ranks it was started for, and those served, kept across restarts
		if kept := l.keptWorldLocked(); kept != 0 && kept != world {
			return fmt.Errorf("the stream was started for %d ranks, not %d (a run keeps its ranks)", kept, world)
		}
		if err := os.WriteFile(filepath.Join(l.StateDir, "world"), []byte(strconv.Itoa(world)), 0o644); err != nil {
			return fmt.Errorf("the stream's state: %w", err)
		}
		for r := range l.served {
			_, err := os.Stat(filepath.Join(l.StateDir, fmt.Sprintf("served-%d", r)))
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("the stream's state: %w", err)
			}
			l.served[r] = err == nil
		}
	}
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
		gen := &generation{cancel: func() {}, queues: make([]chan item, l.world), done: make([]chan struct{}, l.world), last: make([]*item, l.world)}
		for r := range gen.queues {
			gen.queues[r], gen.done[r], gen.last[r] = make(chan item), make(chan struct{}), &item{err: l.hard}
			close(gen.done[r])
		}
		l.gen = gen
		return
	}
	ctx, cancel := context.WithCancel(parent)
	gen := &generation{cancel: cancel, ctx: ctx, queues: make([]chan item, l.world), done: make([]chan struct{}, l.world), last: make([]*item, l.world), budget: l.rankBudget(),
		queuedBytes: make([]atomic.Int64, l.world), reading: make([]atomic.Pointer[[]SourceAt], l.world)}
	for r := range gen.queues {
		gen.queues[r], gen.done[r] = make(chan item, rankQueue), make(chan struct{})
		gen.budgets = append(gen.budgets, semaphore.NewWeighted(gen.budget))
	}
	l.gen = gen
	positions := append([]Position(nil), l.delivered...)
	for r := range positions {
		go func() {
			defer close(gen.done[r])
			err := l.produceRank(ctx, gen, r, positions)
			if ctx.Err() != nil {
				return // made again from other positions, or stopped (or another rank failed)
			}
			l.mu.Lock()
			defer l.mu.Unlock()
			if err == nil {
				gen.last[r] = &item{end: true}
				return
			}
			// a rank's failure is the stream's: every rank is handed it
			l.hard = err
			for q := range gen.last {
				gen.last[q] = &item{err: err}
			}
			cancel()
		}()
	}
}

// produceRank makes rank r's records of one generation into its queue:
// the order's records g with g mod W = r, from r's position, through the
// stages (rank r's share of each stage's workers). It reads the order
// itself (every rank reads the raw records; only its own go through the
// stages), and it waits for its own rank's room only.
func (l *Loader) produceRank(ctx context.Context, gen *generation, r int, positions []Position) error {
	world := int64(len(positions))
	pos := positions[r]
	in, err := newInputs(l.Spec, l.Fetch, l.Learned, l.Identified)
	if err != nil {
		return err
	}
	// the inputs are closed only once the goroutine reading them is done:
	// on every way out, the goroutines of this producer are told to end and
	// waited for
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
	if err := order.seek(ctx, pos.Record*world+int64(r)); err != nil {
		return err
	}
	errs := make(chan error, len(l.Spec.Stages)+1)
	groups := make(chan group, 64)
	var out <-chan group = groups
	for _, st := range l.Spec.Stages {
		st.Workers = max(1, max(st.Workers, 1)/int(world))
		out = runStage(ctx, st, out, errs, l.Stderr)
	}
	orderErr := make(chan error, 1)
	started = true
	go func() {
		defer close(orderDone)
		defer close(groups)
		var at []SourceAt
		for {
			g, rec, ok, err := order.next(ctx)
			if err != nil || !ok {
				orderErr <- err
				return
			}
			if now := in.reading(at); now != nil {
				at = now
				gen.reading[r].Store(&now)
			}
			if int(g%world) != r || g/world < pos.Record {
				continue // another rank's, or taken before
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
		k := grp.g / world
		outs, sub := grp.recs, int64(0)
		if k == pos.Record && pos.Sub > 0 {
			// a position within a record names one of its outputs: a record
			// giving fewer is not what it gave before (its stages changed)
			sub = pos.Sub
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
	total, records := order.g, int64(0)
	if total > int64(r) {
		records = (total - int64(r) + world - 1) / world
	}
	if pos.Record > records || pos.Record == records && pos.Sub > 0 {
		return fmt.Errorf("rank %d was to start at %+v, past the stream's end (its %d records)", r, pos, records)
	}
	return nil
}

// put hands it to rank r's queue, waiting for room there (its producer is
// its own: a rank away, or slow, holds no other up).
func (l *Loader) put(ctx context.Context, gen *generation, r int, it item) error {
	cost := gen.cost(it)
	if err := gen.budgets[r].Acquire(ctx, cost); err != nil {
		return err
	}
	gen.queuedBytes[r].Add(cost)
	select {
	case gen.queues[r] <- it:
		return nil
	case <-ctx.Done():
		gen.budgets[r].Release(cost)
		gen.queuedBytes[r].Add(-cost)
		return ctx.Err()
	}
}

// attach: a process takes rank r now.
func (l *Loader) attach(_ context.Context, r int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.present[r] = true
}

// detach: no process takes rank r now (its producer waits for room, holding
// no other rank up).
func (l *Loader) detach(r int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.present[r] = false
}

// keptWorld is the ranks the stream was started for before (StateDir; 0:
// not started before).
func (l *Loader) keptWorld() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.keptWorldLocked()
}

func (l *Loader) keptWorldLocked() int {
	if l.StateDir == "" {
		return 0
	}
	body, err := os.ReadFile(filepath.Join(l.StateDir, "world"))
	if err != nil {
		return 0
	}
	n, _ := strconv.Atoi(strings.TrimSpace(string(body)))
	return n
}

// servedBefore: rank r was handed an item since the stream started.
func (l *Loader) servedBefore(r int) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.served[r]
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
	gen, counters := l.gen, l.counters
	l.mu.Unlock()
	if gen == nil {
		return item{}, false
	}
	select {
	case it := <-gen.queues[r]:
		return l.took(gen, r, it)
	default:
	}
	// asked with nothing made: the taker waits for the loader
	if r < len(counters) {
		defer func(start time.Time) { counters[r].starved.Add(int64(time.Since(start))) }(time.Now())
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case it := <-gen.queues[r]:
		return l.took(gen, r, it)
	case <-gen.done[r]:
		// what was made is handed over first, then the end, again and again
		select {
		case it := <-gen.queues[r]:
			return l.took(gen, r, it)
		default:
		}
		l.mu.Lock()
		last, current := gen.last[r], l.gen == gen
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
		gen.queuedBytes[r].Add(-gen.cost(it))
	}
	l.mu.Lock()
	current := l.gen == gen
	if current && !it.end && it.err == nil && !it.skip && r < len(l.counters) {
		l.counters[r].taken.Add(1)
		l.counters[r].takenBytes.Add(int64(len(it.data)))
	}
	if current && !it.end && it.err == nil && !l.served[r] {
		l.served[r] = true
		if l.StateDir != "" {
			f, err := os.Create(filepath.Join(l.StateDir, fmt.Sprintf("served-%d", r)))
			if err == nil {
				err = f.Close()
			}
			if err != nil {
				// a rank served but not known so after a restart could start
				// again where it was: the stream fails instead
				l.hard = fmt.Errorf("the stream's state: %w", err)
				l.startLocked(context.Background())
				l.mu.Unlock()
				return item{}, false
			}
		}
	}
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
