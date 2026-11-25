package loader

import (
	"context"
	"errors"
	"fmt"
)

// Spec is a stream as declared ([streams.NAME]), its sources resolved: the
// sources and their weights, the seed of every permutation, how many records
// are shuffled together (Block; 0 or 1: none), whether each source's files
// are permuted per epoch, how many epochs of the first source the stream
// lasts (0: it does not end), and the stages records go through.
type Spec struct {
	Sources      []SourceSpec `json:"sources"`
	Seed         uint64       `json:"seed"`
	Block        int          `json:"block,omitempty"`
	ShuffleFiles bool         `json:"shuffle_files,omitempty"`
	Epochs       int64        `json:"epochs"`
	Stages       []Stage      `json:"stages,omitempty"`
	// MemoryBytes is what the loader may hold (the stream's memory_bytes):
	// a quarter of it, shared by the ranks, bounds what is made ahead for
	// them (0: 64 MiB a rank).
	MemoryBytes int64 `json:"memory_bytes,omitempty"`
}

// maxBlock bounds the records shuffled together (each kept in memory).
const maxBlock = 1 << 24

func (s Spec) check() error {
	if len(s.Sources) == 0 {
		return fmt.Errorf("a stream has at least one source")
	}
	if s.Block < 0 || s.Block > maxBlock {
		return fmt.Errorf("block %d: from 0 to %d records", s.Block, maxBlock)
	}
	if s.Epochs < 0 {
		return fmt.Errorf("epochs %d: 0 (no end) or more", s.Epochs)
	}
	for i, st := range s.Stages {
		if len(st.Argv) == 0 {
			return fmt.Errorf("stage %d has no argv", i)
		}
		if st.Workers < 0 || st.Workers > 256 {
			return fmt.Errorf("stage %d: from 1 to 256 workers", i)
		}
	}
	return nil
}

// inputs is the mixed sequence of every source's records (an input's index
// counts from the stream's first), up to the first record of the first
// source's epoch Epochs.
type inputs struct {
	mix     *mixer
	sources []*sourceStream
	epochs  int64
	n       int64 // the next input's index
	ended   bool
}

func newInputs(spec Spec, fetch *Fetcher, learned func(int, File, int64), identified func(int, File)) (*inputs, error) {
	weights := make([]int, len(spec.Sources))
	in := &inputs{epochs: spec.Epochs}
	for i, src := range spec.Sources {
		weights[i] = src.Weight
		if weights[i] == 0 && len(spec.Sources) == 1 {
			weights[i] = 1
		}
		s, err := newSourceStream(src, i, fetch, spec.ShuffleFiles, spec.Seed, learned, identified)
		if err != nil {
			in.Close()
			return nil, err
		}
		in.sources = append(in.sources, s)
	}
	mix, err := newMixer(weights)
	if err != nil {
		in.Close()
		return nil, err
	}
	in.mix = mix
	if spec.Epochs > 0 {
		in.sources[0].endEpoch = spec.Epochs
	}
	return in, nil
}

func (in *inputs) Close() {
	for _, s := range in.sources {
		s.Close()
	}
}

// seek puts the sequence before input n.
func (in *inputs) seek(ctx context.Context, n int64) error {
	in.n, in.ended = n, false
	for s, count := range in.mix.before(n) {
		if err := in.sources[s].seek(ctx, count); err != nil {
			return err
		}
	}
	return nil
}

// next is the next input; ok false once the stream ended.
func (in *inputs) next(ctx context.Context) ([]byte, bool, error) {
	if in.ended {
		return nil, false, nil
	}
	s := in.mix.source(in.n)
	rec, _, err := in.sources[s].next(ctx)
	if errors.Is(err, errSourceEnded) {
		in.ended = true
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	in.n++
	return rec, true, nil
}

// orderer gives the inputs block by block, each block's records permuted by
// (seed, block): the record at output g is that of input
// block(g)*B + perm[g mod B].
type orderer struct {
	in    *inputs
	block int64
	seed  uint64
	j     int64    // the block being given
	recs  [][]byte // its records, in input order
	perm  []int
	t     int // the next of it to give
	g     int64
	ended bool
}

func newOrderer(in *inputs, block int, seed uint64) *orderer {
	if block < 1 {
		block = 1
	}
	return &orderer{in: in, block: int64(block), seed: seed, j: -1}
}

// seek puts the order before output g.
func (o *orderer) seek(ctx context.Context, g int64) error {
	o.j, o.recs, o.perm, o.ended = g/o.block, nil, nil, false
	if err := o.in.seek(ctx, o.j*o.block); err != nil {
		return err
	}
	if err := o.fill(ctx); err != nil {
		return err
	}
	o.t, o.g = int(g%o.block), g
	if o.t > len(o.recs) {
		return fmt.Errorf("output %d is past the stream's end (%d)", g, o.j*o.block+int64(len(o.recs)))
	}
	return nil
}

// fill reads block j's records.
func (o *orderer) fill(ctx context.Context) error {
	o.recs = o.recs[:0]
	for int64(len(o.recs)) < o.block {
		rec, ok, err := o.in.next(ctx)
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		o.recs = append(o.recs, rec)
	}
	if o.block > 1 {
		o.perm = permutation(len(o.recs), seedOf(o.seed, "block", uint64(o.j)))
	} else {
		o.perm = nil
	}
	o.t = 0
	return nil
}

// next is output g's record; ok false once the stream ended.
func (o *orderer) next(ctx context.Context) (int64, []byte, bool, error) {
	if o.ended {
		return 0, nil, false, nil
	}
	if o.t >= len(o.recs) {
		if o.recs != nil && int64(len(o.recs)) < o.block {
			o.ended = true // the last block was short
			return 0, nil, false, nil
		}
		o.j++
		if err := o.fill(ctx); err != nil {
			return 0, nil, false, err
		}
		if len(o.recs) == 0 {
			o.ended = true
			return 0, nil, false, nil
		}
	}
	i := o.t
	if o.perm != nil {
		i = o.perm[o.t]
	}
	rec := o.recs[i]
	o.recs[i] = nil
	g := o.g
	o.t++
	o.g++
	return g, rec, true, nil
}
