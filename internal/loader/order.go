// Package loader makes a stream (issue #43): the records of raw data in one
// fixed order, mixed, shuffled, split between ranks and put through the
// project's stages, handed to the processes of an attempt over a local
// socket, from any position as exactly as from the start.
//
// Everything random here is fixed by this package's own generator
// (splitMix64, below, permutation), not math/rand: an order made today is the
// same order made from the same declaration years on.
package loader

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math/bits"
)

// splitMix64 is the generator of every permutation (Steele, Lea, Flood 2014).
type splitMix64 struct{ state uint64 }

func (r *splitMix64) next() uint64 {
	r.state += 0x9e3779b97f4a7c15
	z := r.state
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

// below is uniform in [0, n), n > 0 (Lemire's multiply and reject).
func (r *splitMix64) below(n uint64) uint64 {
	hi, lo := bits.Mul64(r.next(), n)
	if lo < n {
		threshold := -n % n
		for lo < threshold {
			hi, lo = bits.Mul64(r.next(), n)
		}
	}
	return hi
}

// seedOf is the seed of one permutation: the stream's seed, what it orders
// and which one (an epoch, a block), hashed.
func seedOf(seed uint64, purpose string, numbers ...uint64) uint64 {
	h := sha256.New()
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], seed)
	h.Write(b[:])
	h.Write([]byte(purpose))
	for _, n := range numbers {
		binary.LittleEndian.PutUint64(b[:], n)
		h.Write(b[:])
	}
	return binary.LittleEndian.Uint64(h.Sum(nil))
}

// permutation is a permutation of 0..n-1 (Fisher-Yates, from the last).
func permutation(n int, seed uint64) []int {
	p := make([]int, n)
	for i := range p {
		p[i] = i
	}
	r := splitMix64{state: seed}
	for i := n - 1; i > 0; i-- {
		j := int(r.below(uint64(i + 1)))
		p[i], p[j] = p[j], p[i]
	}
	return p
}

// maxMixPeriod bounds the sum of a mix's weights: one period of its pattern
// is kept.
const maxMixPeriod = 1 << 16

// maxMixTable bounds the counts a mixer keeps (8 bytes each): 8 MiB.
const maxMixTable = 1 << 20

// mixer interleaves sources by weight: a smooth weighted round robin (the
// source whose credit is greatest goes next, the lowest index on a tie),
// whose picks repeat every sum-of-weights inputs, so the source of any input
// and the records each source gave before it are known without replaying.
type mixer struct {
	pattern []int     // the source of each input of a period
	counts  [][]int64 // counts[i][s]: inputs of source s among pattern[:i]
	weights []int64
}

func newMixer(weights []int) (*mixer, error) {
	if len(weights) == 0 {
		return nil, fmt.Errorf("a stream has at least one source")
	}
	total := 0
	for i, w := range weights {
		if w < 1 || w > maxMixPeriod {
			return nil, fmt.Errorf("source %d has weight %d: weights are whole numbers from 1", i, w)
		}
		total += w
	}
	if total > maxMixPeriod {
		return nil, fmt.Errorf("the weights add up to %d, more than %d", total, maxMixPeriod)
	}
	if total*len(weights) > maxMixTable {
		return nil, fmt.Errorf("%d sources whose weights add up to %d: more than %d counts to keep (smaller weights, or fewer sources)", len(weights), total, maxMixTable)
	}
	m := &mixer{pattern: make([]int, total), counts: make([][]int64, total+1), weights: make([]int64, len(weights))}
	credit := make([]int, len(weights))
	m.counts[0] = make([]int64, len(weights))
	for i := 0; i < total; i++ {
		best := 0
		for s, w := range weights {
			credit[s] += w
			if credit[s] > credit[best] {
				best = s
			}
		}
		credit[best] -= total
		m.pattern[i] = best
		m.counts[i+1] = append([]int64(nil), m.counts[i]...)
		m.counts[i+1][best]++
	}
	for s, w := range weights {
		m.weights[s] = int64(w)
	}
	return m, nil
}

// source is the source of input n.
func (m *mixer) source(n int64) int { return m.pattern[n%int64(len(m.pattern))] }

// before is how many of the first n inputs each source gave.
func (m *mixer) before(n int64) []int64 {
	period := int64(len(m.pattern))
	cycles, rest := n/period, n%period
	out := make([]int64, len(m.weights))
	for s, w := range m.weights {
		out[s] = cycles*w + m.counts[rest][s]
	}
	return out
}
