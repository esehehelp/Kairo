package bench

import (
	"math"
	"math/bits"
)

// histogram is a log-linear histogram of nanosecond latencies: values below
// 128 are exact; above, each power of two is split into 128 buckets, so a
// reported percentile is within 0.4% of a recorded value. kairo-bench-gpu
// implements the same buckets and quantile rule, so their percentiles agree.
type histogram struct {
	counts   [histogramBuckets]uint64
	count    uint64
	sum      float64
	min, max uint64
}

const (
	histogramSubBits = 7
	histogramSub     = 1 << histogramSubBits // 128
	histogramBuckets = histogramSub + (64-histogramSubBits)*histogramSub
)

// bucketOf is the bucket holding v.
func bucketOf(v uint64) int {
	if v < histogramSub {
		return int(v)
	}
	msb := bits.Len64(v) - 1
	shift := msb - histogramSubBits
	sub := int(v>>shift) - histogramSub
	return histogramSub + shift*histogramSub + sub
}

// bucketRange is the first value of bucket i and its width.
func bucketRange(i int) (low, width uint64) {
	if i < histogramSub {
		return uint64(i), 1
	}
	shift := (i - histogramSub) / histogramSub
	sub := (i - histogramSub) % histogramSub
	return uint64(histogramSub+sub) << shift, 1 << shift
}

func (h *histogram) record(v uint64) {
	h.counts[bucketOf(v)]++
	if h.count == 0 || v < h.min {
		h.min = v
	}
	if v > h.max {
		h.max = v
	}
	h.count++
	h.sum += float64(v)
}

// merge adds o's values to h.
func (h *histogram) merge(o *histogram) {
	if o.count == 0 {
		return
	}
	for i, c := range o.counts {
		h.counts[i] += c
	}
	if h.count == 0 || o.min < h.min {
		h.min = o.min
	}
	h.max = max(h.max, o.max)
	h.count += o.count
	h.sum += o.sum
}

func (h *histogram) reset() { *h = histogram{} }

// quantile is the nearest-rank q-quantile: the middle of the bucket holding
// the ceil(q*count)-th smallest value, clamped to the recorded min and max.
func (h *histogram) quantile(q float64) uint64 {
	if h.count == 0 {
		return 0
	}
	rank := max(uint64(math.Ceil(q*float64(h.count))), 1)
	var seen uint64
	for i, c := range h.counts {
		seen += c
		if seen >= rank {
			low, width := bucketRange(i)
			return min(max(low+(width-1)/2, h.min), h.max)
		}
	}
	return h.max
}

// latencySummary is the latency of the requests of an interval or a run.
type latencySummary struct {
	Count  uint64 `json:"count"`
	MinNS  uint64 `json:"min_ns"`
	MeanNS uint64 `json:"mean_ns"`
	P50NS  uint64 `json:"p50_ns"`
	P99NS  uint64 `json:"p99_ns"`
	P999NS uint64 `json:"p999_ns"`
	MaxNS  uint64 `json:"max_ns"`
}

// summary is nil for an empty histogram.
func (h *histogram) summary() *latencySummary {
	if h.count == 0 {
		return nil
	}
	return &latencySummary{
		Count:  h.count,
		MinNS:  h.min,
		MeanNS: uint64(math.Round(h.sum / float64(h.count))),
		P50NS:  h.quantile(0.5),
		P99NS:  h.quantile(0.99),
		P999NS: h.quantile(0.999),
		MaxNS:  h.max,
	}
}
