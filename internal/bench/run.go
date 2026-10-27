// Package bench runs fixed, deterministic workloads for a given time and
// streams what they did as JSON lines (kairo bench run, kairo bench serve).
// The records are facts — work done per interval on the monotonic clock,
// latencies, the identity of the device — for an orchestrator that windows
// them, compares them to a reference and combines them with energy; bench
// itself never compares to anything.
package bench

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// RefusedError is a run refused before it began, because the machine could
// not run it safely (too little free space or memory, missing device).
type RefusedError struct{ Reason string }

func (e *RefusedError) Error() string { return "refused: " + e.Reason }

func refuse(format string, args ...any) error {
	return &RefusedError{Reason: fmt.Sprintf(format, args...)}
}

// limitError ends a run at a safety limit (exit 0, end reason "limit").
type limitError struct{ limit, detail string }

func (e *limitError) Error() string { return e.limit + ": " + e.detail }

// plan is a prepared workload: what start reports and the work to run.
type plan struct {
	version int
	units   []string
	params  map[string]any
	device  map[string]any
	setup   map[string]any
	// work runs until ctx ends (return nil) or a worker fails; it reports
	// through m. A *limitError ends the run at a safety limit.
	work func(ctx context.Context, m *meter) error
	// check runs at every interval; a *limitError stops the run.
	check func() error
	// finish releases what prepare took and completes the end record; it
	// runs exactly once, whatever happened.
	finish func(end *endRecord)
}

// meter counts the work of a run; workers add to it, the reporter reads
// and resets it every interval.
type meter struct {
	units []string
	work  []atomic.Uint64
	ops   atomic.Uint64

	mu        sync.Mutex
	recorders []*latencyRecorder
}

func newMeter(units []string) *meter {
	return &meter{units: units, work: make([]atomic.Uint64, len(units))}
}

// add credits ops operations and n units of each kind (in the order of units).
func (m *meter) add(ops uint64, n ...uint64) {
	for i, v := range n {
		m.work[i].Add(v)
	}
	m.ops.Add(ops)
}

// latencyRecorder is a worker's latency histogram; each worker has its own
// so recording does not contend.
type latencyRecorder struct {
	mu sync.Mutex
	h  histogram
}

func (m *meter) recorder() *latencyRecorder {
	r := &latencyRecorder{}
	m.mu.Lock()
	m.recorders = append(m.recorders, r)
	m.mu.Unlock()
	return r
}

// record records the latency of a request that began at began (monoNow).
func (r *latencyRecorder) record(began int64) {
	d := max(monoNow()-began, 0)
	r.mu.Lock()
	r.h.record(uint64(d))
	r.mu.Unlock()
}

// take moves the counts since the last take into an interval.
func (m *meter) take(into *histogram) (map[string]uint64, uint64) {
	work := make(map[string]uint64, len(m.units))
	for i, u := range m.units {
		work[u] = m.work[i].Swap(0)
	}
	ops := m.ops.Swap(0)
	m.mu.Lock()
	recorders := m.recorders
	m.mu.Unlock()
	for _, r := range recorders {
		r.mu.Lock()
		into.merge(&r.h)
		r.h.reset()
		r.mu.Unlock()
	}
	return work, ops
}

// prepareFunc sets a workload up; it is given the params already known.
type prepareFunc func(ctx context.Context, cfg *Config, p *plan) error

var preparers = map[string]prepareFunc{
	"cpu-compute":    prepareCompute,
	"cpu-single":     prepareCompute,
	"cpu-stream":     prepareStream,
	"disk-seq":       prepareStorage,
	"disk-rand":      prepareStorage,
	"disk-fsync":     prepareStorage,
	"disk-sustained": prepareStorage,
	"net-latency":    prepareNetLatency,
	"net-bandwidth":  prepareNetBandwidth,
}

// Run runs cfg's workload, writing its records to out, until cfg.Seconds
// pass, ctx ends (SIGTERM, Ctrl-C: the end record says "signal") or a
// safety limit is reached. It returns nil for those; a refused or failed
// run returns the error its end record carries.
func Run(ctx context.Context, out io.Writer, cfg Config) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	em := newEmitter(out, cfg.Workload)
	p := &plan{version: 1, params: baseParams(&cfg), device: map[string]any{}, setup: map[string]any{}, finish: func(*endRecord) {}}
	err := preparers[cfg.Workload](ctx, &cfg, p)
	if err != nil {
		end := &endRecord{Params: p.params, Device: p.device, Work: map[string]uint64{}}
		if len(p.device) == 0 {
			end.Device = nil
		}
		p.finish(end)
		if ctx.Err() != nil {
			end.Reason = "signal"
			em.write(finish(em, end, now()))
			return nil
		}
		return fail(em, end, err)
	}
	return runPlan(ctx, em, &cfg, p)
}

// fail writes an end record for err and returns err.
func fail(em *emitter, end *endRecord, err error) error {
	end.Reason = "error"
	if errors.As(err, new(*RefusedError)) {
		end.Reason = "refused"
	}
	msg := err.Error()
	end.Error = &msg
	em.write(finish(em, end, now()))
	return err
}

func finish(em *emitter, end *endRecord, at instant) *endRecord {
	end.header = em.header("end", at)
	return end
}

// baseParams are the parameters every run reports.
func baseParams(cfg *Config) map[string]any {
	return map[string]any{"seconds": cfg.Seconds, "interval_ms": cfg.interval().Milliseconds()}
}

func runPlan(ctx context.Context, em *emitter, cfg *Config, p *plan) error {
	m := newMeter(p.units)
	start := now()
	em.started, em.origin = true, start.mono
	em.write(&startRecord{header: em.header("start", start), Version: p.version, Build: build(), Params: p.params,
		Units: p.units, Host: currentHost(), Device: p.device, Setup: p.setup})

	workCtx, stopWork := context.WithCancel(context.Background())
	defer stopWork()
	workDone := make(chan error, 1)
	go func() { workDone <- p.work(workCtx, m) }()

	total := &histogram{}
	totalWork := make(map[string]uint64, len(p.units))
	var totalOps uint64
	index := 0
	begin := start
	emit := func(at instant) {
		var h histogram
		work, ops := m.take(&h)
		em.write(&intervalRecord{header: em.header("interval", at), Index: index, BeginNS: em.since(begin),
			EndNS: em.since(at), Work: work, Ops: ops, Latency: h.summary()})
		for u, v := range work {
			totalWork[u] += v
		}
		totalOps += ops
		total.merge(&h)
		index++
		begin = at
	}

	end := &endRecord{}
	var workErr error
	workEnded := false
	interval := cfg.interval()
	deadline, timed := start.mono+int64(cfg.duration()), cfg.duration() > 0
	// Boundaries sit at start + k*interval (no drift); the last one is the
	// deadline, which closes the final interval.
	untilNext := func() time.Duration {
		b := start.mono + int64(index+1)*int64(interval)
		if timed && deadline <= b {
			b = deadline
		}
		return time.Duration(b - monoNow())
	}
	timer := time.NewTimer(untilNext())
	defer timer.Stop()
loop:
	for {
		select {
		case <-ctx.Done():
			end.Reason = "signal"
			break loop
		case workErr = <-workDone:
			workEnded = true
			break loop
		case <-timer.C:
			at := now()
			// Within a millisecond of the deadline is the deadline: the timer
			// runs on the runtime's clock, monoNow may differ a little.
			if timed && at.mono >= deadline-int64(time.Millisecond) {
				end.Reason = "duration"
				break loop
			}
			emit(at)
			if p.check != nil {
				if err := p.check(); err != nil {
					workErr = err
					break loop
				}
			}
			timer.Reset(untilNext())
		}
	}
	stopWork()
	if !workEnded {
		if err := <-workDone; workErr == nil {
			workErr = err
		}
	}
	// The final interval holds what completed since the last boundary,
	// including the requests in flight when the run stopped.
	emit(now())
	end.ElapsedNS = em.since(begin) // the measurement window: the last interval's end
	end.Intervals, end.Work, end.Ops, end.Latency = index, totalWork, totalOps, total.summary()
	p.finish(end)
	var limit *limitError
	switch {
	case errors.As(workErr, &limit):
		end.Reason, end.Limit, end.LimitDetail = "limit", limit.limit, limit.detail
	case workErr != nil:
		return fail(em, end, workErr)
	case end.Reason == "":
		// The work returned without being stopped: it ran out of work.
		end.Reason = "duration"
	}
	em.write(finish(em, end, now()))
	return nil
}
