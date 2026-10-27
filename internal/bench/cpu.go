package bench

import (
	"context"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/cpu"
)

// The cpu-compute kernel: per iteration, 8 independent FP64 FMA chains
// (x = x*a + b, 2 ops each) and one xorshift64 step (3 shifts, 3 xors): 22
// ops, all in registers. The chains converge to 1.0, never denormal or
// infinite, so the speed does not depend on the data; the iterations per
// thread scale with the core clock alone.
const (
	computeOpsPerIteration = 8*2 + 6
	computeIterations      = 1 << 18 // per chunk, the unit the meter is credited in
	fmaMul, fmaAdd         = 0.999999, 1e-6
)

type computeState struct {
	f [8]float64
	x uint64
	_ [56]byte // keep each thread's state on its own cache line
}

func computeChunk(st *computeState) {
	s0, s1, s2, s3, s4, s5, s6, s7 := st.f[0], st.f[1], st.f[2], st.f[3], st.f[4], st.f[5], st.f[6], st.f[7]
	x := st.x
	for range computeIterations {
		s0 = math.FMA(s0, fmaMul, fmaAdd)
		s1 = math.FMA(s1, fmaMul, fmaAdd)
		s2 = math.FMA(s2, fmaMul, fmaAdd)
		s3 = math.FMA(s3, fmaMul, fmaAdd)
		s4 = math.FMA(s4, fmaMul, fmaAdd)
		s5 = math.FMA(s5, fmaMul, fmaAdd)
		s6 = math.FMA(s6, fmaMul, fmaAdd)
		s7 = math.FMA(s7, fmaMul, fmaAdd)
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
	}
	st.f = [8]float64{s0, s1, s2, s3, s4, s5, s6, s7}
	st.x = x
}

// stopFlag is set when ctx ends; a worker loop polls it between chunks
// (cheaper than ctx.Err).
func stopFlag(ctx context.Context) *atomic.Bool {
	var stop atomic.Bool
	go func() {
		<-ctx.Done()
		stop.Store(true)
	}()
	return &stop
}

func cpuDevice() map[string]any {
	d := map[string]any{"kind": "cpu", "model": nil, "logical_cpus": runtime.NumCPU(), "gomaxprocs": runtime.GOMAXPROCS(0)}
	if m := cpuModel(); m != "" {
		d["model"] = m
	}
	if runtime.GOARCH == "amd64" {
		d["fma"] = cpu.X86.HasFMA // without it math.FMA runs in software
	}
	return d
}

func prepareCompute(_ context.Context, cfg *Config, p *plan) error {
	threads, pin := cfg.threads(), -1
	if cfg.Workload == "cpu-single" {
		threads, pin = 1, cfg.CPU
	}
	p.units = []string{"op"}
	p.params["threads"] = threads
	p.params["ops_per_iteration"] = computeOpsPerIteration
	p.params["iterations_per_chunk"] = computeIterations
	p.device = cpuDevice()
	if pin >= 0 {
		p.params["cpu"] = pin
		// Try the pin on a thread that then exits (a locked goroutine's
		// thread is not reused), so start can say whether it holds.
		errc := make(chan error)
		go func() {
			runtime.LockOSThread()
			errc <- pinThread(pin)
		}()
		err := <-errc
		p.setup["pinned"] = err == nil
		if err != nil {
			p.setup["pin_error"] = err.Error()
			pin = -1
		}
	}
	p.work = func(ctx context.Context, m *meter) error {
		stop := stopFlag(ctx)
		var wg sync.WaitGroup
		errs := make(chan error, threads)
		for t := range threads {
			wg.Go(func() {
				runtime.LockOSThread() // the thread exits with the goroutine
				if pin >= 0 {
					if err := pinThread(pin); err != nil {
						errs <- err
						return
					}
				}
				st := &computeState{x: 0x9e3779b97f4a7c15 ^ uint64(t+1)}
				for i := range st.f {
					st.f[i] = float64(i+1) / 8
				}
				for !stop.Load() {
					computeChunk(st)
					m.add(1, computeIterations*computeOpsPerIteration)
				}
				sink.Store(math.Float64bits(st.f[0]) ^ st.x)
			})
		}
		wg.Wait()
		close(errs)
		return <-errs
	}
	return nil
}

// sink keeps the kernels' results alive.
var sink atomic.Uint64

// The cpu-stream kernel: STREAM triad a[i] = b[i] + s*c[i] over float64
// arrays, each thread on its own contiguous slice. A pass over n
// elements moves 3*8*n bytes (two reads, one write, as STREAM counts them;
// write-allocate traffic is not counted).
const (
	streamChunk  = 1 << 15 // elements per credit
	streamScalar = 3.0
	// streamMargin is the memory left to the rest of the machine.
	streamMargin = 2 * gib
)

func prepareStream(ctx context.Context, cfg *Config, p *plan) error {
	threads, n := cfg.threads(), int(cfg.Bytes/8)
	p.units = []string{"byte"}
	p.params["threads"] = threads
	p.params["bytes_per_array"] = cfg.Bytes
	p.params["arrays"] = 3
	p.params["bytes_per_element"] = 24
	total, available, err := memory()
	if cfg.availableRAM != nil {
		available, err = cfg.availableRAM()
	}
	p.device = cpuDevice()
	p.device["kind"] = "memory"
	if err == nil {
		p.device["available_bytes"] = available
		if total > 0 {
			p.device["total_bytes"] = total
		}
	}
	need := 3 * cfg.Bytes
	switch {
	case err != nil && need > 64*mib:
		return refuse("cannot read the available memory to check 3 x %s fits: %v", formatBytes(cfg.Bytes), err)
	case err == nil && need+streamMargin > available:
		return refuse("3 arrays of %s need %s, more than the available %s less a %s margin", formatBytes(cfg.Bytes), formatBytes(need), formatBytes(available), formatBytes(streamMargin))
	}
	a, b, c := make([]float64, n), make([]float64, n), make([]float64, n)
	parts := make([][2]int, threads)
	for t := range threads {
		parts[t] = [2]int{n * t / threads, n * (t + 1) / threads}
	}
	began := time.Now()
	var wg sync.WaitGroup
	for _, r := range parts {
		wg.Go(func() { // first touch on the thread that will use the pages
			for i := r[0]; i < r[1]; i++ {
				a[i], b[i], c[i] = 0, 1, 2
			}
		})
	}
	wg.Wait()
	p.setup["init_ns"] = time.Since(began).Nanoseconds()
	if err := ctx.Err(); err != nil {
		return err
	}
	p.work = func(ctx context.Context, m *meter) error {
		stop := stopFlag(ctx)
		var wg sync.WaitGroup
		for _, r := range parts {
			wg.Go(func() {
				runtime.LockOSThread()
				for !stop.Load() {
					for lo := r[0]; lo < r[1] && !stop.Load(); lo += streamChunk {
						hi := min(lo+streamChunk, r[1])
						triad(a[lo:hi], b[lo:hi], c[lo:hi])
						m.add(1, uint64(hi-lo)*24)
					}
				}
			})
		}
		wg.Wait()
		sink.Store(math.Float64bits(a[0]))
		return nil
	}
	return nil
}

func triad(a, b, c []float64) {
	b, c = b[:len(a)], c[:len(a)]
	for i := range a {
		a[i] = b[i] + streamScalar*c[i]
	}
}
