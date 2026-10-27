package bench

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// The storage workloads read or write one test file in --path, bypassing
// the OS cache. Safety: the file must fit in the free space less
// --keep-free and a 1 GiB margin; every byte written (probe and prefill
// included) counts against --max-write-bytes, and the run stops when the
// next request would pass it; a write run stops when free space falls below
// --keep-free; the file is removed on every exit — Windows opens it
// FILE_FLAG_DELETE_ON_CLOSE and Linux unlinks it once open, so even a
// killed run leaves nothing behind.

// storageMargin is the free space a test file must leave on top of --keep-free.
const storageMargin = gib

// prefillChunk is the request size of the prefill.
const prefillChunk = mib

// fileHandle is a test file handle as the workloads use it.
type fileHandle interface {
	io.ReaderAt
	io.WriterAt
	sync() error
	close() error
	dropDirect() error
}

// writeBudget enforces --max-write-bytes across the run.
type writeBudget struct {
	limit    int64
	reserved atomic.Int64
	written  atomic.Int64
}

// reserve claims n bytes before a write; false when they would pass the limit.
func (b *writeBudget) reserve(n int64) bool {
	if b.reserved.Add(n) > b.limit {
		b.reserved.Add(-n)
		return false
	}
	return true
}

func (b *writeBudget) exhausted() error {
	return &limitError{"max-write-bytes", fmt.Sprintf("the next write would pass --max-write-bytes %s", formatBytes(b.limit))}
}

// alignedBuffer is a size-byte buffer starting at a multiple of align (Go
// does not move heap objects).
func alignedBuffer(size, align int) []byte {
	b := make([]byte, size+align)
	off := (align - int(uintptr(unsafe.Pointer(&b[0]))%uintptr(align))) % align
	return b[off : off+size : off+size]
}

// fillPattern fills b with xorshift bytes: incompressible, so compressing
// filesystems and drives store what was asked.
func fillPattern(b []byte, seed uint64) {
	x := seed | 1
	for i := 0; i+8 <= len(b); i += 8 {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		binary.LittleEndian.PutUint64(b[i:], x)
	}
}

// stamp writes the file offset of every 4 KiB sector of b into its first
// bytes, so no two sectors of the file are equal (deduplication).
func stamp(b []byte, off int64) {
	for s := 0; s < len(b); s += 4096 {
		binary.LittleEndian.PutUint64(b[s:], uint64(off)+uint64(s))
	}
}

type storageRun struct {
	cfg      *Config
	dir      string
	path     string
	block    int64
	handles  []fileHandle
	direct   bool
	budget   writeBudget
	free     func(string) (int64, error)
	closeErr error
	closed   bool
}

func prepareStorage(ctx context.Context, cfg *Config, p *plan) error {
	s := &storageRun{cfg: cfg, block: cfg.blockSize(), free: cfg.freeSpace}
	s.budget.limit = cfg.MaxWriteBytes
	if s.free == nil {
		s.free = diskFree
	}
	qd := cfg.QD
	if cfg.Workload == "disk-fsync" {
		qd = 1
	}
	access := "sequential"
	if cfg.Workload == "disk-rand" {
		access = "random"
	}
	prefill := cfg.Workload != "disk-sustained"
	p.units = []string{"byte"}
	for k, v := range map[string]any{"mode": cfg.Mode, "qd": qd, "block_bytes": s.block, "file_size": cfg.FileSize, "access": access,
		"keep_free": cfg.KeepFree, "max_write_bytes": cfg.MaxWriteBytes, "prefill": prefill, "flush_each_write": cfg.Workload == "disk-fsync"} {
		p.params[k] = v
	}
	if cfg.Workload == "disk-rand" {
		p.params["seed"] = cfg.Seed
	}
	p.finish = func(end *endRecord) { s.finish(end) }

	dir, err := filepath.Abs(cfg.Path)
	if err != nil {
		return err
	}
	if st, err := os.Stat(dir); err != nil || !st.IsDir() {
		return refuse("--path %s is not an existing directory", cfg.Path)
	}
	s.dir = dir
	p.params["path"] = dir
	p.device = volumeInfo(dir)
	p.device["kind"], p.device["path"] = "storage", dir
	free, err := s.free(dir)
	if err != nil {
		return fmt.Errorf("free space of %s: %w", dir, err)
	}
	p.device["free_bytes"] = free
	if room := free - cfg.KeepFree - storageMargin; cfg.FileSize > room {
		return refuse("a %s test file needs free space of at least file-size + keep-free %s + %s margin; %s has %s free",
			formatBytes(cfg.FileSize), formatBytes(cfg.KeepFree), formatBytes(storageMargin), dir, formatBytes(free))
	}
	if prefill && cfg.FileSize+s.block > cfg.MaxWriteBytes {
		return refuse("the prefill of the %s test file would pass --max-write-bytes %s", formatBytes(cfg.FileSize), formatBytes(cfg.MaxWriteBytes))
	}
	align := 4096
	for _, key := range []string{"logical_sector_bytes", "physical_sector_bytes"} {
		if v, ok := p.device[key]; ok {
			if n := toInt(v); n > align {
				align = n
			}
		}
	}
	if s.block%int64(align) != 0 {
		return refuse("the %d-byte requests are not a multiple of the device's %d-byte sectors", s.block, align)
	}
	s.path = filepath.Join(dir, "kairo-bench-"+newRunID()+".tmp")
	p.setup["file"] = s.path
	p.setup["alignment"] = align
	if err := s.open(qd, true); err != nil {
		return err
	}
	// A probe write: some filesystems accept O_DIRECT at open and refuse the request.
	probe := alignedBuffer(int(s.block), align)
	fillPattern(probe, 1)
	if err := s.write(s.handles[0], probe, 0); err != nil {
		if !s.direct || !isDirectIOError(err) {
			return fmt.Errorf("probe write: %w", err)
		}
		p.setup["direct_io_fallback"] = err.Error()
		if err := s.reopenBuffered(qd); err != nil {
			return err
		}
		if err := s.write(s.handles[0], probe, 0); err != nil {
			return fmt.Errorf("probe write: %w", err)
		}
	}
	p.setup["direct_io"] = s.direct
	if earlyUnlink(s.path) {
		p.setup["unlinked_while_open"] = true
	}
	if prefill {
		began := time.Now()
		if err := s.prefill(ctx, align); err != nil {
			return err
		}
		p.setup["prefill_ns"] = time.Since(began).Nanoseconds()
	}
	p.setup["setup_bytes_written"] = s.budget.written.Load()
	p.work = func(ctx context.Context, m *meter) error { return s.work(ctx, m, align) }
	if cfg.Mode == "write" {
		p.check = s.checkFree
	}
	return nil
}

func toInt(v any) int {
	switch n := v.(type) {
	case uint32:
		return int(n)
	case int64:
		return int(n)
	case int:
		return n
	}
	return 0
}

// open opens qd handles of the test file, the first creating it.
func (s *storageRun) open(qd int, wantDirect bool) error {
	for i := range qd {
		f, direct, err := openTestFile(s.path, i == 0, wantDirect && (i == 0 || s.direct))
		if err != nil {
			return fmt.Errorf("open test file: %w", err)
		}
		if i == 0 {
			s.direct = direct
		}
		s.handles = append(s.handles, f)
	}
	return nil
}

// reopenBuffered moves the handles to buffered I/O after the device
// refused direct requests.
func (s *storageRun) reopenBuffered(qd int) error {
	dropped := true
	for _, h := range s.handles {
		if h.dropDirect() != nil {
			dropped = false
			break
		}
	}
	s.direct = false
	if dropped {
		return nil
	}
	s.closeHandles()
	_ = os.Remove(s.path)
	s.handles, s.closed = nil, false
	return s.open(qd, false)
}

// write writes b at off through h, counted against the budget.
func (s *storageRun) write(h fileHandle, b []byte, off int64) error {
	if !s.budget.reserve(int64(len(b))) {
		return s.budget.exhausted()
	}
	stamp(b, off)
	n, err := h.WriteAt(b, off)
	if n > 0 {
		s.budget.written.Add(int64(n))
	}
	if err == nil && n < len(b) {
		err = io.ErrShortWrite
	}
	return err
}

// prefill writes the whole file once, so reads and overwrites meet
// allocated, written blocks.
func (s *storageRun) prefill(ctx context.Context, align int) error {
	var cursor atomic.Int64
	size := s.cfg.FileSize
	return runWorkers(ctx, min(len(s.handles), 4), func(ctx context.Context, i int) error {
		buf := alignedBuffer(int(prefillChunk), align)
		fillPattern(buf, uint64(i)+2)
		for ctx.Err() == nil {
			off := cursor.Add(prefillChunk) - prefillChunk
			if off >= size {
				return nil
			}
			n := min(prefillChunk, size-off)
			if err := s.write(s.handles[i], buf[:n], off); err != nil {
				return fmt.Errorf("prefill: %w", err)
			}
		}
		return ctx.Err()
	})
}

func (s *storageRun) work(ctx context.Context, m *meter, align int) error {
	var cursor atomic.Int64
	size, block := s.cfg.FileSize, s.block
	blocks := uint64(size / block)
	write, random, flush := s.cfg.Mode == "write", s.cfg.Workload == "disk-rand", s.cfg.Workload == "disk-fsync"
	err := runWorkers(ctx, len(s.handles), func(ctx context.Context, i int) error {
		h := s.handles[i]
		rec := m.recorder()
		buf := alignedBuffer(int(block), align)
		fillPattern(buf, uint64(i)+100)
		rng := s.cfg.Seed*0x9e3779b97f4a7c15 + uint64(i) + 1
		for ctx.Err() == nil {
			var off int64
			if random {
				rng ^= rng << 13
				rng ^= rng >> 7
				rng ^= rng << 17
				off = int64(rng%blocks) * block
			} else {
				off = (cursor.Add(block) - block) % size
			}
			began := monoNow()
			var err error
			if write {
				err = s.write(h, buf, off)
				if err == nil && flush {
					err = h.sync()
				}
			} else {
				var n int
				n, err = h.ReadAt(buf, off)
				if err == nil && n < len(buf) {
					err = io.ErrUnexpectedEOF
				}
			}
			if err != nil {
				return err
			}
			rec.record(began)
			m.add(1, uint64(block))
		}
		return nil
	})
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

// checkFree stops a write run when free space falls below --keep-free.
func (s *storageRun) checkFree() error {
	free, err := s.free(s.dir)
	if err != nil || free >= s.cfg.KeepFree {
		return nil
	}
	return &limitError{"keep-free", fmt.Sprintf("free space %s fell below --keep-free %s", formatBytes(free), formatBytes(s.cfg.KeepFree))}
}

func (s *storageRun) closeHandles() {
	if s.closed {
		return
	}
	s.closed = true
	for _, h := range s.handles {
		if err := h.close(); err != nil && s.closeErr == nil {
			s.closeErr = err
		}
	}
}

// finish closes and removes the test file and reports what was written.
func (s *storageRun) finish(end *endRecord) {
	s.closeHandles()
	written := s.budget.written.Load()
	end.BytesWritten = &written
	if s.path == "" {
		return
	}
	_ = os.Remove(s.path)
	_, err := os.Stat(s.path)
	removed := errors.Is(err, fs.ErrNotExist)
	end.FileRemoved = &removed
}

// runWorkers runs n workers; the first error cancels the others and is returned.
func runWorkers(ctx context.Context, n int, worker func(ctx context.Context, i int) error) error {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			if err := worker(ctx, i); err != nil {
				cancel(err)
			}
		})
	}
	wg.Wait()
	if err := context.Cause(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return ctx.Err()
}
