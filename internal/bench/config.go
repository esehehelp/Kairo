package bench

import (
	"errors"
	"flag"
	"fmt"
	"math"
	"net"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// Config is one run: a workload and its parameters. Defaults gives a
// workload's defaults; RegisterFlags exposes the parameters it takes.
type Config struct {
	Workload string
	Seconds  float64 // run length; 0 runs until the context ends
	Interval float64 // seconds between interval records

	Threads int   // cpu-compute, cpu-stream; 0 = every logical CPU
	CPU     int   // cpu-single: the logical CPU to pin to
	Bytes   int64 // cpu-stream: bytes per array

	Path          string // storage: directory on the filesystem under test
	Mode          string // storage: read|write
	QD            int    // storage: requests in flight
	FileSize      int64  // storage: test file size
	KeepFree      int64  // storage: free space the run must leave
	MaxWriteBytes int64  // storage: bytes the whole run may write
	Seed          uint64 // disk-rand offsets

	Peer    string // network: host:port of kairo bench serve
	Streams int    // net-bandwidth: connections
	Dir     string // net-bandwidth: send|recv

	// Tests replace the system probes.
	freeSpace    func(dir string) (int64, error)
	availableRAM func() (int64, error)
}

// Workload is a workload kairo bench run runs.
type Workload struct {
	Name    string
	Summary string
	Unit    string
	flags   []string
	defs    func(*Config)
}

const (
	kib = int64(1) << 10
	mib = int64(1) << 20
	gib = int64(1) << 30
)

var workloads = []Workload{
	{"cpu-compute", "FP64 FMA chains and integer xorshift in registers on --threads threads (compute-bound)", "op",
		[]string{"threads"}, func(c *Config) {}},
	{"cpu-single", "the cpu-compute kernel on one thread pinned to --cpu", "op",
		[]string{"cpu"}, func(c *Config) {}},
	{"cpu-stream", "STREAM triad a=b+s*c over three --bytes arrays (memory bandwidth)", "byte",
		[]string{"threads", "bytes"}, func(c *Config) { c.Bytes = gib }},
	{"disk-seq", "1 MiB sequential reads or writes at --qd 1|32, OS cache bypassed", "byte",
		[]string{"path", "mode", "qd", "file-size", "keep-free", "max-write-bytes"}, func(c *Config) { c.FileSize = gib }},
	{"disk-rand", "4 KiB random reads or writes at --qd 1 (latency) or 32 (IOPS), OS cache bypassed", "byte",
		[]string{"path", "mode", "qd", "file-size", "keep-free", "max-write-bytes", "seed"}, func(c *Config) { c.FileSize = gib }},
	{"disk-fsync", "4 KiB write + flush loop (fdatasync / FlushFileBuffers), latency percentiles", "byte",
		[]string{"path", "file-size", "keep-free", "max-write-bytes"}, func(c *Config) { c.Mode = "write"; c.FileSize = 64 * mib }},
	{"disk-sustained", "continuous 1 MiB writes until --max-write-bytes, per-interval bandwidth (SLC cache cliffs)", "byte",
		[]string{"path", "qd", "file-size", "keep-free", "max-write-bytes"}, func(c *Config) { c.Mode = "write"; c.QD = 32; c.FileSize = 8 * gib }},
	{"net-latency", "TCP 64-byte ping-pong with kairo bench serve at --peer (TCP_NODELAY)", "request",
		[]string{"peer"}, func(c *Config) {}},
	{"net-bandwidth", "TCP bulk transfer to (send) or from (recv) kairo bench serve at --peer", "byte",
		[]string{"peer", "streams", "dir"}, func(c *Config) { c.Dir = "send" }},
}

// Workloads lists the workloads of kairo bench run.
func Workloads() []Workload { return workloads }

func findWorkload(name string) *Workload {
	for i := range workloads {
		if workloads[i].Name == name {
			return &workloads[i]
		}
	}
	return nil
}

// Defaults is the configuration of workload name with every default set.
func Defaults(name string) (Config, error) {
	w := findWorkload(name)
	if w == nil {
		names := make([]string, len(workloads))
		for i, w := range workloads {
			names[i] = w.Name
		}
		return Config{}, fmt.Errorf("unknown workload %q (one of %s)", name, strings.Join(names, ", "))
	}
	c := Config{Workload: name, Seconds: 60, Interval: 1, Mode: "read", QD: 1, KeepFree: 5 * gib, MaxWriteBytes: 20 * gib, Seed: 1, Streams: 1}
	w.defs(&c)
	return c, nil
}

// RegisterFlags adds the common flags and those of c's workload to fs, with
// c's values as defaults; parsing fs sets c.
func (c *Config) RegisterFlags(fs *flag.FlagSet) {
	fs.Float64Var(&c.Seconds, "seconds", c.Seconds, "run length in seconds; 0 runs until SIGTERM or Ctrl-C")
	fs.Float64Var(&c.Interval, "interval", c.Interval, "seconds between interval records (at least 0.05)")
	w := findWorkload(c.Workload)
	if w == nil {
		return
	}
	for _, name := range w.flags {
		switch name {
		case "threads":
			fs.IntVar(&c.Threads, name, c.Threads, "threads; 0 = every logical CPU")
		case "cpu":
			fs.IntVar(&c.CPU, name, c.CPU, "logical CPU to pin the thread to")
		case "bytes":
			fs.Var((*byteSize)(&c.Bytes), name, "bytes per array (3 arrays; suffixes KiB MiB GiB)")
		case "path":
			fs.StringVar(&c.Path, name, c.Path, "directory on the filesystem to test (required); the test file is deleted on exit")
		case "mode":
			fs.StringVar(&c.Mode, name, c.Mode, "read or write")
		case "qd":
			fs.IntVar(&c.QD, name, c.QD, "requests in flight: 1 or 32")
		case "file-size":
			fs.Var((*byteSize)(&c.FileSize), name, "test file size (suffixes KiB MiB GiB)")
		case "keep-free":
			fs.Var((*byteSize)(&c.KeepFree), name, "free space to leave: refuse unless file-size <= free - keep-free - 1GiB; stop if free falls below it")
		case "max-write-bytes":
			fs.Var((*byteSize)(&c.MaxWriteBytes), name, "bytes the whole run may write (prefill included); the run stops when reached")
		case "seed":
			fs.Uint64Var(&c.Seed, name, c.Seed, "seed of the random offsets")
		case "peer":
			fs.StringVar(&c.Peer, name, c.Peer, "HOST:PORT of kairo bench serve (required)")
		case "streams":
			fs.IntVar(&c.Streams, name, c.Streams, "TCP connections: 1 or 4")
		case "dir":
			fs.StringVar(&c.Dir, name, c.Dir, "send (to the peer) or recv (from the peer)")
		}
	}
}

// Validate checks what can be checked without touching the machine; the
// checks against free space and memory come when the run starts.
func (c *Config) Validate() error {
	w := findWorkload(c.Workload)
	if w == nil {
		_, err := Defaults(c.Workload)
		return err
	}
	if math.IsNaN(c.Seconds) || c.Seconds < 0 || c.Seconds > 7*24*3600 {
		return errors.New("--seconds must be between 0 and 604800")
	}
	if math.IsNaN(c.Interval) || c.Interval < 0.05 || c.Interval > 3600 {
		return errors.New("--interval must be between 0.05 and 3600 seconds")
	}
	if c.Seconds > 0 && c.Interval > c.Seconds {
		return errors.New("--interval must not exceed --seconds")
	}
	cpus := runtime.NumCPU()
	switch c.Workload {
	case "cpu-compute", "cpu-stream":
		if c.Threads < 0 || c.Threads > cpus {
			return fmt.Errorf("--threads must be between 0 (all) and %d, the logical CPUs", cpus)
		}
	case "cpu-single":
		if c.CPU < 0 || c.CPU >= cpus {
			return fmt.Errorf("--cpu must be between 0 and %d", cpus-1)
		}
	}
	switch c.Workload {
	case "cpu-stream":
		if c.Bytes < 64*kib || c.Bytes%4096 != 0 {
			return errors.New("--bytes must be at least 64KiB and a multiple of 4096")
		}
	case "disk-seq", "disk-rand", "disk-fsync", "disk-sustained":
		return c.validateStorage()
	case "net-latency", "net-bandwidth":
		if _, _, err := net.SplitHostPort(c.Peer); c.Peer == "" || err != nil {
			return errors.New("--peer HOST:PORT is required (the address of kairo bench serve)")
		}
		if c.Workload == "net-bandwidth" {
			if c.Streams != 1 && c.Streams != 4 {
				return errors.New("--streams must be 1 or 4")
			}
			if c.Dir != "send" && c.Dir != "recv" {
				return errors.New("--dir must be send or recv")
			}
		}
	}
	return nil
}

func (c *Config) validateStorage() error {
	if c.Path == "" {
		return errors.New("--path DIR is required: a directory on the filesystem to test")
	}
	if c.Mode != "read" && c.Mode != "write" {
		return errors.New("--mode must be read or write")
	}
	if c.QD != 1 && c.QD != 32 {
		return errors.New("--qd must be 1 or 32")
	}
	block := c.blockSize()
	if c.FileSize < 4*block || c.FileSize%block != 0 {
		return fmt.Errorf("--file-size must be a multiple of the %d-byte block and at least 4 blocks", block)
	}
	if c.KeepFree < 0 {
		return errors.New("--keep-free must not be negative")
	}
	if c.MaxWriteBytes < block {
		return errors.New("--max-write-bytes must allow at least one block")
	}
	return nil
}

// blockSize is the request size of a storage workload.
func (c *Config) blockSize() int64 {
	switch c.Workload {
	case "disk-rand", "disk-fsync":
		return 4 * kib
	}
	return mib
}

func (c *Config) duration() time.Duration { return time.Duration(c.Seconds * float64(time.Second)) }
func (c *Config) interval() time.Duration { return time.Duration(c.Interval * float64(time.Second)) }

// threads is --threads with 0 resolved to every logical CPU.
func (c *Config) threads() int {
	if c.Threads == 0 {
		return runtime.NumCPU()
	}
	return c.Threads
}

// byteSize is a flag of a byte count: an integer with an optional binary
// suffix (K, KiB, M, MiB, G, GiB, T, TiB) or decimal one (KB, MB, GB, TB).
type byteSize int64

func (b *byteSize) String() string {
	if b == nil {
		return "0"
	}
	return formatBytes(int64(*b))
}

func (b *byteSize) Set(s string) error {
	v, err := parseBytes(s)
	if err != nil {
		return err
	}
	*b = byteSize(v)
	return nil
}

var byteSuffixes = []struct {
	suffix string
	scale  int64
}{
	{"KiB", kib}, {"MiB", mib}, {"GiB", gib}, {"TiB", 1 << 40},
	{"KB", 1e3}, {"MB", 1e6}, {"GB", 1e9}, {"TB", 1e12},
	{"K", kib}, {"M", mib}, {"G", gib}, {"T", 1 << 40}, {"B", 1},
}

func parseBytes(s string) (int64, error) {
	t := strings.TrimSpace(s)
	scale := int64(1)
	for _, x := range byteSuffixes {
		if strings.HasSuffix(t, x.suffix) {
			t, scale = strings.TrimSpace(strings.TrimSuffix(t, x.suffix)), x.scale
			break
		}
	}
	n, err := strconv.ParseInt(t, 10, 64)
	if err != nil || n < 0 || n > math.MaxInt64/scale {
		return 0, fmt.Errorf("invalid byte count %q", s)
	}
	return n * scale, nil
}

// formatBytes is n with the largest binary suffix that divides it.
func formatBytes(n int64) string {
	for _, x := range []struct {
		suffix string
		scale  int64
	}{{"TiB", 1 << 40}, {"GiB", gib}, {"MiB", mib}, {"KiB", kib}} {
		if n != 0 && n%x.scale == 0 {
			return strconv.FormatInt(n/x.scale, 10) + x.suffix
		}
	}
	return strconv.FormatInt(n, 10)
}
