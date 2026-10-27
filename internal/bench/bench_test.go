package bench

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"os"
	"strings"
	"testing"
	"time"
)

func TestHistogramBucketsCoverEveryValueOnce(t *testing.T) {
	values := []uint64{0, 1, 127, 128, 129, 255, 256, 257, 1000, 4095, 4096, 1 << 20, 1<<20 + 12345, 1<<40 - 1, 1 << 62, math.MaxUint64}
	for v := uint64(0); v < 5000; v++ {
		values = append(values, v)
	}
	prev := -1
	for _, v := range values {
		i := bucketOf(v)
		if i < 0 || i >= histogramBuckets {
			t.Fatalf("bucket of %d: %d out of range", v, i)
		}
		low, width := bucketRange(i)
		if v < low || v-low >= width {
			t.Fatalf("%d in bucket %d [%d, +%d)", v, i, low, width)
		}
		if v < 5000 && v > 0 && i < prev {
			t.Fatalf("buckets not ordered at %d", v)
		}
		if v < 5000 {
			prev = i
		}
	}
	if bucketOf(math.MaxUint64) != histogramBuckets-1 {
		t.Fatalf("the last bucket is %d, want %d", bucketOf(math.MaxUint64), histogramBuckets-1)
	}
}

func TestQuantilesAreNearestRankWithinABucket(t *testing.T) {
	var h histogram
	if h.summary() != nil {
		t.Fatal("an empty histogram has no summary")
	}
	for v := uint64(1); v <= 1000; v++ {
		h.record(v)
	}
	s := h.summary()
	if s.Count != 1000 || s.MinNS != 1 || s.MaxNS != 1000 || s.MeanNS != 501 { // 500.5 rounds up
		t.Fatalf("summary %+v", s)
	}
	for _, c := range []struct {
		q    float64
		want uint64
	}{{0.5, 500}, {0.99, 990}, {0.999, 999}, {1, 1000}, {0, 1}} {
		got := h.quantile(c.q)
		if math.Abs(float64(got)-float64(c.want)) > float64(c.want)/128+1 {
			t.Fatalf("q%v = %d, want %d within a bucket", c.q, got, c.want)
		}
	}
	// Values under 128 are exact.
	var small histogram
	for _, v := range []uint64{5, 7, 9, 11} {
		small.record(v)
	}
	if small.quantile(0.5) != 7 || small.quantile(0.75) != 9 || small.quantile(0.99) != 11 {
		t.Fatalf("small quantiles %d %d %d", small.quantile(0.5), small.quantile(0.75), small.quantile(0.99))
	}
	// One value: every quantile is it, clamped to min and max.
	var one histogram
	one.record(123456789)
	if s := one.summary(); s.P50NS != 123456789 || s.P999NS != 123456789 {
		t.Fatalf("one value: %+v", s)
	}
	// Merge equals recording into one.
	var a, b, all histogram
	for v := uint64(0); v < 3000; v += 7 {
		a.record(v * 13)
		all.record(v * 13)
		b.record(v * 101)
		all.record(v * 101)
	}
	a.merge(&b)
	if *a.summary() != *all.summary() {
		t.Fatalf("merged %+v, recorded %+v", a.summary(), all.summary())
	}
}

func TestParseBytes(t *testing.T) {
	for s, want := range map[string]int64{"0": 0, "4096": 4096, "4KiB": 4096, "4K": 4096, "1MiB": mib, "2GiB": 2 * gib, "1GB": 1e9, "3 MiB": 3 * mib, "512B": 512} {
		if got, err := parseBytes(s); err != nil || got != want {
			t.Fatalf("%q: %d %v, want %d", s, got, err, want)
		}
	}
	for _, s := range []string{"", "x", "-1", "1.5GiB", "1PiB", "99999999999TiB"} {
		if _, err := parseBytes(s); err == nil {
			t.Fatalf("%q parsed", s)
		}
	}
	if formatBytes(5*gib) != "5GiB" || formatBytes(1536) != "1536" || formatBytes(3*mib) != "3MiB" {
		t.Fatal("formatBytes")
	}
}

func mustDefaults(t *testing.T, workload string) Config {
	t.Helper()
	c, err := Defaults(workload)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestValidateRefusesBadParameters(t *testing.T) {
	if _, err := Defaults("gpu-gemm"); err == nil || !strings.Contains(err.Error(), "cpu-compute") {
		t.Fatalf("unknown workload: %v", err)
	}
	for _, w := range Workloads() {
		c := mustDefaults(t, w.Name)
		c.Path, c.Peer = t.TempDir(), "127.0.0.1:1"
		if err := c.Validate(); err != nil {
			t.Fatalf("%s defaults: %v", w.Name, err)
		}
	}
	for _, c := range []struct {
		workload string
		edit     func(*Config)
		want     string
	}{
		{"cpu-compute", func(c *Config) { c.Seconds = -1 }, "--seconds"},
		{"cpu-compute", func(c *Config) { c.Seconds = math.NaN() }, "--seconds"},
		{"cpu-compute", func(c *Config) { c.Interval = 0.01 }, "--interval"},
		{"cpu-compute", func(c *Config) { c.Seconds, c.Interval = 1, 2 }, "--interval must not exceed"},
		{"cpu-compute", func(c *Config) { c.Threads = 100000 }, "--threads"},
		{"cpu-single", func(c *Config) { c.CPU = -1 }, "--cpu"},
		{"cpu-stream", func(c *Config) { c.Bytes = 1000 }, "--bytes"},
		{"disk-seq", func(c *Config) { c.Path = "" }, "--path"},
		{"disk-seq", func(c *Config) { c.Mode = "append" }, "--mode"},
		{"disk-seq", func(c *Config) { c.QD = 7 }, "--qd"},
		{"disk-seq", func(c *Config) { c.FileSize = 3 * mib }, "--file-size"},
		{"disk-rand", func(c *Config) { c.FileSize = 4097 }, "--file-size"},
		{"disk-rand", func(c *Config) { c.KeepFree = -1 }, "--keep-free"},
		{"disk-sustained", func(c *Config) { c.MaxWriteBytes = 0 }, "--max-write-bytes"},
		{"net-latency", func(c *Config) { c.Peer = "" }, "--peer"},
		{"net-latency", func(c *Config) { c.Peer = "nocolon" }, "--peer"},
		{"net-bandwidth", func(c *Config) { c.Streams = 2 }, "--streams"},
		{"net-bandwidth", func(c *Config) { c.Dir = "both" }, "--dir"},
	} {
		cfg := mustDefaults(t, c.workload)
		cfg.Path, cfg.Peer = t.TempDir(), "127.0.0.1:1"
		c.edit(&cfg)
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("%s: %v, want %q", c.workload, err, c.want)
		}
		// Run validates too, and writes nothing for a usage error.
		var out bytes.Buffer
		if err := Run(context.Background(), &out, cfg); err == nil || out.Len() != 0 {
			t.Fatalf("%s: Run %v wrote %q", c.workload, err, out.String())
		}
	}
}

// records parses a run's output and checks the contract: a start record
// (or none for a refused run), intervals numbered from 0 that tile the run
// on the monotonic clock, and an end record whose totals are their sums.
func records(t *testing.T, out []byte) (start map[string]any, intervals []map[string]any, end map[string]any) {
	t.Helper()
	s := bufio.NewScanner(bytes.NewReader(out))
	var all []map[string]any
	for s.Scan() {
		var r map[string]any
		if err := json.Unmarshal(s.Bytes(), &r); err != nil {
			t.Fatalf("line %q: %v", s.Text(), err)
		}
		all = append(all, r)
	}
	if len(all) == 0 {
		t.Fatal("no records")
	}
	runID := all[0]["run_id"]
	for _, r := range all {
		if r["schema"] != Schema || r["program"] != "kairo" || r["run_id"] != runID || len(runID.(string)) != 16 {
			t.Fatalf("header of %v", r)
		}
		if _, err := time.Parse(time.RFC3339Nano, r["wall"].(string)); err != nil {
			t.Fatalf("wall %v: %v", r["wall"], err)
		}
	}
	end = all[len(all)-1]
	if end["type"] != "end" {
		t.Fatalf("last record %v", end)
	}
	if all[0]["type"] != "start" {
		if len(all) != 1 {
			t.Fatalf("records without a start: %v", all)
		}
		return nil, nil, end
	}
	start = all[0]
	if start["mono_ns"].(float64) != 0 || start["version"].(float64) < 1 || len(start["units"].([]any)) == 0 {
		t.Fatalf("start %v", start)
	}
	host := start["host"].(map[string]any)
	if host["logical_cpus"].(float64) < 1 || host["os"] == "" {
		t.Fatalf("host %v", host)
	}
	units := start["units"].([]any)
	totals := map[string]float64{}
	var ops, prevEnd float64
	for i, r := range all[1 : len(all)-1] {
		if r["type"] != "interval" || r["index"].(float64) != float64(i) || r["begin_ns"].(float64) != prevEnd || r["end_ns"].(float64) < r["begin_ns"].(float64) {
			t.Fatalf("interval %d: %v (previous end %v)", i, r, prevEnd)
		}
		prevEnd = r["end_ns"].(float64)
		work := r["work"].(map[string]any)
		for _, u := range units {
			v, ok := work[u.(string)]
			if !ok {
				t.Fatalf("interval %d has no %v: %v", i, u, work)
			}
			totals[u.(string)] += v.(float64)
		}
		ops += r["ops"].(float64)
		intervals = append(intervals, r)
	}
	if len(intervals) == 0 || end["intervals"].(float64) != float64(len(intervals)) || end["ops"].(float64) != ops {
		t.Fatalf("end %v after %d intervals, %v ops", end, len(intervals), ops)
	}
	for u, v := range totals {
		if end["work"].(map[string]any)[u].(float64) != v {
			t.Fatalf("end work %v, intervals sum %v", end["work"], totals)
		}
	}
	if end["elapsed_ns"].(float64) != prevEnd || end["mono_ns"].(float64) < prevEnd {
		t.Fatalf("end elapsed %v mono %v, the last interval ends at %v", end["elapsed_ns"], end["mono_ns"], prevEnd)
	}
	return start, intervals, end
}

func run(t *testing.T, cfg Config) ([]byte, error) {
	t.Helper()
	var out bytes.Buffer
	err := Run(context.Background(), &out, cfg)
	return out.Bytes(), err
}

func short(c *Config) { c.Seconds, c.Interval = 0.2, 0.05 }

func TestCPUComputeRecordsWork(t *testing.T) {
	cfg := mustDefaults(t, "cpu-compute")
	short(&cfg)
	cfg.Threads = 2
	out, err := run(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	start, intervals, end := records(t, out)
	if start["params"].(map[string]any)["threads"].(float64) != 2 || start["device"].(map[string]any)["kind"] != "cpu" {
		t.Fatalf("start %v", start)
	}
	if len(intervals) < 3 || end["reason"] != "duration" || end["error"] != nil {
		t.Fatalf("%d intervals, end %v", len(intervals), end)
	}
	if ops := end["work"].(map[string]any)["op"].(float64); ops == 0 || math.Mod(ops, computeIterations*computeOpsPerIteration) != 0 {
		t.Fatalf("op total %v is not whole chunks", ops)
	}
}

func TestCPUSinglePinsOneThread(t *testing.T) {
	cfg := mustDefaults(t, "cpu-single")
	short(&cfg)
	out, err := run(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	start, _, _ := records(t, out)
	setup := start["setup"].(map[string]any)
	if start["params"].(map[string]any)["threads"].(float64) != 1 || (setup["pinned"] != true && setup["pin_error"] == nil) {
		t.Fatalf("start %v", start)
	}
}

func TestCPUStreamCountsThreeArraysAndRefusesWhatDoesNotFit(t *testing.T) {
	cfg := mustDefaults(t, "cpu-stream")
	short(&cfg)
	cfg.Threads, cfg.Bytes = 2, 256*kib
	out, err := run(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	_, _, end := records(t, out)
	if b := end["work"].(map[string]any)["byte"].(float64); b == 0 || math.Mod(b, 24) != 0 {
		t.Fatalf("bytes %v", b)
	}
	cfg.Bytes = gib
	cfg.availableRAM = func() (int64, error) { return 4 * gib, nil }
	out, err = run(t, cfg)
	if !errors.As(err, new(*RefusedError)) {
		t.Fatalf("3 GiB with 4 GiB available: %v", err)
	}
	if _, _, end := records(t, out); end["reason"] != "refused" || end["params"] == nil {
		t.Fatalf("refused end %v", end)
	}
}

func TestCancelEndsWithSignal(t *testing.T) {
	cfg := mustDefaults(t, "cpu-compute")
	cfg.Seconds, cfg.Interval, cfg.Threads = 0, 0.05, 1
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	var out bytes.Buffer
	if err := Run(ctx, &out, cfg); err != nil {
		t.Fatal(err)
	}
	if _, _, end := records(t, out.Bytes()); end["reason"] != "signal" {
		t.Fatalf("end %v", end)
	}
}

func dirEmpty(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("%s holds %v (%v)", dir, entries, err)
	}
}

func storageConfig(t *testing.T, workload string) Config {
	cfg := mustDefaults(t, workload)
	short(&cfg)
	cfg.Path, cfg.KeepFree, cfg.FileSize, cfg.MaxWriteBytes = t.TempDir(), 0, 4*mib, 64*mib
	if cfg.blockSize() == 4*kib {
		cfg.FileSize = mib
	}
	// The test machine's free space does not matter: report plenty.
	cfg.freeSpace = func(string) (int64, error) { return 100 * gib, nil }
	return cfg
}

func TestStorageRunsDeleteTheirFile(t *testing.T) {
	for _, c := range []struct {
		workload, mode string
		qd             int
	}{{"disk-seq", "read", 1}, {"disk-seq", "write", 32}, {"disk-rand", "read", 32}, {"disk-rand", "write", 1}, {"disk-fsync", "write", 1}, {"disk-sustained", "write", 1}} {
		cfg := storageConfig(t, c.workload)
		cfg.Mode, cfg.QD = c.mode, c.qd
		cfg.MaxWriteBytes = 16 * mib
		out, err := run(t, cfg)
		if err != nil {
			t.Fatalf("%v: %v\n%s", c, err, out)
		}
		start, _, end := records(t, out)
		if end["reason"] != "duration" && end["reason"] != "limit" {
			t.Fatalf("%v: end %v", c, end)
		}
		if end["file_removed"] != true || end["bytes_written"].(float64) > float64(cfg.MaxWriteBytes) {
			t.Fatalf("%v: end %v", c, end)
		}
		dev := start["device"].(map[string]any)
		if dev["kind"] != "storage" || dev["free_bytes"].(float64) != float64(100*gib) {
			t.Fatalf("%v: device %v", c, dev)
		}
		if _, ok := start["setup"].(map[string]any)["direct_io"].(bool); !ok {
			t.Fatalf("%v: setup %v", c, start["setup"])
		}
		if end["latency"] == nil || (c.mode == "read" && end["work"].(map[string]any)["byte"].(float64) == 0) {
			t.Fatalf("%v: end %v", c, end)
		}
		dirEmpty(t, cfg.Path)
	}
}

func TestStorageRefusesWithoutFreeSpace(t *testing.T) {
	cfg := storageConfig(t, "disk-seq")
	cfg.KeepFree = 5 * gib
	cfg.freeSpace = func(string) (int64, error) { return 6*gib + 3*mib, nil } // 1 MiB short
	out, err := run(t, cfg)
	if !errors.As(err, new(*RefusedError)) || !strings.Contains(err.Error(), "keep-free") {
		t.Fatalf("refusal: %v", err)
	}
	start, _, end := records(t, out)
	if start != nil || end["reason"] != "refused" || end["device"].(map[string]any)["kind"] != "storage" || end["params"] == nil {
		t.Fatalf("end %v", end)
	}
	dirEmpty(t, cfg.Path)
	cfg.freeSpace = func(string) (int64, error) { return 6*gib + 4*mib, nil } // fits exactly
	if _, err := run(t, cfg); err != nil {
		t.Fatalf("exact fit: %v", err)
	}

	cfg = storageConfig(t, "disk-rand")
	cfg.MaxWriteBytes = cfg.FileSize // the prefill alone would pass it
	if _, err := run(t, cfg); !errors.As(err, new(*RefusedError)) || !strings.Contains(err.Error(), "prefill") {
		t.Fatalf("prefill over the cap: %v", err)
	}
	cfg = storageConfig(t, "disk-seq")
	cfg.Path += "/missing"
	if _, err := run(t, cfg); !errors.As(err, new(*RefusedError)) {
		t.Fatalf("missing dir: %v", err)
	}
}

func TestStorageStopsAtTheWriteCap(t *testing.T) {
	cfg := storageConfig(t, "disk-seq")
	cfg.Mode, cfg.Seconds, cfg.MaxWriteBytes = "write", 0, 9*mib // probe 1 + prefill 4 + 4 more
	out, err := run(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	_, _, end := records(t, out)
	if end["reason"] != "limit" || end["limit"] != "max-write-bytes" || end["bytes_written"].(float64) != float64(9*mib) || end["file_removed"] != true {
		t.Fatalf("end %v", end)
	}
	if w := end["work"].(map[string]any)["byte"].(float64); w != float64(4*mib) {
		t.Fatalf("timed writes %v, want 4 MiB", w)
	}
	dirEmpty(t, cfg.Path)
}

func TestStorageStopsWhenFreeSpaceFalls(t *testing.T) {
	cfg := storageConfig(t, "disk-fsync")
	cfg.Seconds, cfg.KeepFree = 0, gib
	calls := 0
	cfg.freeSpace = func(string) (int64, error) {
		calls++
		if calls > 2 {
			return gib - 1, nil
		}
		return 10 * gib, nil
	}
	out, err := run(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, intervals, end := records(t, out); end["reason"] != "limit" || end["limit"] != "keep-free" || len(intervals) != 3 {
		t.Fatalf("end %v after %d intervals", end, len(intervals))
	}
	dirEmpty(t, cfg.Path)
}

func TestStorageCancelRemovesTheFile(t *testing.T) {
	cfg := storageConfig(t, "disk-rand")
	cfg.Seconds = 0
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	var out bytes.Buffer
	if err := Run(ctx, &out, cfg); err != nil {
		t.Fatal(err)
	}
	if _, _, end := records(t, out.Bytes()); end["reason"] != "signal" || end["file_removed"] != true {
		t.Fatalf("end %v", end)
	}
	dirEmpty(t, cfg.Path)
	// Cancelled before the run starts (during setup): an end record alone, nothing left.
	ctx, cancel = context.WithCancel(context.Background())
	cancel()
	out.Reset()
	if err := Run(ctx, &out, cfg); err != nil {
		t.Fatal(err)
	}
	if start, _, end := records(t, out.Bytes()); start != nil || end["reason"] != "signal" {
		t.Fatalf("start %v end %v", start, end)
	}
	dirEmpty(t, cfg.Path)
}
