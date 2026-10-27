package bench

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"runtime"
	"sync"
	"time"

	"kairo/internal/buildinfo"
)

// The output contract: one JSON object per line on stdout, a start record,
// interval records and an end record. kairo-bench-gpu (bench/gpu) writes the
// same records. README.md "Hardware benchmarks" documents the fields.

// Schema names the record format; it changes when a field changes meaning.
const Schema = "kairo.bench/v1"

// program is the "program" field of the records kairo writes.
const program = "kairo"

// header is the part of every record.
type header struct {
	Schema   string `json:"schema"`
	Type     string `json:"type"`
	RunID    string `json:"run_id"`
	Program  string `json:"program"`
	Workload string `json:"workload"`
	Wall     string `json:"wall"`
	MonoNS   int64  `json:"mono_ns"`
}

type startRecord struct {
	header
	Version int            `json:"version"`
	Build   string         `json:"build"`
	Params  map[string]any `json:"params"`
	Units   []string       `json:"units"`
	Host    hostInfo       `json:"host"`
	Device  map[string]any `json:"device"`
	Setup   map[string]any `json:"setup"`
}

type intervalRecord struct {
	header
	Index   int               `json:"index"`
	BeginNS int64             `json:"begin_ns"`
	EndNS   int64             `json:"end_ns"`
	Work    map[string]uint64 `json:"work"`
	Ops     uint64            `json:"ops"`
	// DeviceBusyNS is the time the device measured itself busy (GPU events);
	// null for the workloads kairo runs.
	DeviceBusyNS *int64          `json:"device_busy_ns"`
	Latency      *latencySummary `json:"latency"`
}

type endRecord struct {
	header
	// Reason is duration, signal or limit (the run ended as asked: exit 0),
	// error or refused (exit 1).
	Reason string `json:"reason"`
	// Limit names the safety limit that ended a "limit" run.
	Limit        string            `json:"limit,omitempty"`
	LimitDetail  string            `json:"limit_detail,omitempty"`
	Error        *string           `json:"error"`
	ElapsedNS    int64             `json:"elapsed_ns"`
	Intervals    int               `json:"intervals"`
	Work         map[string]uint64 `json:"work"`
	Ops          uint64            `json:"ops"`
	DeviceBusyNS *int64            `json:"device_busy_ns"`
	Latency      *latencySummary   `json:"latency"`
	// BytesWritten (storage) counts every byte written to the test file,
	// the probe and the prefill included; FileRemoved is checked after cleanup.
	BytesWritten *int64 `json:"bytes_written,omitempty"`
	FileRemoved  *bool  `json:"file_removed,omitempty"`
	// Params and Device are repeated when no start record was written.
	Params map[string]any `json:"params,omitempty"`
	Device map[string]any `json:"device,omitempty"`
}

// hostInfo identifies the machine a run measured.
type hostInfo struct {
	Hostname    string  `json:"hostname"`
	OS          string  `json:"os"`
	OSVersion   string  `json:"os_version"`
	Arch        string  `json:"arch"`
	CPUModel    *string `json:"cpu_model"`
	LogicalCPUs int     `json:"logical_cpus"`
}

func currentHost() hostInfo {
	name, _ := os.Hostname()
	h := hostInfo{Hostname: name, OS: runtime.GOOS, OSVersion: osVersion(), Arch: runtime.GOARCH, LogicalCPUs: runtime.NumCPU()}
	if model := cpuModel(); model != "" {
		h.CPUModel = &model
	}
	return h
}

// emitter writes the records of one run.
type emitter struct {
	mu       sync.Mutex
	enc      *json.Encoder
	runID    string
	workload string
	started  bool
	origin   int64 // monoNow at the start record
}

func newEmitter(w io.Writer, workload string) *emitter {
	return &emitter{enc: json.NewEncoder(w), runID: newRunID(), workload: workload}
}

func newRunID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// wallFormat is RFC 3339 in UTC with nanoseconds, fixed width.
const wallFormat = "2006-01-02T15:04:05.000000000Z"

// instant is a moment on both clocks.
type instant struct {
	wall time.Time
	mono int64 // monoNow
}

func now() instant { return instant{time.Now(), monoNow()} }

// since is the monotonic time from the start record to at.
func (e *emitter) since(at instant) int64 {
	if !e.started {
		return 0
	}
	return at.mono - e.origin
}

// header stamps a record of type kind at at.
func (e *emitter) header(kind string, at instant) header {
	return header{Schema: Schema, Type: kind, RunID: e.runID, Program: program, Workload: e.workload, Wall: at.wall.UTC().Format(wallFormat), MonoNS: e.since(at)}
}

// write encodes one record as a line; a failed write (stdout closed) cannot
// be reported anywhere else, so it is dropped.
func (e *emitter) write(v any) {
	e.mu.Lock()
	defer e.mu.Unlock()
	_ = e.enc.Encode(v)
}

func build() string { return buildinfo.Version() }
