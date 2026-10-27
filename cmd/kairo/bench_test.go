package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestBenchRunHelpListsWorkloadsAndTheirFlags(t *testing.T) {
	out, err := captureStdout(t, func() error { return run([]string{"bench", "run", "--help"}) })
	if err != nil || !strings.HasPrefix(out, "usage: kairo bench run WORKLOAD") {
		t.Fatalf("%v %q", err, out)
	}
	for _, w := range []string{"cpu-compute", "cpu-single", "cpu-stream", "disk-seq", "disk-rand", "disk-fsync", "disk-sustained", "net-latency", "net-bandwidth"} {
		if !strings.Contains(out, "\n  "+w+" ") {
			t.Fatalf("bench run --help does not list %s: %q", w, out)
		}
	}
	out, err = captureStdout(t, func() error { return run([]string{"bench", "run", "disk-rand", "--help"}) })
	if err != nil || !strings.HasPrefix(out, "usage: kairo bench run disk-rand [flags]\n4 KiB random") ||
		!strings.Contains(out, "  -keep-free") || !strings.Contains(out, "  -max-write-bytes") || strings.Contains(out, "  -threads") {
		t.Fatalf("disk-rand --help: %v %q", err, out)
	}
}

func TestBenchRunUsageErrors(t *testing.T) {
	for _, c := range []struct {
		args []string
		want string
	}{
		{[]string{"bench", "run"}, "needs a WORKLOAD"},
		{[]string{"bench", "run", "gpu-gemm"}, `unknown workload "gpu-gemm"`},
		{[]string{"bench", "run", "disk-seq"}, "--path DIR is required"},
		{[]string{"bench", "run", "cpu-compute", "--qd", "32"}, "-qd"},
		{[]string{"bench", "run", "disk-seq", "--path", ".", "--qd", "8"}, "--qd must be 1 or 32"},
		{[]string{"bench", "run", "net-bandwidth", "--peer", "h:1", "--streams", "3"}, "--streams"},
		{[]string{"bench", "run", "cpu-compute", "extra"}, "one WORKLOAD"},
		{[]string{"bench", "serve"}, "--listen"},
	} {
		out, err := captureStdout(t, func() error { return run(c.args) })
		if err == nil || exitCode(err) != exitUsage || !strings.Contains(err.Error(), c.want) || out != "" {
			t.Fatalf("%v: %v (stdout %q)", c.args, err, out)
		}
	}
}

func TestBenchRunWritesTheRecords(t *testing.T) {
	out, err := captureStdout(t, func() error {
		return run([]string{"bench", "run", "cpu-compute", "--threads", "1", "--seconds", "0.1", "--interval", "0.05"})
	})
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	var types []string
	for _, l := range lines {
		var r struct{ Type, Schema string }
		if err := json.Unmarshal([]byte(l), &r); err != nil || r.Schema != "kairo.bench/v1" {
			t.Fatalf("line %q: %v", l, err)
		}
		types = append(types, r.Type)
	}
	if types[0] != "start" || types[len(types)-1] != "end" || len(types) < 3 {
		t.Fatalf("records %v", types)
	}
}
