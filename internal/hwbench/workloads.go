package hwbench

import (
	"fmt"
	"strconv"

	"kairo/internal/store"
)

// workload is how a bench kind runs: which primitive (kairo-bench-gpu, or
// kairo bench run), its workload and its flags. The primitives' own safety
// limits (free space, written bytes, memory) are left at their defaults.
type workload struct {
	gpu   bool
	name  string
	flags []string
	// untilEnd: the measurement is the whole run after the settle, up to its
	// end record (disk-sustained writes until its write limit).
	untilEnd bool
}

var workloads = map[string]workload{
	"gemm":             {gpu: true, name: "gemm"},
	"copy":             {gpu: true, name: "copy"},
	"gemv-b1":          {gpu: true, name: "gemv", flags: []string{"--batch", "1"}},
	"gemv-b8":          {gpu: true, name: "gemv", flags: []string{"--batch", "8"}},
	"pcie-h2d":         {gpu: true, name: "pcie", flags: []string{"--dir", "h2d", "--mode", "bandwidth"}},
	"pcie-d2h":         {gpu: true, name: "pcie", flags: []string{"--dir", "d2h", "--mode", "bandwidth"}},
	"pcie-h2d-latency": {gpu: true, name: "pcie", flags: []string{"--dir", "h2d", "--mode", "latency"}},
	"pcie-d2h-latency": {gpu: true, name: "pcie", flags: []string{"--dir", "d2h", "--mode", "latency"}},

	"cpu-compute": {name: "cpu-compute"},
	"cpu-single":  {name: "cpu-single"},
	"cpu-stream":  {name: "cpu-stream"},

	"disk-seq-read":      {name: "disk-seq", flags: []string{"--mode", "read", "--qd", "32"}},
	"disk-seq-write":     {name: "disk-seq", flags: []string{"--mode", "write", "--qd", "32"}},
	"disk-rand-read":     {name: "disk-rand", flags: []string{"--mode", "read", "--qd", "32"}},
	"disk-rand-read-qd1": {name: "disk-rand", flags: []string{"--mode", "read", "--qd", "1"}},
	"disk-rand-write":    {name: "disk-rand", flags: []string{"--mode", "write", "--qd", "32"}},
	"disk-fsync":         {name: "disk-fsync"},
	"disk-sustained":     {name: "disk-sustained", untilEnd: true},

	"net-latency":        {name: "net-latency"},
	"net-bandwidth-send": {name: "net-bandwidth", flags: []string{"--dir", "send", "--streams", "4"}},
	"net-bandwidth-recv": {name: "net-bandwidth", flags: []string{"--dir", "recv", "--streams", "4"}},
}

// argv is the command line of kind for the run status describes, ending by
// itself after seconds (the driver stops it sooner when it is done). peer is
// the network bench's server (rank 0's address and port).
func argv(kind string, status store.BenchStatus, peer string, seconds int) ([]string, error) {
	w, ok := workloads[kind]
	if !ok {
		return nil, fmt.Errorf("bench kind %q has no workload", kind)
	}
	common := []string{"--seconds", strconv.Itoa(seconds), "--interval", "1"}
	if w.gpu {
		if status.Programs.GPU == "" {
			return nil, fmt.Errorf("node reports no GPU bench binary ([bench] gpu_binary): cannot run %s", kind)
		}
		out := append([]string{status.Programs.GPU, w.name, "--device", status.Run.DeviceID}, common...)
		return append(out, w.flags...), nil
	}
	if status.Programs.Kairo == "" {
		return nil, fmt.Errorf("node reports no kairo binary ([bench] kairo_binary): cannot run %s", kind)
	}
	out := append([]string{status.Programs.Kairo, "bench", "run", w.name}, common...)
	out = append(out, w.flags...)
	switch status.Run.Class {
	case store.BenchStorage:
		out = append(out, "--path", status.Run.Target)
	case store.BenchNetwork:
		out = append(out, "--peer", peer)
	}
	return out, nil
}

// serveArgv is rank 0's server of a network bench.
func serveArgv(status store.BenchStatus, listen string) ([]string, error) {
	if status.Programs.Kairo == "" {
		return nil, fmt.Errorf("node reports no kairo binary ([bench] kairo_binary): cannot serve the network bench")
	}
	return []string{status.Programs.Kairo, "bench", "serve", "--listen", listen, "--idle-timeout", "3m"}, nil
}
