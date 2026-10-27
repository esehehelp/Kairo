package bench

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// syncBuffer is a bytes.Buffer the server goroutine writes while the test reads.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) lines(t *testing.T) []map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []map[string]any
	sc := bufio.NewScanner(bytes.NewReader(s.b.Bytes()))
	for sc.Scan() {
		var r map[string]any
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			t.Fatalf("serve line %q: %v", sc.Text(), err)
		}
		out = append(out, r)
	}
	return out
}

// startServer serves on a loopback port until the test ends.
func startServer(t *testing.T, cfg ServeConfig) (addr string, out *syncBuffer, done <-chan error) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	out = &syncBuffer{}
	errc, finished := make(chan error, 1), make(chan struct{})
	go func() {
		errc <- serveListener(ctx, out, ln, cfg)
		close(finished)
	}()
	t.Cleanup(func() {
		cancel()
		<-finished
	})
	return ln.Addr().String(), out, errc
}

func TestNetLatencyAndBandwidthAgainstServe(t *testing.T) {
	addr, serveOut, _ := startServer(t, ServeConfig{IdleTimeout: time.Minute, MaxConnDuration: time.Minute})

	cfg := mustDefaults(t, "net-latency")
	short(&cfg)
	cfg.Peer = addr
	out, err := run(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	start, intervals, end := records(t, out)
	dev := start["device"].(map[string]any)
	if dev["kind"] != "network" || dev["remote_addr"] != addr || !strings.HasPrefix(dev["local_addr"].(string), "127.0.0.1:") {
		t.Fatalf("device %v", dev)
	}
	if dev["interface"] == nil || dev["mtu"] == nil {
		t.Logf("loopback interface not resolved: %v", dev) // some systems list no loopback address
	}
	lat := end["latency"].(map[string]any)
	if lat["count"].(float64) != end["work"].(map[string]any)["request"].(float64) || lat["p50_ns"].(float64) <= 0 || lat["p99_ns"].(float64) < lat["p50_ns"].(float64) {
		t.Fatalf("latency %v, end %v", lat, end)
	}
	if intervals[0]["latency"] == nil {
		t.Fatalf("interval latency missing: %v", intervals[0])
	}

	for _, dir := range []string{"send", "recv"} {
		cfg := mustDefaults(t, "net-bandwidth")
		short(&cfg)
		cfg.Peer, cfg.Streams, cfg.Dir = addr, 4, dir
		out, err := run(t, cfg)
		if err != nil {
			t.Fatalf("%s: %v", dir, err)
		}
		_, _, end := records(t, out)
		if end["work"].(map[string]any)["byte"].(float64) == 0 || end["reason"] != "duration" {
			t.Fatalf("%s: end %v", dir, end)
		}
	}
	// Every connection ended and was reported: 1 echo + 4 sink + 4 source.
	deadline := time.Now().Add(5 * time.Second)
	for {
		modes := map[string]int{}
		for _, r := range serveOut.lines(t) {
			if r["type"] == "connection" {
				modes[r["mode"].(string)]++
			}
		}
		if modes["echo"] == 1 && modes["sink"] == 4 && modes["source"] == 4 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("server saw %v", modes)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestServeRefusesAForeignHelloAndExitsWhenIdle(t *testing.T) {
	addr, out, done := startServer(t, ServeConfig{IdleTimeout: 300 * time.Millisecond, MaxConnDuration: time.Minute})
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	conn.Write([]byte(`{"schema":"other/v9","mode":"source"}` + "\n"))
	reply, _ := bufio.NewReader(conn).ReadString('\n')
	conn.Close()
	if !strings.Contains(reply, `"ok":false`) || !strings.Contains(reply, "schema") {
		t.Fatalf("reply %q", reply)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serve did not exit when idle")
	}
	lines := out.lines(t)
	last := lines[len(lines)-1]
	if lines[0]["type"] != "serve" || lines[0]["listen"] != addr || last["type"] != "serve_end" || last["reason"] != "idle" || last["connections"].(float64) != 1 {
		t.Fatalf("serve output %v", lines)
	}
}

func TestNetRefusesWithoutAServer(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	cfg := mustDefaults(t, "net-latency")
	short(&cfg)
	cfg.Peer = addr
	out, err := run(t, cfg)
	if err == nil {
		t.Fatal("ran without a server")
	}
	if start, _, end := records(t, out); start != nil || end["reason"] != "error" || end["error"] == nil {
		t.Fatalf("end %v", end)
	}
}
