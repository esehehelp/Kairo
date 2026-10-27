package bench

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// The network workloads talk to kairo bench serve. A connection starts
// with a hello line from the client, {"schema":"kairo.bench/v1","mode":M,
// "message_bytes":N}, answered by {"ok":true} or {"ok":false,"error":E};
// then, by mode: echo returns every message_bytes message, sink reads and
// discards, source writes until the client closes.

const (
	latencyMessageBytes = 64
	bulkBufferBytes     = 128 * 1024
	maxMessageBytes     = 64 * 1024
	dialTimeout         = 5 * time.Second
	helloTimeout        = 10 * time.Second
	serveMaxConnections = 16
)

type hello struct {
	Schema       string `json:"schema"`
	Mode         string `json:"mode"`
	MessageBytes int    `json:"message_bytes,omitempty"`
}

type helloReply struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

// dialBench connects to the server and opens a connection in mode.
func dialBench(ctx context.Context, peer, mode string, messageBytes int) (*net.TCPConn, *bufio.Reader, error) {
	d := net.Dialer{Timeout: dialTimeout}
	c, err := d.DialContext(ctx, "tcp", peer)
	if err != nil {
		return nil, nil, err
	}
	conn := c.(*net.TCPConn)
	if err := conn.SetNoDelay(true); err != nil {
		conn.Close()
		return nil, nil, err
	}
	conn.SetDeadline(time.Now().Add(helloTimeout))
	line, _ := json.Marshal(hello{Schema: Schema, Mode: mode, MessageBytes: messageBytes})
	r := bufio.NewReader(conn)
	var reply helloReply
	if _, err = conn.Write(append(line, '\n')); err == nil {
		var raw []byte
		if raw, err = r.ReadBytes('\n'); err == nil {
			err = json.Unmarshal(raw, &reply)
		}
	}
	if err == nil && !reply.OK {
		err = fmt.Errorf("peer refused the connection: %s", reply.Error)
	}
	if err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("hello to %s (is kairo bench serve listening there?): %w", peer, err)
	}
	conn.SetDeadline(time.Time{})
	return conn, r, nil
}

// connectionDevice describes the path a connection takes: its addresses,
// the local interface holding the local address (the one the route chose),
// its MTU, and the link speed the OS reports — a fact, which for a virtual
// NIC or loopback may mean nothing.
func connectionDevice(conn net.Conn) map[string]any {
	local, remote := conn.LocalAddr().(*net.TCPAddr), conn.RemoteAddr().(*net.TCPAddr)
	d := map[string]any{"kind": "network", "local_addr": local.String(), "remote_addr": remote.String(), "interface": nil, "mtu": nil}
	ifc := interfaceOf(local.IP)
	if ifc == nil {
		return d
	}
	d["interface"], d["interface_index"], d["mtu"] = ifc.Name, ifc.Index, ifc.MTU
	for k, v := range linkSpeed(ifc.Index, ifc.Name) {
		d[k] = v
	}
	return d
}

func interfaceOf(ip net.IP) *net.Interface {
	ifs, err := net.Interfaces()
	if err != nil {
		return nil
	}
	for i := range ifs {
		addrs, err := ifs[i].Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			if n, ok := a.(*net.IPNet); ok && n.IP.Equal(ip) {
				return &ifs[i]
			}
		}
	}
	return nil
}

// unblockOnDone makes the connections' blocked reads and writes return when ctx ends.
func unblockOnDone(ctx context.Context, conns ...net.Conn) (stop func() bool) {
	return context.AfterFunc(ctx, func() {
		for _, c := range conns {
			c.SetDeadline(time.Now())
		}
	})
}

func prepareNetLatency(ctx context.Context, cfg *Config, p *plan) error {
	p.units = []string{"request"}
	p.params["peer"] = cfg.Peer
	p.params["message_bytes"] = latencyMessageBytes
	p.params["tcp_nodelay"] = true
	conn, r, err := dialBench(ctx, cfg.Peer, "echo", latencyMessageBytes)
	if err != nil {
		return err
	}
	p.finish = func(*endRecord) { conn.Close() }
	p.device = connectionDevice(conn)
	p.work = func(ctx context.Context, m *meter) error {
		defer unblockOnDone(ctx, conn)()
		rec := m.recorder()
		out, in := make([]byte, latencyMessageBytes), make([]byte, latencyMessageBytes)
		fillPattern(out, 7)
		for seq := uint64(0); ctx.Err() == nil; seq++ {
			binary.LittleEndian.PutUint64(out, seq)
			began := monoNow()
			_, err := conn.Write(out)
			if err == nil {
				_, err = io.ReadFull(r, in)
			}
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return fmt.Errorf("round trip %d: %w", seq, err)
			}
			rec.record(began)
			if !bytes.Equal(in, out) {
				return fmt.Errorf("round trip %d: the echo differs from the message", seq)
			}
			m.add(1, 1)
		}
		return nil
	}
	return nil
}

func prepareNetBandwidth(ctx context.Context, cfg *Config, p *plan) error {
	p.units = []string{"byte"}
	p.params["peer"] = cfg.Peer
	p.params["streams"] = cfg.Streams
	p.params["dir"] = cfg.Dir
	p.params["buffer_bytes"] = bulkBufferBytes
	mode := "sink"
	if cfg.Dir == "recv" {
		mode = "source"
	}
	var conns []net.Conn
	p.finish = func(*endRecord) {
		for _, c := range conns {
			c.Close()
		}
	}
	for range cfg.Streams {
		conn, _, err := dialBench(ctx, cfg.Peer, mode, 0)
		if err != nil {
			return err
		}
		conns = append(conns, conn)
	}
	p.device = connectionDevice(conns[0])
	p.work = func(ctx context.Context, m *meter) error {
		defer unblockOnDone(ctx, conns...)()
		err := runWorkers(ctx, len(conns), func(ctx context.Context, i int) error {
			buf := make([]byte, bulkBufferBytes)
			fillPattern(buf, uint64(i)+11)
			for ctx.Err() == nil {
				var n int
				var err error
				if cfg.Dir == "send" {
					n, err = conns[i].Write(buf)
				} else {
					n, err = conns[i].Read(buf)
				}
				if n > 0 {
					m.add(1, uint64(n))
				}
				if err != nil {
					if ctx.Err() != nil {
						return nil
					}
					return fmt.Errorf("stream %d: %w", i, err)
				}
			}
			return nil
		})
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}
	return nil
}

// ServeConfig is kairo bench serve's.
type ServeConfig struct {
	Listen string
	// IdleTimeout ends the server once no connection has been open for it.
	IdleTimeout time.Duration
	// MaxConnDuration closes a connection that has been open for it.
	MaxConnDuration time.Duration
}

// serveRecord is a line of kairo bench serve's output.
type serveRecord struct {
	Schema      string `json:"schema"`
	Type        string `json:"type"` // serve, connection, serve_end
	Program     string `json:"program"`
	Wall        string `json:"wall"`
	Listen      string `json:"listen,omitempty"`
	Remote      string `json:"remote,omitempty"`
	Mode        string `json:"mode,omitempty"`
	BytesIn     int64  `json:"bytes_in,omitempty"`
	BytesOut    int64  `json:"bytes_out,omitempty"`
	DurationNS  int64  `json:"duration_ns,omitempty"`
	Error       string `json:"error,omitempty"`
	Reason      string `json:"reason,omitempty"`
	Connections int    `json:"connections,omitempty"`
}

// Serve listens on cfg.Listen and serves kairo bench run's network
// workloads, one JSON line per connection on out, until ctx ends or it has
// been idle for cfg.IdleTimeout.
func Serve(ctx context.Context, out io.Writer, cfg ServeConfig) error {
	if cfg.IdleTimeout <= 0 || cfg.MaxConnDuration <= 0 {
		return errors.New("--idle-timeout and --max-conn-duration must be positive")
	}
	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return err
	}
	return serveListener(ctx, out, ln, cfg)
}

func serveListener(ctx context.Context, out io.Writer, ln net.Listener, cfg ServeConfig) error {
	var outMu sync.Mutex
	enc := json.NewEncoder(out)
	emit := func(r serveRecord) {
		r.Schema, r.Program, r.Wall = Schema, program, time.Now().UTC().Format(wallFormat)
		outMu.Lock()
		defer outMu.Unlock()
		_ = enc.Encode(r)
	}
	emit(serveRecord{Type: "serve", Listen: ln.Addr().String()})

	var (
		mu     sync.Mutex
		open   = map[net.Conn]bool{}
		reason string
		total  int
		wg     sync.WaitGroup
	)
	stopWith := func(why string) {
		mu.Lock()
		if reason == "" {
			reason = why
		}
		mu.Unlock()
		ln.Close()
	}
	idle := time.AfterFunc(cfg.IdleTimeout, func() {
		mu.Lock()
		quiet := len(open) == 0
		mu.Unlock()
		if quiet {
			stopWith("idle")
		}
	})
	defer idle.Stop()
	defer context.AfterFunc(ctx, func() { stopWith("signal") })()

	for {
		conn, err := ln.Accept()
		if err != nil {
			mu.Lock()
			why := reason
			for c := range open {
				c.Close()
			}
			mu.Unlock()
			wg.Wait()
			if why == "" {
				return err
			}
			emit(serveRecord{Type: "serve_end", Reason: why, Connections: total})
			return nil
		}
		mu.Lock()
		full := len(open) >= serveMaxConnections
		if !full {
			open[conn] = true
			total++
			idle.Stop()
		}
		mu.Unlock()
		if full {
			conn.SetDeadline(time.Now().Add(time.Second))
			conn.Write([]byte(`{"ok":false,"error":"too many connections"}` + "\n"))
			conn.Close()
			continue
		}
		wg.Go(func() {
			r := serveConn(conn, cfg.MaxConnDuration)
			conn.Close()
			mu.Lock()
			delete(open, conn)
			if len(open) == 0 {
				idle.Reset(cfg.IdleTimeout)
			}
			mu.Unlock()
			r.Type = "connection"
			emit(r)
		})
	}
}

// countingConn counts the bytes through a connection.
type countingConn struct {
	net.Conn
	in, out atomic.Int64
}

func (c *countingConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.in.Add(int64(n))
	return n, err
}

func (c *countingConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	c.out.Add(int64(n))
	return n, err
}

// serveConn serves one connection for at most limit.
func serveConn(raw net.Conn, limit time.Duration) serveRecord {
	began := time.Now()
	c := &countingConn{Conn: raw}
	rec := serveRecord{Remote: raw.RemoteAddr().String()}
	defer func() {
		rec.BytesIn, rec.BytesOut, rec.DurationNS = c.in.Load(), c.out.Load(), time.Since(began).Nanoseconds()
	}()
	if tcp, ok := raw.(*net.TCPConn); ok {
		tcp.SetNoDelay(true)
	}
	raw.SetDeadline(began.Add(min(helloTimeout, limit)))
	r := bufio.NewReader(c)
	line, err := r.ReadBytes('\n')
	var h hello
	if err == nil {
		err = json.Unmarshal(line, &h)
	}
	reply := func(e string) {
		b, _ := json.Marshal(helloReply{OK: e == "", Error: e})
		c.Write(append(b, '\n'))
	}
	switch {
	case err != nil:
		rec.Error = "hello: " + err.Error()
		return rec
	case h.Schema != Schema:
		rec.Error = fmt.Sprintf("schema %q, want %q", h.Schema, Schema)
		reply(rec.Error)
		return rec
	case h.Mode == "echo" && (h.MessageBytes < 1 || h.MessageBytes > maxMessageBytes):
		rec.Error = fmt.Sprintf("message_bytes %d out of 1..%d", h.MessageBytes, maxMessageBytes)
		reply(rec.Error)
		return rec
	case h.Mode != "echo" && h.Mode != "sink" && h.Mode != "source":
		rec.Error = fmt.Sprintf("unknown mode %q", h.Mode)
		reply(rec.Error)
		return rec
	}
	rec.Mode = h.Mode
	raw.SetDeadline(began.Add(limit))
	reply("")
	switch h.Mode {
	case "echo":
		buf := make([]byte, h.MessageBytes)
		for {
			if _, err = io.ReadFull(r, buf); err == nil {
				_, err = c.Write(buf)
			}
			if err != nil {
				break
			}
		}
	case "sink":
		_, err = io.CopyBuffer(io.Discard, r, make([]byte, bulkBufferBytes))
	case "source":
		buf := make([]byte, bulkBufferBytes)
		fillPattern(buf, 13)
		for err == nil {
			_, err = c.Write(buf)
		}
	}
	if err != nil && !errors.Is(err, io.EOF) && !isPeerClose(err) {
		rec.Error = err.Error()
	}
	return rec
}

// isPeerClose: the client ended the connection (the normal end of a test).
func isPeerClose(err error) bool {
	var op *net.OpError
	return errors.As(err, &op) && !op.Timeout()
}
