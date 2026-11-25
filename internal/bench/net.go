package bench

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
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
	maxHelloBytes       = 4096
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

// PackPathPrefix is where an agent's transfer endpoint (its pack server)
// takes a network bench's connections: /bench/net/<run>, upgraded from
// HTTP/1.1 (Upgrade: PackUpgrade) to the bench's protocol.
const (
	PackPathPrefix = "/bench/net/"
	PackUpgrade    = "kairo-bench"
	// TokenEnv names the variable holding a network bench's token for the
	// transfer endpoint it measures.
	TokenEnv = "KAIRO_BENCH_NET_TOKEN"
)

// dialBench connects to the server and opens a connection in mode: to kairo
// bench serve (cfg.Peer), or through an agent's transfer endpoint
// (cfg.PackPeer: TLS, as transfers go, upgraded to the bench's protocol).
func dialBench(ctx context.Context, cfg *Config, mode string, messageBytes int) (net.Conn, *bufio.Reader, error) {
	var conn net.Conn
	var r *bufio.Reader
	peer := cfg.Peer
	if cfg.PackPeer != "" {
		peer = cfg.PackPeer
		c, br, err := dialPack(ctx, cfg)
		if err != nil {
			return nil, nil, err
		}
		conn, r = c, br
	} else {
		d := net.Dialer{Timeout: dialTimeout}
		c, err := d.DialContext(ctx, "tcp", cfg.Peer)
		if err != nil {
			return nil, nil, err
		}
		if err := c.(*net.TCPConn).SetNoDelay(true); err != nil {
			c.Close()
			return nil, nil, err
		}
		conn, r = c, bufio.NewReader(c)
	}
	conn.SetDeadline(time.Now().Add(helloTimeout))
	line, _ := json.Marshal(hello{Schema: Schema, Mode: mode, MessageBytes: messageBytes})
	var reply helloReply
	_, err := conn.Write(append(line, '\n'))
	if err == nil {
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
		return nil, nil, fmt.Errorf("hello to %s (is kairo bench serve, or the agent's transfer endpoint, listening there?): %w", peer, err)
	}
	conn.SetDeadline(time.Time{})
	return conn, r, nil
}

// dialPack opens a connection to an agent's transfer endpoint and upgrades
// it to the bench's protocol, with the run's token (TokenEnv).
// With no --pack-ca, the system's roots are trusted, as transfers do with
// no ca_file.
func dialPack(ctx context.Context, cfg *Config) (net.Conn, *bufio.Reader, error) {
	var pool *x509.CertPool
	if cfg.PackCA != "" {
		pem, err := os.ReadFile(cfg.PackCA)
		if err != nil {
			return nil, nil, err
		}
		pool = x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, nil, fmt.Errorf("no certificate in %s", cfg.PackCA)
		}
	}
	host, _, _ := net.SplitHostPort(cfg.PackPeer)
	d := tls.Dialer{NetDialer: &net.Dialer{Timeout: dialTimeout}, Config: &tls.Config{RootCAs: pool, ServerName: host, NextProtos: []string{"http/1.1"}, MinVersion: tls.VersionTLS12}}
	c, err := d.DialContext(ctx, "tcp", cfg.PackPeer)
	if err != nil {
		return nil, nil, err
	}
	if tcp, ok := c.(*tls.Conn).NetConn().(*net.TCPConn); ok {
		tcp.SetNoDelay(true)
	}
	c.SetDeadline(time.Now().Add(helloTimeout))
	req := "GET " + PackPathPrefix + cfg.PackRun + " HTTP/1.1\r\nHost: " + cfg.PackPeer + "\r\nAuthorization: Bearer " + os.Getenv(TokenEnv) +
		"\r\nConnection: Upgrade\r\nUpgrade: " + PackUpgrade + "\r\n\r\n"
	br := bufio.NewReader(c)
	var resp *http.Response
	if _, err = io.WriteString(c, req); err == nil {
		resp, err = http.ReadResponse(br, nil)
	}
	if err == nil && resp.StatusCode != http.StatusSwitchingProtocols {
		err = fmt.Errorf("the transfer endpoint answered %s", resp.Status)
	}
	if err != nil {
		c.Close()
		return nil, nil, fmt.Errorf("network bench %s at %s: %w", cfg.PackRun, cfg.PackPeer, err)
	}
	return c, br, nil
}

// ServeConn serves one network bench connection for at most limit (an
// agent's transfer endpoint hands it over once upgraded).
func ServeConn(conn net.Conn, limit time.Duration) error {
	if rec := serveConn(conn, limit); rec.Error != "" {
		return errors.New(rec.Error)
	}
	return nil
}

// benchPeer is the peer measured: kairo bench serve's address, or the
// agent's transfer endpoint's (pack:HOST:PORT).
func benchPeer(cfg *Config) string {
	if cfg.PackPeer != "" {
		return "pack:" + cfg.PackPeer
	}
	return cfg.Peer
}

// connectionDevice describes the path a connection takes: its addresses,
// the local interface holding the local address (the one the route chose),
// its MTU, and the link speed the OS reports — a fact, which for a virtual
// NIC or loopback may mean nothing.
func connectionDevice(conn net.Conn) map[string]any {
	if t, ok := conn.(*tls.Conn); ok {
		conn = t.NetConn()
	}
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
	p.params["peer"] = benchPeer(cfg)
	p.params["message_bytes"] = latencyMessageBytes
	p.params["tcp_nodelay"] = true
	conn, r, err := dialBench(ctx, cfg, "echo", latencyMessageBytes)
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
	p.params["peer"] = benchPeer(cfg)
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
		conn, _, err := dialBench(ctx, cfg, mode, 0)
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
	// the hello is a line of one buffer at most (ErrBufferFull past it)
	r := bufio.NewReaderSize(c, maxHelloBytes)
	line, err := r.ReadSlice('\n')
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
