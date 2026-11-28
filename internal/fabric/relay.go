package fabric

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"sync"

	"kairo/internal/store"
)

// A relay (issue #47) forwards WireGuard's datagrams between two nodes that
// reach each other on no direct path: a UDP port for each side; what comes
// to one side's port goes out of the other's to where that side was last
// heard from. The datagrams stay encrypted end to end: the relay sees no
// more than their sizes. A side's datagrams are taken only from the host it
// is seen at on the fabric (store.FabricRelay HostA, HostB): a relay waits
// until both are known (the relay node's own links to them), so that no one
// else takes a side's place.

// relayKey names a relayed pair.
func relayKey(r store.FabricRelay) string {
	return fmt.Sprintf("%s|%s|%d|%d", r.A, r.B, r.PortA, r.PortB)
}

// forwarder relays one pair.
type forwarder struct {
	a, b   *net.UDPConn
	hostA  netip.Addr // the only host side A is taken from
	hostB  netip.Addr
	mu     sync.Mutex
	peerA  netip.AddrPort // where A was last heard from
	peerB  netip.AddrPort
	cancel context.CancelFunc
	done   sync.WaitGroup
}

func listenRelay(port int) (*net.UDPConn, error) {
	return net.ListenUDP("udp", &net.UDPAddr{Port: port})
}

func newForwarder(ctx context.Context, r store.FabricRelay) (*forwarder, error) {
	hostA, errA := netip.ParseAddr(r.HostA)
	hostB, errB := netip.ParseAddr(r.HostB)
	if errA != nil || errB != nil {
		return nil, fmt.Errorf("where its sides are is not known yet (a: %q, b: %q)", r.HostA, r.HostB)
	}
	a, err := listenRelay(r.PortA)
	if err != nil {
		return nil, err
	}
	b, err := listenRelay(r.PortB)
	if err != nil {
		a.Close()
		return nil, err
	}
	f := &forwarder{a: a, b: b, hostA: hostA.Unmap(), hostB: hostB.Unmap()}
	ctx, f.cancel = context.WithCancel(ctx)
	f.done.Add(2)
	go f.pump(ctx, a, b, true)
	go f.pump(ctx, b, a, false)
	go func() {
		<-ctx.Done()
		a.Close()
		b.Close()
	}()
	return f, nil
}

// pump reads from's datagrams (side A's when fromA) and sends each out of
// to, to where the other side was last heard from.
func (f *forwarder) pump(ctx context.Context, from, to *net.UDPConn, fromA bool) {
	defer f.done.Done()
	buf := make([]byte, 65535)
	for {
		n, src, err := from.ReadFromUDPAddrPort(buf)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			continue
		}
		f.mu.Lock()
		var dst netip.AddrPort
		if fromA {
			if src.Addr().Unmap() != f.hostA {
				f.mu.Unlock()
				continue
			}
			f.peerA, dst = src, f.peerB
		} else {
			if src.Addr().Unmap() != f.hostB {
				f.mu.Unlock()
				continue
			}
			f.peerB, dst = src, f.peerA
		}
		f.mu.Unlock()
		if dst.IsValid() {
			_, _ = to.WriteToUDPAddrPort(buf[:n], dst)
		}
	}
}

func (f *forwarder) stop() {
	f.cancel()
	f.done.Wait()
}

// relays keeps a node's forwarders as its view says.
type relays struct {
	mu  sync.Mutex
	ctx context.Context
	on  map[string]*forwarder
}

// set stops the forwarders running not in want, then starts those of want
// not running (a pair seen at other hosts is started again, on the same
// ports: its old forwarder goes first).
func (rs *relays) set(want []store.FabricRelay, logf func(msg string, args ...any)) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if rs.on == nil {
		rs.on = map[string]*forwarder{}
	}
	keyOf := func(r store.FabricRelay) string { return relayKey(r) + "|" + r.HostA + "|" + r.HostB }
	keep := map[string]bool{}
	for _, r := range want {
		keep[keyOf(r)] = true
	}
	for key, f := range rs.on {
		if !keep[key] {
			f.stop()
			delete(rs.on, key)
		}
	}
	for _, r := range want {
		key := keyOf(r)
		if _, ok := rs.on[key]; ok {
			continue
		}
		f, err := newForwarder(rs.ctx, r)
		if err != nil {
			logf("fabric relay not started", "a", r.A, "b", r.B, "error", err)
			continue
		}
		rs.on[key] = f
		logf("fabric relay started", "a", r.A, "b", r.B, "port_a", r.PortA, "port_b", r.PortB)
	}
}
