package fabric

import (
	"context"
	"net"
	"testing"
	"time"

	"kairo/internal/store"
)

// freePorts are two UDP ports nobody listens on.
func freePorts(t *testing.T) (int, int) {
	t.Helper()
	var ports []int
	for range 2 {
		c, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
		if err != nil {
			t.Fatal(err)
		}
		ports = append(ports, c.LocalAddr().(*net.UDPAddr).Port)
		c.Close()
	}
	return ports[0], ports[1]
}

// A relay passes each side's datagrams to where the other was last heard
// from, both ways, once both have sent; a datagram to a side's port from
// another host than that side is seen at is dropped (it neither reaches the
// other side nor takes the side's place). A relay whose sides are not both
// known is not started; one whose side is seen at another host is started
// again on the same ports.
func TestARelayPassesDatagramsBothWays(t *testing.T) {
	pa, pb := freePorts(t)
	rs := &relays{ctx: context.Background()}
	quiet := func(string, ...any) {}
	rs.set([]store.FabricRelay{{A: "a", B: "b", PortA: pa, PortB: pb, HostA: "127.0.0.1"}}, quiet)
	if len(rs.on) != 0 {
		t.Fatal("a relay started without where side b is")
	}
	// b first seen elsewhere, then here: started again on the same ports
	rs.set([]store.FabricRelay{{A: "a", B: "b", PortA: pa, PortB: pb, HostA: "127.0.0.1", HostB: "192.0.2.1"}}, quiet)
	rs.set([]store.FabricRelay{{A: "a", B: "b", PortA: pa, PortB: pb, HostA: "127.0.0.1", HostB: "127.0.0.1"}}, quiet)
	if len(rs.on) != 1 {
		t.Fatalf("%d relays after b moved", len(rs.on))
	}
	defer rs.set(nil, quiet)
	listen := func(ip net.IP) *net.UDPConn {
		t.Helper()
		c, err := net.ListenUDP("udp", &net.UDPAddr{IP: ip})
		if err != nil {
			t.Skipf("no %s here: %v", ip, err)
		}
		return c
	}
	a, b := listen(net.IPv4(127, 0, 0, 1)), listen(net.IPv4(127, 0, 0, 1))
	defer a.Close()
	defer b.Close()
	relayA := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: pa}
	relayB := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: pb}
	recv := func(c *net.UDPConn, within time.Duration) (string, bool) {
		t.Helper()
		buf := make([]byte, 64)
		c.SetReadDeadline(time.Now().Add(within))
		n, _, err := c.ReadFromUDP(buf)
		if err != nil {
			return "", false
		}
		return string(buf[:n]), true
	}
	// both sides say something (the first of each is how the relay learns them)
	a.WriteToUDP([]byte("hello from a"), relayA)
	time.Sleep(50 * time.Millisecond)
	b.WriteToUDP([]byte("hello from b"), relayB)
	if got, _ := recv(a, 2*time.Second); got != "hello from b" {
		t.Fatalf("a got %q", got)
	}
	a.WriteToUDP([]byte("a again"), relayA)
	if got, _ := recv(b, 2*time.Second); got != "a again" {
		t.Fatalf("b got %q", got)
	}
	// another host at a's port: dropped
	stranger := listen(net.IPv4(127, 0, 0, 2))
	defer stranger.Close()
	stranger.WriteToUDP([]byte("not a"), relayA)
	if got, ok := recv(b, 300*time.Millisecond); ok {
		t.Fatalf("b got %q from another host", got)
	}
	// a's place is still a's: b's answer reaches it
	b.WriteToUDP([]byte("b again"), relayB)
	if got, _ := recv(a, 2*time.Second); got != "b again" {
		t.Fatalf("a got %q after the stranger", got)
	}
	if got, ok := recv(stranger, 300*time.Millisecond); ok {
		t.Fatalf("the stranger got %q", got)
	}
}
