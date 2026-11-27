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
// from, both ways, once both have sent; a side's datagrams from another host
// than it is seen at are dropped.
func TestARelayPassesDatagramsBothWays(t *testing.T) {
	pa, pb := freePorts(t)
	rs := &relays{ctx: context.Background()}
	rs.set([]store.FabricRelay{{A: "a", B: "b", PortA: pa, PortB: pb, HostA: "127.0.0.1"}}, func(string, ...any) {})
	defer rs.set(nil, func(string, ...any) {})
	a, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	relayA := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: pa}
	relayB := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: pb}
	recv := func(c *net.UDPConn) string {
		t.Helper()
		buf := make([]byte, 64)
		c.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, _, err := c.ReadFromUDP(buf)
		if err != nil {
			t.Fatal(err)
		}
		return string(buf[:n])
	}
	// both sides say something (the first of each is how the relay learns them)
	a.WriteToUDP([]byte("hello from a"), relayA)
	time.Sleep(50 * time.Millisecond)
	b.WriteToUDP([]byte("hello from b"), relayB)
	if got := recv(a); got != "hello from b" {
		t.Fatalf("a got %q", got)
	}
	a.WriteToUDP([]byte("a again"), relayA)
	if got := recv(b); got != "a again" {
		t.Fatalf("b got %q", got)
	}
}
