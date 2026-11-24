//go:build linux

package executor

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// The init waits for a default route when the container has a network
// (its DHCP comes after the start), and not at all when it has none.
func TestWaitForNetwork(t *testing.T) {
	dir := t.TempDir()
	netDir := filepath.Join(dir, "net")
	for _, d := range []string{"lo", "eth0"} {
		if err := os.MkdirAll(filepath.Join(netDir, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	route4, route6 := filepath.Join(dir, "route"), filepath.Join(dir, "ipv6_route")
	header := "Iface\tDestination\tGateway\tFlags\tRefCnt\tUse\tMetric\tMask\tMTU\tWindow\tIRTT\n"
	write := func(p, body string) {
		t.Helper()
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write(route4, header+"eth0\t0001A8C0\t00000000\t0001\t0\t0\t0\t00FFFFFF\t0\t0\t0\n")
	write(route6, "00000000000000000000000000000000 00 00000000000000000000000000000000 00 00000000000000000000000000000000 ffffffff 00000001 00000000 00200200 lo\n")
	if waitForNetwork(netDir, route4, route6, 50*time.Millisecond) {
		t.Fatal("no default route, yet waited for none")
	}
	write(route4, header+"eth0\t00000000\t0101A8C0\t0003\t0\t0\t0\t00000000\t0\t0\t0\n")
	if !waitForNetwork(netDir, route4, route6, time.Second) {
		t.Fatal("an IPv4 default route not seen")
	}
	write(route4, header)
	write(route6, "00000000000000000000000000000000 00 00000000000000000000000000000000 00 fe800000000000000000000000000001 00000400 00000001 00000000 00000003 eth0\n")
	if !waitForNetwork(netDir, route4, route6, time.Second) {
		t.Fatal("an IPv6 default route not seen")
	}
	if err := os.RemoveAll(filepath.Join(netDir, "eth0")); err != nil {
		t.Fatal(err)
	}
	write(route6, "")
	if !waitForNetwork(netDir, route4, route6, 50*time.Millisecond) {
		t.Fatal("waited although the container has no network")
	}
}
