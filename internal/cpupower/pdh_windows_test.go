//go:build windows && (amd64 || arm64)

package cpupower

import (
	"os"
	"testing"
	"time"
)

// TestPDHReadsThisMachinesPackagePower reads the real Energy Meter counter
// (it only reads). It needs a CPU whose RAPL Windows exposes, so it runs
// only with KAIRO_TEST_PDH=1.
func TestPDHReadsThisMachinesPackagePower(t *testing.T) {
	if os.Getenv("KAIRO_TEST_PDH") != "1" {
		t.Skip("set KAIRO_TEST_PDH=1 to read this machine's Energy Meter counter")
	}
	m := New()
	m.Read()
	time.Sleep(1100 * time.Millisecond)
	r, ok := m.Read()
	if !ok {
		t.Fatal("no CPU package power from the Energy Meter counter")
	}
	if r.Packages < 1 || r.Watts <= 0 || r.Watts > 2000 {
		t.Fatalf("implausible reading %+v", r)
	}
	t.Logf("CPU package power %.1f W over %d package(s)", r.Watts, r.Packages)
}
