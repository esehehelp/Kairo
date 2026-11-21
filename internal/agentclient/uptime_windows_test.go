package agentclient

import (
	"testing"
	"time"

	"golang.org/x/sys/windows"
)

func TestWindowsUptime(t *testing.T) {
	if err := queryInterruptTimePrecise.Find(); err != nil {
		t.Skipf("biased interrupt time unavailable: %v", err)
	}
	before := windows.DurationSinceBoot().Seconds()
	seconds, ok := uptimeSeconds()
	// GetTickCount64 can lag precise interrupt time by a clock tick.
	after := windows.DurationSinceBoot().Seconds() + 0.05
	if !ok || seconds <= 0 || seconds < before || seconds > after {
		t.Fatalf("uptime = %g seconds, known=%v; want between %g and %g", seconds, ok, before, after)
	}
	time.Sleep(20 * time.Millisecond)
	if later, ok := uptimeSeconds(); !ok || later <= seconds {
		t.Fatalf("uptime did not advance: %g -> %g (known=%v)", seconds, later, ok)
	}
}
