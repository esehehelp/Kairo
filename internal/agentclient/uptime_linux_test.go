package agentclient

import (
	"math"
	"testing"
)

func TestParseUptime(t *testing.T) {
	for _, tc := range []struct {
		text string
		want float64
	}{
		{" 123.45 678.90\n", 123.46},
		{"0.00 0.00\n", 0.01},
	} {
		if got, ok := parseUptime(tc.text); !ok || math.Abs(got-tc.want) > 1e-9 {
			t.Errorf("parseUptime(%q) = %g, %v; want %g", tc.text, got, ok, tc.want)
		}
	}
	for _, text := range []string{"", "not uptime", "-1.00 0.00", "NaN 0.00", "+Inf 0.00"} {
		if got, ok := parseUptime(text); ok {
			t.Errorf("parseUptime(%q) accepted %g", text, got)
		}
	}
}
