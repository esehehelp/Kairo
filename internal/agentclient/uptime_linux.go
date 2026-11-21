package agentclient

import (
	"math"
	"os"
	"strconv"
	"strings"
)

func uptimeSeconds() (float64, bool) {
	data, err := os.ReadFile("/proc/uptime")
	if err != nil {
		return 0, false
	}
	return parseUptime(string(data))
}

func parseUptime(text string) (float64, bool) {
	fields := strings.Fields(text)
	if len(fields) == 0 {
		return 0, false
	}
	seconds, err := strconv.ParseFloat(fields[0], 64)
	if err != nil || math.Signbit(seconds) || math.IsNaN(seconds) || math.IsInf(seconds, 0) {
		return 0, false
	}
	// /proc/uptime counts suspend but truncates to centiseconds.
	return seconds + 0.01, true
}
