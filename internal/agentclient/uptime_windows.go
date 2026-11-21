package agentclient

import (
	"unsafe"

	"golang.org/x/sys/windows"
)

var queryInterruptTimePrecise = windows.NewLazySystemDLL("kernelbase.dll").NewProc("QueryInterruptTimePrecise")

func uptimeSeconds() (float64, bool) {
	if err := queryInterruptTimePrecise.Find(); err != nil {
		return 0, false
	}
	// Biased interrupt time counts sleep and hibernation without using wall time.
	var ticks uint64
	queryInterruptTimePrecise.Call(uintptr(unsafe.Pointer(&ticks)))
	// Round up one 100 ns tick so quantization cannot shorten the uptime.
	return float64(ticks+1) / 10_000_000, true
}
