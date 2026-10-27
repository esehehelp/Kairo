//go:build windows

package bench

import (
	"syscall"
	"unsafe"
)

// Go's monotonic clock on Windows advances with the timer interrupt
// (0.5–15.6 ms), too coarse for request latencies: monoNow reads the
// performance counter.
var (
	procQueryPerformanceCounter   = kernel32.NewProc("QueryPerformanceCounter")
	procQueryPerformanceFrequency = kernel32.NewProc("QueryPerformanceFrequency")
	qpcFrequency                  = func() int64 {
		var f int64
		procQueryPerformanceFrequency.Call(uintptr(unsafe.Pointer(&f)))
		return f
	}()
)

// monoNow is nanoseconds on the monotonic clock (QueryPerformanceCounter).
func monoNow() int64 {
	var c int64
	syscall.SyscallN(procQueryPerformanceCounter.Addr(), uintptr(unsafe.Pointer(&c)))
	return c/qpcFrequency*1e9 + c%qpcFrequency*1e9/qpcFrequency
}
