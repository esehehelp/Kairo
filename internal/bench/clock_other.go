//go:build !windows

package bench

import "time"

var clockBase = time.Now()

// monoNow is nanoseconds on the monotonic clock (clock_gettime
// CLOCK_MONOTONIC through the runtime).
func monoNow() int64 { return int64(time.Since(clockBase)) }
