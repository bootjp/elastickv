//go:build !(linux || darwin || freebsd)

package monoclock

import "time"

// epoch anchors the fallback monotonic counter. time.Since uses Go's
// runtime monotonic component and is step-immune, though unlike
// CLOCK_MONOTONIC_RAW it is still subject to NTP rate adjustment. On
// platforms without CLOCK_MONOTONIC_RAW this is the closest portable
// substitute; lease safety on those platforms therefore matches the
// pre-#551 behaviour. Linux / Darwin / FreeBSD use the raw clock
// (monoclock_unix.go).
var epoch = time.Now()

func nowNanos() int64 {
	return int64(time.Since(epoch))
}
