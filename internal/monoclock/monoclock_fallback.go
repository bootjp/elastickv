//go:build !(linux || darwin)

package monoclock

import "time"

// epoch anchors the fallback monotonic counter. time.Since uses Go's
// runtime monotonic component and is step-immune, though unlike
// CLOCK_MONOTONIC_RAW it is still subject to NTP rate adjustment. On
// platforms where golang.org/x/sys/unix does not export
// CLOCK_MONOTONIC_RAW (FreeBSD, Windows, Plan 9, ...) this is the
// closest portable substitute; lease safety on those platforms
// therefore matches the pre-#551 behaviour. Linux and Darwin use
// the raw clock (monoclock_unix.go).
var epoch = time.Now()

func nowNanos() int64 {
	return int64(time.Since(epoch))
}
