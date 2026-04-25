//go:build linux || darwin

package monoclock

import "golang.org/x/sys/unix"

// nowNanos reads CLOCK_MONOTONIC_RAW via clock_gettime(3). Only Linux
// and Darwin export this constant in golang.org/x/sys/unix; FreeBSD
// lacks the binding (its kernel exposes CLOCK_MONOTONIC_PRECISE, a
// different clock) and all other platforms use the portable fallback
// in monoclock_fallback.go.
//
// A non-nil error from ClockGettime should be essentially impossible
// on supported platforms — the syscall fails only on invalid clock
// IDs (compile-time constant here) or EFAULT on the timespec pointer
// (stack-allocated here). The realistic failure mode is a
// seccomp/sandbox profile that denies clock_gettime. We return 0 in
// that case: callers (leaseState.valid, engineLeaseAckValid) treat a
// zero Instant as "clock unavailable" and force the slow path, so a
// persistent syscall failure cannot leave a warmed lease valid.
func nowNanos() int64 {
	var ts unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_MONOTONIC_RAW, &ts); err != nil {
		return 0
	}
	return ts.Nano()
}
