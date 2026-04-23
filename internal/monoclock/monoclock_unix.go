//go:build linux || darwin || freebsd

package monoclock

import "golang.org/x/sys/unix"

// nowNanos reads CLOCK_MONOTONIC_RAW via clock_gettime(3). Linux and
// Darwin both expose this clock; FreeBSD labels the equivalent clock
// CLOCK_MONOTONIC_RAW as well. Windows and Plan 9 use the portable
// fallback (monoclock_fallback.go).
//
// A non-nil error from ClockGettime should be essentially impossible
// on supported platforms (the syscall fails only on invalid clock IDs
// or EFAULT on the timespec pointer, neither of which applies here),
// but returning zero instead of panicking keeps the lease path live
// under bizarre sandboxes; the existing engineLeaseAckValid guards
// (ack.IsZero, ack.After(now)) still hold.
func nowNanos() int64 {
	var ts unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_MONOTONIC_RAW, &ts); err != nil {
		return 0
	}
	return ts.Nano()
}
