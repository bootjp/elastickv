// Package monoclock exposes a monotonic-raw clock for the lease-read
// path.
//
// Go's time.Now() returns a wall-clock value backed internally by the
// kernel's CLOCK_MONOTONIC (Linux) or its equivalent — which is
// rate-adjusted ("slewed") by NTP at up to 500 ppm. That slew is small
// in steady state (~0.35 ms over a 700 ms lease window), but the safety
// case for leader-local lease reads should not depend on NTP being
// well-behaved: a misconfigured or abused time daemon can push the
// slew rate far past the 500 ppm POSIX cap, and other monotonic time
// sources (e.g. CLOCK_MONOTONIC_COARSE) can compound the error.
// CLOCK_MONOTONIC_RAW is immune to NTP rate adjustment and step events
// and is what TiKV's lease path uses.
//
// Instant values are opaque int64 nanosecond counters. They are only
// comparable within the same process lifetime and MUST NOT be
// persisted, serialized, or sent over the wire — the zero point is
// arbitrary and changes across processes. Callers that need an
// externally-meaningful timestamp should sample time.Now() separately;
// Instant is only for intra-process lease-safety reasoning.
package monoclock

import "time"

// Instant is a reading from the monotonic-raw clock. The zero value
// represents "no reading" and compares equal to Zero.
type Instant struct {
	ns int64
}

// Zero is the unset Instant.
var Zero = Instant{}

// Now returns the current monotonic-raw instant.
func Now() Instant { return Instant{ns: nowNanos()} }

// IsZero reports whether i is the zero Instant.
func (i Instant) IsZero() bool { return i.ns == 0 }

// After reports whether i is strictly after j.
func (i Instant) After(j Instant) bool { return i.ns > j.ns }

// Before reports whether i is strictly before j.
func (i Instant) Before(j Instant) bool { return i.ns < j.ns }

// Sub returns i - j as a Duration. Meaningful only when neither i nor
// j is the zero Instant; callers must guard with IsZero first.
func (i Instant) Sub(j Instant) time.Duration { return time.Duration(i.ns - j.ns) }

// Add returns i advanced by d.
func (i Instant) Add(d time.Duration) Instant { return Instant{ns: i.ns + int64(d)} }

// Nanos returns the raw int64 counter. Intended for atomic.Int64
// storage where a whole Instant struct cannot be stored atomically
// (see internal/raftengine/etcd/quorum_ack.go).
func (i Instant) Nanos() int64 { return i.ns }

// FromNanos reconstructs an Instant from a raw counter previously
// obtained via Nanos(). Counterpart to Nanos; the same intra-process
// caveats apply.
func FromNanos(ns int64) Instant { return Instant{ns: ns} }
