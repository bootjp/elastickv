package monoclock

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestInstant_ZeroIsZero(t *testing.T) {
	t.Parallel()
	require.True(t, Zero.IsZero())
	var i Instant
	require.True(t, i.IsZero())
	require.True(t, FromNanos(0).IsZero())
}

func TestNow_IsNonZeroAndMonotonic(t *testing.T) {
	t.Parallel()
	// CLOCK_MONOTONIC_RAW must advance across two Now() calls (modulo
	// nanosecond-granularity ties; use a sleep to ensure monotonic
	// progress). A regression that returns 0 or runs the clock
	// backwards would break every lease-read safety guard.
	a := Now()
	require.False(t, a.IsZero(), "Now must return non-zero instant on supported platforms")
	time.Sleep(100 * time.Microsecond)
	b := Now()
	require.False(t, b.Before(a), "monotonic-raw clock must not regress across calls")
	require.True(t, b.After(a) || b == a)
}

func TestInstant_AddAndSub(t *testing.T) {
	t.Parallel()
	base := FromNanos(1_000_000)
	later := base.Add(250 * time.Millisecond)
	require.True(t, later.After(base))
	require.Equal(t, 250*time.Millisecond, later.Sub(base))
	require.Equal(t, -250*time.Millisecond, base.Sub(later))
}

func TestInstant_NanosRoundtrip(t *testing.T) {
	t.Parallel()
	i := FromNanos(42)
	require.Equal(t, int64(42), i.Nanos())
}

func TestInstant_BeforeAfterOrdering(t *testing.T) {
	t.Parallel()
	a := FromNanos(100)
	b := FromNanos(200)
	require.True(t, a.Before(b))
	require.True(t, b.After(a))
	require.False(t, a.After(b))
	require.False(t, b.Before(a))
	// Equal instants: neither Before nor After.
	c := FromNanos(100)
	require.False(t, a.Before(c))
	require.False(t, a.After(c))
}
