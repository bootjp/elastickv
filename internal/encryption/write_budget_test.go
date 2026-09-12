package encryption_test

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/stretchr/testify/require"
)

// TestWriteBudgetAllowsUpToTheRefusalThreshold pins the ordinary path:
// writes proceed until 90% of the ceiling.
func TestWriteBudgetAllowsUpToTheRefusalThreshold(t *testing.T) {
	t.Parallel()

	// Ceiling 100 -> threshold 90.
	b := encryption.NewWriteBudget(100)
	for i := range 90 {
		require.True(t, b.Record(1).Allowed(), "write %d must be allowed", i)
	}
	require.Zero(t, b.Remaining(1))
}

// TestWriteBudgetRefusesAndSignalsRotationAtNinetyPercent is the §5.2
// trigger. Refusing BEFORE the ceiling is the point: rotation needs a
// Raft round trip, so waiting until the budget is actually spent would
// mean either blocking writes while it commits or issuing writes past
// the ceiling.
func TestWriteBudgetRefusesAndSignalsRotationAtNinetyPercent(t *testing.T) {
	t.Parallel()

	b := encryption.NewWriteBudget(100)
	for range 90 {
		require.True(t, b.Record(1).Allowed())
	}
	v := b.Record(1)
	require.Equal(t, encryption.WriteBudgetRotate, v)
	require.False(t, v.Allowed())
}

// TestWriteBudgetDoesNotCountRefusedWrites is the property that keeps
// the ceiling meaningful. A caller that retries on refusal must not be
// able to walk the counter past the ceiling.
func TestWriteBudgetDoesNotCountRefusedWrites(t *testing.T) {
	t.Parallel()

	b := encryption.NewWriteBudget(100)
	for range 90 {
		b.Record(1)
	}
	used := b.Used(1)

	for range 1000 {
		require.False(t, b.Record(1).Allowed())
	}
	require.Equal(t, used, b.Used(1),
		"a refused write must not consume budget, or retries would breach the ceiling")
}

// TestWriteBudgetFailsClosedPastTheCeiling covers a caller that ignored
// the rotate signal: it must still stop rather than keep encrypting
// under a DEK whose budget is spent.
func TestWriteBudgetFailsClosedPastTheCeiling(t *testing.T) {
	t.Parallel()

	b := encryption.NewWriteBudget(10)
	// Threshold is 9, so nine writes are allowed and the tenth is
	// refused. (Before the threshold arithmetic was fixed this
	// ceiling produced a threshold of 0 and the comment was false:
	// every write here was refused and the test asserted nothing.)
	for range 10 {
		b.Record(1)
	}
	require.Equal(t, uint64(9), b.Used(1))
	// Even having crossed into rotate territory, the verdict must
	// never become Allow again.
	for range 50 {
		require.False(t, b.Record(1).Allowed())
	}
}

// TestWriteBudgetIsPerDEK pins that one DEK's exhaustion does not
// refuse writes under another — rotation installs a new key_id, and
// that new key must start with a full budget or the cluster would be
// wedged the moment it rotated.
func TestWriteBudgetIsPerDEK(t *testing.T) {
	t.Parallel()

	b := encryption.NewWriteBudget(100)
	for range 95 {
		b.Record(1)
	}
	require.False(t, b.Record(1).Allowed())

	require.True(t, b.Record(2).Allowed(), "a freshly rotated DEK starts with a full budget")
	require.Equal(t, uint64(89), b.Remaining(2))
}

// TestWriteBudgetForgetDropsARetiredDEK pins the cleanup path: a
// long-lived process that rotates repeatedly must not accumulate a
// counter per historical DEK.
func TestWriteBudgetForgetDropsARetiredDEK(t *testing.T) {
	t.Parallel()

	b := encryption.NewWriteBudget(100)
	b.Record(1)
	require.Equal(t, uint64(1), b.Used(1))

	b.Forget(1)
	require.Zero(t, b.Used(1))
}

// TestWriteBudgetDefaultCeilingMatchesTheDesign pins the §5.2 number
// itself: 2^32 per (DEK, process-load), per NIST SP 800-38D §8.3.
func TestWriteBudgetDefaultCeilingMatchesTheDesign(t *testing.T) {
	t.Parallel()

	require.Equal(t, uint64(1)<<32, encryption.DefaultWriteBudgetCeiling)

	b := encryption.NewWriteBudget(0)
	// Threshold is 90% of 2^32, stated as the specification rather than
	// as the implementation's expression. The previous form here was
	// `1<<32/100*90`, which mirrored the production division order and
	// therefore could not catch an error in it: it asserted the lossy
	// 3865470480 instead of the exact 3865470566.
	require.Equal(t, uint64(1)<<32*9/10, b.Remaining(1))
	require.Equal(t, uint64(3865470566), b.Remaining(1))
}

// TestWriteBudgetIsRaceFree exercises the hot path from many
// goroutines: this sits on every encrypted write, so a torn counter
// would be a live data race in production.
func TestWriteBudgetIsRaceFree(t *testing.T) {
	t.Parallel()

	const goroutines = uint32(16)
	const perGoroutine = 64
	const distinctKeys = uint32(4)

	b := encryption.NewWriteBudget(1 << 20)
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range perGoroutine {
				b.Record(g % distinctKeys)
			}
		}()
	}
	wg.Wait()

	total := uint64(0)
	for keyID := range distinctKeys {
		total += b.Used(keyID)
	}
	require.Equal(t, uint64(goroutines)*uint64(perGoroutine), total)
}

// TestWriteBudgetNilReceiverAllows covers a node with no budget wired:
// it must not refuse every write.
func TestWriteBudgetNilReceiverAllows(t *testing.T) {
	t.Parallel()

	var b *encryption.WriteBudget
	require.True(t, b.Record(1).Allowed())
	require.Zero(t, b.Used(1))
	require.NotPanics(t, func() { b.Forget(1) })
}

func TestWriteBudgetVerdictStringsAreStable(t *testing.T) {
	t.Parallel()

	require.Equal(t, "allow", encryption.WriteBudgetAllow.String())
	require.Equal(t, "rotate", encryption.WriteBudgetRotate.String())
	require.Equal(t, "exhausted", encryption.WriteBudgetExhausted.String())
}

// TestWriteBudgetRefusedWritesDoNotConsumeBudgetUnderConcurrency is the
// concurrent form of TestWriteBudgetDoesNotCountRefusedWrites, which
// only ever exercised the sequential path and so passed while refused
// writes were being counted.
//
// Load-then-Add admits every writer that read a count below the
// threshold; they all increment, so writers that are subsequently
// refused still spend budget. Sequentially the bug is invisible -- the
// Load sees the threshold and returns first -- so only sustained
// contention at the boundary exposes it.
//
// The invariant asserted here is the one that makes the overshoot
// impossible: the counter can never exceed the refusal threshold,
// because only a write that won the CAS is recorded. That also means
// the number of permitted writes equals the threshold exactly, no
// matter how many writers raced.
func TestWriteBudgetRefusedWritesDoNotConsumeBudgetUnderConcurrency(t *testing.T) {
	t.Parallel()

	const (
		ceiling   = uint64(10000)
		threshold = uint64(9000)
		racers    = 64
		trials    = 20
	)

	for trial := range trials {
		b := encryption.NewWriteBudget(ceiling)

		// Every racer hammers Record until it is refused, so the
		// writers contending at the boundary are many and arrive
		// continuously rather than in one staged burst.
		var allowed atomic.Uint64
		var wg sync.WaitGroup
		for range racers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for b.Record(1).Allowed() {
					allowed.Add(1)
				}
			}()
		}
		wg.Wait()

		require.LessOrEqual(t, b.Used(1), threshold,
			"trial %d: the counter passed the refusal threshold, so refused "+
				"writes consumed budget", trial)
		require.Equal(t, threshold, allowed.Load(),
			"trial %d: exactly the threshold many writes may be permitted", trial)
		require.Equal(t, allowed.Load(), b.Used(1),
			"trial %d: the counter must record permitted writes and nothing else", trial)
	}
}

// TestWriteBudgetThresholdIsNinetyPercent pins the threshold arithmetic
// across ceilings that are not multiples of 100, where dividing before
// multiplying silently collapsed the budget.
func TestWriteBudgetThresholdIsNinetyPercent(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		ceiling uint64
		want    uint64
	}{
		{"design default 2^32", encryption.DefaultWriteBudgetCeiling, 3865470566},
		{"exact multiple of 100", 100, 90},
		{"not a multiple of 100", 199, 179},
		{"just under 100", 99, 89},
		{"single digit", 10, 9},
		{"smallest with a nonzero 90%", 2, 1},
		{"degenerate ceiling of 1 still allows one write", 1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := encryption.NewWriteBudget(tc.ceiling)
			// Remaining from a fresh budget is the threshold.
			require.Equal(t, tc.want, b.Remaining(1))
		})
	}
}

// TestWriteBudgetTinyCeilingDoesNotWedgeTheDEK covers the anti-wedge
// guard. A ceiling whose 90% point floors to zero would refuse the very
// first write, so the cluster would propose rotations forever and never
// issue a write under the new DEK either.
func TestWriteBudgetTinyCeilingDoesNotWedgeTheDEK(t *testing.T) {
	t.Parallel()

	for ceiling := uint64(1); ceiling <= 9; ceiling++ {
		b := encryption.NewWriteBudget(ceiling)
		require.True(t, b.Record(1).Allowed(),
			"ceiling %d must allow at least one write", ceiling)
	}
}

// TestWriteBudgetReachesExhaustedAtADegenerateCeiling covers the
// ceiling branch, which atomic reservation otherwise makes unreachable:
// with a ceiling of 1 the threshold equals the ceiling, so the counter
// does land on it.
func TestWriteBudgetReachesExhaustedAtADegenerateCeiling(t *testing.T) {
	t.Parallel()

	b := encryption.NewWriteBudget(1)
	require.Equal(t, encryption.WriteBudgetAllow, b.Record(1))
	require.Equal(t, encryption.WriteBudgetExhausted, b.Record(1))
}
