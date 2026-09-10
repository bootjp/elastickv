package encryption_test

import (
	"sync"
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
	// Threshold is 9; drive the counter to the ceiling directly.
	for range 10 {
		b.Record(1)
	}
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
	// Threshold is 90% of 2^32.
	require.Equal(t, uint64(1)<<32/100*90, b.Remaining(1))
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
