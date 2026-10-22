package encryption_test

import (
	"math"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/stretchr/testify/require"
)

const (
	retiringDEK = uint32(7)
	activeDEK   = uint32(8)
	retainFloor = uint64(100)
)

func version(over encryption.MVCCVersionRef) encryption.MVCCVersionRef {
	if over.CommitTS == 0 {
		over.CommitTS = retainFloor + 50
	}
	return over
}

// TestClassifyMVCCRewriteCoversEveryVersionShape is the §5.4 iteration
// contract. The unit is (user_key, version_ts): a rewrite that touched
// only live values would leave older versions under the retiring DEK
// and break snapshot reads the moment it was unloaded.
func TestClassifyMVCCRewriteCoversEveryVersionShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		version    encryption.MVCCVersionRef
		migrate    bool
		wantVerb   encryption.RewriteVerdict
		wantReason string
	}{
		{
			name:       "version under the retiring DEK",
			version:    version(encryption.MVCCVersionRef{KeyID: retiringDEK}),
			wantVerb:   encryption.RewriteReencrypt,
			wantReason: encryption.RewriteReasonRetiringDEK,
		},
		{
			name:       "version already under the active DEK",
			version:    version(encryption.MVCCVersionRef{KeyID: activeDEK}),
			wantVerb:   encryption.RewriteSkip,
			wantReason: encryption.RewriteReasonDifferentDEK,
		},
		{
			name:       "version under another old DEK",
			version:    version(encryption.MVCCVersionRef{KeyID: activeDEK + 1}),
			wantVerb:   encryption.RewriteSkip,
			wantReason: encryption.RewriteReasonDifferentDEK,
		},
		{
			name:       "tombstone carries no value bytes",
			version:    version(encryption.MVCCVersionRef{KeyID: retiringDEK, Tombstone: true}),
			wantVerb:   encryption.RewriteSkip,
			wantReason: encryption.RewriteReasonTombstone,
		},
		{
			name:       "cleartext during a migration sweep",
			version:    version(encryption.MVCCVersionRef{Cleartext: true}),
			migrate:    true,
			wantVerb:   encryption.RewriteReencrypt,
			wantReason: encryption.RewriteReasonCleartext,
		},
		{
			name:       "cleartext outside a migration sweep",
			version:    version(encryption.MVCCVersionRef{Cleartext: true}),
			migrate:    false,
			wantVerb:   encryption.RewriteSkip,
			wantReason: encryption.RewriteReasonCleartext,
		},
		{
			name:       "retained anchor below the retention floor",
			version:    encryption.MVCCVersionRef{CommitTS: retainFloor - 1, KeyID: retiringDEK},
			wantVerb:   encryption.RewriteReencrypt,
			wantReason: encryption.RewriteReasonRetiringDEK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := encryption.ClassifyMVCCRewrite(tc.version, retiringDEK, tc.migrate)
			require.Equal(t, tc.wantVerb, got.Verdict)
			require.Equal(t, tc.wantReason, got.Reason)
		})
	}
}

// TestClassifyMVCCRewriteDoesNotEncryptCleartextDuringAPlainRotation
// keeps the two jobs separate. A routine rotation must not silently
// start encrypting data the operator never opted in to encrypting —
// that is the §7.1 migration, a different decision with a different
// blast radius.
func TestClassifyMVCCRewriteDoesNotEncryptCleartextDuringAPlainRotation(t *testing.T) {
	t.Parallel()

	v := version(encryption.MVCCVersionRef{Cleartext: true})
	require.Equal(t, encryption.RewriteSkip,
		encryption.ClassifyMVCCRewrite(v, retiringDEK, false).Verdict)
	require.Equal(t, encryption.RewriteReencrypt,
		encryption.ClassifyMVCCRewrite(v, retiringDEK, true).Verdict)
}

// TestClassifyMVCCRewriteRewritesHistoricalVersionsNotJustTheLatest is
// the property §5.4 opens with: every RETAINED version under the
// retiring DEK must be rewritten, however old, or unloading the DEK
// breaks snapshot reads.
func TestClassifyMVCCRewriteRewritesHistoricalVersionsNotJustTheLatest(t *testing.T) {
	t.Parallel()

	for _, ts := range []uint64{retainFloor + 1, retainFloor + 10, retainFloor + 10_000} {
		got := encryption.ClassifyMVCCRewrite(
			encryption.MVCCVersionRef{CommitTS: ts, KeyID: retiringDEK},
			retiringDEK, false)
		require.Equal(t, encryption.RewriteReencrypt, got.Verdict,
			"retained version at ts=%d must be rewritten", ts)
	}
}

// TestClassifyMVCCRewriteKeepsTheRetentionFloorAnchor pins the
// version an exact-floor read can still reach.
func TestClassifyMVCCRewriteKeepsTheRetentionFloorAnchor(t *testing.T) {
	t.Parallel()

	at := encryption.ClassifyMVCCRewrite(
		encryption.MVCCVersionRef{CommitTS: retainFloor, KeyID: retiringDEK},
		retiringDEK, false)
	require.Equal(t, encryption.RewriteReencrypt, at.Verdict)

	above := encryption.ClassifyMVCCRewrite(
		encryption.MVCCVersionRef{CommitTS: retainFloor + 1, KeyID: retiringDEK},
		retiringDEK, false)
	require.Equal(t, encryption.RewriteReencrypt, above.Verdict)
}

// ---------------------------------------------------------------------------
// Rate budget
// ---------------------------------------------------------------------------

func TestRewriteThrottleYieldsToHoldTheConfiguredRate(t *testing.T) {
	t.Parallel()

	start := time.Unix(1_700_000_000, 0)
	// 1 MiB/s.
	th := encryption.NewRewriteThrottle(1, start)

	// 2 MiB written instantly needs 2s of elapsed time.
	require.Equal(t, 2*time.Second, th.Record(2*1024*1024, start))

	// After 2s have actually passed, no further yield is owed.
	require.Zero(t, th.Record(0, start.Add(2*time.Second)))
}

// TestRewriteThrottleCarriesDebtAcrossBackToBackBatches pins the
// anti-burst property for successive batches at the same instant.
func TestRewriteThrottleCarriesDebtAcrossBackToBackBatches(t *testing.T) {
	t.Parallel()

	start := time.Unix(1_700_000_000, 0)
	// 1 MiB/s.
	th := encryption.NewRewriteThrottle(1, start)

	// First MiB at t=0: owes 1s under either rule.
	require.Equal(t, time.Second, th.Record(1024*1024, start))

	// Second MiB, still at t=0, extends the virtual deadline to t=2s.
	require.Equal(t, 2*time.Second, th.Record(1024*1024, start),
		"the debt must accumulate across batches")

	// Third MiB after 2s have passed: 3 MiB owes 3s, 2s elapsed.
	require.Equal(t, time.Second,
		th.Record(1024*1024, start.Add(2*time.Second)))
}

func TestRewriteThrottleDoesNotBankIdleTime(t *testing.T) {
	t.Parallel()
	start := time.Unix(1_700_000_000, 0)
	th := encryption.NewRewriteThrottle(1, start)
	require.Equal(t, time.Second, th.Record(1024*1024, start))

	afterIdle := start.Add(time.Minute)
	require.Equal(t, time.Second, th.Record(1024*1024, afterIdle))
	require.Equal(t, 2*time.Second, th.Record(1024*1024, afterIdle))
}

func TestRewriteThrottleDisabledAtNonPositiveRate(t *testing.T) {
	t.Parallel()

	start := time.Unix(1_700_000_000, 0)
	for _, rate := range []float64{0, -1} {
		th := encryption.NewRewriteThrottle(rate, start)
		require.Zero(t, th.Record(1<<30, start), "--rate=%v means run unthrottled", rate)
		require.Equal(t, int64(1<<30), th.Written())
	}
}

func TestRewriteThrottleClampsUnrepresentableDelay(t *testing.T) {
	t.Parallel()
	start := time.Unix(1_700_000_000, 0)
	th := encryption.NewRewriteThrottle(math.SmallestNonzeroFloat64, start)
	require.Equal(t, time.Duration(math.MaxInt64), th.Record(1, start))
	require.Equal(t, int64(1), th.Written())
}

func TestRewriteThrottleTracksCumulativeBytes(t *testing.T) {
	t.Parallel()

	start := time.Unix(1_700_000_000, 0)
	th := encryption.NewRewriteThrottle(1, start)
	th.Record(1024, start)
	th.Record(2048, start)
	require.Equal(t, int64(3072), th.Written())
}

func TestRewriteThrottleNilReceiverIsInert(t *testing.T) {
	t.Parallel()

	var th *encryption.RewriteThrottle
	require.Zero(t, th.Record(1<<20, time.Unix(0, 0)))
	require.Zero(t, th.Written())
}

func TestRewriteVerdictStringsAreStable(t *testing.T) {
	t.Parallel()

	require.Equal(t, "reencrypt", encryption.RewriteReencrypt.String())
	require.Equal(t, "skip", encryption.RewriteSkip.String())
}
