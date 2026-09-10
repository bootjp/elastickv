package encryption_test

import (
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
			wantReason: encryption.RewriteReasonAlreadyFresh,
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
			name:       "version below the retention floor",
			version:    encryption.MVCCVersionRef{CommitTS: retainFloor - 1, KeyID: retiringDEK},
			wantVerb:   encryption.RewriteSkip,
			wantReason: encryption.RewriteReasonUnretained,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := encryption.ClassifyMVCCRewrite(tc.version, retiringDEK, tc.migrate, retainFloor)
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
		encryption.ClassifyMVCCRewrite(v, retiringDEK, false, retainFloor).Verdict)
	require.Equal(t, encryption.RewriteReencrypt,
		encryption.ClassifyMVCCRewrite(v, retiringDEK, true, retainFloor).Verdict)
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
			retiringDEK, false, retainFloor)
		require.Equal(t, encryption.RewriteReencrypt, got.Verdict,
			"retained version at ts=%d must be rewritten", ts)
	}
}

// TestClassifyMVCCRewriteSkipsAtTheRetentionFloorBoundary pins the
// boundary against §5.4 item 4, which states retirement in terms of
// minRetainedTS: a version AT the floor is not reachable, so rewriting
// it is pure write amplification.
func TestClassifyMVCCRewriteSkipsAtTheRetentionFloorBoundary(t *testing.T) {
	t.Parallel()

	at := encryption.ClassifyMVCCRewrite(
		encryption.MVCCVersionRef{CommitTS: retainFloor, KeyID: retiringDEK},
		retiringDEK, false, retainFloor)
	require.Equal(t, encryption.RewriteSkip, at.Verdict)

	above := encryption.ClassifyMVCCRewrite(
		encryption.MVCCVersionRef{CommitTS: retainFloor + 1, KeyID: retiringDEK},
		retiringDEK, false, retainFloor)
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

// TestRewriteThrottleMeasuresCumulativeRateNotPerBatch pins the
// anti-burst property, and needs MULTIPLE batches to do it: with a
// single batch the cumulative and per-batch rules agree, so a one-batch
// test proves nothing. Successive batches at the same instant are what
// separate them — which is also the realistic burst shape, a job
// draining several batches back to back.
func TestRewriteThrottleMeasuresCumulativeRateNotPerBatch(t *testing.T) {
	t.Parallel()

	start := time.Unix(1_700_000_000, 0)
	// 1 MiB/s.
	th := encryption.NewRewriteThrottle(1, start)

	// First MiB at t=0: owes 1s under either rule.
	require.Equal(t, time.Second, th.Record(1024*1024, start))

	// Second MiB, still at t=0. Cumulatively the job has now written
	// 2 MiB and owes 2s against 0s elapsed. A per-batch rule would see
	// only this batch's 1 MiB and owe 1s, letting the job sustain
	// double the configured rate indefinitely.
	require.Equal(t, 2*time.Second, th.Record(1024*1024, start),
		"the debt must accumulate across batches, not reset with each one")

	// Third MiB after 2s have passed: 3 MiB owes 3s, 2s elapsed.
	require.Equal(t, time.Second,
		th.Record(1024*1024, start.Add(2*time.Second)))
}

func TestRewriteThrottleDisabledAtNonPositiveRate(t *testing.T) {
	t.Parallel()

	start := time.Unix(1_700_000_000, 0)
	for _, rate := range []float64{0, -1} {
		th := encryption.NewRewriteThrottle(rate, start)
		require.Zero(t, th.Record(1<<30, start), "--rate=%v means run unthrottled", rate)
	}
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
