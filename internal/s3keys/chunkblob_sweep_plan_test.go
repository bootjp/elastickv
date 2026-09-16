package s3keys_test

import (
	"testing"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/stretchr/testify/require"
)

const sweepEntryTS = uint64(1_700_000_000_000) << 16

// TestClassifyChunkBlobSweepCoversEveryRecordShape is the table the
// §3.5 correctness argument rests on. The design is explicit that an
// UNCONDITIONAL queue delete would let the sweeper local-delete a blob
// that is currently live — a correctness bug, not a space leak — so
// these verdicts are what keep that from happening.
func TestClassifyChunkBlobSweepCoversEveryRecordShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		rc         []byte
		found      bool
		wantVerb   s3keys.ChunkBlobSweepVerdict
		wantReason string
	}{
		{
			name:       "no record at all",
			found:      false,
			wantVerb:   s3keys.SweepReclaim,
			wantReason: s3keys.SweepReasonUnreferenced,
		},
		{
			name:       "zero count queued at this entry",
			rc:         s3keys.EncodeChunkRefRC(s3keys.ChunkRefRC{Count: 0, QueuedAtTS: sweepEntryTS}),
			found:      true,
			wantVerb:   s3keys.SweepReclaim,
			wantReason: s3keys.SweepReasonUnreferenced,
		},
		{
			name:       "referenced again",
			rc:         s3keys.EncodeChunkRefRC(s3keys.ChunkRefRC{Count: 1}),
			found:      true,
			wantVerb:   s3keys.SweepDropQueueEntryOnly,
			wantReason: s3keys.SweepReasonReferencedAgain,
		},
		{
			name: "superseded by a newer queueing",
			rc: s3keys.EncodeChunkRefRC(s3keys.ChunkRefRC{
				Count: 0, QueuedAtTS: sweepEntryTS + (1 << 16),
			}),
			found:      true,
			wantVerb:   s3keys.SweepDropQueueEntryOnly,
			wantReason: s3keys.SweepReasonSupersededEntry,
		},
		{
			name:       "zero count claiming no queue entry",
			rc:         s3keys.EncodeChunkRefRC(s3keys.ChunkRefRC{Count: 0}),
			found:      true,
			wantVerb:   s3keys.SweepSkip,
			wantReason: s3keys.SweepReasonRecordNotQueued,
		},
		{
			name:       "malformed record",
			rc:         []byte{0x01, 0x02},
			found:      true,
			wantVerb:   s3keys.SweepSkip,
			wantReason: s3keys.SweepReasonRecordUnreadable,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := s3keys.ClassifyChunkBlobSweep(sweepEntryTS, tc.rc, tc.found)
			require.Equal(t, tc.wantVerb, got.Verdict, "verdict for %s", tc.name)
			require.Equal(t, tc.wantReason, got.Reason)
		})
	}
}

// TestClassifyChunkBlobSweepNeverReclaimsALiveBlob is the single
// property that matters most: no record shape carrying a live
// reference may produce a verdict that deletes the blob.
func TestClassifyChunkBlobSweepNeverReclaimsALiveBlob(t *testing.T) {
	t.Parallel()

	for _, count := range []uint64{1, 2, 7, 1 << 20} {
		for _, queuedAt := range []uint64{0, sweepEntryTS, sweepEntryTS + (1 << 16)} {
			rc := s3keys.EncodeChunkRefRC(s3keys.ChunkRefRC{Count: count, QueuedAtTS: queuedAt})
			got := s3keys.ClassifyChunkBlobSweep(sweepEntryTS, rc, true)
			require.NotEqual(t, s3keys.SweepReclaim, got.Verdict,
				"count=%d queuedAt=%d must never reclaim", count, queuedAt)
		}
	}
}

// TestClassifyChunkBlobSweepNeverReclaimsOnCorruption pins that a
// value the decoder rejects is never read as "count zero". Treating
// corruption as zero would delete live data on the strength of a bad
// byte.
func TestClassifyChunkBlobSweepNeverReclaimsOnCorruption(t *testing.T) {
	t.Parallel()

	for _, bad := range [][]byte{{}, {0x00}, make([]byte, 8), make([]byte, 15), make([]byte, 17)} {
		got := s3keys.ClassifyChunkBlobSweep(sweepEntryTS, bad, true)
		require.Equal(t, s3keys.SweepSkip, got.Verdict)
		require.Equal(t, s3keys.SweepReasonRecordUnreadable, got.Reason)
	}
}

// TestChunkBlobSweepVerdictStringsAreStable guards the metric label:
// these strings are a closed set a sweeper can emit directly.
func TestChunkBlobSweepVerdictStringsAreStable(t *testing.T) {
	t.Parallel()

	require.Equal(t, "reclaim", s3keys.SweepReclaim.String())
	require.Equal(t, "drop_queue_entry_only", s3keys.SweepDropQueueEntryOnly.String())
	require.Equal(t, "skip", s3keys.SweepSkip.String())
	require.Equal(t, "unknown", s3keys.ChunkBlobSweepVerdict(99).String())
}
