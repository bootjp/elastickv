package s3keys_test

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"math"
	"sort"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/kv"
	"github.com/stretchr/testify/require"
)

func testSHA(seed string) [32]byte {
	return sha256.Sum256([]byte(seed))
}

func TestChunkRefRCKeyRoundTrip(t *testing.T) {
	t.Parallel()

	sha := testSHA("payload")
	key := s3keys.ChunkRefRCKey(sha)
	require.True(t, bytes.HasPrefix(key, []byte(s3keys.ChunkRefRCPrefix)))

	got, ok := s3keys.ParseChunkRefRCKey(key)
	require.True(t, ok)
	require.Equal(t, sha, got)
}

func TestParseChunkRefRCKeyRejectsMalformed(t *testing.T) {
	t.Parallel()

	sha := testSHA("payload")
	valid := s3keys.ChunkRefRCKey(sha)

	tests := []struct {
		name string
		key  []byte
	}{
		{"empty", nil},
		{"wrong prefix", []byte("!s3|chunkblob|" + string(valid[len(s3keys.ChunkRefRCPrefix):]))},
		{"truncated hex", valid[:len(valid)-2]},
		{"padded hex", append(append([]byte(nil), valid...), 'a', 'b')},
		{"non hex", append(append([]byte(nil), valid[:len(valid)-2]...), 'z', 'z')},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, ok := s3keys.ParseChunkRefRCKey(tc.key)
			require.False(t, ok)
		})
	}
}

// TestChunkRefRCValueFailsClosedOnMalformedValue pins that a corrupt
// counter is not read as zero. Zero means "no live reference", so
// defaulting to it would make a live blob look collectable.
func TestChunkRefRCValueFailsClosedOnMalformedValue(t *testing.T) {
	t.Parallel()

	got, ok := s3keys.DecodeChunkRefRC(s3keys.EncodeChunkRefRC(s3keys.ChunkRefRC{Count: 7}))
	require.True(t, ok)
	require.Equal(t, uint64(7), got.Count)
	require.False(t, got.Queued())

	for _, bad := range [][]byte{nil, {}, {0x01}, make([]byte, 8), make([]byte, 15), make([]byte, 17)} {
		_, ok := s3keys.DecodeChunkRefRC(bad)
		require.False(t, ok, "a malformed record must not decode to a zero count")
	}
}

// TestChunkRefRCCarriesTheQueueTimestamp pins the field that makes the
// §3.5 re-reference path implementable.
//
// When a SHA is referenced again after its count reached zero, the
// same txn must delete the existing GC-queue entry. That key embeds the
// eligibility timestamp, which the re-referencing txn has no other way
// to learn — so the count record has to carry it. Without it the txn
// cannot name the key it must delete, and a stale queue entry would
// point the sweeper at a blob that is live again.
func TestChunkRefRCCarriesTheQueueTimestamp(t *testing.T) {
	t.Parallel()

	sha := testSHA("payload")
	const queuedAt = uint64(1_700_000_000_000_000_000)

	// Count dropped to zero: the txn records when, and queues.
	zeroed := s3keys.ChunkRefRC{Count: 0, QueuedAtTS: queuedAt}
	decoded, ok := s3keys.DecodeChunkRefRC(s3keys.EncodeChunkRefRC(zeroed))
	require.True(t, ok)
	require.Zero(t, decoded.Count)
	require.True(t, decoded.Queued())

	// A re-referencing txn can now reconstruct the exact queue key it
	// has to delete.
	require.Equal(t,
		s3keys.ChunkBlobGCQueueKey(queuedAt, sha),
		s3keys.ChunkBlobGCQueueKey(decoded.QueuedAtTS, sha),
		"the recorded timestamp must reproduce the queue key exactly")

	// Re-referenced: count back above zero, no queue entry.
	live := s3keys.ChunkRefRC{Count: 1}
	decoded, ok = s3keys.DecodeChunkRefRC(s3keys.EncodeChunkRefRC(live))
	require.True(t, ok)
	require.Equal(t, uint64(1), decoded.Count)
	require.False(t, decoded.Queued(),
		"a live SHA must not claim a queue entry")
}

func TestChunkBlobGCQueueKeyRoundTrip(t *testing.T) {
	t.Parallel()

	sha := testSHA("payload")
	const ts = uint64(1_700_000_000_123_456_789)

	key := s3keys.ChunkBlobGCQueueKey(ts, sha)
	require.True(t, bytes.HasPrefix(key, []byte(s3keys.ChunkBlobGCQueuePrefix)))

	gotTS, gotSHA, ok := s3keys.ParseChunkBlobGCQueueKey(key)
	require.True(t, ok)
	require.Equal(t, ts, gotTS)
	require.Equal(t, sha, gotSHA)
}

// TestChunkBlobGCQueueSortsByEligibilityTime is the property the whole
// grace window rests on. A decimal or variable-width timestamp would
// order 9 after 10 and make the boundary scan return the wrong set.
func TestChunkBlobGCQueueSortsByEligibilityTime(t *testing.T) {
	t.Parallel()

	timestamps := []uint64{1, 9, 10, 99, 100, 1 << 32, math.MaxUint64 - 1, math.MaxUint64}
	keys := make([][]byte, 0, len(timestamps))
	for i, ts := range timestamps {
		keys = append(keys, s3keys.ChunkBlobGCQueueKey(ts, testSHA(fmt.Sprintf("blob-%d", i))))
	}

	shuffled := append([][]byte(nil), keys...)
	sort.Slice(shuffled, func(i, j int) bool { return bytes.Compare(shuffled[i], shuffled[j]) < 0 })

	for i, key := range shuffled {
		gotTS, _, ok := s3keys.ParseChunkBlobGCQueueKey(key)
		require.True(t, ok)
		require.Equal(t, timestamps[i], gotTS,
			"byte order must match eligibility-time order at position %d", i)
	}
}

// TestChunkBlobGCQueueScanEndIsExclusive pins the grace boundary.
// Callers pass now-grace; an entry stamped exactly at the boundary has
// not yet served the full window and must be excluded.
func TestChunkBlobGCQueueScanEndIsExclusive(t *testing.T) {
	t.Parallel()

	const boundary = uint64(1_000)
	sha := testSHA("payload")
	start := s3keys.ChunkBlobGCQueueScanStart()
	end := s3keys.ChunkBlobGCQueueScanEnd(boundary)

	inWindow := s3keys.ChunkBlobGCQueueKey(boundary-1, sha)
	atBoundary := s3keys.ChunkBlobGCQueueKey(boundary, sha)
	afterBoundary := s3keys.ChunkBlobGCQueueKey(boundary+1, sha)

	require.Negative(t, bytes.Compare(start, inWindow))
	require.Negative(t, bytes.Compare(inWindow, end),
		"an entry older than the boundary must fall inside the scan")
	require.GreaterOrEqual(t, bytes.Compare(atBoundary, end), 0,
		"an entry exactly at the boundary has not served the full grace period")
	require.Positive(t, bytes.Compare(afterBoundary, end))
}

// TestChunkBlobGCQueueScanStartCoversTheWholeQueue guards the lower
// bound: the earliest possible entry must sort at or after the scan
// start rather than below it.
func TestChunkBlobGCQueueScanStartCoversTheWholeQueue(t *testing.T) {
	t.Parallel()

	start := s3keys.ChunkBlobGCQueueScanStart()
	earliest := s3keys.ChunkBlobGCQueueKey(1, testSHA("earliest"))
	require.LessOrEqual(t, bytes.Compare(start, earliest), 0)
	require.Negative(t, bytes.Compare(earliest, s3keys.ChunkBlobGCQueueScanEnd(2)))
}

// TestHLCLogicalBitsMatchesKV pins the duplicated constant.
// internal/s3keys cannot import kv (kv -> distribution -> s3keys), so
// the shift width is mirrored locally; this external test closes the
// loop so the two cannot drift apart silently and leave the grace
// boundary computing against the wrong field width.
func TestHLCLogicalBitsMatchesKV(t *testing.T) {
	t.Parallel()

	// Derived rather than read directly: a commit timestamp whose
	// physical half is 1 ms must shift down to exactly 1.
	oneMs := uint64(1) << kv.HLCLogicalBits
	require.Equal(t, uint64(1),
		s3keys.ChunkBlobGCGraceBoundary(oneMs, 0)>>kv.HLCLogicalBits,
		"s3keys' mirrored HLC logical width must match kv.HLCLogicalBits")
}

// TestChunkBlobGCGraceBoundaryWorksInTheHLCDomain pins that the grace
// boundary is computed against HLC commit timestamps, not Unix
// nanoseconds. Subtracting a duration means subtracting milliseconds
// from the PHYSICAL half; treating the whole value as nanoseconds
// would be off by orders of magnitude and either sweep everything
// immediately or never sweep at all.
func TestChunkBlobGCGraceBoundaryWorksInTheHLCDomain(t *testing.T) {
	t.Parallel()

	nowMs := uint64(1_700_000_000_000)
	nowTS := nowMs << kv.HLCLogicalBits

	boundary := s3keys.ChunkBlobGCGraceBoundary(nowTS, time.Hour)
	require.Equal(t, (nowMs-3_600_000)<<kv.HLCLogicalBits, boundary)

	// An entry committed before the boundary falls inside the scan;
	// one committed after it does not.
	sha := testSHA("payload")
	inside := s3keys.ChunkBlobGCQueueKey(boundary-1, sha)
	outside := s3keys.ChunkBlobGCQueueKey(nowTS, sha)
	end := s3keys.ChunkBlobGCQueueScanEnd(boundary)
	require.Negative(t, bytes.Compare(inside, end))
	require.Positive(t, bytes.Compare(outside, end))
}

// TestChunkBlobGCGraceBoundaryClampsAtTheEpoch pins that an absurd
// grace period sweeps NOTHING rather than wrapping around to sweep
// everything.
func TestChunkBlobGCGraceBoundaryClampsAtTheEpoch(t *testing.T) {
	t.Parallel()

	nowTS := uint64(1_000) << kv.HLCLogicalBits
	require.Zero(t, s3keys.ChunkBlobGCGraceBoundary(nowTS, 999*time.Hour))
	require.Zero(t, s3keys.ChunkBlobGCGraceBoundary(0, time.Hour))
}

func TestParseChunkBlobGCQueueKeyRejectsMalformed(t *testing.T) {
	t.Parallel()

	sha := testSHA("payload")
	valid := s3keys.ChunkBlobGCQueueKey(42, sha)

	tests := []struct {
		name string
		key  []byte
	}{
		{"empty", nil},
		{"prefix only", []byte(s3keys.ChunkBlobGCQueuePrefix)},
		{"truncated", valid[:len(valid)-1]},
		{"padded", append(append([]byte(nil), valid...), 'a')},
		{"wrong prefix", append([]byte("!s3|chunkref-rc|"), valid[len(s3keys.ChunkBlobGCQueuePrefix):]...)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, _, ok := s3keys.ParseChunkBlobGCQueueKey(tc.key)
			require.False(t, ok)
		})
	}
}

// TestChunkBlobGCQueueSeparatorCannotBeForged pins that a SHA cannot
// contain the separator byte, so the timestamp/SHA split is
// unambiguous. Hex output is [0-9a-f] only.
func TestChunkBlobGCQueueSeparatorCannotBeForged(t *testing.T) {
	t.Parallel()

	key := s3keys.ChunkBlobGCQueueKey(1, testSHA("payload"))
	body := key[len(s3keys.ChunkBlobGCQueuePrefix):]
	// Exactly one separator, at the fixed offset after the timestamp.
	require.Equal(t, 1, bytes.Count(body, []byte{'|'}))
	require.Equal(t, byte('|'), body[8])
}

// TestGCKeyspacesDoNotCollideWithExistingPrefixes guards the reserved
// key namespace: a new prefix that another parser also accepts would
// let GC keys be read as chunkblobs or chunkrefs.
func TestGCKeyspacesDoNotCollideWithExistingPrefixes(t *testing.T) {
	t.Parallel()

	sha := testSHA("payload")
	rcKey := s3keys.ChunkRefRCKey(sha)
	queueKey := s3keys.ChunkBlobGCQueueKey(7, sha)

	_, ok := s3keys.ParseChunkBlobKey(rcKey)
	require.False(t, ok, "an RC key must not parse as a chunkblob key")
	_, ok = s3keys.ParseChunkBlobKey(queueKey)
	require.False(t, ok, "a queue key must not parse as a chunkblob key")

	_, _, _, _, _, _, ok = s3keys.ParseChunkRefKey(rcKey)
	require.False(t, ok, "an RC key must not parse as a chunkref key")

	_, ok = s3keys.ParseChunkRefRCKey(s3keys.ChunkBlobKey(sha))
	require.False(t, ok)
	_, _, ok = s3keys.ParseChunkBlobGCQueueKey(s3keys.ChunkBlobKey(sha))
	require.False(t, ok)
}

// TestGCKeyspacesSortOutsideTheChunkBlobRange pins the byte ordering
// between the new prefixes and the existing chunkblob keyspace.
//
// This is the failure mode that scanning-by-prefix invites: '-' (0x2D)
// sorts BELOW '|' (0x7C), so `!s3|chunkblob-gc-queue|` lands before
// `!s3|chunkblob|` rather than inside it. A range scan over the
// chunkblob keyspace must therefore not pick up queue entries, and a
// scan of the queue must not run into chunkblobs.
func TestGCKeyspacesSortOutsideTheChunkBlobRange(t *testing.T) {
	t.Parallel()

	sha := testSHA("payload")
	blobKey := s3keys.ChunkBlobKey(sha)
	queueKey := s3keys.ChunkBlobGCQueueKey(math.MaxUint64, sha)
	rcKey := s3keys.ChunkRefRCKey(sha)

	// The queue sorts strictly below every chunkblob key, even at the
	// maximum timestamp.
	require.Negative(t, bytes.Compare(queueKey, blobKey),
		"the GC queue must sort entirely below the chunkblob keyspace")

	// A chunkblob prefix scan cannot reach the queue.
	require.Negative(t, bytes.Compare(s3keys.ChunkBlobGCQueueScanEnd(math.MaxUint64),
		[]byte(s3keys.ChunkBlobPrefix)),
		"the queue scan's upper bound must stay below the chunkblob prefix")

	// And the RC keyspace is disjoint from both.
	require.NotEqual(t, 0, bytes.Compare(rcKey, blobKey))
	require.False(t, bytes.HasPrefix(rcKey, []byte(s3keys.ChunkBlobPrefix)))
	require.False(t, bytes.HasPrefix(blobKey, []byte(s3keys.ChunkRefRCPrefix)))
}
