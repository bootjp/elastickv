package store

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExtractListUserKeyFromFullWideColumnKeys(t *testing.T) {
	t.Parallel()

	userKey := []byte("list:user")
	require.Equal(t, userKey, ExtractListUserKeyFromDelta(ListMetaDeltaKey(userKey, 10, 2)))
	require.Equal(t, userKey, ExtractListUserKeyFromClaim(ListClaimKey(userKey, -3)))
}

func TestExtractListUserKeyRejectsMalformedFullKeyLength(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		prefix    string
		suffixLen int
		extract   func([]byte) []byte
	}{
		{
			name:      "delta",
			prefix:    ListMetaDeltaPrefix,
			suffixLen: deltaKeyTSSize + deltaKeySeqSize,
			extract:   ExtractListUserKeyFromDelta,
		},
		{
			name:      "claim",
			prefix:    ListClaimPrefix,
			suffixLen: sortableInt64Bytes,
			extract:   ExtractListUserKeyFromClaim,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			key := make([]byte, 0, len(tc.prefix)+wideColKeyLenSize+tc.suffixLen)
			key = append(key, tc.prefix...)
			lenOffset := len(key)
			key = append(key, 0xff, 0xff, 0xff, 0xff)
			key = append(key, make([]byte, tc.suffixLen)...)
			binary.BigEndian.PutUint32(key[lenOffset:lenOffset+wideColKeyLenSize], ^uint32(0))

			require.Nil(t, tc.extract(key))
		})
	}
}

func TestExtractListUserKeyFromScanPrefixes(t *testing.T) {
	t.Parallel()

	userKey := []byte("list:user")
	deltaPrefix := ListMetaDeltaScanPrefix(userKey)
	claimPrefix := ListClaimScanPrefix(userKey)

	for _, tc := range []struct {
		name    string
		key     []byte
		extract func([]byte) []byte
		want    []byte
	}{
		{
			name:    "delta valid",
			key:     deltaPrefix,
			extract: ExtractListUserKeyFromDeltaScanPrefix,
			want:    userKey,
		},
		{
			name:    "delta rejects different prefix",
			key:     claimPrefix,
			extract: ExtractListUserKeyFromDeltaScanPrefix,
		},
		{
			name:    "delta rejects truncated user key",
			key:     deltaPrefix[:len(deltaPrefix)-1],
			extract: ExtractListUserKeyFromDeltaScanPrefix,
		},
		{
			name:    "delta rejects trailing bytes",
			key:     append(append([]byte{}, deltaPrefix...), 0),
			extract: ExtractListUserKeyFromDeltaScanPrefix,
		},
		{
			name:    "claim valid",
			key:     claimPrefix,
			extract: ExtractListUserKeyFromClaimScanPrefix,
			want:    userKey,
		},
		{
			name:    "claim rejects different prefix",
			key:     deltaPrefix,
			extract: ExtractListUserKeyFromClaimScanPrefix,
		},
		{
			name:    "claim rejects truncated user key",
			key:     claimPrefix[:len(claimPrefix)-1],
			extract: ExtractListUserKeyFromClaimScanPrefix,
		},
		{
			name:    "claim rejects trailing bytes",
			key:     append(append([]byte{}, claimPrefix...), 0),
			extract: ExtractListUserKeyFromClaimScanPrefix,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tc.want, tc.extract(tc.key))
		})
	}
}

func TestExtractListUserKeyFromScanKeyBoundsOverflow(t *testing.T) {
	t.Parallel()

	var lenPrefix [wideColKeyLenSize]byte
	binary.BigEndian.PutUint32(lenPrefix[:], math.MaxUint32)

	for _, tc := range []struct {
		name    string
		key     []byte
		extract func([]byte) []byte
	}{
		{
			name:    "delta scan",
			key:     append(append([]byte(nil), []byte(ListMetaDeltaPrefix)...), lenPrefix[:]...),
			extract: ExtractListUserKeyFromDeltaScanKey,
		},
		{
			name:    "claim scan",
			key:     append(append([]byte(nil), []byte(ListClaimPrefix)...), lenPrefix[:]...),
			extract: ExtractListUserKeyFromClaimScanKey,
		},
		{
			name:    "full delta",
			key:     append(append(append([]byte(nil), []byte(ListMetaDeltaPrefix)...), lenPrefix[:]...), make([]byte, deltaKeyTSSize+deltaKeySeqSize)...),
			extract: ExtractListUserKeyFromDelta,
		},
		{
			name:    "full claim",
			key:     append(append(append([]byte(nil), []byte(ListClaimPrefix)...), lenPrefix[:]...), make([]byte, sortableInt64Bytes)...),
			extract: ExtractListUserKeyFromClaim,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.extract(tc.key); got != nil {
				t.Fatalf("overflow user-key length: want nil, got %q", got)
			}
		})
	}
}

func TestExtractListUserKeyFromScanKeyRoundTrip(t *testing.T) {
	t.Parallel()

	userKey := []byte("list-user")
	if got := ExtractListUserKeyFromDeltaScanKey(ListMetaDeltaScanPrefix(userKey)); !bytes.Equal(got, userKey) {
		t.Fatalf("delta scan round trip: want %q, got %q", userKey, got)
	}
	if got := ExtractListUserKeyFromClaimScanKey(ListClaimScanPrefix(userKey)); !bytes.Equal(got, userKey) {
		t.Fatalf("claim scan round trip: want %q, got %q", userKey, got)
	}
}

func TestExtractListUserKeyFromDeltaRequiresExactDeltaShape(t *testing.T) {
	t.Parallel()

	userKey := []byte("d|list")
	deltaKey := ListMetaDeltaKey(userKey, 42, 7)
	require.True(t, IsListMetaDeltaKey(deltaKey))
	require.Equal(t, userKey, ExtractListUserKeyFromDelta(deltaKey))

	baseMetaWithDeltaLookingUserKey := ListMetaKey(userKey)
	require.False(t, IsListMetaDeltaKey(baseMetaWithDeltaLookingUserKey))
	require.Nil(t, ExtractListUserKeyFromDelta(baseMetaWithDeltaLookingUserKey))

	trailingGarbage := append([]byte{}, deltaKey...)
	trailingGarbage = append(trailingGarbage, 0)
	require.False(t, IsListMetaDeltaKey(trailingGarbage))
	require.Nil(t, ExtractListUserKeyFromDelta(trailingGarbage))

	require.False(t, IsListMetaDeltaKey([]byte("not-a-delta-key")))
	require.Nil(t, ExtractListUserKeyFromDelta([]byte("not-a-delta-key")))
}

func TestListMetaDeltaPrefixDoesNotOverlapBaseMetaKeys(t *testing.T) {
	t.Parallel()

	fakeUserKey := []byte("fake-user")
	userKey := deltaLookingListMetaUserKey(fakeUserKey, 42, 7)
	baseMeta := ListMetaKey(userKey)
	deltaKey := ListMetaDeltaKey(userKey, 42, 7)

	require.True(t, IsListMetaKey(baseMeta))
	require.False(t, IsListMetaDeltaKey(baseMeta))
	require.Equal(t, userKey, ExtractListUserKey(baseMeta))
	require.Nil(t, ExtractListUserKeyFromDelta(baseMeta))

	require.False(t, IsListMetaKey(deltaKey))
	require.True(t, IsListMetaDeltaKey(deltaKey))
	require.Equal(t, userKey, ExtractListUserKeyFromDelta(deltaKey))
}

func TestLegacyListMetaDeltaHelpersScanOldPrefixWithoutReclassifyingNewDelta(t *testing.T) {
	t.Parallel()

	userKey := []byte("list")
	legacyPrefix := LegacyListMetaDeltaScanPrefix(userKey)
	legacyKey := append(append([]byte(nil), legacyPrefix...), make([]byte, deltaKeyTSSize+deltaKeySeqSize)...)

	require.Equal(t, userKey, ExtractLegacyListUserKeyFromDeltaScanPrefix(legacyPrefix))
	require.Equal(t, userKey, ExtractLegacyListUserKeyFromDelta(legacyKey))
	require.False(t, IsListMetaDeltaKey(legacyKey), "legacy prefix is ambiguous with base !lst|meta| keys")
	require.True(t, IsListMetaDeltaValue(MarshalListMetaDelta(ListMetaDelta{HeadDelta: 1, LenDelta: 2})))

	metaValue, err := MarshalListMeta(ListMeta{Head: 1, Tail: 2, Len: 1})
	require.NoError(t, err)
	require.False(t, IsListMetaDeltaValue(metaValue))
}

func deltaLookingListMetaUserKey(fakeUserKey []byte, commitTS uint64, seqInTxn uint32) []byte {
	buf := make([]byte, 0, len("d|")+4+len(fakeUserKey)+8+4)
	buf = append(buf, "d|"...)
	var keyLen [4]byte
	binary.BigEndian.PutUint32(keyLen[:], uint32(len(fakeUserKey))) //nolint:gosec // test data is small.
	buf = append(buf, keyLen[:]...)
	buf = append(buf, fakeUserKey...)
	var ts [8]byte
	binary.BigEndian.PutUint64(ts[:], commitTS)
	buf = append(buf, ts[:]...)
	var seq [4]byte
	binary.BigEndian.PutUint32(seq[:], seqInTxn)
	return append(buf, seq[:]...)
}
