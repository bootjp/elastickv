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
