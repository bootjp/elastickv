package store

import (
	"encoding/binary"
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
