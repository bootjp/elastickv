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
