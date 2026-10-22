package store

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLegacyListMetaDeltaKey(t *testing.T) {
	for _, seq := range []uint32{0, 1, 0xffffffff} {
		key := LegacyListMetaDeltaKey([]byte{'a', 0, 'b'}, 123, seq)
		require.Equal(t, []byte{'a', 0, 'b'}, ExtractLegacyListUserKeyFromDelta(key))
		require.Equal(t, uint64(123), binary.BigEndian.Uint64(key[len(key)-12:]))
		require.Equal(t, seq, binary.BigEndian.Uint32(key[len(key)-4:]))
	}
}
