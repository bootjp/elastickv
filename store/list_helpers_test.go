package store

import (
	"testing"

	"github.com/stretchr/testify/require"
)

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
