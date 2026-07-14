package store

import (
	"encoding/binary"
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
