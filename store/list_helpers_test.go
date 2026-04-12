package store

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestListMetaDeltaMarshalRoundTrip(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		delta ListMetaDelta
	}{
		{"rpush", ListMetaDelta{HeadDelta: 0, LenDelta: 3}},
		{"lpush", ListMetaDelta{HeadDelta: -2, LenDelta: 2}},
		{"lpop", ListMetaDelta{HeadDelta: 1, LenDelta: -1}},
		{"rpop", ListMetaDelta{HeadDelta: 0, LenDelta: -1}},
		{"zero", ListMetaDelta{HeadDelta: 0, LenDelta: 0}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data := MarshalListMetaDelta(tc.delta)
			require.Len(t, data, listMetaDeltaBinarySize)

			got, err := UnmarshalListMetaDelta(data)
			require.NoError(t, err)
			require.Equal(t, tc.delta, got)
		})
	}
}

func TestUnmarshalListMetaDelta_InvalidLength(t *testing.T) {
	t.Parallel()

	_, err := UnmarshalListMetaDelta([]byte{1, 2, 3})
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid list meta delta length")
}

func TestListMetaDeltaKey_Structure(t *testing.T) {
	t.Parallel()

	userKey := []byte("mylist")
	key := ListMetaDeltaKey(userKey, 1000, 0)

	require.True(t, IsListMetaDeltaKey(key))
	require.False(t, IsListMetaKey(key))
	require.False(t, IsListItemKey(key))
	require.False(t, IsListClaimKey(key))

	extracted := ExtractListUserKeyFromDelta(key)
	require.Equal(t, userKey, extracted)
}

func TestListMetaDeltaKey_Ordering(t *testing.T) {
	t.Parallel()

	userKey := []byte("mylist")
	k1 := ListMetaDeltaKey(userKey, 100, 0)
	k2 := ListMetaDeltaKey(userKey, 200, 0)
	k3 := ListMetaDeltaKey(userKey, 200, 1)

	// Keys must sort in commitTS, seqInTxn order.
	require.True(t, string(k1) < string(k2))
	require.True(t, string(k2) < string(k3))
}

func TestListMetaDeltaScanPrefix(t *testing.T) {
	t.Parallel()

	userKey := []byte("mylist")
	prefix := ListMetaDeltaScanPrefix(userKey)

	// All delta keys for this user key should start with the prefix.
	k1 := ListMetaDeltaKey(userKey, 100, 0)
	k2 := ListMetaDeltaKey(userKey, 999, 5)
	require.True(t, len(k1) > len(prefix))
	require.Equal(t, prefix, k1[:len(prefix)])
	require.Equal(t, prefix, k2[:len(prefix)])

	// Delta keys for a different user key should NOT match.
	otherKey := ListMetaDeltaKey([]byte("other"), 100, 0)
	require.NotEqual(t, prefix, otherKey[:len(prefix)])
}

func TestListClaimKey_Structure(t *testing.T) {
	t.Parallel()

	userKey := []byte("mylist")
	key := ListClaimKey(userKey, 42)

	require.True(t, IsListClaimKey(key))
	require.False(t, IsListMetaKey(key))
	require.False(t, IsListItemKey(key))
	require.False(t, IsListMetaDeltaKey(key))

	extracted := ExtractListUserKeyFromClaim(key)
	require.Equal(t, userKey, extracted)
}

func TestListClaimKey_Ordering(t *testing.T) {
	t.Parallel()

	userKey := []byte("mylist")
	k1 := ListClaimKey(userKey, -10)
	k2 := ListClaimKey(userKey, 0)
	k3 := ListClaimKey(userKey, 10)

	// Claim keys must sort by sequence number (sortable int64).
	require.True(t, string(k1) < string(k2))
	require.True(t, string(k2) < string(k3))
}

func TestListClaimScanPrefix(t *testing.T) {
	t.Parallel()

	userKey := []byte("mylist")
	prefix := ListClaimScanPrefix(userKey)

	k1 := ListClaimKey(userKey, 0)
	k2 := ListClaimKey(userKey, 100)
	require.Equal(t, prefix, k1[:len(prefix)])
	require.Equal(t, prefix, k2[:len(prefix)])
}

func TestExtractListUserKeyFromDelta_EdgeCases(t *testing.T) {
	t.Parallel()

	require.Nil(t, ExtractListUserKeyFromDelta([]byte("short")))
	require.Nil(t, ExtractListUserKeyFromDelta([]byte(ListMetaDeltaPrefix)))

	// Binary user key with null bytes.
	userKey := []byte("a\x00b")
	key := ListMetaDeltaKey(userKey, 1, 0)
	require.Equal(t, userKey, ExtractListUserKeyFromDelta(key))
}

func TestExtractListUserKeyFromClaim_EdgeCases(t *testing.T) {
	t.Parallel()

	require.Nil(t, ExtractListUserKeyFromClaim([]byte("short")))
	require.Nil(t, ExtractListUserKeyFromClaim([]byte(ListClaimPrefix)))

	// Binary user key with null bytes.
	userKey := []byte("x\x00y")
	key := ListClaimKey(userKey, 5)
	require.Equal(t, userKey, ExtractListUserKeyFromClaim(key))
}

func TestIsListInternalKey(t *testing.T) {
	t.Parallel()

	require.True(t, IsListInternalKey(ListMetaKey([]byte("k"))))
	require.True(t, IsListInternalKey(ListItemKey([]byte("k"), 0)))
	require.True(t, IsListInternalKey(ListMetaDeltaKey([]byte("k"), 1, 0)))
	require.True(t, IsListInternalKey(ListClaimKey([]byte("k"), 0)))
	require.False(t, IsListInternalKey([]byte("!hs|meta|k")))
	require.False(t, IsListInternalKey([]byte("random")))
}

func TestPrefixScanEnd(t *testing.T) {
	t.Parallel()

	t.Run("normal", func(t *testing.T) {
		end := PrefixScanEnd([]byte("abc"))
		require.Equal(t, []byte("abd"), end)
	})

	t.Run("trailing_ff", func(t *testing.T) {
		end := PrefixScanEnd([]byte{0x01, 0xff})
		require.Equal(t, []byte{0x02}, end)
	})

	t.Run("all_ff", func(t *testing.T) {
		end := PrefixScanEnd([]byte{0xff, 0xff})
		require.Nil(t, end)
	})

	t.Run("empty", func(t *testing.T) {
		end := PrefixScanEnd(nil)
		require.Nil(t, end)
	})
}

func TestListMetaMarshalRoundTrip(t *testing.T) {
	t.Parallel()

	meta := ListMeta{Head: -5, Tail: 10, Len: 15}
	data, err := MarshalListMeta(meta)
	require.NoError(t, err)

	got, err := UnmarshalListMeta(data)
	require.NoError(t, err)
	require.Equal(t, meta, got)
}
