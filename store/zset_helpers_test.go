package store

import (
	"bytes"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncodeSortableFloat64RoundTrip(t *testing.T) {
	t.Parallel()
	cases := []float64{
		math.Inf(-1), -1e100, -1.0, -math.SmallestNonzeroFloat64,
		0.0, math.SmallestNonzeroFloat64, 1.0, 1e100, math.MaxFloat64, math.Inf(1),
	}
	for _, score := range cases {
		var dst [8]byte
		EncodeSortableFloat64(dst[:], score)
		got := DecodeSortableFloat64(dst[:])
		require.Equal(t, score, got, "round-trip failed for %v", score)
	}
}

func TestEncodeSortableFloat64NegativeZero(t *testing.T) {
	t.Parallel()
	var pos, neg [8]byte
	EncodeSortableFloat64(pos[:], 0.0)
	EncodeSortableFloat64(neg[:], math.Copysign(0, -1))
	require.Equal(t, pos, neg, "-0.0 and +0.0 must produce identical encodings")
}

func TestEncodeSortableFloat64Ordering(t *testing.T) {
	t.Parallel()
	scores := []float64{
		math.Inf(-1), -1e100, -1.0, -math.SmallestNonzeroFloat64,
		0.0, math.SmallestNonzeroFloat64, 1.0, 1e100, math.MaxFloat64, math.Inf(1),
	}
	prev := make([]byte, 8)
	EncodeSortableFloat64(prev, scores[0])
	for i := 1; i < len(scores); i++ {
		cur := make([]byte, 8)
		EncodeSortableFloat64(cur, scores[i])
		require.True(t, bytes.Compare(prev, cur) < 0,
			"encoding of %v should be less than %v", scores[i-1], scores[i])
		prev = cur
	}
}

func TestMarshalZSetMetaRoundTrip(t *testing.T) {
	t.Parallel()
	for _, length := range []int64{0, 1, 100, math.MaxInt64} {
		b, err := MarshalZSetMeta(ZSetMeta{Len: length})
		require.NoError(t, err)
		got, err := UnmarshalZSetMeta(b)
		require.NoError(t, err)
		require.Equal(t, length, got.Len)
	}
}

func TestMarshalZSetMetaNegativeLen(t *testing.T) {
	t.Parallel()
	_, err := MarshalZSetMeta(ZSetMeta{Len: -1})
	require.Error(t, err)
}

func TestUnmarshalZSetMetaInvalidLength(t *testing.T) {
	t.Parallel()
	_, err := UnmarshalZSetMeta([]byte{1, 2, 3})
	require.Error(t, err)
}

func TestPrefixEnd(t *testing.T) {
	t.Parallel()
	require.Equal(t, []byte{0x01}, PrefixEnd([]byte{0x00}))
	require.Equal(t, []byte{0x01, 0x03}, PrefixEnd([]byte{0x01, 0x02}))
	require.Equal(t, []byte{0x02}, PrefixEnd([]byte{0x01, 0xff}))
	require.Nil(t, PrefixEnd([]byte{0xff, 0xff}))
	require.Nil(t, PrefixEnd(nil))
}

func TestExtractZSetUserKey(t *testing.T) {
	t.Parallel()
	userKey := []byte("mykey")
	for _, key := range [][]byte{
		ZSetMetaKey(userKey),
		ZSetMemberKey(userKey, []byte("member")),
		ZSetScoreKey(userKey, 1.5, []byte("member")),
	} {
		got := ExtractZSetUserKey(key)
		require.Equal(t, userKey, got, "failed for key %q", key)
	}
}

func TestExtractZSetScoreAndMember(t *testing.T) {
	t.Parallel()
	userKey := []byte("mykey")
	member := []byte("alice")
	score := 3.14
	key := ZSetScoreKey(userKey, score, member)
	gotScore, gotMember := ExtractZSetScoreAndMember(key, len(userKey))
	require.Equal(t, score, gotScore)
	require.Equal(t, member, gotMember)
}

func TestIsZSetInternalKey(t *testing.T) {
	t.Parallel()
	userKey := []byte("k")
	require.True(t, IsZSetMetaKey(ZSetMetaKey(userKey)))
	require.True(t, IsZSetMemberKey(ZSetMemberKey(userKey, []byte("m"))))
	require.True(t, IsZSetScoreKey(ZSetScoreKey(userKey, 1.0, []byte("m"))))
	require.True(t, IsZSetInternalKey(ZSetMetaKey(userKey)))
	require.False(t, IsZSetInternalKey([]byte("plain")))
}
