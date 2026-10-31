package raftbootstrap

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEntryIdentityGoldenAndRoundTrip(t *testing.T) {
	identity := EntryIdentity{
		Cluster: UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		Group:   0x1112131415161718,
		Index:   0x2122232425262728,
		Term:    0x3132333435363738,
		Digest:  Digest{1, 2, 3},
	}
	encoded, err := EncodeEntryIdentity(identity)
	require.NoError(t, err)
	require.Equal(t,
		"0102030405060708090a0b0c0d0e0f10"+
			"1112131415161718"+
			"2122232425262728"+
			"3132333435363738"+
			"0102030000000000000000000000000000000000000000000000000000000000",
		hex.EncodeToString(encoded),
	)
	parsed, err := ParseEntryIdentity(encoded)
	require.NoError(t, err)
	require.Equal(t, identity, parsed)
}

func TestEntryIdentityRejectsInvalidAndTruncated(t *testing.T) {
	valid := EntryIdentity{Cluster: UUID{1}, Index: 1, Term: 1, Digest: Digest{1}}
	encoded, err := EncodeEntryIdentity(valid)
	require.NoError(t, err)
	for i := range encoded {
		_, err = ParseEntryIdentity(encoded[:i])
		require.ErrorIs(t, err, ErrInvalid, "length %d", i)
	}
	for _, mutate := range []func(*EntryIdentity){
		func(v *EntryIdentity) { v.Cluster = UUID{} },
		func(v *EntryIdentity) { v.Index = 0 },
		func(v *EntryIdentity) { v.Term = 0 },
		func(v *EntryIdentity) { v.Digest = Digest{} },
	} {
		invalid := valid
		mutate(&invalid)
		_, err = EncodeEntryIdentity(invalid)
		require.ErrorIs(t, err, ErrInvalid)
	}
}

func TestReceiptKeyCollidesOnlyOnEntrySlot(t *testing.T) {
	base := EntryIdentity{Cluster: UUID{1}, Group: 7, Index: 9, Term: 2, Digest: Digest{3}}
	key, err := ReceiptKey(base)
	require.NoError(t, err)
	require.Len(t, key, receiptKeySize)
	require.Equal(t, receiptKeyPrefix, string(key[:len(receiptKeyPrefix)]))

	conflict := base
	conflict.Term++
	conflict.Digest[0]++
	conflictKey, err := ReceiptKey(conflict)
	require.NoError(t, err)
	require.Equal(t, key, conflictKey, "same log slot must force full identity comparison")

	next := base
	next.Index++
	nextKey, err := ReceiptKey(next)
	require.NoError(t, err)
	require.NotEqual(t, key, nextKey)
}
