package raftbootstrap

import (
	"bytes"
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

func TestGuardedReceiptRoundTripAndGoldenSize(t *testing.T) {
	command := testGuardedCommand(ControlGuardedWatermark)
	receipt := GuardedReceipt{
		Entry: EntryIdentity{Cluster: command.Witness.Cluster, Group: command.Witness.Group, Index: 21, Term: 8, Digest: Digest{9}},
		Kind:  ControlGuardedWatermark, Verdict: GuardedReceiptApplied,
		OperationID: command.OperationID, Witness: command.Witness, RequestedWatermark: 99, ReturnedWatermark: 99,
	}
	encoded, err := EncodeGuardedReceipt(receipt)
	require.NoError(t, err)
	require.Len(t, encoded, guardedReceiptSize)
	require.Equal(t, guardedReceiptMagic, string(encoded[:len(guardedReceiptMagic)]))
	parsed, err := ParseGuardedReceipt(encoded)
	require.NoError(t, err)
	require.Equal(t, receipt, parsed)

	encoded[len(encoded)-1] ^= 1
	require.Equal(t, uint64(99), parsed.ReturnedWatermark, "parsed receipt must not alias input")
}

func TestGuardedReceiptRejectsMalformedAndInconsistentRecords(t *testing.T) {
	command := testGuardedCommand(ControlGuardedCatalog)
	valid := GuardedReceipt{
		Entry: EntryIdentity{Cluster: command.Witness.Cluster, Group: command.Witness.Group, Index: 21, Term: 8, Digest: Digest{9}},
		Kind:  ControlGuardedCatalog, Verdict: GuardedReceiptApplied,
		OperationID: command.OperationID, Witness: command.Witness,
	}
	encoded, err := EncodeGuardedReceipt(valid)
	require.NoError(t, err)
	for i := range encoded {
		_, err = ParseGuardedReceipt(encoded[:i])
		require.ErrorIs(t, err, ErrInvalid, "length %d", i)
	}
	badMagic := bytes.Clone(encoded)
	badMagic[0] ^= 1
	_, err = ParseGuardedReceipt(badMagic)
	require.ErrorIs(t, err, ErrInvalid)

	for _, mutate := range []func(*GuardedReceipt){
		func(r *GuardedReceipt) { r.Entry.Cluster[0]++ },
		func(r *GuardedReceipt) { r.Entry.Group++ },
		func(r *GuardedReceipt) { r.Entry.Index = 0 },
		func(r *GuardedReceipt) { r.Kind = ControlActivate },
		func(r *GuardedReceipt) { r.Verdict = 0 },
		func(r *GuardedReceipt) { r.OperationID = OperationID{} },
		func(r *GuardedReceipt) { r.Witness.Generation = 0 },
		func(r *GuardedReceipt) { r.ReturnedWatermark = 1 },
	} {
		candidate := valid
		mutate(&candidate)
		_, err = EncodeGuardedReceipt(candidate)
		require.ErrorIs(t, err, ErrInvalid)
	}
}

func TestGuardedReceiptRejectionsCannotCarryInnerResult(t *testing.T) {
	command := testGuardedCommand(ControlGuardedWatermark)
	for _, verdict := range []GuardedReceiptVerdict{GuardedReceiptRejectedStaleWitness, GuardedReceiptRejectedReservation} {
		receipt := GuardedReceipt{
			Entry: EntryIdentity{Cluster: command.Witness.Cluster, Group: command.Witness.Group, Index: 21, Term: 8, Digest: Digest{9}},
			Kind:  ControlGuardedWatermark, Verdict: verdict,
			OperationID: command.OperationID, Witness: command.Witness, RequestedWatermark: 99,
		}
		encoded, err := EncodeGuardedReceipt(receipt)
		require.NoError(t, err)
		_, err = ParseGuardedReceipt(encoded)
		require.NoError(t, err)

		receipt.ReturnedWatermark = 99
		_, err = EncodeGuardedReceipt(receipt)
		require.ErrorIs(t, err, ErrInvalid)
	}
}

func TestGuardedWatermarkReceiptMustCoverRequestedValue(t *testing.T) {
	command := testGuardedCommand(ControlGuardedWatermark)
	receipt := GuardedReceipt{
		Entry: EntryIdentity{Cluster: command.Witness.Cluster, Group: command.Witness.Group, Index: 21, Term: 8, Digest: Digest{9}},
		Kind:  ControlGuardedWatermark, Verdict: GuardedReceiptApplied,
		OperationID: command.OperationID, Witness: command.Witness,
		RequestedWatermark: 99, ReturnedWatermark: 98,
	}
	_, err := EncodeGuardedReceipt(receipt)
	require.ErrorIs(t, err, ErrInvalid)
	receipt.ReturnedWatermark = 100
	_, err = EncodeGuardedReceipt(receipt)
	require.NoError(t, err)
}
