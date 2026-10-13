package kv

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncodeTxnMetaDefaultsToV1ForRollingUpgradeSafety(t *testing.T) {
	t.Parallel()

	meta := TxnMeta{
		PrimaryKey: []byte("pk"),
		LockTTLms:  defaultTxnLockTTLms,
	}
	encoded := EncodeTxnMeta(meta)

	require.Equal(t, txnMetaVersionV1, encoded[0])
	require.Len(t, encoded, 1+uint64FieldSize+uint64FieldSize+uint64FieldSize+len(meta.PrimaryKey))

	decoded, err := DecodeTxnMeta(encoded)
	require.NoError(t, err)
	require.Equal(t, meta, decoded)
}

func TestEncodeTxnMetaV2OmitsCommitTSForPrepareMeta(t *testing.T) {
	t.Parallel()

	meta := TxnMeta{
		PrimaryKey: []byte("pk"),
		LockTTLms:  defaultTxnLockTTLms,
	}
	encoded := encodeTxnMetaV2(meta)

	require.Equal(t, txnMetaVersionV2, encoded[0])
	require.Equal(t, txnMetaFlagLockTTL, encoded[1])
	require.Len(t, encoded, 1+1+uint64FieldSize+len(meta.PrimaryKey)+uint64FieldSize)

	decoded, err := DecodeTxnMeta(encoded)
	require.NoError(t, err)
	require.Equal(t, meta, decoded)
}

func TestEncodeTxnMetaV2OmitsLockTTLForCommitMeta(t *testing.T) {
	t.Parallel()

	meta := TxnMeta{
		PrimaryKey: []byte("pk"),
		CommitTS:   42,
	}
	encoded := encodeTxnMetaV2(meta)

	require.Equal(t, txnMetaVersionV2, encoded[0])
	require.Equal(t, txnMetaFlagCommitTS, encoded[1])
	require.Len(t, encoded, 1+1+uint64FieldSize+len(meta.PrimaryKey)+uint64FieldSize)

	decoded, err := DecodeTxnMeta(encoded)
	require.NoError(t, err)
	require.Equal(t, meta, decoded)
}

func TestDecodeTxnMetaV1Compatibility(t *testing.T) {
	t.Parallel()

	meta := TxnMeta{
		PrimaryKey: []byte("legacy"),
		LockTTLms:  9,
		CommitTS:   21,
	}

	encoded := encodeTxnMetaV1(meta)

	decoded, err := DecodeTxnMeta(encoded)
	require.NoError(t, err)
	require.Equal(t, meta, decoded)
}

func TestEncodeTxnMetaEmitsV2OnlyWhenPrevCommitTSSet(t *testing.T) {
	t.Parallel()

	// Without PrevCommitTS, even with a CommitTS set, the default wire format
	// stays V1 — preserving rolling-upgrade safety for every non-retry caller.
	v1 := EncodeTxnMeta(TxnMeta{PrimaryKey: []byte("pk"), CommitTS: 42})
	require.Equal(t, txnMetaVersionV1, v1[0])

	// With PrevCommitTS, EncodeTxnMeta upgrades to V2 so the field can ride.
	v2 := EncodeTxnMeta(TxnMeta{PrimaryKey: []byte("pk"), CommitTS: 42, PrevCommitTS: 41})
	require.Equal(t, txnMetaVersionV2, v2[0])
	require.Equal(t, txnMetaFlagCommitTS|txnMetaFlagPrevCommitTS, v2[1])
}

func TestTxnMetaPrevCommitTSRoundTrip(t *testing.T) {
	t.Parallel()

	meta := TxnMeta{
		PrimaryKey:   []byte("primary"),
		LockTTLms:    7,
		CommitTS:     200,
		PrevCommitTS: 100,
	}
	decoded, err := DecodeTxnMeta(EncodeTxnMeta(meta))
	require.NoError(t, err)
	require.Equal(t, meta, decoded)

	// PrevCommitTS alone (no lock TTL, no commit TS) must also survive.
	prevOnly := TxnMeta{PrimaryKey: []byte("pk"), PrevCommitTS: 99}
	decodedPrev, err := DecodeTxnMeta(EncodeTxnMeta(prevOnly))
	require.NoError(t, err)
	require.Equal(t, prevOnly, decodedPrev)
}

func TestDecodeTxnMetaV2RejectsUnknownFlags(t *testing.T) {
	t.Parallel()

	encoded := encodeTxnMetaV2(TxnMeta{PrimaryKey: []byte("pk")})
	encoded[1] |= 0x80

	_, err := DecodeTxnMeta(encoded)
	require.ErrorContains(t, err, "unsupported flags")
}

func TestDecodeTxnMetaV2RejectsTrailingBytes(t *testing.T) {
	t.Parallel()

	encoded := append(encodeTxnMetaV2(TxnMeta{PrimaryKey: []byte("pk")}), 0x01)

	_, err := DecodeTxnMeta(encoded)
	require.ErrorContains(t, err, "unexpected trailing bytes")
}

func TestTxnLockCommitTSRoundTrip(t *testing.T) {
	t.Parallel()

	lock := txnLock{
		StartTS:      11,
		TTLExpireAt:  99,
		PrimaryKey:   []byte("primary"),
		IsPrimaryKey: true,
		CommitTS:     101,
	}
	encoded := encodeTxnLock(lock)

	require.Equal(t, txnLockVersion, encoded[0])
	require.Equal(t, txnLockFlagPrimary|txnLockFlagCommitTS, encoded[17])
	require.Equal(t, lock.CommitTS, binary.BigEndian.Uint64(encoded[len(encoded)-uint64FieldSize:]))

	decoded, err := decodeTxnLock(encoded)
	require.NoError(t, err)
	require.Equal(t, lock, decoded)
}

func TestTxnLockLegacyRoundTripOmitsCommitTS(t *testing.T) {
	t.Parallel()

	lock := txnLock{
		StartTS:      11,
		TTLExpireAt:  99,
		PrimaryKey:   []byte("primary"),
		IsPrimaryKey: true,
	}
	encoded := encodeTxnLock(lock)

	require.Equal(t, txnLockFlagPrimary, encoded[17])
	require.Len(t, encoded, 1+uint64FieldSize+uint64FieldSize+1+uint64FieldSize+len(lock.PrimaryKey))

	decoded, err := decodeTxnLock(encoded)
	require.NoError(t, err)
	require.Equal(t, lock, decoded)
}

func TestDecodeTxnLockRejectsInvalidOptionalCommitTS(t *testing.T) {
	t.Parallel()

	encoded := encodeTxnLock(txnLock{StartTS: 11, PrimaryKey: []byte("primary")})
	encoded[17] |= txnLockFlagCommitTS

	_, err := decodeTxnLock(encoded)
	require.ErrorContains(t, err, "commit ts truncated")
}
