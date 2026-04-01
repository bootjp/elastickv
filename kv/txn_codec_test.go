package kv

import (
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
