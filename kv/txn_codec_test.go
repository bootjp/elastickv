package kv

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncodeTxnMetaOmitsCommitTSForPrepareMeta(t *testing.T) {
	t.Parallel()

	meta := TxnMeta{
		PrimaryKey: []byte("pk"),
		LockTTLms:  defaultTxnLockTTLms,
	}
	encoded := EncodeTxnMeta(meta)

	require.Equal(t, txnMetaVersion, encoded[0])
	require.Equal(t, txnMetaFlagLockTTL, encoded[1])
	require.Len(t, encoded, 1+1+uint64FieldSize+len(meta.PrimaryKey)+uint64FieldSize)

	decoded, err := DecodeTxnMeta(encoded)
	require.NoError(t, err)
	require.Equal(t, meta, decoded)
}

func TestEncodeTxnMetaOmitsLockTTLForCommitMeta(t *testing.T) {
	t.Parallel()

	meta := TxnMeta{
		PrimaryKey: []byte("pk"),
		CommitTS:   42,
	}
	encoded := EncodeTxnMeta(meta)

	require.Equal(t, txnMetaVersion, encoded[0])
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

	encoded := encodeTxnMetaV1ForTest(meta)

	decoded, err := DecodeTxnMeta(encoded)
	require.NoError(t, err)
	require.Equal(t, meta, decoded)
}

func encodeTxnMetaV1ForTest(m TxnMeta) []byte {
	size := 1 + uint64FieldSize + uint64FieldSize + uint64FieldSize + len(m.PrimaryKey)
	b := make([]byte, size)
	b[0] = txnMetaVersionV1
	binary.BigEndian.PutUint64(b[1:], m.LockTTLms)
	binary.BigEndian.PutUint64(b[9:], m.CommitTS)
	binary.BigEndian.PutUint64(b[17:], uint64(len(m.PrimaryKey)))
	copy(b[25:], m.PrimaryKey)
	return b
}
