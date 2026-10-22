package kv

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTxnLockLegacyEncodingRemainsUnchanged(t *testing.T) {
	t.Parallel()
	lock := txnLock{StartTS: 10, TTLExpireAt: 30, PrimaryKey: []byte("p"), IsPrimaryKey: true}
	wire, err := hex.DecodeString("01000000000000000a000000000000001e01000000000000000170")
	require.NoError(t, err)
	require.Equal(t, wire, encodeTxnLock(lock))
	decoded, err := decodeTxnLock(wire)
	require.NoError(t, err)
	require.Equal(t, lock, decoded)
}

func TestTxnLockPreparedCommitEvidenceRoundTrip(t *testing.T) {
	t.Parallel()
	lock := txnLock{StartTS: 10, CommitTS: 20, TTLExpireAt: 30, PrimaryKey: []byte("p"), IsPrimaryKey: true}
	wire := encodeTxnLock(lock)
	decoded, err := decodeTxnLock(wire)
	require.NoError(t, err)
	require.Equal(t, lock, decoded)
	for _, malformed := range [][]byte{
		wire[:len(wire)-1], append(append([]byte(nil), wire...), 0),
		encodeTxnLock(txnLock{StartTS: 10, CommitTS: 10, PrimaryKey: []byte("p")}),
	} {
		_, err := decodeTxnLock(malformed)
		require.Error(t, err)
	}
}

func TestTxnMetaPreparedCommitEvidenceRequiresV3(t *testing.T) {
	t.Parallel()
	meta := TxnMeta{PrimaryKey: []byte("p"), LockTTLms: 30, PrepareCommitTS: 20, PrevCommitTS: 15}
	wire := EncodeTxnMeta(meta)
	require.Equal(t, txnMetaVersionV3, wire[0])
	decoded, err := DecodeTxnMeta(wire)
	require.NoError(t, err)
	require.Equal(t, meta, decoded)
	for _, malformed := range [][]byte{wire[:len(wire)-1], append(append([]byte(nil), wire...), 0)} {
		_, err := DecodeTxnMeta(malformed)
		require.Error(t, err)
	}
}
