package kv

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// "!txn|foo" is an ordinary user key: it carries no reserved transaction
// sub-prefix, so raw writes must be accepted and scans must return it.
// Reserving the whole !txn| umbrella made such keys unwritable through
// handleRawRequest and invisible to ScanAt/ScanKeysAt while point reads still
// returned them -- a split view of data users had already stored.
func TestIsTxnInternalKeyReservesOnlyKnownFamilies(t *testing.T) {
	t.Parallel()

	for _, key := range [][]byte{
		[]byte(txnLockPrefix + "k"),
		[]byte(txnIntentPrefix + "k"),
		[]byte(txnCommitPrefix + "k"),
		[]byte(txnRollbackPrefix + "k"),
		[]byte(txnMetaPrefix + "k"),
		backupTimestampFloorKey,
	} {
		require.True(t, isTxnInternalKey(key), "reserved key %q", key)
	}

	for _, key := range [][]byte{
		[]byte("!txn|foo"),
		[]byte("!txn|"),
		[]byte("!txnfoo"),
		[]byte("user"),
	} {
		require.False(t, isTxnInternalKey(key), "user key %q", key)
	}
}
