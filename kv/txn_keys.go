package kv

import (
	"bytes"
	"encoding/binary"
)

const (
	// TxnKeyPrefix is the common prefix shared by all transaction internal
	// key namespaces. It is exported so that the store package can reference
	// it for compaction skipping without duplicating the literal.
	TxnKeyPrefix = "!txn|"

	txnLockPrefix     = TxnKeyPrefix + "lock|"
	txnIntentPrefix   = TxnKeyPrefix + "int|"
	txnCommitPrefix   = TxnKeyPrefix + "cmt|"
	txnRollbackPrefix = TxnKeyPrefix + "rb|"
	txnMetaPrefix     = TxnKeyPrefix + "meta|"
)

// TxnMetaPrefix is the key prefix used for transaction metadata mutations.
const TxnMetaPrefix = txnMetaPrefix

var (
	txnLockPrefixBytes     = []byte(txnLockPrefix)
	txnIntentPrefixBytes   = []byte(txnIntentPrefix)
	txnCommitPrefixBytes   = []byte(txnCommitPrefix)
	txnRollbackPrefixBytes = []byte(txnRollbackPrefix)
	txnMetaPrefixBytes     = []byte(txnMetaPrefix)
	txnCommonPrefix        = []byte(TxnKeyPrefix)
)

const txnStartTSSuffixLen = 8

func txnLockKey(userKey []byte) []byte {
	k := make([]byte, 0, len(txnLockPrefixBytes)+len(userKey))
	k = append(k, txnLockPrefixBytes...)
	k = append(k, userKey...)
	return k
}

func txnIntentKey(userKey []byte) []byte {
	k := make([]byte, 0, len(txnIntentPrefixBytes)+len(userKey))
	k = append(k, txnIntentPrefixBytes...)
	k = append(k, userKey...)
	return k
}

func txnCommitKey(primaryKey []byte, startTS uint64) []byte {
	k := make([]byte, 0, len(txnCommitPrefix)+len(primaryKey)+txnStartTSSuffixLen)
	k = append(k, txnCommitPrefix...)
	k = append(k, primaryKey...)
	var raw [txnStartTSSuffixLen]byte
	binary.BigEndian.PutUint64(raw[:], startTS)
	k = append(k, raw[:]...)
	return k
}

func txnRollbackKey(primaryKey []byte, startTS uint64) []byte {
	k := make([]byte, 0, len(txnRollbackPrefix)+len(primaryKey)+txnStartTSSuffixLen)
	k = append(k, txnRollbackPrefix...)
	k = append(k, primaryKey...)
	var raw [txnStartTSSuffixLen]byte
	binary.BigEndian.PutUint64(raw[:], startTS)
	k = append(k, raw[:]...)
	return k
}

func isTxnInternalKey(key []byte) bool {
	if !bytes.HasPrefix(key, txnCommonPrefix) {
		return false
	}
	return bytes.HasPrefix(key, txnLockPrefixBytes) ||
		bytes.HasPrefix(key, txnIntentPrefixBytes) ||
		bytes.HasPrefix(key, txnCommitPrefixBytes) ||
		bytes.HasPrefix(key, txnRollbackPrefixBytes) ||
		bytes.HasPrefix(key, txnMetaPrefixBytes)
}

func isTxnMetaKey(key []byte) bool {
	return bytes.HasPrefix(key, txnMetaPrefixBytes)
}

// txnRouteKey strips transaction-internal key prefixes to recover the embedded
// logical user key for shard routing.
func txnRouteKey(key []byte) ([]byte, bool) {
	switch {
	case bytes.HasPrefix(key, txnLockPrefixBytes):
		return key[len(txnLockPrefixBytes):], true
	case bytes.HasPrefix(key, txnIntentPrefixBytes):
		return key[len(txnIntentPrefixBytes):], true
	case bytes.HasPrefix(key, txnMetaPrefixBytes):
		return key[len(txnMetaPrefixBytes):], true
	case bytes.HasPrefix(key, txnCommitPrefixBytes):
		rest := key[len(txnCommitPrefixBytes):]
		if len(rest) < txnStartTSSuffixLen {
			return nil, false
		}
		return rest[:len(rest)-txnStartTSSuffixLen], true
	case bytes.HasPrefix(key, txnRollbackPrefixBytes):
		rest := key[len(txnRollbackPrefixBytes):]
		if len(rest) < txnStartTSSuffixLen {
			return nil, false
		}
		return rest[:len(rest)-txnStartTSSuffixLen], true
	default:
		return nil, false
	}
}

// ExtractTxnUserKey returns the logical user key embedded in a transaction-
// internal key such as !txn|lock|, !txn|cmt|, or !txn|meta|. It returns nil
// when the key does not use a transaction-internal namespace or is malformed.
func ExtractTxnUserKey(key []byte) []byte {
	userKey, ok := txnRouteKey(key)
	if !ok {
		return nil
	}
	return userKey
}
