package kv

import (
	"bytes"
	"encoding/binary"
)

const (
	txnLockPrefix     = "!txn|lock|"
	txnIntentPrefix   = "!txn|int|"
	txnCommitPrefix   = "!txn|cmt|"
	txnRollbackPrefix = "!txn|rb|"
	txnMetaPrefix     = "!txn|meta|"
)

const txnStartTSSuffixLen = 8

func txnLockKey(userKey []byte) []byte {
	return append([]byte(txnLockPrefix), userKey...)
}

func txnIntentKey(userKey []byte) []byte {
	return append([]byte(txnIntentPrefix), userKey...)
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
	return bytes.HasPrefix(key, []byte(txnLockPrefix)) ||
		bytes.HasPrefix(key, []byte(txnIntentPrefix)) ||
		bytes.HasPrefix(key, []byte(txnCommitPrefix)) ||
		bytes.HasPrefix(key, []byte(txnRollbackPrefix)) ||
		bytes.HasPrefix(key, []byte(txnMetaPrefix))
}

func isTxnMetaKey(key []byte) bool {
	return bytes.HasPrefix(key, []byte(txnMetaPrefix))
}

// txnRouteKey strips transaction-internal key prefixes to recover the embedded
// logical user key for shard routing.
func txnRouteKey(key []byte) ([]byte, bool) {
	switch {
	case bytes.HasPrefix(key, []byte(txnLockPrefix)):
		return key[len(txnLockPrefix):], true
	case bytes.HasPrefix(key, []byte(txnIntentPrefix)):
		return key[len(txnIntentPrefix):], true
	case bytes.HasPrefix(key, []byte(txnMetaPrefix)):
		return key[len(txnMetaPrefix):], true
	case bytes.HasPrefix(key, []byte(txnCommitPrefix)):
		rest := key[len(txnCommitPrefix):]
		if len(rest) < txnStartTSSuffixLen {
			return nil, false
		}
		return rest[:len(rest)-txnStartTSSuffixLen], true
	case bytes.HasPrefix(key, []byte(txnRollbackPrefix)):
		rest := key[len(txnRollbackPrefix):]
		if len(rest) < txnStartTSSuffixLen {
			return nil, false
		}
		return rest[:len(rest)-txnStartTSSuffixLen], true
	default:
		return nil, false
	}
}
