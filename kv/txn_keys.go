package kv

import (
	"bytes"
	"encoding/binary"
)

const (
	// TxnKeyPrefix is the common prefix shared by all transaction internal
	// key namespaces. All per-namespace prefixes below are derived from it.
	// NOTE: store/store.go duplicates this literal as txnInternalKeyPrefix
	// because an import cycle prevents store from importing kv.
	TxnKeyPrefix = "!txn|"

	txnLockPrefix     = TxnKeyPrefix + "lock|"
	txnIntentPrefix   = TxnKeyPrefix + "int|"
	txnCommitPrefix   = TxnKeyPrefix + "cmt|"
	txnRollbackPrefix = TxnKeyPrefix + "rb|"
	txnSuccessPrefix  = TxnKeyPrefix + "ok|"
	txnMetaPrefix     = TxnKeyPrefix + "meta|"
)

// TxnMetaPrefix is the key prefix used for transaction metadata mutations.
const TxnMetaPrefix = txnMetaPrefix

var (
	txnLockPrefixBytes     = []byte(txnLockPrefix)
	txnIntentPrefixBytes   = []byte(txnIntentPrefix)
	txnCommitPrefixBytes   = []byte(txnCommitPrefix)
	txnRollbackPrefixBytes = []byte(txnRollbackPrefix)
	txnSuccessPrefixBytes  = []byte(txnSuccessPrefix)
	txnMetaPrefixBytes     = []byte(txnMetaPrefix)
	txnCommonPrefix        = []byte(TxnKeyPrefix)
)

const txnStartTSSuffixLen = 8
const txnSuccessMarkerVersion = byte(1)
const maxIntValue = int(^uint(0) >> 1)

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
		bytes.HasPrefix(key, txnSuccessPrefixBytes) ||
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
	case bytes.HasPrefix(key, txnSuccessPrefixBytes):
		return txnSuccessLockedKey(key)
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

// TxnSuccessMarkerKey builds the route-local transaction-success marker key
// used by the migration planner. Normal transaction traffic only writes this
// after the migration capability gate opens in a later PR.
func TxnSuccessMarkerKey(lockedKey []byte, startTS, commitTS uint64, primaryKey []byte) []byte {
	out := make([]byte, 0, len(txnSuccessPrefixBytes)+1+binary.MaxVarintLen64+len(lockedKey)+2*txnStartTSSuffixLen+binary.MaxVarintLen64+len(primaryKey))
	out = append(out, txnSuccessPrefixBytes...)
	out = append(out, txnSuccessMarkerVersion)
	out = binary.AppendUvarint(out, uint64(len(lockedKey)))
	out = append(out, lockedKey...)
	var raw [txnStartTSSuffixLen]byte
	binary.BigEndian.PutUint64(raw[:], startTS)
	out = append(out, raw[:]...)
	binary.BigEndian.PutUint64(raw[:], commitTS)
	out = append(out, raw[:]...)
	out = binary.AppendUvarint(out, uint64(len(primaryKey)))
	out = append(out, primaryKey...)
	return out
}

func txnSuccessLockedKey(key []byte) ([]byte, bool) {
	rest := key[len(txnSuccessPrefixBytes):]
	if len(rest) == 0 || rest[0] != txnSuccessMarkerVersion {
		return nil, false
	}
	rest = rest[1:]
	lockedLenRaw, n := binary.Uvarint(rest)
	lockedLen, ok := uvarintToInt(lockedLenRaw)
	if n <= 0 || !ok || lockedLen > len(rest)-n {
		return nil, false
	}
	lockedStart := n
	lockedEnd := lockedStart + lockedLen
	rest = rest[lockedEnd:]
	if len(rest) < 2*txnStartTSSuffixLen {
		return nil, false
	}
	rest = rest[2*txnStartTSSuffixLen:]
	primaryLenRaw, n := binary.Uvarint(rest)
	primaryLen, ok := uvarintToInt(primaryLenRaw)
	if n <= 0 || !ok || primaryLen != len(rest)-n {
		return nil, false
	}
	return key[len(txnSuccessPrefixBytes)+1+lockedStart : len(txnSuccessPrefixBytes)+1+lockedEnd], true
}

func uvarintToInt(v uint64) (int, bool) {
	if v > uint64(maxIntValue) {
		return 0, false
	}
	return int(v), true
}
