package kv

import (
	"bytes"
	"encoding/binary"
	"strconv"

	"github.com/cockroachdb/errors"
)

const (
	txnMetaVersion     byte = 1
	txnLockVersion     byte = 1
	txnIntentVersion   byte = 1
	txnCommitVersion   byte = 1
	txnRollbackVersion byte = 1
)

const txnLockFlagPrimary byte = 0x01

// TxnMeta is embedded into transactional raft log requests via a synthetic
// mutation (key prefix "!txn|meta|"). It is not persisted in the MVCC store.
type TxnMeta struct {
	PrimaryKey []byte
	LockTTLms  uint64
	CommitTS   uint64
}

func EncodeTxnMeta(m TxnMeta) []byte {
	var buf bytes.Buffer
	buf.WriteByte(txnMetaVersion)
	_ = binary.Write(&buf, binary.BigEndian, m.LockTTLms)
	_ = binary.Write(&buf, binary.BigEndian, m.CommitTS)
	primaryLen := uint64(len(m.PrimaryKey))
	_ = binary.Write(&buf, binary.BigEndian, primaryLen)
	if primaryLen > 0 {
		buf.Write(m.PrimaryKey)
	}
	return buf.Bytes()
}

func DecodeTxnMeta(b []byte) (TxnMeta, error) {
	if len(b) < 1 {
		return TxnMeta{}, errors.New("txn meta: empty")
	}
	if b[0] != txnMetaVersion {
		return TxnMeta{}, errors.WithStack(errors.Newf("txn meta: unsupported version %d", b[0]))
	}
	r := bytes.NewReader(b[1:])
	var ttl uint64
	var commitTS uint64
	var primaryLen uint64
	if err := binary.Read(r, binary.BigEndian, &ttl); err != nil {
		return TxnMeta{}, errors.WithStack(err)
	}
	if err := binary.Read(r, binary.BigEndian, &commitTS); err != nil {
		return TxnMeta{}, errors.WithStack(err)
	}
	if err := binary.Read(r, binary.BigEndian, &primaryLen); err != nil {
		return TxnMeta{}, errors.WithStack(err)
	}
	if primaryLen == 0 {
		return TxnMeta{PrimaryKey: nil, LockTTLms: ttl, CommitTS: commitTS}, nil
	}
	plen, err := u64ToInt(primaryLen)
	if err != nil {
		return TxnMeta{}, errors.WithStack(err)
	}
	if plen > r.Len() {
		return TxnMeta{}, errors.New("txn meta: primary key truncated")
	}
	pk := make([]byte, plen)
	if _, err := r.Read(pk); err != nil {
		return TxnMeta{}, errors.WithStack(err)
	}
	return TxnMeta{PrimaryKey: pk, LockTTLms: ttl, CommitTS: commitTS}, nil
}

type txnLock struct {
	StartTS      uint64
	TTLExpireAt  uint64
	PrimaryKey   []byte
	IsPrimaryKey bool
}

func encodeTxnLock(l txnLock) []byte {
	var buf bytes.Buffer
	buf.WriteByte(txnLockVersion)
	_ = binary.Write(&buf, binary.BigEndian, l.StartTS)
	_ = binary.Write(&buf, binary.BigEndian, l.TTLExpireAt)
	var flags byte
	if l.IsPrimaryKey {
		flags |= txnLockFlagPrimary
	}
	buf.WriteByte(flags)
	primaryLen := uint64(len(l.PrimaryKey))
	_ = binary.Write(&buf, binary.BigEndian, primaryLen)
	if primaryLen > 0 {
		buf.Write(l.PrimaryKey)
	}
	return buf.Bytes()
}

func decodeTxnLock(b []byte) (txnLock, error) {
	if len(b) < 1 {
		return txnLock{}, errors.New("txn lock: empty")
	}
	if b[0] != txnLockVersion {
		return txnLock{}, errors.WithStack(errors.Newf("txn lock: unsupported version %d", b[0]))
	}
	r := bytes.NewReader(b[1:])
	var startTS uint64
	var ttlExpireAt uint64
	if err := binary.Read(r, binary.BigEndian, &startTS); err != nil {
		return txnLock{}, errors.WithStack(err)
	}
	if err := binary.Read(r, binary.BigEndian, &ttlExpireAt); err != nil {
		return txnLock{}, errors.WithStack(err)
	}
	flags, err := r.ReadByte()
	if err != nil {
		return txnLock{}, errors.WithStack(err)
	}
	var primaryLen uint64
	if err := binary.Read(r, binary.BigEndian, &primaryLen); err != nil {
		return txnLock{}, errors.WithStack(err)
	}
	plen, err := u64ToInt(primaryLen)
	if err != nil {
		return txnLock{}, errors.WithStack(err)
	}
	if plen > r.Len() {
		return txnLock{}, errors.New("txn lock: primary key truncated")
	}
	primaryKey := make([]byte, plen)
	if _, err := r.Read(primaryKey); err != nil {
		return txnLock{}, errors.WithStack(err)
	}
	return txnLock{
		StartTS:      startTS,
		TTLExpireAt:  ttlExpireAt,
		PrimaryKey:   primaryKey,
		IsPrimaryKey: (flags & txnLockFlagPrimary) != 0,
	}, nil
}

type txnIntent struct {
	StartTS uint64
	Op      byte // 0=put, 1=del
	Value   []byte
}

const (
	txnIntentOpPut byte = 0
	txnIntentOpDel byte = 1
)

func encodeTxnIntent(i txnIntent) []byte {
	var buf bytes.Buffer
	buf.WriteByte(txnIntentVersion)
	_ = binary.Write(&buf, binary.BigEndian, i.StartTS)
	buf.WriteByte(i.Op)
	valLen := uint64(len(i.Value))
	_ = binary.Write(&buf, binary.BigEndian, valLen)
	if valLen > 0 {
		buf.Write(i.Value)
	}
	return buf.Bytes()
}

func decodeTxnIntent(b []byte) (txnIntent, error) {
	if len(b) < 1 {
		return txnIntent{}, errors.New("txn intent: empty")
	}
	if b[0] != txnIntentVersion {
		return txnIntent{}, errors.WithStack(errors.Newf("txn intent: unsupported version %d", b[0]))
	}
	r := bytes.NewReader(b[1:])
	var startTS uint64
	if err := binary.Read(r, binary.BigEndian, &startTS); err != nil {
		return txnIntent{}, errors.WithStack(err)
	}
	op, err := r.ReadByte()
	if err != nil {
		return txnIntent{}, errors.WithStack(err)
	}
	var valLen uint64
	if err := binary.Read(r, binary.BigEndian, &valLen); err != nil {
		return txnIntent{}, errors.WithStack(err)
	}
	vlen, err := u64ToInt(valLen)
	if err != nil {
		return txnIntent{}, errors.WithStack(err)
	}
	if vlen > r.Len() {
		return txnIntent{}, errors.New("txn intent: value truncated")
	}
	var val []byte
	if vlen > 0 {
		val = make([]byte, vlen)
		if _, err := r.Read(val); err != nil {
			return txnIntent{}, errors.WithStack(err)
		}
	}
	return txnIntent{StartTS: startTS, Op: op, Value: val}, nil
}

func encodeTxnCommitRecord(commitTS uint64) []byte {
	var buf bytes.Buffer
	buf.WriteByte(txnCommitVersion)
	_ = binary.Write(&buf, binary.BigEndian, commitTS)
	return buf.Bytes()
}

func decodeTxnCommitRecord(b []byte) (uint64, error) {
	if len(b) < 1 {
		return 0, errors.New("txn commit record: empty")
	}
	if b[0] != txnCommitVersion {
		return 0, errors.WithStack(errors.Newf("txn commit record: unsupported version %d", b[0]))
	}
	r := bytes.NewReader(b[1:])
	var commitTS uint64
	if err := binary.Read(r, binary.BigEndian, &commitTS); err != nil {
		return 0, errors.WithStack(err)
	}
	return commitTS, nil
}

func encodeTxnRollbackRecord() []byte {
	return []byte{txnRollbackVersion}
}

func u64ToInt(v uint64) (int, error) {
	if strconv.IntSize == 32 && v > uint64(^uint32(0)>>1) {
		return 0, errors.New("txn codec: length overflows int32")
	}
	if strconv.IntSize == 64 && v > (^uint64(0)>>1) {
		return 0, errors.New("txn codec: length overflows int64")
	}
	return int(v), nil
}
