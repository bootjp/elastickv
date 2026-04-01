package kv

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"

	"github.com/cockroachdb/errors"
)

const (
	txnMetaVersion     byte = 1
	txnMetaVersionV1   byte = txnMetaVersion
	txnMetaVersionV2   byte = 2
	txnLockVersion     byte = 1
	txnIntentVersion   byte = 1
	txnCommitVersion   byte = 1
	txnRollbackVersion byte = 1
	txnReadChunkSize        = 4096
)

const txnLockFlagPrimary byte = 0x01

const (
	txnMetaFlagLockTTL  byte = 0x01
	txnMetaFlagCommitTS byte = 0x02
	txnMetaKnownFlags   byte = txnMetaFlagLockTTL | txnMetaFlagCommitTS
)

const txnMetaHeaderSize = 2

// uint64FieldSize is the byte size of a serialized uint64 field.
const uint64FieldSize = 8

// TxnMeta is embedded into transactional raft log requests via a synthetic
// mutation (key prefix "!txn|meta|"). It is not persisted in the MVCC store.
type TxnMeta struct {
	PrimaryKey []byte
	LockTTLms  uint64
	CommitTS   uint64
}

func EncodeTxnMeta(m TxnMeta) []byte {
	// Keep v1 as the default wire format until the cluster can guarantee that
	// every node understands v2 during rolling upgrades.
	return encodeTxnMetaV1(m)
}

func encodeTxnMetaV1(m TxnMeta) []byte {
	// version(1) + LockTTLms(8) + CommitTS(8) + primaryLen(8) + primaryKey
	size := 1 + uint64FieldSize + uint64FieldSize + uint64FieldSize + len(m.PrimaryKey)
	b := make([]byte, size)
	b[0] = txnMetaVersionV1
	binary.BigEndian.PutUint64(b[1:], m.LockTTLms)
	binary.BigEndian.PutUint64(b[9:], m.CommitTS)
	binary.BigEndian.PutUint64(b[17:], uint64(len(m.PrimaryKey)))
	copy(b[25:], m.PrimaryKey)
	return b
}

func encodeTxnMetaV2(m TxnMeta) []byte {
	// version(1) + flags(1) + primaryLen(8) + primaryKey + optional fields.
	flags := txnMetaFlags(m)
	size := txnMetaHeaderSize + uint64FieldSize + len(m.PrimaryKey)
	if flags&txnMetaFlagLockTTL != 0 {
		size += uint64FieldSize
	}
	if flags&txnMetaFlagCommitTS != 0 {
		size += uint64FieldSize
	}
	b := make([]byte, size)
	b[0] = txnMetaVersionV2
	b[1] = flags
	binary.BigEndian.PutUint64(b[txnMetaHeaderSize:], uint64(len(m.PrimaryKey)))
	offset := txnMetaHeaderSize + uint64FieldSize
	copy(b[offset:], m.PrimaryKey)
	offset += len(m.PrimaryKey)
	if flags&txnMetaFlagLockTTL != 0 {
		binary.BigEndian.PutUint64(b[offset:], m.LockTTLms)
		offset += uint64FieldSize
	}
	if flags&txnMetaFlagCommitTS != 0 {
		binary.BigEndian.PutUint64(b[offset:], m.CommitTS)
	}
	return b
}

func DecodeTxnMeta(b []byte) (TxnMeta, error) {
	if len(b) < 1 {
		return TxnMeta{}, errors.New("txn meta: empty")
	}
	switch b[0] {
	case txnMetaVersionV1:
		return decodeTxnMetaV1(b)
	case txnMetaVersionV2:
		return decodeTxnMetaV2(b)
	default:
		return TxnMeta{}, errors.WithStack(errors.Newf("txn meta: unsupported version %d", b[0]))
	}
}

func txnMetaFlags(m TxnMeta) byte {
	var flags byte
	if m.LockTTLms != 0 {
		flags |= txnMetaFlagLockTTL
	}
	if m.CommitTS != 0 {
		flags |= txnMetaFlagCommitTS
	}
	return flags
}

func decodeTxnMetaV1(b []byte) (TxnMeta, error) {
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
	pk, err := readTxnField(r, primaryLen, "txn meta: primary key truncated")
	if err != nil {
		return TxnMeta{}, err
	}
	return TxnMeta{PrimaryKey: pk, LockTTLms: ttl, CommitTS: commitTS}, nil
}

func decodeTxnMetaV2(b []byte) (TxnMeta, error) {
	if len(b) < txnMetaHeaderSize {
		return TxnMeta{}, errors.New("txn meta: truncated flags")
	}
	flags := b[1]
	if flags&^txnMetaKnownFlags != 0 {
		return TxnMeta{}, errors.WithStack(errors.Newf("txn meta: unsupported flags 0x%02x", flags))
	}
	r := bytes.NewReader(b[txnMetaHeaderSize:])
	primaryLen, err := readTxnUint64(r, "txn meta: primary key length truncated")
	if err != nil {
		return TxnMeta{}, err
	}
	pk, err := readTxnField(r, primaryLen, "txn meta: primary key truncated")
	if err != nil {
		return TxnMeta{}, err
	}

	meta := TxnMeta{PrimaryKey: pk}
	if flags&txnMetaFlagLockTTL != 0 {
		meta.LockTTLms, err = readTxnUint64(r, "txn meta: lock ttl truncated")
		if err != nil {
			return TxnMeta{}, err
		}
	}
	if flags&txnMetaFlagCommitTS != 0 {
		meta.CommitTS, err = readTxnUint64(r, "txn meta: commit ts truncated")
		if err != nil {
			return TxnMeta{}, err
		}
	}
	if r.Len() != 0 {
		return TxnMeta{}, errors.WithStack(errors.Newf("txn meta: unexpected trailing bytes %d", r.Len()))
	}
	return meta, nil
}

type txnLock struct {
	StartTS      uint64
	TTLExpireAt  uint64
	PrimaryKey   []byte
	IsPrimaryKey bool
}

func encodeTxnLock(l txnLock) []byte {
	// version(1) + StartTS(8) + TTLExpireAt(8) + flags(1) + primaryLen(8) + primaryKey
	size := 1 + uint64FieldSize + uint64FieldSize + 1 + uint64FieldSize + len(l.PrimaryKey)
	b := make([]byte, size)
	b[0] = txnLockVersion
	binary.BigEndian.PutUint64(b[1:], l.StartTS)
	binary.BigEndian.PutUint64(b[9:], l.TTLExpireAt)
	var flags byte
	if l.IsPrimaryKey {
		flags |= txnLockFlagPrimary
	}
	b[17] = flags
	binary.BigEndian.PutUint64(b[18:], uint64(len(l.PrimaryKey)))
	copy(b[26:], l.PrimaryKey)
	return b
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
	primaryKey, err := readTxnField(r, primaryLen, "txn lock: primary key truncated")
	if err != nil {
		return txnLock{}, err
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
	// version(1) + StartTS(8) + Op(1) + valLen(8) + value
	size := 1 + uint64FieldSize + 1 + uint64FieldSize + len(i.Value)
	b := make([]byte, size)
	b[0] = txnIntentVersion
	binary.BigEndian.PutUint64(b[1:], i.StartTS)
	b[9] = i.Op
	binary.BigEndian.PutUint64(b[10:], uint64(len(i.Value)))
	copy(b[18:], i.Value)
	return b
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
	val, err := readTxnField(r, valLen, "txn intent: value truncated")
	if err != nil {
		return txnIntent{}, err
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

func readTxnField(r *bytes.Reader, n uint64, truncatedMessage string) ([]byte, error) {
	// Fast-path zero-length fields.
	if n == 0 {
		return nil, nil
	}

	// Guard against excessively large fields before attempting to read them.
	if n > uint64(math.MaxInt) {
		return nil, errors.Newf("%s: field size %d overflows int", truncatedMessage, n) //nolint:wrapcheck // creating new error, nothing to wrap
	}

	b, err := readTxnSizedBytes(r, n)
	if err == nil {
		return b, nil
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, errors.New(truncatedMessage)
	}
	return nil, errors.WithStack(err)
}

func readTxnSizedBytes(r *bytes.Reader, n uint64) ([]byte, error) {
	if n == 0 {
		return nil, nil
	}

	// n has already been validated by readTxnField to be <= math.MaxInt,
	// so this conversion is safe.
	out := make([]byte, int(n)) //nolint:gosec // n validated as <= math.MaxInt by caller
	if _, err := io.ReadFull(r, out); err != nil {
		return nil, errors.WithStack(err)
	}
	return out, nil
}

func readTxnUint64(r *bytes.Reader, truncatedMessage string) (uint64, error) {
	var buf [uint64FieldSize]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return 0, errors.New(truncatedMessage)
		}
		return 0, errors.WithStack(err)
	}
	return binary.BigEndian.Uint64(buf[:]), nil
}
