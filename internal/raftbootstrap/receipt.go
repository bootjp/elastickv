package raftbootstrap

import (
	"encoding/binary"

	"github.com/cockroachdb/errors"
)

const (
	entryIdentitySize   = 16 + 8 + 8 + 8 + 32
	receiptKeyPrefix    = "R/"
	receiptKeySize      = len(receiptKeyPrefix) + 16 + 8 + 8
	guardedReceiptMagic = "BGR1"
	guardedReceiptSize  = len(guardedReceiptMagic) + entryIdentitySize + 1 + 1 + operationIDSize + WitnessSize + 8 + 8
)

// EntryIdentity is the durable identity of one committed protected entry.
// Digest covers the entry type and canonical plaintext payload. Group zero is
// valid; index and term zero are not committed-entry identities.
type EntryIdentity struct {
	Cluster UUID
	Group   uint64
	Index   uint64
	Term    uint64
	Digest  Digest
}

type GuardedReceiptVerdict uint8

const (
	GuardedReceiptApplied GuardedReceiptVerdict = iota + 1
	GuardedReceiptRejectedStaleWitness
	GuardedReceiptRejectedReservation
)

// GuardedReceipt is the immutable terminal result for one guarded watermark
// or catalog entry. Rejected entries have no inner effects and no returned
// watermark, but still occupy their exact receipt slot.
type GuardedReceipt struct {
	Entry              EntryIdentity
	Kind               ControlKind
	Verdict            GuardedReceiptVerdict
	OperationID        OperationID
	Witness            Witness
	RequestedWatermark uint64
	ReturnedWatermark  uint64
}

func EncodeGuardedReceipt(receipt GuardedReceipt) ([]byte, error) {
	if !validGuardedReceipt(receipt) {
		return nil, errors.WithStack(ErrInvalid)
	}
	entry, err := EncodeEntryIdentity(receipt.Entry)
	if err != nil {
		return nil, err
	}
	out := make([]byte, guardedReceiptSize)
	copy(out, guardedReceiptMagic)
	offset := len(guardedReceiptMagic)
	copy(out[offset:], entry)
	offset += entryIdentitySize
	out[offset], out[offset+1] = byte(receipt.Kind), byte(receipt.Verdict)
	offset += 2
	copy(out[offset:], receipt.OperationID[:])
	offset += operationIDSize
	encodeWitness(out[offset:offset+WitnessSize], receipt.Witness)
	offset += WitnessSize
	binary.BigEndian.PutUint64(out[offset:], receipt.RequestedWatermark)
	offset += 8
	binary.BigEndian.PutUint64(out[offset:], receipt.ReturnedWatermark)
	return out, nil
}

func ParseGuardedReceipt(data []byte) (GuardedReceipt, error) {
	if len(data) != guardedReceiptSize || string(data[:len(guardedReceiptMagic)]) != guardedReceiptMagic {
		return GuardedReceipt{}, errors.WithStack(ErrInvalid)
	}
	offset := len(guardedReceiptMagic)
	entry, err := ParseEntryIdentity(data[offset : offset+entryIdentitySize])
	if err != nil {
		return GuardedReceipt{}, err
	}
	offset += entryIdentitySize
	receipt := GuardedReceipt{Entry: entry, Kind: ControlKind(data[offset]), Verdict: GuardedReceiptVerdict(data[offset+1])}
	offset += 2
	copy(receipt.OperationID[:], data[offset:offset+operationIDSize])
	offset += operationIDSize
	receipt.Witness = decodeWitness(data[offset : offset+WitnessSize])
	offset += WitnessSize
	receipt.RequestedWatermark = binary.BigEndian.Uint64(data[offset:])
	offset += 8
	receipt.ReturnedWatermark = binary.BigEndian.Uint64(data[offset:])
	if !validGuardedReceipt(receipt) {
		return GuardedReceipt{}, errors.WithStack(ErrInvalid)
	}
	return receipt, nil
}

func EncodeEntryIdentity(identity EntryIdentity) ([]byte, error) {
	if !validEntryIdentity(identity) {
		return nil, errors.WithStack(ErrInvalid)
	}
	out := make([]byte, entryIdentitySize)
	copy(out[:16], identity.Cluster[:])
	binary.BigEndian.PutUint64(out[16:24], identity.Group)
	binary.BigEndian.PutUint64(out[24:32], identity.Index)
	binary.BigEndian.PutUint64(out[32:40], identity.Term)
	copy(out[40:], identity.Digest[:])
	return out, nil
}

func ParseEntryIdentity(data []byte) (EntryIdentity, error) {
	if len(data) != entryIdentitySize {
		return EntryIdentity{}, errors.WithStack(ErrInvalid)
	}
	var identity EntryIdentity
	copy(identity.Cluster[:], data[:16])
	identity.Group = binary.BigEndian.Uint64(data[16:24])
	identity.Index = binary.BigEndian.Uint64(data[24:32])
	identity.Term = binary.BigEndian.Uint64(data[32:40])
	copy(identity.Digest[:], data[40:])
	if !validEntryIdentity(identity) {
		return EntryIdentity{}, errors.WithStack(ErrInvalid)
	}
	return identity, nil
}

// ReceiptKey returns the protected metadata key for an entry slot. Term and
// digest deliberately remain in the value identity: a replay at the same
// cluster/group/index must find the same key and compare the complete identity,
// so a conflicting committed entry is detected as corruption rather than a
// second receipt.
func ReceiptKey(identity EntryIdentity) ([]byte, error) {
	if !validEntryIdentity(identity) {
		return nil, errors.WithStack(ErrInvalid)
	}
	out := make([]byte, receiptKeySize)
	copy(out, receiptKeyPrefix)
	copy(out[len(receiptKeyPrefix):], identity.Cluster[:])
	offset := len(receiptKeyPrefix) + len(identity.Cluster)
	binary.BigEndian.PutUint64(out[offset:offset+8], identity.Group)
	binary.BigEndian.PutUint64(out[offset+8:], identity.Index)
	return out, nil
}

func validEntryIdentity(identity EntryIdentity) bool {
	return identity.Cluster != (UUID{}) && identity.Index != 0 && identity.Term != 0 && identity.Digest != (Digest{})
}

func validGuardedReceipt(receipt GuardedReceipt) bool {
	if !validEntryIdentity(receipt.Entry) || !guardedControlKind(receipt.Kind) || receipt.OperationID == (OperationID{}) ||
		!validActiveWitness(receipt.Witness) || receipt.Entry.Cluster != receipt.Witness.Cluster || receipt.Entry.Group != receipt.Witness.Group {
		return false
	}
	switch receipt.Verdict {
	case GuardedReceiptApplied:
		return validAppliedGuardedResult(receipt.Kind, receipt.RequestedWatermark, receipt.ReturnedWatermark)
	case GuardedReceiptRejectedStaleWitness, GuardedReceiptRejectedReservation:
		return validRejectedGuardedResult(receipt.Kind, receipt.RequestedWatermark, receipt.ReturnedWatermark)
	default:
		return false
	}
}

func validAppliedGuardedResult(kind ControlKind, requested, returned uint64) bool {
	if kind == ControlGuardedWatermark {
		return requested != 0 && requested != ^uint64(0) && returned >= requested && returned != ^uint64(0)
	}
	return requested == 0 && returned == 0
}

func validRejectedGuardedResult(kind ControlKind, requested, returned uint64) bool {
	if returned != 0 {
		return false
	}
	if kind == ControlGuardedWatermark {
		return requested != 0 && requested != ^uint64(0)
	}
	return requested == 0
}
