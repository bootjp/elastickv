package raftbootstrap

import "github.com/cockroachdb/errors"

const (
	activationReceiptMagic = "BGA1"
	activationReceiptSize  = len(activationReceiptMagic) + entryIdentitySize + WitnessSize + operationIDSize + 2*checksumSize + 1
)

type ActivationReceiptVerdict uint8

const (
	ActivationReceiptApplied ActivationReceiptVerdict = iota + 1
	ActivationReceiptRejectedStale
	ActivationReceiptAlreadyActive
)

// ActivationReceipt is the immutable terminal result in ReceiptKey(Entry), the
// same R/ slot used by guarded receipts. The full entry identity must be checked
// before replay, even when the slot already holds another receipt format.
// Witness is the command's inactive W0, never the resulting active witness.
// Applied binds the newly installed policy; AlreadyActive binds the existing
// policy without changing it. RejectedStale carries no policy result. No verdict
// carries a requested/returned watermark or proves historical correctness.
type ActivationReceipt struct {
	Entry          EntryIdentity
	Witness        Witness
	OperationID    OperationID
	ManifestDigest Digest
	PolicyDigest   Digest
	Verdict        ActivationReceiptVerdict
}

// EncodeActivationReceipt uses a fixed-length, domain-tagged format matching
// the existing receipt style. The tag is not authentication or a checksum.
// Callers own manifest/policy digest validation and atomic receipt/index writes.
func EncodeActivationReceipt(receipt ActivationReceipt) ([]byte, error) {
	if !validActivationReceipt(receipt) {
		return nil, errors.WithStack(ErrInvalid)
	}
	entry, err := EncodeEntryIdentity(receipt.Entry)
	if err != nil {
		return nil, err
	}
	out := make([]byte, activationReceiptSize)
	copy(out, activationReceiptMagic)
	offset := len(activationReceiptMagic)
	copy(out[offset:], entry)
	offset += entryIdentitySize
	encodeWitness(out[offset:offset+WitnessSize], receipt.Witness)
	offset += WitnessSize
	copy(out[offset:], receipt.OperationID[:])
	offset += operationIDSize
	copy(out[offset:], receipt.ManifestDigest[:])
	offset += checksumSize
	copy(out[offset:], receipt.PolicyDigest[:])
	offset += checksumSize
	out[offset] = byte(receipt.Verdict)
	return out, nil
}

func ParseActivationReceipt(data []byte) (ActivationReceipt, error) {
	if len(data) != activationReceiptSize || string(data[:len(activationReceiptMagic)]) != activationReceiptMagic {
		return ActivationReceipt{}, errors.WithStack(ErrInvalid)
	}
	offset := len(activationReceiptMagic)
	entry, err := ParseEntryIdentity(data[offset : offset+entryIdentitySize])
	if err != nil {
		return ActivationReceipt{}, err
	}
	offset += entryIdentitySize
	receipt := ActivationReceipt{Entry: entry, Witness: decodeWitness(data[offset : offset+WitnessSize])}
	offset += WitnessSize
	copy(receipt.OperationID[:], data[offset:offset+operationIDSize])
	offset += operationIDSize
	copy(receipt.ManifestDigest[:], data[offset:offset+checksumSize])
	offset += checksumSize
	copy(receipt.PolicyDigest[:], data[offset:offset+checksumSize])
	offset += checksumSize
	receipt.Verdict = ActivationReceiptVerdict(data[offset])
	if !validActivationReceipt(receipt) {
		return ActivationReceipt{}, errors.WithStack(ErrInvalid)
	}
	return receipt, nil
}

func validActivationReceipt(receipt ActivationReceipt) bool {
	if !validEntryIdentity(receipt.Entry) || !validActivationReceiptWitness(receipt) ||
		receipt.OperationID == (OperationID{}) || receipt.ManifestDigest == (Digest{}) {
		return false
	}
	switch receipt.Verdict {
	case ActivationReceiptApplied:
		return receipt.PolicyDigest != (Digest{}) && receipt.Entry.Index > receipt.Witness.ConfigurationIndex
	case ActivationReceiptAlreadyActive:
		return receipt.PolicyDigest != (Digest{})
	case ActivationReceiptRejectedStale:
		return receipt.PolicyDigest == (Digest{})
	default:
		return false
	}
}

func validActivationReceiptWitness(receipt ActivationReceipt) bool {
	w := receipt.Witness
	return w.Cluster == receipt.Entry.Cluster && w.Group == receipt.Entry.Group &&
		w.ActivationIndex == 0 && w.Generation == 0 && w.ConfigurationIndex != 0 && w.ConfigurationDigest != (Digest{})
}
