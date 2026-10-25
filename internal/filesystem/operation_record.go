package filesystem

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"io"

	"github.com/bootjp/elastickv/internal/fskeys"
	"github.com/cockroachdb/errors"
)

const (
	operationRecordVersion       = 2
	operationHeaderBytes         = 16
	operationCommitTSOffset      = 8
	maxOperationRecordBytes      = 8 << 20
	operationReservationOverhead = 4096
	maxOperationProofGroups      = 16
)

var (
	ErrOperationRecordCorrupt = errors.New("filesystem: corrupt operation record")
	ErrOperationIDReused      = errors.New("filesystem: operation ID reused with different intent")
	ErrOperationRetired       = errors.New("filesystem: operation identity retired")
	ErrOperationPending       = errors.New("filesystem: operation outcome pending")
	ErrOperationGeneration    = errors.New("filesystem: operation generation changed")
)

type OperationID struct {
	Filesystem [16]byte `json:"filesystem"`
	Session    [16]byte `json:"session"`
	Sequence   uint64   `json:"sequence"`
}

func (id OperationID) key() []byte {
	return fskeys.OperationKey(id.Filesystem, id.Session, id.Sequence)
}

type OperationState uint8

const (
	OperationPending OperationState = iota + 1
	OperationCommitted
	OperationRejected
	OperationCancelled
)

type operationRecord struct {
	ID          OperationID    `json:"id"`
	State       OperationState `json:"state"`
	Fingerprint [32]byte       `json:"fingerprint"`
	Intent      []byte         `json:"intent"`
	Generation  uint64         `json:"generation"`
	AttemptID   [16]byte       `json:"attempt_id"`
	Result      []byte         `json:"result,omitempty"`
	EffectKeys  [][]byte       `json:"effect_keys,omitempty"`
	ProofGroups []uint64       `json:"proof_groups,omitempty"`
	Reservation uint64         `json:"reservation"`
	ResultLimit uint32         `json:"result_limit"`
	EffectLimit uint32         `json:"effect_limit"`
	CommitTS    uint64         `json:"-"`
}

func newOperationRecord(id OperationID, intent []byte, resultLimit, effectLimit uint32) operationRecord {
	owned := append([]byte(nil), intent...)
	return operationRecord{
		ID:          id,
		State:       OperationPending,
		Fingerprint: sha256.Sum256(owned),
		Intent:      owned,
		Reservation: 2*(uint64(len(owned))+uint64(resultLimit)+uint64(effectLimit)) + operationHeaderBytes + operationReservationOverhead,
		ResultLimit: resultLimit,
		EffectLimit: effectLimit,
	}
}

func encodeOperationRecord(record operationRecord) ([]byte, error) {
	if err := validateOperationRecord(record); err != nil {
		return nil, err
	}
	body, err := json.Marshal(record)
	if err != nil {
		return nil, errors.Wrap(err, "encode operation record")
	}
	if len(body) > maxOperationRecordBytes-operationHeaderBytes {
		return nil, errors.WithStack(ErrOperationRecordCorrupt)
	}
	out := make([]byte, operationHeaderBytes+len(body))
	copy(out, "FSOP")
	out[4] = operationRecordVersion
	out[5] = byte(record.State)
	binary.BigEndian.PutUint64(out[operationCommitTSOffset:], record.CommitTS)
	copy(out[operationHeaderBytes:], body)
	return out, nil
}

func decodeOperationRecord(raw []byte) (operationRecord, error) {
	if !validOperationHeader(raw) {
		return operationRecord{}, errors.WithStack(ErrOperationRecordCorrupt)
	}
	var record operationRecord
	decoder := json.NewDecoder(bytes.NewReader(raw[operationHeaderBytes:]))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&record); err != nil {
		return operationRecord{}, errors.Wrap(ErrOperationRecordCorrupt, err.Error())
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return operationRecord{}, errors.WithStack(ErrOperationRecordCorrupt)
	}
	record.CommitTS = binary.BigEndian.Uint64(raw[operationCommitTSOffset:])
	if record.State != OperationPending && record.CommitTS == 0 {
		return operationRecord{}, errors.WithStack(ErrOperationRecordCorrupt)
	}
	if record.State != OperationState(raw[5]) || validateOperationRecord(record) != nil {
		return operationRecord{}, errors.WithStack(ErrOperationRecordCorrupt)
	}
	return record, nil
}

func validOperationHeader(raw []byte) bool {
	return len(raw) >= operationHeaderBytes && len(raw) <= maxOperationRecordBytes &&
		bytes.Equal(raw[:4], []byte("FSOP")) && raw[4] == operationRecordVersion &&
		raw[6] == 0 && raw[7] == 0
}

func validateOperationRecord(record operationRecord) error {
	if !validOperationCore(record) || !validOperationEffects(record) || !validOperationState(record) {
		return errors.WithStack(ErrOperationRecordCorrupt)
	}
	return nil
}

func validOperationCore(record operationRecord) bool {
	return record.ID.Sequence != 0 && record.ID.Filesystem != [16]byte{} && record.ID.Session != [16]byte{} &&
		record.Fingerprint == sha256.Sum256(record.Intent) && record.Reservation != 0 &&
		record.ResultLimit != 0 && uint64(record.ResultLimit) <= maxOperationRecordBytes &&
		uint64(len(record.Result)) <= uint64(record.ResultLimit) &&
		record.Reservation == 2*(uint64(len(record.Intent))+uint64(record.ResultLimit)+uint64(record.EffectLimit))+operationHeaderBytes+operationReservationOverhead
}

func validOperationEffects(record operationRecord) bool {
	if len(record.ProofGroups) > maxOperationProofGroups ||
		(len(record.EffectKeys) == 0) != (len(record.ProofGroups) == 0) {
		return false
	}
	for index, group := range record.ProofGroups {
		if index > 0 && group <= record.ProofGroups[index-1] {
			return false
		}
	}
	effectBytes := uint64(0)
	seen := make(map[string]struct{}, len(record.EffectKeys))
	for _, key := range record.EffectKeys {
		if len(key) == 0 {
			return false
		}
		if _, exists := seen[string(key)]; exists {
			return false
		}
		seen[string(key)] = struct{}{}
		effectBytes += uint64(len(key))
	}
	return effectBytes <= uint64(record.EffectLimit)
}

func validOperationState(record operationRecord) bool {
	switch record.State {
	case OperationPending:
		return record.CommitTS == 0 && len(record.Result) == 0 && len(record.EffectKeys) == 0 && len(record.ProofGroups) == 0
	case OperationCommitted:
		return record.Generation != 0 && record.AttemptID != [16]byte{}
	case OperationRejected, OperationCancelled:
		return len(record.EffectKeys) == 0 && len(record.ProofGroups) == 0
	default:
		return false
	}
}
