package filesystem

import (
	"bytes"
	"encoding/json"
	"io"
	"slices"

	"github.com/bootjp/elastickv/internal/fskeys"
	"github.com/cockroachdb/errors"
)

const (
	operationSessionVersion    = 1
	defaultOperationQuota      = 64 << 20
	defaultOperationCountQuota = 4096
	maxOperationAckGaps        = 1024
)

var (
	ErrOperationSessionNotFound = errors.New("filesystem: operation session not found")
	ErrOperationSessionClosed   = errors.New("filesystem: operation session closed")
	ErrOperationQuota           = errors.New("filesystem: operation journal quota exceeded")
)

type OperationSessionID struct {
	Filesystem [16]byte
	Session    [16]byte
}

func (id OperationSessionID) key() []byte {
	return fskeys.OperationSessionKey(id.Filesystem, id.Session)
}

type operationSession struct {
	Version       uint8              `json:"version"`
	ID            OperationSessionID `json:"id"`
	Owner         string             `json:"owner"`
	Closed        bool               `json:"closed"`
	RejectBefore  uint64             `json:"reject_before"`
	AckGaps       []uint64           `json:"ack_gaps,omitempty"`
	Pending       uint32             `json:"pending"`
	RetainedCount uint32             `json:"retained_count"`
	RetainedBytes uint64             `json:"retained_bytes"`
	MaxCount      uint32             `json:"max_count"`
	MaxBytes      uint64             `json:"max_bytes"`
}

func newOperationSession(id OperationSessionID, owner string) operationSession {
	return operationSession{
		Version:  operationSessionVersion,
		ID:       id,
		Owner:    owner,
		MaxCount: defaultOperationCountQuota,
		MaxBytes: defaultOperationQuota,
	}
}

func encodeOperationSession(session operationSession) ([]byte, error) {
	if err := validateOperationSession(session); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(session)
	return raw, errors.Wrap(err, "encode operation session")
}

func decodeOperationSession(raw []byte) (operationSession, error) {
	var session operationSession
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&session); err != nil {
		return operationSession{}, errors.Wrap(ErrOperationRecordCorrupt, err.Error())
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return operationSession{}, errors.WithStack(ErrOperationRecordCorrupt)
	}
	if err := validateOperationSession(session); err != nil {
		return operationSession{}, err
	}
	return session, nil
}

func validateOperationSession(session operationSession) error {
	if !validOperationSessionIdentity(session) || !validOperationSessionUsage(session) || !validOperationAckGaps(session) {
		return errors.WithStack(ErrOperationRecordCorrupt)
	}
	return nil
}

func validOperationSessionIdentity(session operationSession) bool {
	return session.Version == operationSessionVersion && session.ID.Filesystem != [16]byte{} &&
		session.ID.Session != [16]byte{} && session.Owner != ""
}

func validOperationSessionUsage(session operationSession) bool {
	return session.MaxCount != 0 && session.MaxBytes != 0 && session.Pending <= session.RetainedCount &&
		session.RetainedCount <= session.MaxCount && session.RetainedBytes <= session.MaxBytes
}

func validOperationAckGaps(session operationSession) bool {
	if len(session.AckGaps) > maxOperationAckGaps {
		return false
	}
	if !slices.IsSorted(session.AckGaps) {
		return false
	}
	for i, sequence := range session.AckGaps {
		if sequence <= session.RejectBefore || (i > 0 && sequence == session.AckGaps[i-1]) {
			return false
		}
	}
	return true
}
