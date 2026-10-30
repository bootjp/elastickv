package kv

import (
	"bytes"
	"context"
	"encoding/binary"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

var ErrTimestampBindingConflict = errors.New("timestamp binding conflicts with durable data-group state")
var ErrTimestampBindingInvalid = errors.New("invalid operation timestamp binding")

const timestampBindingVersion = byte(1)
const timestampBindingRecordSize = 59
const timestampBindingTimestampBytes = 8

// TimestampBinding is the data group's immutable association between one
// operation and its persistence timestamp. It does not attest TSO issuance.
type TimestampBinding struct {
	Timestamp      uint64
	OperationID    [16]byte
	ManifestDigest [32]byte
	IsTxn          bool
	Phase          pb.Phase
}

func (b TimestampBinding) valid() bool {
	if b.Timestamp == 0 || b.OperationID == ([16]byte{}) || b.ManifestDigest == ([32]byte{}) {
		return false
	}
	if !b.IsTxn {
		return b.Phase == pb.Phase_NONE
	}
	return b.Phase == pb.Phase_NONE || b.Phase == pb.Phase_PREPARE
}

func (b TimestampBinding) encoded() []byte {
	out := make([]byte, timestampBindingRecordSize)
	out[0] = timestampBindingVersion
	binary.BigEndian.PutUint64(out[1:9], b.Timestamp)
	copy(out[9:25], b.OperationID[:])
	copy(out[25:57], b.ManifestDigest[:])
	if b.IsTxn {
		out[57] = 1
	}
	if b.Phase == pb.Phase_PREPARE {
		out[58] = 1
	}
	return out
}

func (b TimestampBinding) keys() ([]byte, []byte) {
	operationKey := append([]byte("timestamp-binding/operation/"), b.OperationID[:]...)
	timestampKey := make([]byte, len("timestamp-binding/timestamp/")+timestampBindingTimestampBytes)
	copy(timestampKey, "timestamp-binding/timestamp/")
	binary.BigEndian.PutUint64(timestampKey[len("timestamp-binding/timestamp/"):], b.Timestamp)
	return operationKey, timestampKey
}

// StageTimestampBinding must run in the same indexed ApplyRaftEntry scope as
// the write or PREPARE. Both indexes are staged atomically with the data.
// An already applied binding must not cause the caller to apply data again.
func StageTimestampBinding(ctx context.Context, st store.RaftControlMetadataStore, binding TimestampBinding) (alreadyApplied bool, err error) {
	if st == nil || !binding.valid() {
		return false, errors.WithStack(ErrTimestampBindingInvalid)
	}
	operationKey, timestampKey := binding.keys()
	want := binding.encoded()
	already, err := existingTimestampBinding(ctx, st, operationKey, timestampKey, want)
	if err != nil || already {
		return already, err
	}
	if err := st.StageRaftControlMetadata(ctx, operationKey, want); err != nil {
		return false, errors.Wrap(err, "stage operation binding")
	}
	return false, errors.Wrap(st.StageRaftControlMetadata(ctx, timestampKey, want), "stage timestamp binding")
}

func existingTimestampBinding(ctx context.Context, st store.RaftControlMetadataStore, operationKey, timestampKey, want []byte) (bool, error) {
	operation, operationFound, err := readTimestampBinding(ctx, st, operationKey)
	if err != nil {
		return false, errors.Wrap(err, "read operation binding")
	}
	timestamp, timestampFound, err := readTimestampBinding(ctx, st, timestampKey)
	if err != nil {
		return false, errors.Wrap(err, "read timestamp binding")
	}
	if operationFound || timestampFound {
		if !operationFound || !timestampFound || !bytes.Equal(operation, want) || !bytes.Equal(timestamp, want) {
			return false, errors.WithStack(ErrTimestampBindingConflict)
		}
		return true, nil
	}
	return false, nil
}

func readTimestampBinding(ctx context.Context, st store.RaftControlMetadataStore, key []byte) ([]byte, bool, error) {
	value, err := st.ReadRaftControlMetadata(ctx, key)
	if errors.Is(err, store.ErrKeyNotFound) {
		return nil, false, nil
	}
	return value, err == nil, errors.WithStack(err)
}
