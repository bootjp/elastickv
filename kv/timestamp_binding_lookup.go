package kv

import (
	"bytes"
	"context"
	"encoding/binary"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

// LookupTimestampBinding resolves an uncertain creation by its stable operation
// identity. The two durable indexes must agree before the result is trusted.
// The caller must first establish a leader read barrier before treating absence
// as an authoritative unknown-outcome result.
func LookupTimestampBinding(ctx context.Context, st store.RaftControlMetadataStore, operationID [16]byte) (TimestampBinding, bool, error) {
	var empty TimestampBinding
	if st == nil || operationID == ([16]byte{}) {
		return empty, false, errors.WithStack(ErrTimestampBindingInvalid)
	}
	operationKey, _ := (TimestampBinding{OperationID: operationID}).keys()
	value, found, err := readTimestampBinding(ctx, st, operationKey)
	if err != nil || !found {
		return empty, false, err
	}
	binding, err := decodeTimestampBinding(value)
	if err != nil {
		return empty, false, err
	}
	if binding.OperationID != operationID {
		return empty, false, errors.WithStack(ErrTimestampBindingConflict)
	}
	_, timestampKey := binding.keys()
	other, otherFound, err := readTimestampBinding(ctx, st, timestampKey)
	if err != nil {
		return empty, false, err
	}
	if !otherFound || !bytes.Equal(value, other) {
		return empty, false, errors.WithStack(ErrTimestampBindingConflict)
	}
	return binding, true, nil
}

func decodeTimestampBinding(value []byte) (TimestampBinding, error) {
	var binding TimestampBinding
	if len(value) != timestampBindingRecordSize || value[0] != timestampBindingVersion ||
		value[57] > 1 || value[58] > 1 {
		return binding, errors.WithStack(ErrTimestampBindingInvalid)
	}
	binding.Timestamp = binary.BigEndian.Uint64(value[1:9])
	copy(binding.OperationID[:], value[9:25])
	copy(binding.ManifestDigest[:], value[25:57])
	binding.IsTxn = value[57] == 1
	if value[58] == 1 {
		binding.Phase = pb.Phase_PREPARE
	}
	if !binding.valid() {
		return TimestampBinding{}, errors.WithStack(ErrTimestampBindingInvalid)
	}
	return binding, nil
}
