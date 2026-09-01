package kv

import (
	"context"
	"encoding/binary"

	"github.com/cockroachdb/errors"
)

const migrationRetireCommandPayloadLen = 8

var ErrMigrationRetireApply = errors.New("migration retire: FSM apply failed; halting apply")

// MarshalMigrationRetireCommand encodes target-group migration metadata
// retirement as a Raft FSM command.
func MarshalMigrationRetireCommand(jobID uint64) ([]byte, error) {
	if jobID == 0 {
		return nil, errors.WithStack(ErrInvalidRequest)
	}
	payload := make([]byte, migrationRetireCommandPayloadLen)
	binary.BigEndian.PutUint64(payload, jobID)
	return prependByte(raftEncodeMigrationRetire, payload), nil
}

func (f *kvFSM) applyMigrationRetire(ctx context.Context, data []byte) any {
	if len(data) != migrationRetireCommandPayloadLen {
		return haltErr(errors.Wrapf(
			errors.Mark(ErrInvalidRequest, ErrMigrationRetireApply),
			"kv/fsm: decode migration retire: expected %d bytes, got %d",
			migrationRetireCommandPayloadLen,
			len(data),
		))
	}
	jobID := binary.BigEndian.Uint64(data)
	if jobID == 0 {
		return errors.Wrap(ErrInvalidRequest, "kv/fsm: apply migration retire")
	}
	if err := f.store.RetireMigrationRaft(ctx, jobID, f.pendingApplyIdx); err != nil {
		return haltErr(errors.Wrap(errors.Mark(err, ErrMigrationRetireApply), "kv/fsm: apply migration retire"))
	}
	return nil
}
