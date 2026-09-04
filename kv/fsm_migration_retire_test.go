package kv

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type captureMigrationRetireStore struct {
	store.MVCCStore
	jobID        uint64
	appliedIndex uint64
}

func (s *captureMigrationRetireStore) RetireMigrationRaft(_ context.Context, jobID, appliedIndex uint64) error {
	s.jobID = jobID
	s.appliedIndex = appliedIndex
	return nil
}

func TestApplyMigrationRetireThreadsPendingApplyIndex(t *testing.T) {
	t.Parallel()

	capturing := &captureMigrationRetireStore{}
	fsm := &kvFSM{store: capturing, pendingApplyIdx: 1234}
	cmd, err := MarshalMigrationRetireCommand(9)
	require.NoError(t, err)

	applied := fsm.Apply(cmd)
	require.Nil(t, applied)
	require.Equal(t, uint64(9), capturing.jobID)
	require.Equal(t, uint64(1234), capturing.appliedIndex)
}

func TestMarshalMigrationRetireCommandRejectsZeroJobID(t *testing.T) {
	t.Parallel()

	_, err := MarshalMigrationRetireCommand(0)
	require.ErrorIs(t, err, ErrInvalidRequest)
}

func TestApplyMigrationRetireMalformedPayloadHalts(t *testing.T) {
	t.Parallel()

	fsm := &kvFSM{store: store.NewMVCCStore()}
	err := haltApplyOf(fsm.Apply([]byte{raftEncodeMigrationRetire, 0xff}))
	require.True(t, errors.Is(err, ErrMigrationRetireApply), "got %v", err)
}

func TestApplyMigrationRetireZeroJobIDReturnsOrdinaryError(t *testing.T) {
	t.Parallel()

	fsm := &kvFSM{store: store.NewMVCCStore()}
	payload := make([]byte, migrationRetireCommandPayloadLen)
	resp := fsm.Apply(append([]byte{raftEncodeMigrationRetire}, payload...))
	require.Nil(t, haltApplyOf(resp))
	err, ok := resp.(error)
	require.True(t, ok, "got %T: %v", resp, resp)
	require.ErrorIs(t, err, ErrInvalidRequest)
	require.False(t, errors.Is(err, ErrMigrationRetireApply))
}
