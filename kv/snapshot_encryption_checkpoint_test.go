package kv

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

type checkpointApplier struct {
	fakeApplier
	checkpoint encryption.SnapshotCheckpoint
}

func (a *checkpointApplier) SnapshotCheckpoint() (encryption.SnapshotCheckpoint, error) {
	return a.checkpoint, nil
}

func TestEncryptionSnapshotCheckpointRoundTrip(t *testing.T) {
	cp := encryption.SnapshotCheckpoint{Index: 6, State: [32]byte{1, 2, 3}}
	for _, headerOnly := range []bool{false, true} {
		t.Run(map[bool]string{false: "full-physical-restore", true: "skip-gate-header-restore"}[headerOnly], func(t *testing.T) {
			source, ok := NewKvFSMWithHLC(store.NewMVCCStore(), NewHLC()).(*kvFSM)
			require.True(t, ok)
			source.encryption = &checkpointApplier{checkpoint: cp}
			require.NoError(t, source.captureEncryptionCheckpoint(6))
			snapshot, err := source.Snapshot()
			require.NoError(t, err)
			defer snapshot.Close()
			var payload bytes.Buffer
			_, err = snapshot.WriteTo(&payload)
			require.NoError(t, err)
			_, _, parsed, err := ReadSnapshotHeaderWithCheckpoint(bufio.NewReader(bytes.NewReader(payload.Bytes())))
			require.NoError(t, err)
			require.Equal(t, cp, parsed)
			target, ok := NewKvFSMWithHLC(store.NewMVCCStore(), NewHLC()).(*kvFSM)
			require.True(t, ok)
			if headerOnly {
				err = target.RestoreSnapshotHeader(bytes.NewReader(payload.Bytes()))
			} else {
				err = target.Restore(bytes.NewReader(payload.Bytes()))
			}
			require.NoError(t, err)
			require.Equal(t, cp, target.encryptionCheckpoint)
			// A rollback on disk must not overwrite the restored witness merely
			// because this process starts making newer snapshots.
			target.encryption = &checkpointApplier{checkpoint: encryption.SnapshotCheckpoint{Index: 5}}
			require.NoError(t, target.captureEncryptionCheckpoint(4))
			require.Equal(t, cp, target.encryptionCheckpoint)
			next, err := target.Snapshot()
			require.NoError(t, err)
			defer next.Close()
			var nextPayload bytes.Buffer
			_, err = next.WriteTo(&nextPayload)
			require.NoError(t, err)
			_, _, carried, err := ReadSnapshotHeaderWithCheckpoint(bufio.NewReader(&nextPayload))
			require.NoError(t, err)
			require.Equal(t, cp, carried)
		})
	}
}

func TestEncryptionSnapshotCheckpointRejectsTruncationWithoutMutation(t *testing.T) {
	fsm, ok := NewKvFSMWithHLC(store.NewMVCCStore(), NewHLC()).(*kvFSM)
	require.True(t, ok)
	fsm.encryptionCheckpoint = encryption.SnapshotCheckpoint{Index: 6, State: [32]byte{1}}
	snapshot, err := fsm.Snapshot()
	require.NoError(t, err)
	defer snapshot.Close()
	var payload bytes.Buffer
	_, err = snapshot.WriteTo(&payload)
	require.NoError(t, err)
	before := fsm.encryptionCheckpoint
	require.Error(t, fsm.RestoreSnapshotHeader(bytes.NewReader(payload.Bytes()[:snapshotCheckpointHeaderLen-1])))
	require.Equal(t, before, fsm.encryptionCheckpoint)
}

func TestEncryptionCheckpointAllowsTrailingV2Extensions(t *testing.T) {
	cp := encryption.SnapshotCheckpoint{Index: 6, State: [32]byte{1, 2, 3}}
	header := &kvFSMSnapshot{useV2: true, checkpoint: cp}
	encoded, size := header.encodeHeader()
	const extension = "FUTURE01"
	payload := append(bytes.Clone(encoded[:size]), extension...)
	binary.BigEndian.PutUint16(payload[8:10], uint16(hlcSnapshotV2MinPayload+encryptionCheckpointBytes+len(extension)))
	body := []byte("unchanged physical snapshot body")
	reader := bufio.NewReader(bytes.NewReader(append(payload, body...)))
	_, _, got, err := ReadSnapshotHeaderWithCheckpoint(reader)
	require.NoError(t, err)
	require.Equal(t, cp, got)
	remaining, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, body, remaining)
}

type durableCheckpointFixture struct {
	store.MVCCStore
	checkpoint encryption.SnapshotCheckpoint
	applied    uint64
}

func (s *durableCheckpointFixture) LoadEncryptionSnapshotCheckpoint() (encryption.SnapshotCheckpoint, bool, error) {
	return s.checkpoint, true, nil
}

func (s *durableCheckpointFixture) LastAppliedIndex() (uint64, bool, error) {
	return s.applied, true, nil
}

func TestEncryptionCheckpointSkipPreservesPostSnapshotRotation(t *testing.T) {
	old := encryption.SnapshotCheckpoint{Index: 6, State: [32]byte{6}}
	newer := encryption.SnapshotCheckpoint{Index: 890, State: [32]byte{8}}
	header := &kvFSMSnapshot{useV2: true, checkpoint: old}
	encoded, size := header.encodeHeader()
	for _, applied := range []uint64{900, 889} {
		t.Run(map[bool]string{true: "durable-tail-skipped", false: "unapplied-tail-must-replay"}[applied == 900], func(t *testing.T) {
			st := &durableCheckpointFixture{MVCCStore: store.NewMVCCStore(), checkpoint: newer, applied: applied}
			fsm, ok := NewKvFSMWithHLC(st, NewHLC()).(*kvFSM)
			require.True(t, ok)
			require.NoError(t, fsm.RestoreSnapshotHeader(bytes.NewReader(encoded[:size])))
			want := old
			if applied == 900 {
				want = newer
			}
			require.Equal(t, want, fsm.encryptionCheckpoint)
			next, err := fsm.Snapshot()
			if applied == 889 {
				require.Error(t, err, "the future durable record must replay before any new snapshot")
				return
			}
			require.NoError(t, err)
			defer next.Close()
			var out bytes.Buffer
			_, err = next.WriteTo(&out)
			require.NoError(t, err)
			_, _, cp, err := ReadSnapshotHeaderWithCheckpoint(bufio.NewReader(&out))
			require.NoError(t, err)
			require.Equal(t, want, cp)
		})
	}
}

func TestEncryptionCheckpointSkipRejectsChangedDurableRecord(t *testing.T) {
	old := encryption.SnapshotCheckpoint{Index: 6, State: [32]byte{6}}
	header := &kvFSMSnapshot{useV2: true, checkpoint: old}
	encoded, size := header.encodeHeader()
	st := &durableCheckpointFixture{MVCCStore: store.NewMVCCStore(), checkpoint: encryption.SnapshotCheckpoint{Index: 6, State: [32]byte{7}}, applied: 900}
	fsm, ok := NewKvFSMWithHLC(st, NewHLC()).(*kvFSM)
	require.True(t, ok)
	require.Error(t, fsm.RestoreSnapshotHeader(bytes.NewReader(encoded[:size])))
	require.Zero(t, fsm.encryptionCheckpoint)
}
