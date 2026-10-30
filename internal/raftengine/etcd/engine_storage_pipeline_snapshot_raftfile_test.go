package etcd

import (
	"bytes"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap/snappb"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func TestPipelineSnapshotRaftFilePreservesExactTuple(t *testing.T) {
	state := &raftpb.ConfState{Voters: []uint64{1, 2}, VotersOutgoing: []uint64{1, 2, 3},
		Learners: []uint64{4}, LearnersNext: []uint64{3}, AutoLeave: proto.Bool(true)}
	unknown := protowire.AppendTag(nil, 500, protowire.StartGroupType)
	unknown = protowire.AppendTag(unknown, 501, protowire.VarintType)
	unknown = protowire.AppendVarint(unknown, 65535)
	unknown = protowire.AppendTag(unknown, 500, protowire.EndGroupType)
	state.ProtoReflect().SetUnknown(unknown)
	metadata := &raftpb.SnapshotMetadata{Index: uint64Ptr(9), Term: uint64Ptr(3), ConfState: state}
	metadata.ProtoReflect().SetUnknown(unknown)
	data := bytes.Repeat([]byte("exact-old-inline-prefix"), 4096)
	metaBytes, err := proto.Marshal(metadata)
	require.NoError(t, err)
	// Repeated singular message fields MERGE; data fields use the last value.
	inner := protowire.AppendTag(nil, 1, protowire.BytesType)
	inner = protowire.AppendBytes(inner, []byte("superseded"))
	inner = protowire.AppendTag(inner, 2, protowire.BytesType)
	inner = protowire.AppendBytes(inner, metaBytes)
	inner = protowire.AppendTag(inner, 2, protowire.BytesType)
	inner = protowire.AppendBytes(inner, nil)
	inner = append(inner, unknown...)
	inner = protowire.AppendTag(inner, 1, protowire.BytesType)
	inner = protowire.AppendBytes(inner, data)
	path := writePipelineRaftEnvelope(t, inner, crc32.Checksum(inner, crc32.MakeTable(crc32.Castagnoli)))
	var output bytes.Buffer
	count, err := (&pipelineRaftFileSnapshot{path: path, expected: metadata}).WriteTo(&output)
	require.NoError(t, err)
	require.Equal(t, int64(len(data)), count)
	require.Equal(t, data, output.Bytes())
	wrong := proto.CloneOf(metadata)
	wrong.ConfState.AutoLeave = proto.Bool(false)
	output.Reset()
	_, err = (&pipelineRaftFileSnapshot{path: path, expected: wrong}).WriteTo(&output)
	require.ErrorIs(t, err, errInvalidStorageWork)
	require.Empty(t, output.Bytes(), "tuple validation precedes output publication")
	corrupt := writePipelineRaftEnvelope(t, inner, 1)
	_, err = (&pipelineRaftFileSnapshot{path: corrupt, expected: metadata}).WriteTo(&output)
	require.ErrorIs(t, err, snap.ErrCRCMismatch)
	require.Empty(t, output.Bytes(), "CRC covers the entire native protobuf, not just the body")
	zero := writePipelineRaftEnvelope(t, inner, 0)
	_, err = (&pipelineRaftFileSnapshot{path: zero, expected: metadata}).WriteTo(&output)
	require.ErrorIs(t, err, snap.ErrEmptySnapshot, "native zero-CRC rejection is unchanged")
}

func writePipelineRaftEnvelope(t *testing.T, inner []byte, checksum uint32) string {
	t.Helper()
	outer, err := proto.Marshal(&snappb.Snapshot{Crc: proto.Uint32(checksum), Data: inner})
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "selected.snap")
	require.NoError(t, os.WriteFile(path, outer, 0o600))
	return path
}

func TestPipelineSnapshotRecoveryNormalizationRetainsWALTail(t *testing.T) {
	e := newOwnedPipelineApplyWitness(t, &testStateMachine{})
	p := e.storagePipeline
	state := &raftpb.ConfState{Voters: []uint64{1, 2}}
	entries := []*raftpb.Entry{{Index: uint64Ptr(2), Term: uint64Ptr(2)},
		{Index: uint64Ptr(3), Term: uint64Ptr(2)}, {Index: uint64Ptr(4), Term: uint64Ptr(2), Data: []byte("retained-tail")}}
	hs := &raftpb.HardState{Term: uint64Ptr(2), Commit: uint64Ptr(4)}
	require.NoError(t, e.persist.Save(hs, entries))
	source := &raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{Index: uint64Ptr(3), Term: uint64Ptr(2), ConfState: state}, Data: []byte{0, 0, 0, 0}}
	require.NoError(t, e.persist.SaveSnap(source))
	require.NoError(t, e.persist.Sync())
	require.NoError(t, e.storage.ApplySnapshot(source))
	require.NoError(t, e.storage.Append(entries[2:]))
	require.NoError(t, e.storage.SetHardState(hs))
	cfg := OpenConfig{NodeID: 2, ElectionTick: 10, HeartbeatTick: 1, MaxInflightMsg: 16, MaxSizePerMsg: pipelineEntryRoom}
	var err error
	e.rawNode, err = newStorageRawNode(cfg, e.storage, 3)
	require.NoError(t, err)
	require.NoError(t, e.attachPipelineSnapshotView(cfg, source))
	require.False(t, p.snapshotView.ready)
	_, err = p.snapshotView.Snapshot()
	require.Error(t, err, "native snapshot access is fenced until real disk normalization completes")
	// The recovery work keeps no reference to the selected inline body.
	require.Empty(t, p.recoveryNormalization.inline)
	require.Empty(t, p.recoveryNormalization.snapshot.Data)
	require.NoError(t, e.queuePipelineRecoveryNormalization())
	require.NoError(t, p.flush())
	require.NoError(t, e.completePipelineDisk(awaitOwnedPipelineCompletion(t, p.disk)))
	require.Nil(t, p.recoveryNormalization)
	actual, err := p.snapshotView.Snapshot()
	require.NoError(t, err)
	require.True(t, isSnapshotToken(actual.Data))
	require.True(t, proto.Equal(source.Metadata, actual.Metadata))
	read, err := e.storage.Entries(4, 5, 1024)
	require.NoError(t, err)
	require.Len(t, read, 1)
	require.True(t, proto.Equal(entries[2], read[0]))
	after, _, err := e.storage.InitialState()
	require.NoError(t, err)
	require.True(t, proto.Equal(hs, after))
	p.snapshotView.retireClones()
	// Reload the actual native file+WAL pair rather than inspecting only an
	// in-memory token; this is clean reopen evidence, not crash certification.
	require.NoError(t, closePersist(e.persist))
	reopened, err := openDiskState(OpenConfig{DataDir: e.dataDir, StateMachine: &testStateMachine{}}, nil)
	require.NoError(t, err)
	e.persist = reopened.Persist
	require.True(t, isSnapshotToken(reopened.LocalSnap.Data))
	require.True(t, proto.Equal(source.Metadata, reopened.LocalSnap.Metadata))
	read, err = reopened.Storage.Entries(4, 5, 1024)
	require.NoError(t, err)
	require.True(t, proto.Equal(entries[2], read[0]))
}
