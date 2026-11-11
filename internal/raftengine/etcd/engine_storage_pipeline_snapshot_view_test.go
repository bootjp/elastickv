package etcd

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	etcdraft "go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func TestPipelineSnapshotViewClonePreservesJointAndUnknownMetadata(t *testing.T) {
	e := newPipelineAdmissionFixture(t)
	p := e.storagePipeline
	storage := etcdraft.NewMemoryStorage()
	state := &raftpb.ConfState{Voters: []uint64{1, 2}, VotersOutgoing: []uint64{1, 2, 3},
		Learners: []uint64{4}, LearnersNext: []uint64{3}, AutoLeave: proto.Bool(true)}
	unknown := protowire.AppendTag(nil, 65535, protowire.BytesType)
	unknown = protowire.AppendBytes(unknown, bytes.Repeat([]byte{0x7f}, 4096))
	state.ProtoReflect().SetUnknown(unknown)
	source := &raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{Index: uint64Ptr(3), Term: uint64Ptr(2), ConfState: state}, Data: encodeSnapshotToken(3, 123)}
	source.ProtoReflect().SetUnknown(unknown)
	source.Metadata.ProtoReflect().SetUnknown(unknown)
	require.NoError(t, storage.ApplySnapshot(source))
	view := &pipelineSnapshotStorageView{MemoryStorage: storage, root: p.bulkOwnership, failure: p.firstFailure}
	view.publish(source, true)
	actual, err := view.Snapshot()
	require.NoError(t, err)
	require.True(t, proto.Equal(source, actual))
	used, _ := p.bulkOwnership.usage()
	require.GreaterOrEqual(t, used, pipelineOwnedSnapshotBytes(actual), "real clone backing capacities must fit the pre-clone projection")
	actual.Metadata.ConfState.LearnersNext[0] = 100
	actual.Metadata.ProtoReflect().SetUnknown(nil)
	again, err := view.Snapshot()
	require.NoError(t, err)
	require.True(t, proto.Equal(source, again), "native clones own all joint fields and unknown metadata independently")
	view.retireClones()
	requireStorageWorkUsage(t, p.bulkOwnership, 0, 0)
}

func TestPipelineSnapshotViewCreatePreservesOldUnknownProjection(t *testing.T) {
	e := newPipelineAdmissionFixture(t)
	p := e.storagePipeline
	storage := etcdraft.NewMemoryStorage()
	old := raftTestSnapshot(1, 1, []uint64{1, 2}, encodeSnapshotToken(1, 123))
	unknown := protowire.AppendTag(nil, 65535, protowire.BytesType)
	unknown = protowire.AppendBytes(unknown, bytes.Repeat([]byte{0x11}, 4096))
	old.ProtoReflect().SetUnknown(unknown)
	old.Metadata.ProtoReflect().SetUnknown(unknown)
	require.NoError(t, storage.ApplySnapshot(&old))
	require.NoError(t, storage.Append([]*raftpb.Entry{{Index: uint64Ptr(2), Term: uint64Ptr(2)}}))
	view := &pipelineSnapshotStorageView{MemoryStorage: storage, root: p.bulkOwnership, failure: p.firstFailure}
	view.publish(&old, true)
	next := raftTestSnapshot(2, 2, []uint64{1, 2}, encodeSnapshotToken(2, 456))
	_, err := storage.CreateSnapshot(2, next.Metadata.ConfState, next.Data)
	require.NoError(t, err)
	view.publish(&next, false)
	actual, err := view.Snapshot()
	require.NoError(t, err)
	require.Equal(t, unknown, []byte(actual.ProtoReflect().GetUnknown()))
	require.Equal(t, unknown, []byte(actual.Metadata.ProtoReflect().GetUnknown()))
	used, _ := p.bulkOwnership.usage()
	require.GreaterOrEqual(t, used, pipelineOwnedSnapshotBytes(actual), "CreateSnapshot keeps old unknowns even when its request has none")
	view.close()
	requireStorageWorkUsage(t, p.bulkOwnership, 0, 0)
}

func TestPipelineSnapshotViewRejectsBeforeNativeClone(t *testing.T) {
	e := newPipelineAdmissionFixture(t)
	p := e.storagePipeline
	storage := etcdraft.NewMemoryStorage()
	source := raftTestSnapshot(1, 1, []uint64{1}, encodeSnapshotToken(1, 123))
	require.NoError(t, storage.ApplySnapshot(&source))
	view := &pipelineSnapshotStorageView{MemoryStorage: storage, root: p.bulkOwnership, failure: p.firstFailure}
	_, err := view.Snapshot()
	require.ErrorIs(t, err, etcdraft.ErrSnapshotTemporarilyUnavailable)
	require.NoError(t, p.firstFailure(nil), "recovery fencing is temporary, not a fabricated completion or fatal result")
	requireStorageWorkUsage(t, p.bulkOwnership, 0, 0)
	view.publish(&source, true)
	full, err := p.bulkOwnership.acquire(pipelineBulkBytes+pipelineReadyOvershootBytes, 0, true)
	require.NoError(t, err)
	defer full.Release()
	_, err = view.Snapshot()
	require.ErrorIs(t, err, etcdraft.ErrSnapshotTemporarilyUnavailable)
	require.ErrorIs(t, p.firstFailure(nil), errStoragePipelineResource)
	require.Zero(t, view.cloneCount, "no real MemoryStorage clone occurred without its reservation")
	require.Empty(t, view.clones)
}
