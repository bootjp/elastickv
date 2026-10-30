package etcd

import (
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// Direct owner witness complements the full-runtime Close/Restore tests: the
// disk actor is stopped while its actual normalization SaveSnap is blocked.
// The real completion and join, not cancellation, retire its source/file pin.
func TestPipelineSnapshotNormalizationFailureRetainsSourceUntilJoin(t *testing.T) {
	e := newOwnedPipelineApplyWitness(t, &testStateMachine{})
	p := e.storagePipeline
	hs := &raftpb.HardState{Term: uint64Ptr(2), Commit: uint64Ptr(3)}
	entries := []*raftpb.Entry{{Index: uint64Ptr(2), Term: uint64Ptr(2)}, {Index: uint64Ptr(3), Term: uint64Ptr(2)}}
	require.NoError(t, e.persist.Save(hs, entries))
	source := raftTestSnapshot(3, 2, []uint64{1, 2}, []byte{0, 0, 0, 0})
	require.NoError(t, e.persist.SaveSnap(&source))
	require.NoError(t, e.persist.Sync())
	require.NoError(t, e.storage.ApplySnapshot(&source))
	require.NoError(t, e.storage.SetHardState(hs))
	cfg := OpenConfig{NodeID: 2, ElectionTick: 10, HeartbeatTick: 1, MaxInflightMsg: 16, MaxSizePerMsg: pipelineEntryRoom}
	var err error
	e.rawNode, err = newStorageRawNode(cfg, e.storage, 3)
	require.NoError(t, err)
	require.NoError(t, e.attachPipelineSnapshotView(cfg, &source))
	failure := errors.New("normalization native SaveSnap failure")
	gate := newNativeSnapshotGate("save")
	gate.minimum.Store(3)
	gate.failure = failure
	t.Cleanup(gate.unblock)
	p.persist = &nativeSnapshotStorage{Storage: e.persist, gate: gate}
	require.NoError(t, e.queuePipelineRecoveryNormalization())
	require.NoError(t, p.flush())
	select {
	case index := <-gate.entered:
		require.Equal(t, uint64(3), index)
	case <-time.After(time.Second):
		t.Fatal("actual normalization did not enter native SaveSnap")
	}
	p.stopActorsOnFailure()
	select {
	case <-p.disk.done:
		t.Fatal("cancellation joined the disk actor before its real SaveSnap returned")
	default:
	}
	used, items := p.snapshotBudget.usage()
	require.Positive(t, used)
	require.Positive(t, items)
	require.Len(t, p.normalizations, 1)
	require.Equal(t, 1, p.snapshotFiles.pins[3])
	gate.unblock()
	require.ErrorIs(t, e.completePipelineDisk(awaitOwnedPipelineCompletion(t, p.disk)), failure)
	require.NotNil(t, p.recoveryNormalization, "failed normalization remains owned until actual worker joins")
	used, _ = p.snapshotBudget.usage()
	require.Positive(t, used)
	for _, actor := range []*storageWorker{p.disk, p.apply} {
		select {
		case <-actor.done:
		case <-time.After(time.Second):
			t.Fatal("normalization worker did not join after actual operation returned")
		}
	}
	e.rawNode = nil
	p.snapshotView.close()
	e.retirePipelineNormalizations()
	requireStorageWorkUsage(t, p.snapshotBudget, 0, 0)
	requireStorageWorkUsage(t, p.diskBulk, 0, 0)
	require.Empty(t, p.snapshotFiles.pins)
	actual, err := e.storage.Snapshot()
	require.NoError(t, err)
	require.Equal(t, source.Data, actual.Data, "a failed SaveSnap must not publish the new token")
	require.False(t, p.snapshotView.ready)
	require.NoError(t, closePersist(e.persist))
	// Failure happened before the normalized SaveSnap delegated: the original
	// actual inline Raft file/WAL pair must remain selected and restorable.
	disk, err := openDiskState(OpenConfig{DataDir: e.dataDir, StateMachine: &testStateMachine{}}, nil)
	require.NoError(t, err)
	e.persist = disk.Persist
	require.Equal(t, source.Data, disk.LocalSnap.Data)
	require.True(t, proto.Equal(source.Metadata, disk.LocalSnap.Metadata))
}
