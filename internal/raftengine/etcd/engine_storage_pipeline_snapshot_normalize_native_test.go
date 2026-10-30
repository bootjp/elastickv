package etcd

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	etcdraft "go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

// Public legacy inline snapshot + real volatile Raft protocol witness. No
// durable WAL/quorum, TLS, or whole-Go-heap claim is made by this reproduction.
func TestPipelineDebtInlineSnapshotFanoutNormalization(t *testing.T) {
	e := newPipelineAdmissionFixture(t)
	e.dataDir = t.TempDir()
	e.fsmSnapDir = filepath.Join(e.dataDir, fsmSnapDirName)
	p := e.storagePipeline
	pool := newDebtBoundVolatileClusterWithConfig(t, 1, 9, func(i int, cfg *OpenConfig, storage *etcdraft.MemoryStorage, selected *raftpb.Snapshot) {
		if i == 0 {
			view := &pipelineSnapshotStorageView{MemoryStorage: storage, root: p.bulkOwnership, failure: p.firstFailure}
			view.publish(selected, true)
			p.snapshotView, cfg.asyncSnapshotView = view, view
		}
	})
	leader := pool[0]
	e.nodeID, e.rawNode, e.storage = 1, leader.node, leader.storage
	fsm := &testStateMachine{}
	payload := bytes.Repeat([]byte{0xa5}, asyncNativeLargeValueBytes)
	require.NoError(t, leader.node.Propose(encodeProposalEnvelope(100, payload)))
	applied := applyDebtSnapshotPrefix(t, leader.node, leader.storage, fsm)
	require.Equal(t, uint64(3), applied)
	snapshot, err := fsm.Snapshot()
	require.NoError(t, err)
	data, err := snapshotBytesAndClose(snapshot, t.TempDir())
	require.NoError(t, err)
	_, state, err := leader.storage.InitialState()
	require.NoError(t, err)
	source := &raftpb.Snapshot{Data: data, Metadata: &raftpb.SnapshotMetadata{Index: uint64Ptr(applied), Term: uint64Ptr(2), ConfState: state}}
	env := pipelineConfigurationEnvironment{dataDir: e.dataDir, fsmSnapDir: e.fsmSnapDir}
	normalization, err := e.newPipelineNormalization(source, env, nil, false)
	require.NoError(t, err)
	normalization.install = true
	require.NoError(t, e.queuePipelineNormalization(normalization))
	require.NoError(t, p.flush())
	require.NoError(t, e.completePipelineDisk(awaitOwnedPipelineCompletion(t, p.disk)))
	current, err := e.storage.Snapshot()
	require.NoError(t, err)
	require.True(t, isSnapshotToken(current.Data), "the actual Storage view must contain a verified descriptor, never an omitted body")
	first, err := e.storage.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, applied+1, first)
	token, err := decodeSnapshotToken(current.Data)
	require.NoError(t, err)
	reader, err := p.snapshotFiles.openReceived(env, token.Index, token.CRC32C)
	require.NoError(t, err)
	var restored testStateMachine
	require.NoError(t, restored.Restore(reader))
	require.NoError(t, reader.Close())
	require.Equal(t, [][]byte{payload}, restored.Applied())
	for i := 1; i < len(pool); i++ {
		leader.node.ReportUnreachable(uint64(i + 1))
	}
	t.Cleanup(func() {
		e.rawNode = nil
		p.failedReady = nil
		p.failedReadyPermit.Release()
		retirePipelineDeferred(&p.diskDeferred, &p.diskOutstanding)
		retirePipelineDeferred(&p.applyDeferred, &p.applyOutstanding)
		p.ledger.discard()
	})
	require.NoError(t, e.enqueuePipelineStep(t.Context(), raftpb.Message{Type: messageTypePtr(raftpb.MsgProp), From: uint64Ptr(1), To: uint64Ptr(1),
		Entries: []*raftpb.Entry{{Data: encodeProposalEnvelope(101, []byte("small-after-snapshot"))}}}))
	beforeClones := p.snapshotView.cloneCount
	err = e.consumePipelineIngress(<-p.bulk)
	if rd := p.failedReady; rd != nil {
		backings := make(map[*byte]struct{})
		var owned uint64
		for _, message := range rd.Messages {
			if message.GetType() == raftpb.MsgSnap {
				backings[&message.Snapshot.Data[0]] = struct{}{}
				owned += uint64(cap(message.Snapshot.Data))
			}
		}
		t.Logf("real distinct Snapshot clones=%d owned Data capacity=%d R=%d control=%d", len(backings), owned, pipelineReadyOvershootBytes, pipelineControlBytes)
		require.Len(t, backings, 9)
		require.Greater(t, owned, uint64(pipelineReadyOvershootBytes))
	}
	require.NoError(t, err, "a tiny accepted proposal must not create an unreserved oversized native snapshot Ready")
	require.Equal(t, uint64(9), p.snapshotView.cloneCount-beforeClones, "all nine real native snapshot sends must still be emitted")
}

func applyDebtSnapshotPrefix(t *testing.T, node *etcdraft.RawNode, storage *etcdraft.MemoryStorage, fsm *testStateMachine) uint64 {
	t.Helper()
	var applied uint64
	for turn := 0; node.HasReady(); turn++ {
		require.Less(t, turn, 100)
		rd := node.Ready()
		for _, message := range rd.Messages {
			switch {
			case message.GetType() == raftpb.MsgStorageAppend:
				require.NoError(t, storage.Append(message.Entries))
				state := &raftpb.HardState{Term: message.Term, Vote: message.Vote, Commit: message.Commit}
				if !etcdraft.IsEmptyHardState(state) {
					require.NoError(t, storage.SetHardState(state))
				}
				for _, response := range message.Responses {
					require.Equal(t, uint64(1), response.GetTo())
					require.NoError(t, node.Step(response))
				}
			case message.GetType() == raftpb.MsgStorageApply:
				for _, entry := range message.Entries {
					_, data, ok := decodeProposalEnvelope(entry.GetData())
					require.True(t, ok)
					fsm.Apply(data)
					applied = entry.GetIndex()
				}
				for _, response := range message.Responses {
					require.NoError(t, node.Step(response))
				}
			default:
				// Real packet loss with delayed failure reports: no learner
				// receives this entry and no append ACK is manufactured.
			}
		}
	}
	return applied
}
