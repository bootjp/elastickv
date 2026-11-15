package etcd

import (
	"testing"

	"github.com/stretchr/testify/require"
	etcdraft "go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

type ttlInlineMarkerTestFSM struct{ StateMachine }

func (*ttlInlineMarkerTestFSM) IsTTLInlineCapabilityMarkerPayload(payload []byte) bool {
	return len(payload) != 0 && payload[0] == 0x2e
}

func TestTTLInlineMembershipPreflightFindsPendingLogMarker(t *testing.T) {
	t.Parallel()
	storage := etcdraft.NewMemoryStorage()
	require.NoError(t, storage.Append([]*raftpb.Entry{
		{Index: uint64Ptr(1), Term: uint64Ptr(1), Type: entryTypePtr(raftpb.EntryNormal)},
		{Index: uint64Ptr(2), Term: uint64Ptr(1), Type: entryTypePtr(raftpb.EntryNormal), Data: encodeProposalEnvelope(7, []byte{0x2e})},
	}))
	e := &Engine{fsm: &ttlInlineMarkerTestFSM{}, storage: storage, raftCutoverIndex: orInertCutover(nil)}
	e.appliedIndex.Store(1)
	blocked, err := e.ttlInlineMembershipBlocked()
	require.NoError(t, err)
	require.True(t, blocked)

	done := make(chan adminResult, 1)
	e.proposeMembershipChange(adminRequest{id: 8, done: done}, raftpb.ConfChangeAddLearnerNode, Peer{NodeID: 2, ID: "n2"})
	require.ErrorIs(t, (<-done).err, errTTLInlineMembershipProofRequired)
	require.Empty(t, e.pendingConfigs)
}

func TestTTLInlineMembershipPreflightFindsEncryptedLogMarker(t *testing.T) {
	t.Parallel()
	cipher, kid := raftCipherFixture(t)
	entry := envelopeEntry(t, cipher, kid, 2, []byte{0x2e})
	storage := etcdraft.NewMemoryStorage()
	require.NoError(t, storage.Append([]*raftpb.Entry{
		{Index: uint64Ptr(1), Term: uint64Ptr(1), Type: entryTypePtr(raftpb.EntryNormal)},
		&entry,
	}))
	e := &Engine{fsm: &ttlInlineMarkerTestFSM{}, storage: storage, raftCipher: cipher, raftCutoverIndex: func() uint64 { return 1 }}
	e.appliedIndex.Store(1)
	blocked, err := e.ttlInlineMembershipBlocked()
	require.NoError(t, err)
	require.True(t, blocked)
	e.raftCutoverIndex = orInertCutover(nil)
	blocked, err = e.ttlInlineMembershipBlocked()
	require.NoError(t, err)
	require.True(t, blocked, "an unapplied cutover must not hide a wrapped marker")
	_, wrapped, ok := decodeProposalEnvelope(entry.GetData())
	require.True(t, ok)
	require.True(t, e.isTTLInlineCapabilityProposal(wrapped))
	blocked, err = (&Engine{fsm: &ttlInlineMarkerTestFSM{}, pendingTTLInlineMarker: true}).ttlInlineMembershipBlocked()
	require.NoError(t, err)
	require.True(t, blocked, "an accepted local proposal is fenced before Ready persistence")
}

func TestTTLInlineMembershipPreflightRejectsUnreadableEncryptedLog(t *testing.T) {
	t.Parallel()
	storage := etcdraft.NewMemoryStorage()
	require.NoError(t, storage.Append([]*raftpb.Entry{
		{Index: uint64Ptr(1), Term: uint64Ptr(1), Type: entryTypePtr(raftpb.EntryNormal)},
		{Index: uint64Ptr(2), Term: uint64Ptr(1), Type: entryTypePtr(raftpb.EntryNormal), Data: encodeProposalEnvelope(7, []byte{0xff})},
	}))
	e := &Engine{fsm: &ttlInlineMarkerTestFSM{}, storage: storage, raftCutoverIndex: func() uint64 { return 1 }}
	e.appliedIndex.Store(1)
	blocked, err := e.ttlInlineMembershipBlocked()
	require.False(t, blocked)
	require.ErrorIs(t, err, ErrRaftUnwrapFailed)
}

func TestTTLInlineMembershipPreflightIgnoresUnmarkedLog(t *testing.T) {
	t.Parallel()
	storage := etcdraft.NewMemoryStorage()
	require.NoError(t, storage.Append([]*raftpb.Entry{
		{Index: uint64Ptr(1), Term: uint64Ptr(1), Type: entryTypePtr(raftpb.EntryNormal)},
		{Index: uint64Ptr(2), Term: uint64Ptr(1), Type: entryTypePtr(raftpb.EntryNormal), Data: encodeProposalEnvelope(7, []byte{0x01})},
	}))
	e := &Engine{fsm: &ttlInlineMarkerTestFSM{}, storage: storage, raftCutoverIndex: orInertCutover(nil)}
	e.appliedIndex.Store(1)
	blocked, err := e.ttlInlineMembershipBlocked()
	require.NoError(t, err)
	require.False(t, blocked)
}

func TestTTLInlineMembershipPreflightScansAcrossBatches(t *testing.T) {
	t.Parallel()
	entries := make([]*raftpb.Entry, 0, ttlInlineMarkerScanBatch+2)
	for index := uint64(1); index <= ttlInlineMarkerScanBatch+2; index++ {
		payload := []byte{0x01}
		if index == ttlInlineMarkerScanBatch+2 {
			payload[0] = 0x2e
		}
		entries = append(entries, &raftpb.Entry{
			Index: uint64Ptr(index), Term: uint64Ptr(1), Type: entryTypePtr(raftpb.EntryNormal),
			Data: encodeProposalEnvelope(index, payload),
		})
	}
	storage := etcdraft.NewMemoryStorage()
	require.NoError(t, storage.Append(entries))
	e := &Engine{fsm: &ttlInlineMarkerTestFSM{}, storage: storage, raftCutoverIndex: orInertCutover(nil)}
	e.appliedIndex.Store(1)
	blocked, err := e.ttlInlineMembershipBlocked()
	require.NoError(t, err)
	require.True(t, blocked)
}

func TestTTLInlineMembershipPreflightWaitsForAsyncAppend(t *testing.T) {
	t.Parallel()
	e := &Engine{fsm: &ttlInlineMarkerTestFSM{}, storagePipeline: &engineStoragePipeline{diskOutstanding: 1}}
	blocked, err := e.ttlInlineMembershipBlocked()
	require.False(t, blocked)
	require.ErrorIs(t, err, errTTLInlineAdmissionStoragePending)
}

func TestTTLInlineMembershipPreflightWaitsForSnapshotRecovery(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		setup func(*Engine)
	}{
		{"synchronous restore", func(e *Engine) { e.snapshotRestoring.Store(true) }},
		{"asynchronous transition", func(e *Engine) { e.snapshotTransition.Store(true) }},
		{"received snapshot", func(e *Engine) { e.storagePipeline = &engineStoragePipeline{receiving: &pipelineSnapshotPlan{}} }},
		{"normalizing snapshot", func(e *Engine) {
			e.storagePipeline = &engineStoragePipeline{normalizingReceived: &pipelineSnapshotPlan{}}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := &Engine{fsm: &ttlInlineMarkerTestFSM{}}
			tc.setup(e)
			blocked, err := e.ttlInlineMembershipBlocked()
			require.False(t, blocked)
			require.ErrorIs(t, err, errTTLInlineAdmissionSnapshotPending)
		})
	}
}
