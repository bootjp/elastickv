package etcd

import (
	"bytes"
	"context"
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/stretchr/testify/require"
	etcdraft "go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

func TestTTLInlineMarkerRequiresVerifiedProposalPath(t *testing.T) {
	t.Parallel()
	cipher, kid := raftCipherFixture(t)
	e := &Engine{fsm: &ttlInlineMarkerTestFSM{}, raftCipher: cipher}
	for _, payload := range [][]byte{
		{0x2e},
		func() []byte {
			entry := envelopeEntry(t, cipher, kid, 2, []byte{0x2e})
			_, wrapped, ok := decodeProposalEnvelope(entry.GetData())
			require.True(t, ok)
			return wrapped
		}(),
	} {
		done := make(chan proposalResult, 1)
		e.handleProposal(proposalRequest{ctx: context.Background(), payload: payload, done: done})
		require.ErrorIs(t, (<-done).err, errTTLInlineMarkerProposalUnauthorized)
	}
	require.NoError(t, e.rejectUnverifiedTTLInlineMarker([]byte{0x01}))
	normal := envelopeEntry(t, cipher, kid, 3, []byte{0x01})
	_, encrypted, ok := decodeProposalEnvelope(normal.GetData())
	require.True(t, ok)
	require.NoError(t, e.rejectUnverifiedTTLInlineMarker(encrypted))
}

type ttlInlineMarkerTestFSM struct{ StateMachine }

type ttlInlineRecordedMarkerTestFSM struct{ ttlInlineMarkerTestFSM }

func (*ttlInlineRecordedMarkerTestFSM) TTLInlineCapabilityRecorded() bool { return true }

type ttlInlineProposalTestFSM struct {
	ttlInlineMarkerTestFSM
	fenced bool
}

type ttlInlineCertifiedLearnerTestFSM struct {
	ttlInlineMarkerTestFSM
	member uint64
	index  uint64
}

func (f *ttlInlineCertifiedLearnerTestFSM) TTLInlineCertifiedLearner(_ context.Context, member uint64, native raftengine.NativeConfiguration) (bool, error) {
	return member == f.member && native.Index == f.index, nil
}

func TestTTLInlinePromotionRequiresCertifiedExistingLearner(t *testing.T) {
	t.Parallel()
	storage := etcdraft.NewMemoryStorage()
	require.NoError(t, storage.ApplySnapshot(&raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{
		Index: uint64Ptr(5), Term: uint64Ptr(1), ConfState: &raftpb.ConfState{Voters: []uint64{1, 2}, Learners: []uint64{3}},
	}}))
	storage.SetHardState(&raftpb.HardState{Term: uint64Ptr(1), Commit: uint64Ptr(5)})
	e := &Engine{fsm: &ttlInlineCertifiedLearnerTestFSM{member: 3, index: 5}, rawNode: mustRawNode(t, storage, 1),
		recoveredIdentity: &WALIdentity{}, nativeConfigurationIndex: 5, isLearnerNode: map[uint64]bool{3: true}}
	request := adminRequest{ctx: context.Background(), action: adminActionPromoteLearner}
	certified, err := e.ttlInlinePromotionCertified(request, raftpb.ConfChangeAddNode, 3)
	require.NoError(t, err)
	require.True(t, certified)
	for _, tc := range []struct {
		request adminRequest
		change  raftpb.ConfChangeType
		member  uint64
	}{
		{request, raftpb.ConfChangeAddNode, 4},
		{adminRequest{ctx: context.Background(), action: adminActionAddVoter}, raftpb.ConfChangeAddNode, 3},
		{request, raftpb.ConfChangeAddLearnerNode, 3},
	} {
		certified, err = e.ttlInlinePromotionCertified(tc.request, tc.change, tc.member)
		require.NoError(t, err)
		require.False(t, certified)
	}
	e.isLearnerNode[3] = false
	certified, err = e.ttlInlinePromotionCertified(request, raftpb.ConfChangeAddNode, 3)
	require.NoError(t, err)
	require.False(t, certified)
	e.isLearnerNode[3] = true
	e.nativeConfigurationIndex = 6
	certified, err = e.ttlInlinePromotionCertified(request, raftpb.ConfChangeAddNode, 3)
	require.NoError(t, err)
	require.False(t, certified, "a stale native configuration cannot reuse the learner certificate")
}

func (f *ttlInlineProposalTestFSM) TTLInlineFenceReady() bool { return f.fenced }

func (f *ttlInlineProposalTestFSM) ValidateTTLInlineCapabilityMarkerPayload(payload []byte, native raftengine.NativeConfiguration) error {
	if bytes.Equal(payload, []byte{0x2e, 0x01}) && native.Index == 1 {
		return nil
	}
	return errTTLInlineMarkerProposalUnauthorized
}

func TestTTLInlineMarkerNativeProposalRequiresCurrentCut(t *testing.T) {
	t.Parallel()
	storage := etcdraft.NewMemoryStorage()
	raw, err := etcdraft.NewRawNode(&etcdraft.Config{ID: 1, Storage: storage, ElectionTick: 10, HeartbeatTick: 1, MaxInflightMsgs: 1})
	require.NoError(t, err)
	require.NoError(t, raw.Bootstrap([]etcdraft.Peer{{ID: 1}}))
	fsm := &ttlInlineProposalTestFSM{fenced: true}
	e := &Engine{rawNode: raw, recoveredIdentity: &WALIdentity{}, fsm: fsm,
		nativeConfigurationIndex: 1, peers: map[uint64]Peer{1: {NodeID: 1, Address: "n1:50051"}}}
	native := *e.captureApplyNativeConfiguration()
	req := proposalRequest{payload: []byte{0x2e, 0x01}, ttlInlineCut: &ttlInlineProposalCut{native: native, term: raw.BasicStatus().GetTerm()}}
	require.NoError(t, e.authorizeTTLInlineMarkerProposal(req))
	e.raftCutoverIndex = func() uint64 { return 1 }
	e.appliedIndex.Store(1)
	require.ErrorIs(t, e.authorizeTTLInlineMarkerProposal(req), errTTLInlineMarkerProposalUnauthorized,
		"post-cutover marker must use the raft envelope")
	e.raftCutoverIndex = nil
	req.ttlInlineCut.term++
	require.ErrorIs(t, e.authorizeTTLInlineMarkerProposal(req), errTTLInlineMarkerProposalUnauthorized)
	req.ttlInlineCut.term--
	req.ttlInlineCut.native.Members[0].Address = "changed:50051"
	require.ErrorIs(t, e.authorizeTTLInlineMarkerProposal(req), errTTLInlineMarkerProposalUnauthorized)
	req.ttlInlineCut.native.Members[0].Address = "n1:50051"
	fsm.fenced = false
	require.ErrorIs(t, e.authorizeTTLInlineMarkerProposal(req), errTTLInlineMarkerProposalUnauthorized)
	fsm.fenced = true
	req.payload = []byte{0x2e, 0xff}
	require.ErrorIs(t, e.authorizeTTLInlineMarkerProposal(req), errTTLInlineMarkerProposalUnauthorized)

	cipher, kid := raftCipherFixture(t)
	e.raftCipher = cipher
	entry := envelopeEntry(t, cipher, kid, 2, []byte{0x2e, 0x01})
	_, req.payload, _ = decodeProposalEnvelope(entry.GetData())
	e.raftCutoverIndex = func() uint64 { return 1 }
	require.NoError(t, e.authorizeTTLInlineMarkerProposal(req))
}

type ttlInlineRecoveryInvalidationFSM struct {
	StateMachine
	invalidations int
}

func (f *ttlInlineRecoveryInvalidationFSM) InvalidateTTLInlineRecoveryProof() {
	f.invalidations++
}

func (*ttlInlineMarkerTestFSM) IsTTLInlineCapabilityMarkerPayload(payload []byte) bool {
	return len(payload) != 0 && payload[0] == 0x2e
}

func TestTTLInlineRecoveryProofInvalidatedByNativeConfiguration(t *testing.T) {
	t.Parallel()
	for _, v2 := range []bool{false, true} {
		fsm := &ttlInlineRecoveryInvalidationFSM{}
		e := &Engine{nodeID: 1, fsm: fsm}
		conf := raftpb.ConfState{Voters: []uint64{1}}
		if v2 {
			e.publishConfigChangeV2(raftpb.ConfChangeV2{}, 5, conf)
		} else {
			e.publishConfigChange(raftpb.ConfChangeRemoveNode, 2, nil, 5, conf)
		}
		require.Equal(t, 1, fsm.invalidations)
	}
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

func TestTTLInlineRecordedMarkerStillWaitsForNativeRecovery(t *testing.T) {
	t.Parallel()
	e := &Engine{fsm: &ttlInlineRecordedMarkerTestFSM{}, storagePipeline: &engineStoragePipeline{diskOutstanding: 1}}
	blocked, err := e.ttlInlineMembershipBlocked()
	require.False(t, blocked)
	require.ErrorIs(t, err, errTTLInlineAdmissionStoragePending)
	e.storagePipeline.diskOutstanding = 0
	e.snapshotRestoring.Store(true)
	blocked, err = e.ttlInlineMembershipBlocked()
	require.False(t, blocked)
	require.ErrorIs(t, err, errTTLInlineAdmissionSnapshotPending)
	e.snapshotRestoring.Store(false)
	blocked, err = e.ttlInlineMembershipBlocked()
	require.NoError(t, err)
	require.True(t, blocked)
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
