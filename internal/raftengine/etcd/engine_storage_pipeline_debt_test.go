package etcd

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	etcdraft "go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

func TestPipelineDebtNativeThousandEntryReady(t *testing.T) {
	w := newLedgerNativeWitness(t)
	e := newPipelineAdmissionFixture(t)
	e.rawNode = w.node
	e.nodeID = 2
	p := e.storagePipeline
	t.Cleanup(func() {
		e.rawNode = nil
		retirePipelineDeferred(&p.diskDeferred, &p.diskOutstanding)
		retirePipelineDeferred(&p.applyDeferred, &p.applyOutstanding)
		p.ledger.discard()
	})
	entries := make([]*raftpb.Entry, 1000)
	for i := range entries {
		entries[i] = ledgerEntry(uint64(i+2), 2, "small")
	}
	require.NoError(t, e.rawNode.Step(&raftpb.Message{
		Type: messageTypePtr(raftpb.MsgApp), From: uint64Ptr(1), To: uint64Ptr(2),
		Term: uint64Ptr(2), Index: uint64Ptr(1), LogTerm: uint64Ptr(1), Entries: entries,
	}))
	require.NoError(t, e.foldPipelineReady(), "one genuine small append batch is not 1000 outstanding work items")
	require.Len(t, p.diskDeferred, 1)
	require.Len(t, p.diskDeferred[0].message.Entries, len(entries))
}

func TestPipelineDebtSingleSharedAllowance(t *testing.T) {
	root := newStorageWorkBudget(10, 2)
	root.overshootBytes, root.overshootItems = 5, 1
	ingress := newStorageWorkBudget(10, 2)
	ingress.parent = root
	owner := newStorageWorkBudget(10, 2)
	owner.parent, owner.owner = root, true
	full, err := ingress.tryAcquire(10)
	require.NoError(t, err)
	t.Cleanup(full.Release)
	debt, err := owner.tryAcquire(5)
	require.NoError(t, err)
	t.Cleanup(debt.Release)
	bytes, _ := root.debt()
	require.Equal(t, uint64(5), bytes)
	_, err = owner.tryAcquire(1)
	require.ErrorIs(t, err, errStorageWorkBackpressure, "another Ready cannot mint another allowance")
	_, err = ingress.tryAcquire(0)
	require.ErrorIs(t, err, errStorageWorkBackpressure, "even zero-byte bulk work cannot enter during debt")
	debt.Release()
	bytes, items := root.debt()
	require.Zero(t, bytes)
	require.Zero(t, items)
	full.Release()
	requireStorageWorkUsage(t, root, 0, 0)
}

func TestPipelineDebtIngressTransfersToActualReadyOwners(t *testing.T) {
	w := newLedgerNativeWitness(t)
	e := newPipelineAdmissionFixture(t)
	e.rawNode, e.nodeID = w.node, 2
	p := e.storagePipeline
	t.Cleanup(func() {
		e.rawNode = nil
		retirePipelineDeferred(&p.diskDeferred, &p.diskOutstanding)
		p.ledger.discard()
		requireStorageWorkUsage(t, p.bulkOwnership, 0, 0)
	})
	message := raftpb.Message{Type: messageTypePtr(raftpb.MsgApp), From: uint64Ptr(1), To: uint64Ptr(2),
		Term: uint64Ptr(2), Index: uint64Ptr(1), LogTerm: uint64Ptr(1), Entries: []*raftpb.Entry{ledgerEntry(2, 2, "real-owned-input")}}
	require.NoError(t, e.enqueuePipelineStep(context.Background(), message))
	input := <-p.bulk
	require.NoError(t, e.consumePipelineIngress(input))
	require.Len(t, p.diskDeferred, 1)
	requireStorageWorkUsage(t, p.bulkIngressBudget, 0, 0)
	bytes, work := p.bulkOwnership.usage()
	require.Positive(t, bytes)
	require.Equal(t, 2, work, "ledger and deferred append survive ingress retirement")
	message.Entries[0].Data[0] = 'X'
	require.Equal(t, "real-owned-input", string(p.diskDeferred[0].message.Entries[0].Data))
}

func TestPipelineDebtIndependentCommittedBatchAndFirstException(t *testing.T) {
	for _, large := range []bool{false, true} {
		t.Run(map[bool]string{false: "many-small", true: "first-large"}[large], func(t *testing.T) {
			store := etcdraft.NewMemoryStorage()
			require.NoError(t, store.ApplySnapshot(&raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{
				Index: uint64Ptr(1), Term: uint64Ptr(1), ConfState: &raftpb.ConfState{Voters: []uint64{1, 2, 3}},
			}}))
			entries := make([]*raftpb.Entry, 1000)
			for i := range entries {
				entries[i] = ledgerEntry(uint64(i+2), 2, strings.Repeat("x", 512))
				if large {
					entries = entries[:1]
					entries[0].Data = make([]byte, 1<<20)
					break
				}
			}
			require.NoError(t, store.Append(entries))
			require.NoError(t, store.SetHardState(&raftpb.HardState{Term: uint64Ptr(2), Commit: uint64Ptr(entries[len(entries)-1].GetIndex())}))
			node, err := newStorageRawNode(OpenConfig{NodeID: 2, ElectionTick: 10, HeartbeatTick: 1, MaxInflightMsg: 16, MaxSizePerMsg: pipelineEntryRoom}, store, 1)
			require.NoError(t, err)
			require.True(t, node.HasReady())
			rd := node.Ready()
			for _, message := range rd.Messages {
				if message.GetType() == raftpb.MsgStorageApply {
					require.LessOrEqual(t, pipelineTaskMessageBytes(message), uint64(pipelineMaxOwnedInputBytes))
					if large {
						require.Len(t, message.Entries, 1)
						require.Len(t, message.Entries[0].Data, 1<<20)
					} else {
						require.NotEmpty(t, message.Entries)
						require.Less(t, len(message.Entries), len(entries), "committed batch is independent of the much larger transport target")
					}
					return
				}
			}
			t.Fatal("native committed apply work missing")
		})
	}
}

func TestPipelineDebtAggregatePreProposal(t *testing.T) {
	e := newPipelineAdmissionFixture(t)
	full, err := e.storagePipeline.diskBulk.tryAcquire(pipelineBulkBytes)
	require.NoError(t, err)
	t.Cleanup(full.Release)
	permit, err := e.reservePipelineProposal(1)
	t.Cleanup(permit.Release)
	require.ErrorIs(t, err, errStorageWorkBackpressure, "another stage's ownership must constrain pre-copy admission")
	require.Nil(t, permit)
}

func TestPipelineDebtLedgerBillsBackingAndUnknown(t *testing.T) {
	l := newAsyncUnstableLedger(newStorageWorkBudget(4<<20, 128))
	t.Cleanup(l.discard)
	entry := ledgerEntry(1, 1, "")
	entry.Data = make([]byte, 1<<20)[:1]
	unknown := make([]byte, 1<<20)
	copy(unknown, []byte{0xa0, 0x06, 0x01})
	entry.ProtoReflect().SetUnknown(unknown[:3])
	require.NoError(t, l.retain(ledgerAppend(entry)))
	bytes, _ := l.budget.usage()
	require.GreaterOrEqual(t, bytes, uint64(2<<20), "wire length does not bound retained allocation")
}

func TestPipelineDebtMetadataProjectionPrecedesStep(t *testing.T) {
	e := newPipelineAdmissionFixture(t)
	p := e.storagePipeline
	full, err := p.ledger.budget.tryAcquire(pipelineBulkBytes - 2048)
	require.NoError(t, err)
	t.Cleanup(full.Release)
	entries := make([]*raftpb.Entry, 1000)
	for i := range entries {
		entries[i] = ledgerEntry(uint64(i+1), 1, "")
	}
	require.ErrorIs(t, e.enqueuePipelineStep(context.Background(), raftpb.Message{
		Type: messageTypePtr(raftpb.MsgApp), From: uint64Ptr(2), To: uint64Ptr(1), Entries: entries,
	}), errStepQueueFull)
	require.Empty(t, p.bulk, "metadata-heavy input must be rejected before cloning/Step, not halt on an emitted Ready")
}

func TestPipelineDebtSnapshotDefersWithoutBlockingControl(t *testing.T) {
	e := newPipelineAdmissionFixture(t)
	p := e.storagePipeline
	snapshot := raftTestSnapshot(10, 2, []uint64{1, 2, 3}, []byte("inline"))
	require.NoError(t, e.enqueuePipelineStep(context.Background(), raftpb.Message{
		Type: messageTypePtr(raftpb.MsgSnap), From: uint64Ptr(2), To: uint64Ptr(1), Snapshot: &snapshot,
	}))
	input := <-p.priority
	used, _ := p.bulkOwnership.usage()
	full, err := p.proposalBudget.tryAcquire(pipelineBulkBytes - used)
	require.NoError(t, err)
	t.Cleanup(full.Release)
	debt, err := p.diskBulk.tryAcquire(1)
	require.NoError(t, err)
	t.Cleanup(debt.Release)
	require.NoError(t, e.consumePipelineIngress(input))
	require.Len(t, p.deferredSnapshots, 1)
	bytes, items := p.snapshotIngressBudget.usage()
	require.Positive(t, bytes)
	require.Equal(t, 1, items, "defer is not retirement")
	require.NoError(t, e.enqueuePipelineStep(context.Background(), raftpb.Message{
		Type: messageTypePtr(raftpb.MsgHeartbeat), From: uint64Ptr(2), To: uint64Ptr(1),
	}))
	control := <-p.priority
	require.Equal(t, raftpb.MsgHeartbeat, control.message.GetType())
	control.permit.Release()
	p.retireIngress()
	requireStorageWorkUsage(t, p.snapshotIngressBudget, 0, 0)
}

func TestPipelineDebtControlReadyHasIndependentReserve(t *testing.T) {
	w := newLedgerNativeWitness(t)
	e := newPipelineAdmissionFixture(t)
	e.rawNode, e.nodeID = w.node, 2
	p := e.storagePipeline
	full, err := p.proposalBudget.tryAcquire(pipelineBulkBytes)
	require.NoError(t, err)
	t.Cleanup(full.Release)
	debt, err := p.diskBulk.tryAcquire(1)
	require.NoError(t, err)
	t.Cleanup(debt.Release)
	t.Cleanup(func() {
		e.rawNode = nil
		retirePipelineDeferred(&p.diskDeferred, &p.diskOutstanding)
		p.ledger.discard()
	})
	require.NoError(t, e.rawNode.Step(&raftpb.Message{Type: messageTypePtr(raftpb.MsgHeartbeat),
		From: uint64Ptr(1), To: uint64Ptr(2), Term: uint64Ptr(2), Commit: uint64Ptr(1)}))
	require.NoError(t, e.foldPipelineReady())
	require.Len(t, p.diskDeferred, 1)
	require.Empty(t, p.diskDeferred[0].message.Entries)
	control, _ := p.controlOwnership.usage()
	require.Positive(t, control)
}

func TestPipelineDebtControlExhaustionIsExplicitResourceFailure(t *testing.T) {
	w := newLedgerNativeWitness(t)
	e := newPipelineAdmissionFixture(t)
	e.rawNode, e.nodeID = w.node, 2
	p := e.storagePipeline
	full, err := p.priorityIngressBudget.tryAcquire(pipelineControlBytes)
	require.NoError(t, err)
	t.Cleanup(full.Release)
	t.Cleanup(func() {
		p.disk.quiesce()
		p.apply.quiesce()
		<-p.disk.done
		<-p.apply.done
		e.rawNode = nil
		p.failedReadyPermit.Release()
		p.ledger.discard()
	})
	require.NoError(t, e.rawNode.Step(&raftpb.Message{Type: messageTypePtr(raftpb.MsgHeartbeat),
		From: uint64Ptr(1), To: uint64Ptr(2), Term: uint64Ptr(2), Commit: uint64Ptr(1)}))
	require.ErrorIs(t, e.foldPipelineReady(), errStoragePipelineResource)
	require.NotNil(t, p.failedReady, "accepted non-droppable Ready must remain owned through join")
	require.Empty(t, p.diskDeferred)
}

func TestPipelineDebtDescriptorAndMinimumEntryBound(t *testing.T) {
	metadata := reflect.TypeFor[raftpb.Entry]().Size() + reflect.TypeFor[normalApplyInput]().Size() +
		2*reflect.TypeFor[normalApplyReceipt]().Size() + 2*reflect.TypeFor[asyncUnstableEntry]().Size() + 4*pipelinePointerBytes
	require.Less(t, metadata, uintptr(pipelinePhysicalEntryBytes))
	// A valid indexed/termed Entry costs at least four encoded bytes. The
	// 256-KiB target therefore admits at most 65,536 minimum-sized descriptors.
	const maximumDescriptors = pipelineCommittedBatchBytes / 4
	bytes := uint64(maximumDescriptors)*(pipelinePhysicalEntryBytes+2*pipelinePointerBytes) + 2*pipelineCommittedBatchBytes + pipelineTaskOverhead
	require.Less(t, bytes, uint64(pipelineMaxOwnedInputBytes))
	require.Equal(t, uint64(512<<20), uint64(7*pipelineMaxOwnedInputBytes+pipelineControlBytes))
}
