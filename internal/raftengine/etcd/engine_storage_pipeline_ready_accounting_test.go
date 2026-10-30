package etcd

import (
	"testing"

	"github.com/stretchr/testify/require"
	etcdraft "go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestPipelineDebtReadySharedEntriesAndArrays(t *testing.T) {
	entry := ledgerEntry(2, 2, "value")
	entry.Data = make([]byte, 1<<20)[:5]
	unknown := make([]byte, 1<<20)
	copy(unknown, []byte{0xa0, 0x06, 0x01})
	entry.ProtoReflect().SetUnknown(unknown[:3])
	entries := []*raftpb.Entry{entry}
	rd := etcdraft.Ready{Entries: entries, CommittedEntries: entries,
		Messages: []*raftpb.Message{{Entries: entries}, {Entries: entries}}}
	bill := pipelineReadyBytes(rd)
	want := uint64(pipelineTaskOverhead*3 + pipelinePhysicalEntryBytes + 2<<20 + pipelinePointerBytes*3)
	require.Equal(t, want, bill, "Entry payload, unknown capacity and pointer array must be counted once")
	rd.Messages[1].Entries = []*raftpb.Entry{proto.CloneOf(entry)}
	require.Greater(t, pipelineReadyBytes(rd), bill, "equal content is not shared identity")
}

func TestPipelineDebtReadySharedArrayLargestCapacity(t *testing.T) {
	entries := make([]*raftpb.Entry, 2, 100)
	entries[0], entries[1] = ledgerEntry(2, 2, "one"), ledgerEntry(3, 2, "two")
	rd := etcdraft.Ready{Entries: entries[:1:1], CommittedEntries: entries}
	require.Equal(t, uint64(pipelineTaskOverhead+pipelinePointerBytes*100)+pipelineOwnedEntryBytes(entries[0])+pipelineOwnedEntryBytes(entries[1]), pipelineReadyBytes(rd))
	rd.Entries = entries[:1:100]
	require.Equal(t, pipelineReadyBytes(etcdraft.Ready{Entries: entries}), pipelineReadyBytes(rd))
}

func TestPipelineDebtReadyContextsSnapshotsAndReadStates(t *testing.T) {
	message := &raftpb.Message{Context: make([]byte, 8192)[:1]}
	unknown := make([]byte, 16384)
	copy(unknown, []byte{0xa0, 0x06, 0x01})
	message.ProtoReflect().SetUnknown(unknown[:3])
	snapshot := raftTestSnapshot(10, 2, []uint64{1, 2, 3}, make([]byte, 32768))
	message.Snapshot = &snapshot
	rd := etcdraft.Ready{Messages: []*raftpb.Message{message}, ReadStates: []etcdraft.ReadState{{RequestCtx: make([]byte, 65536)[:1]}}}
	want := uint64(2*pipelineTaskOverhead+pipelinePointerBytes+8192+16384+32+65536) + pipelineOwnedSnapshotBytes(&snapshot)
	require.Equal(t, want, pipelineReadyBytes(rd))
	message.Responses = []*raftpb.Message{proto.CloneOf(message)}
	require.Greater(t, pipelineReadyBytes(rd), want, "an independent opaque response must remain billed")
}

func TestPipelineDebtReadyNativeAliasProjection(t *testing.T) {
	pool := newDebtBoundVolatileCluster(t, 3, 57)
	entries := make([]*raftpb.Entry, 40000)
	for i := range entries {
		entries[i] = &raftpb.Entry{Data: encodeProposalEnvelope(uint64(i+100), []byte("x"))}
	}
	require.NoError(t, pool[0].node.Step(&raftpb.Message{Type: messageTypePtr(raftpb.MsgProp), From: uint64Ptr(1), Entries: entries}))
	rd := pool[0].node.Ready()
	remote := 0
	for _, message := range rd.Messages {
		if message.GetType() != raftpb.MsgApp {
			continue
		}
		remote++
		require.Equal(t, &rd.Entries[0], &message.Entries[0], "pinned Raft pure-unstable slices share the pointer array")
		require.Same(t, rd.Entries[0], message.Entries[0])
	}
	require.Equal(t, 59, remote)
	bill := pipelineReadyBytes(rd)
	require.LessOrEqual(t, bill, uint64(pipelineMaxOwnedInputBytes))
	t.Logf("actual Ready: entries=%d remotes=%d unique object/backing bill=%d R=%d", len(rd.Entries), remote, bill, pipelineReadyOvershootBytes)
}

func TestPipelineDebtStableBorrowIdentityAndTaskTakeover(t *testing.T) {
	e := newPipelineAdmissionFixture(t)
	e.storage = etcdraft.NewMemoryStorage()
	require.NoError(t, e.storage.ApplySnapshot(&raftpb.Snapshot{Metadata: testSnapshotMetadata(1, 1, []uint64{1})}))
	entry := ledgerEntry(2, 2, "")
	entry.Data = encodeProposalEnvelope(1, make([]byte, 1<<20))
	require.NoError(t, e.storage.Append([]*raftpb.Entry{entry}))
	base, err := e.pipelineStableBase()
	require.NoError(t, err)
	rd := etcdraft.Ready{Messages: []*raftpb.Message{{Entries: base}}}
	borrowed := pipelineReadyBytesWithStable(rd, base)
	require.Less(t, borrowed, uint64(1<<20))
	rd.Messages[0].Entries = []*raftpb.Entry{proto.CloneOf(entry)}
	require.Greater(t, pipelineReadyBytesWithStable(rd, base), uint64(1<<20), "equal content/index/term does not establish base identity")
	rd.Messages[0].Entries = append([]*raftpb.Entry(nil), base...)
	require.Greater(t, pipelineReadyBytesWithStable(rd, base), borrowed, "a new pointer array remains charged even when its Entry aliases")
	p := e.storagePipeline
	p.failedReady = &rd
	require.Error(t, e.pipelineMemoryStorageMutable(), "compaction/replacement must wait for the last Ready borrow")
	*p.failedReady = etcdraft.Ready{}
	p.failedReady = nil
	require.NoError(t, e.pipelineMemoryStorageMutable())
	message := ledgerAppend(entry)
	require.NoError(t, e.queuePipelineAppend(message))
	require.NoError(t, e.storage.Compact(2))
	bytes, _ := p.diskBulk.usage()
	require.Greater(t, bytes, uint64(1<<20), "task reservation must own the retained payload after base compaction")
	require.Equal(t, entry.Data, p.diskDeferred[0].message.Entries[0].Data)
	retirePipelineDeferred(&p.diskDeferred, &p.diskOutstanding)
	p.ledger.discard()
	requireStorageWorkUsage(t, p.bulkOwnership, 0, 0)
}
