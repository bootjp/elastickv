package etcd

import (
	"math"
	"runtime"
	"sync"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	etcdraft "go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func ledgerAppend(entries ...*raftpb.Entry) *raftpb.Message {
	return &raftpb.Message{
		Type: raftpb.MsgStorageAppend.Enum(), From: proto.Uint64(2),
		To: proto.Uint64(etcdraft.LocalAppendThread), Entries: entries,
	}
}

func ledgerEntry(index, term uint64, data string) *raftpb.Entry {
	return &raftpb.Entry{Index: proto.Uint64(index), Term: proto.Uint64(term), Data: []byte(data)}
}

func ledgerResponse(index, entryTerm, generationTerm uint64) *raftpb.Message {
	return &raftpb.Message{
		Type: raftpb.MsgStorageAppendResp.Enum(), From: proto.Uint64(etcdraft.LocalAppendThread),
		To: proto.Uint64(2), Index: proto.Uint64(index), LogTerm: proto.Uint64(entryTerm), Term: proto.Uint64(generationTerm),
	}
}

func TestAsyncUnstableLedgerPartialPrefixRetained(t *testing.T) {
	t.Parallel()
	b := newStorageWorkBudget(8192, 10)
	l := newAsyncUnstableLedger(b)
	t.Cleanup(l.discard)
	first := ledgerAppend(ledgerEntry(1, 1, "large-prefix"), ledgerEntry(2, 1, "second"))
	second := ledgerAppend(ledgerEntry(3, 1, "third"), ledgerEntry(4, 1, "tail"))
	require.NoError(t, l.retain(first))
	require.NoError(t, l.retain(second))
	bytes, items := b.usage()
	require.Equal(t, 2, items, "two append generations, each retaining two entries")
	require.True(t, l.stabilize(ledgerResponse(2, 1, 1), 1))
	require.Equal(t, []asyncUnstableEntry{{3, 1}, {4, 1}}, l.entries)
	requireStorageWorkUsage(t, b, bytes, items)
	require.False(t, l.stabilize(ledgerResponse(2, 1, 1), 1), "duplicate prefix is not a match")
	require.True(t, l.stabilize(ledgerResponse(3, 1, 1), 1))
	requireStorageWorkUsage(t, b, bytes, items)
	require.True(t, l.stabilize(ledgerResponse(4, 1, 1), 1))
	require.Nil(t, l.entries)
	requireStorageWorkUsage(t, b, 0, 0)
	// A new empty-to-nonempty epoch does not inherit retired reservations.
	next := ledgerEntry(5, 2, "next")
	require.NoError(t, l.retain(ledgerAppend(next)))
	requireStorageWorkUsage(t, b, pipelinePhysicalEntryBytes+uint64(cap(next.Data))+pipelinePointerBytes, 1)
}

func TestAsyncUnstableLedgerOverwrittenSuffixABA(t *testing.T) {
	t.Parallel()
	b := newStorageWorkBudget(8192, 20)
	l := newAsyncUnstableLedger(b)
	t.Cleanup(l.discard)
	for _, message := range []*raftpb.Message{
		ledgerAppend(ledgerEntry(1, 1, "prefix"), ledgerEntry(2, 2, "old-A"), ledgerEntry(3, 2, "old-tail")),
		ledgerAppend(ledgerEntry(2, 3, "B")),
		ledgerAppend(ledgerEntry(2, 2, "new-A-different-allocation")),
	} {
		require.NoError(t, l.retain(message))
	}
	require.Equal(t, []asyncUnstableEntry{{1, 1}, {2, 2}}, l.entries)
	bytes, items := b.usage()
	require.Equal(t, 3, items, "displaced generations remain charged")
	for _, stale := range []*raftpb.Message{
		ledgerResponse(2, 2, 2), ledgerResponse(2, 3, 3), ledgerResponse(3, 2, 4), ledgerResponse(2, 3, 4),
	} {
		require.False(t, l.stabilize(stale, 4))
		requireStorageWorkUsage(t, b, bytes, items)
	}
	require.True(t, l.stabilize(ledgerResponse(2, 2, 4), 4))
	requireStorageWorkUsage(t, b, 0, 0)
}

func TestAsyncUnstableLedgerReplaceBeforeOffset(t *testing.T) {
	t.Parallel()
	b := newStorageWorkBudget(4096, 20)
	l := newAsyncUnstableLedger(b)
	t.Cleanup(l.discard)
	require.NoError(t, l.retain(ledgerAppend(ledgerEntry(5, 1, "old"), ledgerEntry(6, 1, "tail"))))
	require.True(t, l.stabilize(ledgerResponse(5, 1, 1), 1))
	require.NoError(t, l.retain(ledgerAppend(ledgerEntry(4, 2, "replacement"))))
	require.Equal(t, []asyncUnstableEntry{{4, 2}}, l.entries)
	_, items := b.usage()
	require.Equal(t, 2, items)
	require.False(t, l.stabilize(ledgerResponse(6, 1, 2), 2))
	require.True(t, l.stabilize(ledgerResponse(4, 2, 2), 2))
	requireStorageWorkUsage(t, b, 0, 0)
}

func TestAsyncUnstableLedgerFailedAcquireRollback(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"bytes", "items"} {
		t.Run(kind, func(t *testing.T) {
			b := newStorageWorkBudget(8192, 1)
			if kind == "bytes" {
				b = newStorageWorkBudget(4096, 20)
			}
			l := newAsyncUnstableLedger(b)
			t.Cleanup(l.discard)
			require.NoError(t, l.retain(ledgerAppend(ledgerEntry(1, 1, "old"), ledgerEntry(2, 1, "tail"))))
			before := append([]asyncUnstableEntry(nil), l.entries...)
			bytes, items := b.usage()
			message := ledgerAppend(ledgerEntry(2, 2, "replacement"), ledgerEntry(3, 2, "long replacement"))
			for range 3 {
				require.ErrorIs(t, l.retain(message), errStorageWorkBackpressure)
				require.Equal(t, before, l.entries)
				requireStorageWorkUsage(t, b, bytes, items)
			}
			require.True(t, l.stabilize(ledgerResponse(2, 1, 2), 2))
			requireStorageWorkUsage(t, b, 0, 0)
			require.NoError(t, l.retain(message), "failed reservation did not consume capacity")
		})
	}
}

func TestAsyncUnstableLedgerMalformedRetain(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		message *raftpb.Message
	}{
		{"nil", nil},
		{"wrong type", &raftpb.Message{Type: raftpb.MsgApp.Enum(), From: proto.Uint64(2), To: proto.Uint64(etcdraft.LocalAppendThread)}},
		{"wrong destination", &raftpb.Message{Type: raftpb.MsgStorageAppend.Enum(), From: proto.Uint64(2), To: proto.Uint64(1)}},
		{"no owner", &raftpb.Message{Type: raftpb.MsgStorageAppend.Enum(), To: proto.Uint64(etcdraft.LocalAppendThread)}},
		{"local owner", &raftpb.Message{Type: raftpb.MsgStorageAppend.Enum(), From: proto.Uint64(etcdraft.LocalApplyThread), To: proto.Uint64(etcdraft.LocalAppendThread)}},
		{"nil entry", ledgerAppend(nil)},
		{"zero index", ledgerAppend(ledgerEntry(0, 1, ""))},
		{"zero term", ledgerAppend(ledgerEntry(1, 0, ""))},
		{"gap", ledgerAppend(ledgerEntry(1, 1, ""), ledgerEntry(3, 1, ""))},
		{"descending", ledgerAppend(ledgerEntry(2, 1, ""), ledgerEntry(1, 1, ""))},
		{"duplicate", ledgerAppend(ledgerEntry(1, 1, ""), ledgerEntry(1, 1, ""))},
		{"wraparound", ledgerAppend(ledgerEntry(math.MaxUint64, 1, ""), ledgerEntry(1, 1, ""))},
	} {
		t.Run(test.name, func(t *testing.T) {
			b := newStorageWorkBudget(4096, 10)
			l := newAsyncUnstableLedger(b)
			require.ErrorIs(t, l.retain(test.message), errInvalidAsyncUnstable)
			require.Empty(t, l.entries)
			require.Zero(t, l.ownerID)
			requireStorageWorkUsage(t, b, 0, 0)
		})
	}
}

func TestAsyncUnstableLedgerInvalidOwnershipAndSnapshots(t *testing.T) {
	t.Parallel()
	b := newStorageWorkBudget(4096, 10)
	l := newAsyncUnstableLedger(b)
	t.Cleanup(l.discard)
	require.NoError(t, l.retain(ledgerAppend(ledgerEntry(3, 1, "payload"))))
	bytes, items := b.usage()
	other := ledgerAppend(ledgerEntry(4, 1, "other owner"))
	other.From = proto.Uint64(1)
	require.ErrorIs(t, l.retain(other), errInvalidAsyncUnstable)
	require.ErrorIs(t, l.retain(ledgerAppend(ledgerEntry(5, 1, "missing index 4"))), errInvalidAsyncUnstable)
	for _, snapshot := range []*raftpb.Snapshot{{}, {Metadata: &raftpb.SnapshotMetadata{Index: proto.Uint64(3)}}} {
		message := ledgerAppend()
		message.Snapshot = snapshot
		require.ErrorIs(t, l.retain(message), errAsyncUnstableSnapshot)
	}
	requireStorageWorkUsage(t, b, bytes, items)
	for _, budget := range []*storageWorkBudget{nil, newStorageWorkBudget(0, 1), newStorageWorkBudget(1, 0), newStorageWorkBudget(1, 1)} {
		invalid := newAsyncUnstableLedger(budget)
		require.ErrorIs(t, invalid.retain(ledgerAppend(ledgerEntry(1, 1, "too large"))), errStorageWorkInvalidReservation)
		require.Empty(t, invalid.entries)
		requireStorageWorkUsage(t, budget, 0, 0)
	}
	var absent *asyncUnstableLedger
	require.ErrorIs(t, absent.retain(ledgerAppend()), errInvalidAsyncUnstable)
	require.False(t, absent.stabilize(ledgerResponse(3, 1, 1), 1))
	absent.discard()
}

func TestAsyncUnstableLedgerMalformedStabilize(t *testing.T) {
	t.Parallel()
	for _, change := range []struct {
		name string
		edit func(*raftpb.Message)
	}{
		{"wrong type", func(m *raftpb.Message) { m.Type = raftpb.MsgAppResp.Enum() }},
		{"remote source", func(m *raftpb.Message) { m.From = proto.Uint64(1) }},
		{"wrong thread", func(m *raftpb.Message) { m.From = proto.Uint64(etcdraft.LocalApplyThread) }},
		{"wrong destination", func(m *raftpb.Message) { m.To = proto.Uint64(1) }},
		{"no term", func(m *raftpb.Message) { m.Term = nil }},
		{"old term", func(m *raftpb.Message) { m.Term = proto.Uint64(1) }},
		{"future term", func(m *raftpb.Message) { m.Term = proto.Uint64(3) }},
		{"no index", func(m *raftpb.Message) { m.Index = nil }},
		{"no entry term", func(m *raftpb.Message) { m.LogTerm = nil }},
		{"no match", func(m *raftpb.Message) { m.Index = proto.Uint64(4) }},
		{"mismatch", func(m *raftpb.Message) { m.LogTerm = proto.Uint64(2) }},
		{"snapshot", func(m *raftpb.Message) { m.Snapshot = &raftpb.Snapshot{} }},
	} {
		t.Run(change.name, func(t *testing.T) {
			b := newStorageWorkBudget(4096, 10)
			l := newAsyncUnstableLedger(b)
			t.Cleanup(l.discard)
			require.NoError(t, l.retain(ledgerAppend(ledgerEntry(3, 1, "entry"))))
			bytes, items := b.usage()
			response := ledgerResponse(3, 1, 2)
			change.edit(response)
			require.False(t, l.stabilize(response, 2))
			require.False(t, l.stabilize(nil, 2))
			require.False(t, l.stabilize(ledgerResponse(3, 1, 0), 0))
			requireStorageWorkUsage(t, b, bytes, items)
			require.Equal(t, []asyncUnstableEntry{{3, 1}}, l.entries)
		})
	}
}

func TestAsyncUnstableLedgerEntrylessAndActorOwnership(t *testing.T) {
	t.Parallel()
	b := newStorageWorkBudget(4096, 10)
	l := newAsyncUnstableLedger(b)
	t.Cleanup(l.discard)
	require.NoError(t, l.retain(ledgerAppend()))
	requireStorageWorkUsage(t, b, 0, 0)
	require.False(t, l.stabilize(ledgerResponse(1, 1, 1), 1))
	message := ledgerAppend(ledgerEntry(1, 1, "entry"))
	require.NoError(t, l.retain(message))
	actorBytes := uint64(proto.Size(message))
	actor, err := b.tryAcquire(actorBytes)
	require.NoError(t, err)
	t.Cleanup(actor.Release)
	entryless := ledgerAppend()
	entryless.Term = proto.Uint64(2)
	entryless.Responses = []*raftpb.Message{ledgerResponse(1, 1, 2)}
	require.NoError(t, l.retain(entryless))
	require.False(t, l.stabilize(ledgerResponse(1, 1, 1), 2))
	require.True(t, l.stabilize(entryless.Responses[0], 2))
	requireStorageWorkUsage(t, b, actorBytes, 1)
	l.discard()
	l.discard()
	requireStorageWorkUsage(t, b, actorBytes, 1)
	actor.Release()
	requireStorageWorkUsage(t, b, 0, 0)
}

func TestAsyncUnstableLedgerNear64MiBNoPayloadCopy(t *testing.T) {
	const payloadSize = 64<<20 - 1024
	entry := ledgerEntry(1, 1, "")
	entry.Data = make([]byte, payloadSize)
	message := ledgerAppend(entry)
	bytes := uint64(cap(entry.Data)) + pipelinePhysicalEntryBytes + pipelinePointerBytes
	b := newStorageWorkBudget(bytes, 1)
	l := newAsyncUnstableLedger(b)
	t.Cleanup(l.discard)
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	require.NoError(t, l.retain(message))
	runtime.ReadMemStats(&after)
	require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(1<<20), "retain must not allocate a payload-sized copy")
	requireStorageWorkUsage(t, b, bytes, 1)
	require.NoError(t, l.retain(ledgerAppend()), "entryless work adds no unstable reservation even at capacity")
	requireStorageWorkUsage(t, b, bytes, 1)
	require.Equal(t, []asyncUnstableEntry{{1, 1}}, l.entries)
	require.Same(t, entry, message.Entries[0])
	runtime.KeepAlive(message)
	require.True(t, l.stabilize(ledgerResponse(1, 1, 1), 1))
	requireStorageWorkUsage(t, b, 0, 0)
}

func TestAsyncUnstableLedgerConcurrentBudgetOwnership(t *testing.T) {
	t.Parallel()
	b := newStorageWorkBudget(4096, 8)
	var workers sync.WaitGroup
	for range 16 {
		workers.Go(func() {
			l := newAsyncUnstableLedger(b)
			defer l.discard()
			message := ledgerAppend(ledgerEntry(1, 1, "first"), ledgerEntry(2, 1, "second"))
			for range 100 {
				err := l.retain(message)
				if err != nil && !errors.Is(err, errStorageWorkBackpressure) {
					t.Errorf("unexpected reservation error: %v", err)
				}
				bytes, items := b.usage()
				require.LessOrEqual(t, bytes, b.maxBytes)
				require.GreaterOrEqual(t, items, 0)
				require.LessOrEqual(t, items, b.maxItems)
				require.GreaterOrEqual(t, bytes, uint64(items*2*pipelinePhysicalEntryBytes), "each generation atomically retains both entry descriptors")
				if err == nil && !l.stabilize(ledgerResponse(2, 1, 1), 1) {
					t.Error("matching response failed to stabilize")
				}
			}
		})
	}
	workers.Wait()
	requireStorageWorkUsage(t, b, 0, 0)
}

// Observing Storage.Term distinguishes upstream's unstable lookup from its
// stable lookup without reflection or inventing a public stable-index API.
type ledgerWitnessStorage struct {
	*etcdraft.MemoryStorage
	tailTermReads int
}

func (s *ledgerWitnessStorage) Term(index uint64) (uint64, error) {
	if index == 2 {
		s.tailTermReads++
	}
	return s.MemoryStorage.Term(index)
}

type ledgerWitnessTask struct {
	message *raftpb.Message
	permit  *storageWorkPermit
}

type ledgerNativeWitness struct {
	node   *etcdraft.RawNode
	store  *ledgerWitnessStorage
	ledger *asyncUnstableLedger
	actors *storageWorkBudget
	tasks  []ledgerWitnessTask
}

func newLedgerNativeWitness(t *testing.T) *ledgerNativeWitness {
	t.Helper()
	s := &ledgerWitnessStorage{MemoryStorage: etcdraft.NewMemoryStorage()}
	require.NoError(t, s.ApplySnapshot(&raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{
		Index: proto.Uint64(1), Term: proto.Uint64(1), ConfState: &raftpb.ConfState{Voters: []uint64{1, 2, 3, 4, 5}},
	}}))
	require.NoError(t, s.SetHardState(&raftpb.HardState{Term: proto.Uint64(1), Commit: proto.Uint64(1)}))
	node, err := etcdraft.NewRawNode(&etcdraft.Config{
		ID: 2, ElectionTick: 10, HeartbeatTick: 1, Storage: s, Applied: 1,
		AsyncStorageWrites: true, MaxSizePerMsg: math.MaxUint64, MaxInflightMsgs: 16,
	})
	require.NoError(t, err)
	w := &ledgerNativeWitness{
		node: node, store: s, ledger: newAsyncUnstableLedger(newStorageWorkBudget(4096, 20)),
		actors: newStorageWorkBudget(16384, 100),
	}
	t.Cleanup(func() {
		for len(w.tasks) > 0 {
			w.complete(t)
		}
		w.node = nil // No actors/goroutines remain and no further RawNode use.
		w.ledger.discard()
		requireStorageWorkUsage(t, w.ledger.budget, 0, 0)
		requireStorageWorkUsage(t, w.actors, 0, 0)
	})
	return w
}

func (w *ledgerNativeWitness) collect(t *testing.T) *raftpb.Message {
	t.Helper()
	var appendMessage *raftpb.Message
	for w.node.HasReady() {
		rd := w.node.Ready() // Async mode NEVER calls Advance.
		for _, message := range rd.Messages {
			if message.GetType() != raftpb.MsgStorageAppend {
				require.NotEqual(t, raftpb.MsgStorageApply, message.GetType(), "fixture commits no new entries")
				continue
			}
			require.NoError(t, w.ledger.retain(message))
			permit, err := w.actors.tryAcquire(uint64(proto.Size(message)))
			require.NoError(t, err)
			w.tasks = append(w.tasks, ledgerWitnessTask{message, permit})
			appendMessage = message
		}
	}
	require.NotNil(t, appendMessage)
	return appendMessage
}

func (w *ledgerNativeWitness) append(t *testing.T, generationTerm, entryTerm, leader uint64, payload string) *raftpb.Message {
	t.Helper()
	require.NoError(t, w.node.Step(&raftpb.Message{
		Type: raftpb.MsgApp.Enum(), From: proto.Uint64(leader), To: proto.Uint64(2), Term: proto.Uint64(generationTerm),
		Index: proto.Uint64(1), LogTerm: proto.Uint64(1), Entries: []*raftpb.Entry{ledgerEntry(2, entryTerm, payload)},
	}))
	return w.collect(t)
}

// This is a synchronous native protocol witness, not a WAL durability test.
// FIFO MemoryStorage publication precedes original response delivery; actor
// reservations retire separately, after that delivery, just as required at the
// owner boundary. No synthetic storage response or Advance is used.
func (w *ledgerNativeWitness) complete(t *testing.T) bool {
	t.Helper()
	task := w.tasks[0]
	w.tasks = w.tasks[1:]
	defer task.permit.Release()
	m := task.message
	require.NoError(t, w.store.Append(m.Entries))
	if m.GetTerm() != 0 {
		require.NoError(t, w.store.SetHardState(&raftpb.HardState{Term: m.Term, Vote: m.Vote, Commit: m.Commit}))
	}
	matched := false
	for _, response := range m.Responses {
		if response.GetType() == raftpb.MsgStorageAppendResp {
			matched = w.ledger.stabilize(response, w.node.BasicStatus().GetTerm())
			require.NoError(t, w.node.Step(response))
		}
	}
	return matched
}

func (w *ledgerNativeWitness) probe(t *testing.T, term, entryTerm, leader uint64, stable bool) {
	t.Helper()
	w.store.tailTermReads = 0
	require.NoError(t, w.node.Step(&raftpb.Message{
		Type: raftpb.MsgApp.Enum(), From: proto.Uint64(leader), To: proto.Uint64(2), Term: proto.Uint64(term),
		Index: proto.Uint64(2), LogTerm: proto.Uint64(entryTerm),
	}))
	w.collect(t)
	require.Equal(t, stable, w.store.tailTermReads > 0, "native lookup must use Storage only after stability")
}

func TestAsyncUnstableLedgerNativeABA(t *testing.T) {
	w := newLedgerNativeWitness(t)
	w.append(t, 2, 2, 1, "old-A")
	w.append(t, 3, 3, 3, "B")
	w.append(t, 4, 2, 1, "new-A")
	bytes, items := w.ledger.budget.usage()
	require.Equal(t, 3, items)
	require.False(t, w.complete(t))
	w.probe(t, 4, 2, 1, false)
	requireStorageWorkUsage(t, w.ledger.budget, bytes, items)
	require.False(t, w.complete(t))
	w.probe(t, 4, 2, 1, false)
	requireStorageWorkUsage(t, w.ledger.budget, bytes, items)
	require.True(t, w.complete(t))
	requireStorageWorkUsage(t, w.ledger.budget, 0, 0)
	w.probe(t, 4, 2, 1, true)
}

func TestAsyncUnstableLedgerNativeEntrylessCurrentTerm(t *testing.T) {
	w := newLedgerNativeWitness(t)
	w.append(t, 2, 2, 1, "entry")
	require.NoError(t, w.node.Step(&raftpb.Message{
		Type: raftpb.MsgHeartbeat.Enum(), From: proto.Uint64(3), To: proto.Uint64(2), Term: proto.Uint64(3), Commit: proto.Uint64(1),
	}))
	entryless := w.collect(t)
	require.Empty(t, entryless.Entries)
	var response *raftpb.Message
	for _, candidate := range entryless.Responses {
		if candidate.GetType() == raftpb.MsgStorageAppendResp {
			response = candidate
		}
	}
	require.NotNil(t, response)
	require.Equal(t, uint64(3), response.GetTerm())
	require.Equal(t, uint64(2), response.GetIndex())
	require.Equal(t, uint64(2), response.GetLogTerm())
	bytes, items := w.ledger.budget.usage()
	require.False(t, w.complete(t))
	w.probe(t, 3, 2, 3, false)
	requireStorageWorkUsage(t, w.ledger.budget, bytes, items)
	require.True(t, w.complete(t))
	requireStorageWorkUsage(t, w.ledger.budget, 0, 0)
	w.probe(t, 3, 2, 3, true)
}
