package etcd

import (
	"math"

	"github.com/cockroachdb/errors"
	etcdraft "go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

const pipelineReadStateBytes = 32

// Ready is borrowed, not proto-cloned. Raft's pure stable/unstable log slices
// share Entry objects and often the pointer array across every remote append.
// Charge each Entry/message once and each identical-start array's largest capacity.
// Different-start overlaps and shared byte slices still count conservatively
// more than once. No unsafe allocation-base inference or payload omission is
// involved. The 1024-byte Entry and 512-byte message allowances also cover the
// temporary identity-map slots; the maps never escape this owner-side walk.
// Snapshot and distinct-object byte-slice aliases remain conservatively billed
// separately. Stop walking once the Ready cannot fit, bounding scratch growth.
type pipelineReadyAccounting struct {
	entries       map[*raftpb.Entry]struct{}
	messages      map[*raftpb.Message]struct{}
	entryArrays   map[**raftpb.Entry]int
	messageArrays map[**raftpb.Message]int
	bytes         uint64
	stable        []*raftpb.Entry
}

func pipelineReadyBytes(rd etcdraft.Ready) uint64 {
	return pipelineReadyBytesWithStable(rd, nil)
}

func pipelineReadyBytesWithStable(rd etcdraft.Ready, stable []*raftpb.Entry) uint64 {
	a := pipelineReadyAccounting{
		entries: make(map[*raftpb.Entry]struct{}), messages: make(map[*raftpb.Message]struct{}),
		entryArrays: make(map[**raftpb.Entry]int), messageArrays: make(map[**raftpb.Message]int), bytes: pipelineTaskOverhead,
		stable: stable,
	}
	a.entrySlice(rd.Entries)
	a.entrySlice(rd.CommittedEntries)
	a.messageSlice(rd.Messages)
	a.add(pipelineOwnedSnapshotBytes(rd.Snapshot))
	a.add(pipelineArrayBytes(cap(rd.ReadStates), pipelineReadStateBytes))
	for _, state := range rd.ReadStates {
		a.add(uint64(cap(state.RequestCtx)))
	}
	return a.bytes
}

func (a *pipelineReadyAccounting) add(bytes uint64) {
	a.bytes = pipelineAddBytes(a.bytes, bytes)
}

func (a *pipelineReadyAccounting) entrySlice(entries []*raftpb.Entry) {
	if cap(entries) == 0 {
		return
	}
	key := &entries[:cap(entries)][0]
	if !a.stableArray(entries) {
		if old := a.entryArrays[key]; cap(entries) > old {
			a.add(pipelineArrayBytes(cap(entries)-old, pipelinePointerBytes))
			a.entryArrays[key] = cap(entries)
		}
	}
	for _, entry := range entries {
		if a.bytes > pipelineBulkBytes {
			return
		}
		if entry == nil {
			continue
		}
		if offset, ok := a.stableOffset(entry); ok && a.stable[offset] == entry {
			continue
		}
		if _, exists := a.entries[entry]; exists {
			continue
		}
		a.add(pipelineOwnedEntryBytes(entry))
		a.entries[entry] = struct{}{}
	}
}

func (a *pipelineReadyAccounting) stableOffset(entry *raftpb.Entry) (uint64, bool) {
	if entry == nil || len(a.stable) == 0 || a.stable[0] == nil || entry.GetIndex() < a.stable[0].GetIndex() {
		return 0, false
	}
	offset := entry.GetIndex() - a.stable[0].GetIndex()
	return offset, offset < uint64(len(a.stable))
}

func (a *pipelineReadyAccounting) stableArray(entries []*raftpb.Entry) bool {
	full := entries[:cap(entries)]
	offset, ok := a.stableOffset(full[0])
	return ok && uint64(cap(entries)) <= uint64(len(a.stable))-offset && &full[0] == &a.stable[offset]
}

// The owner captures an exact borrowed MemoryStorage range, not just indices or
// equal contents. That base cannot be replaced/compacted during Ready routing.
// Tasks detach pointer arrays and reserve their own retained Entry/data before
// yielding; outbound clones likewise own independent permits. Thus ONLY this
// non-yielding Ready borrow can exclude base objects. Mixed concat arrays are
// different identities and remain charged, including their complete capacity.
func (e *Engine) pipelineStableBase() ([]*raftpb.Entry, error) {
	if e.storage == nil {
		return nil, nil
	}
	first, err := e.storage.FirstIndex()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	last, err := e.storage.LastIndex()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if first > last {
		return nil, nil
	}
	if last == math.MaxUint64 {
		return nil, errors.WithStack(errStorageWorkInvalidReservation)
	}
	entries, err := e.storage.Entries(first, last+1, math.MaxUint64)
	return entries, errors.WithStack(err)
}

func (e *Engine) borrowPipelineReady() (*etcdraft.Ready, uint64, error) {
	stable, err := e.pipelineStableBase()
	if err != nil {
		return nil, 0, err
	}
	rd := e.rawNode.Ready()
	// Ready has accepted upstream work. Failure must retain it through joins;
	// no MemoryStorage publication can invalidate its base during that time.
	e.storagePipeline.failedReady = &rd
	return &rd, pipelineReadyBytesWithStable(rd, stable), nil
}

func (e *Engine) pipelineMemoryStorageMutable() error {
	if e.storagePipeline.failedReady != nil {
		return errors.Wrap(errInvalidStorageWork, "MemoryStorage mutation during borrowed Ready")
	}
	return nil
}

func (a *pipelineReadyAccounting) messageSlice(messages []*raftpb.Message) {
	if cap(messages) == 0 {
		return
	}
	key := &messages[:cap(messages)][0]
	if old := a.messageArrays[key]; cap(messages) > old {
		a.add(pipelineArrayBytes(cap(messages)-old, pipelinePointerBytes))
		a.messageArrays[key] = cap(messages)
	}
	for _, message := range messages {
		if a.bytes > pipelineBulkBytes {
			return
		}
		if message == nil {
			continue
		}
		if _, exists := a.messages[message]; exists {
			continue
		}
		a.messages[message] = struct{}{}
		a.add(pipelineTaskOverhead)
		a.add(uint64(cap(message.Context)))
		a.add(uint64(cap(message.ProtoReflect().GetUnknown())))
		a.add(pipelineOwnedSnapshotBytes(message.Snapshot))
		a.entrySlice(message.Entries)
		a.messageSlice(message.Responses)
	}
}
