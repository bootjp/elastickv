package etcd

import (
	"math"

	"github.com/cockroachdb/errors"
	etcdraft "go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

var (
	errInvalidAsyncUnstable  = errors.New("invalid asynchronous unstable ownership")
	errAsyncUnstableSnapshot = errors.New("asynchronous unstable snapshot ownership is not implemented")
)

type asyncUnstableEntry struct {
	index, term uint64
}

// asyncUnstableLedger is an owner-only shadow of raft v3.7.0 unstable entries.
// Fold every mutation's Ready before the next mutation or storage completion.
// It retains metadata, not entry/message pointers or payload copies. All entry
// backing capacity and one item per append generation remain charged for the entire nonempty
// epoch, including displaced suffixes and logically stable prefixes: upstream
// log_unstable.go stableTo/shrinkEntriesArray can retain their backing array.
// Append actor references MUST have separate reservations and retirement.
type asyncUnstableLedger struct {
	budget        *storageWorkBudget
	ownerID       uint64
	entries       []asyncUnstableEntry
	retainedBytes uint64
	retainedItems int
	permits       []*storageWorkPermit
}

func newAsyncUnstableLedger(budget *storageWorkBudget) *asyncUnstableLedger {
	return &asyncUnstableLedger{budget: budget}
}

// retain accepts only genuine owner-emitted local append work. In particular,
// message.Term can be zero on an entry-only Ready; it is NOT entry generation
// identity. The generation-time term is carried by the opaque append response.
// On error the owner must halt or retain/retry the Ready, never drop local work.
func (l *asyncUnstableLedger) retain(message *raftpb.Message) error {
	if !l.validAppend(message) {
		return errors.WithStack(errInvalidAsyncUnstable)
	}
	if message.Snapshot != nil {
		return errors.WithStack(errAsyncUnstableSnapshot)
	}
	bytes, err := asyncUnstableEntryBytes(message.Entries)
	if err != nil {
		return err
	}
	keep, err := l.prefixToKeep(message.Entries)
	if err != nil {
		return err
	}
	if err := l.reserve(bytes, len(message.Entries)); err != nil {
		return err
	}
	l.ownerID = message.GetFrom()
	if len(message.Entries) == 0 {
		return nil
	}
	l.entries = l.entries[:keep]
	for _, entry := range message.Entries {
		l.entries = append(l.entries, asyncUnstableEntry{index: entry.GetIndex(), term: entry.GetTerm()})
	}
	return nil
}

func (l *asyncUnstableLedger) validAppend(message *raftpb.Message) bool {
	return l != nil && message != nil && message.GetType() == raftpb.MsgStorageAppend &&
		message.GetTo() == etcdraft.LocalAppendThread && message.GetFrom() != 0 &&
		!etcdraft.IsLocalMsgTarget(message.GetFrom()) && (l.ownerID == 0 || message.GetFrom() == l.ownerID)
}

func asyncUnstableEntryBytes(entries []*raftpb.Entry) (uint64, error) {
	var bytes, previous uint64
	for i, entry := range entries {
		if entry == nil || entry.GetIndex() == 0 || entry.GetTerm() == 0 ||
			(i > 0 && (previous == math.MaxUint64 || entry.GetIndex() != previous+1)) {
			return 0, errors.WithStack(errInvalidAsyncUnstable)
		}
		size := pipelineOwnedEntryBytes(entry)
		if size > math.MaxUint64-bytes {
			return 0, errors.WithStack(errStorageWorkInvalidReservation)
		}
		bytes += size
		previous = entry.GetIndex()
	}
	return bytes, nil
}

func (l *asyncUnstableLedger) prefixToKeep(entries []*raftpb.Entry) (int, error) {
	if len(entries) == 0 || len(l.entries) == 0 {
		return len(l.entries), nil
	}
	first, last := entries[0].GetIndex(), l.entries[len(l.entries)-1].index
	if first > last && first-last > 1 {
		return 0, errors.WithStack(errInvalidAsyncUnstable)
	}
	if first <= l.entries[0].index {
		return 0, nil
	}
	keep := first - l.entries[0].index
	if keep > math.MaxInt {
		return 0, errors.WithStack(errInvalidAsyncUnstable)
	}
	return int(keep), nil
}

// Reserve the whole append atomically, unlike a loop of single-item permits.
// Preflight uses subtraction so neither counters nor shadow need rollback on
// failure, and competing budget users cannot observe a partial reservation.
func (l *asyncUnstableLedger) reserve(bytes uint64, items int) error {
	if items == 0 {
		return nil
	}
	// One append generation is one outstanding work item, not one item per
	// entry. Entry objects, pointer capacity and shadow descriptors are bytes.
	bytes = pipelineAddBytes(bytes, pipelineArrayBytes(items, pipelinePointerBytes))
	permit, err := l.budget.tryAcquire(bytes)
	if err != nil {
		return err
	}
	l.permits = append(l.permits, permit)
	l.retainedBytes += bytes
	l.retainedItems++
	return nil
}

// stabilize mirrors rawnode.go newStorageAppendRespMsg and raft.go Step's
// current-term stableTo path. The caller must supply an authentic FIFO durable
// completion, publish its entries to MemoryStorage first, and Step its original
// responses on the owner. Header checks cannot prove durability or provenance.
// Index/LogTerm identify the response's tail within the CURRENT shadow, not an
// old append task. Entryless appends can attest an earlier outstanding prefix.
// True means a matching prefix was removed, not that actor ownership retired.
func (l *asyncUnstableLedger) stabilize(response *raftpb.Message, currentTerm uint64) bool {
	if l == nil || len(l.entries) == 0 || !l.validResponse(response, currentTerm) {
		return false
	}
	index := response.GetIndex()
	first, last := l.entries[0].index, l.entries[len(l.entries)-1].index
	if index < first || index > last {
		return false
	}
	offset := index - first
	if offset > math.MaxInt {
		return false
	}
	position := int(offset)
	if l.entries[position].term != response.GetLogTerm() {
		return false
	}
	l.entries = l.entries[position+1:]
	if len(l.entries) == 0 {
		l.releaseEpoch()
	}
	return true
}

func (l *asyncUnstableLedger) validResponse(response *raftpb.Message, currentTerm uint64) bool {
	return response != nil && response.GetType() == raftpb.MsgStorageAppendResp &&
		response.GetFrom() == etcdraft.LocalAppendThread && response.GetTo() == l.ownerID &&
		currentTerm != 0 && response.GetTerm() == currentTerm && response.Snapshot == nil
}

// discard is legal ONLY after RawNode is disposed and all actors have stopped.
// It does not retire actor reservations or authorize closing storage early.
func (l *asyncUnstableLedger) discard() {
	if l != nil {
		l.releaseEpoch()
	}
}

func (l *asyncUnstableLedger) releaseEpoch() {
	if l.retainedItems != 0 {
		for _, permit := range l.permits {
			permit.Release()
		}
	}
	l.entries = nil
	l.retainedBytes, l.retainedItems = 0, 0
	l.permits = nil
}
