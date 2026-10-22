package kv

import (
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
)

// ApplyObserver receives a notification after a logical mutation is
// successfully applied by kvFSM. Implementations run inline on the Raft apply
// goroutine and must stay non-blocking.
type ApplyObserver interface {
	OnApply(op pb.Op, key []byte)
}

// WithApplyObserver registers an observer for successful logical mutations.
// Nil observers are ignored so callers can pass optional wiring directly.
func WithApplyObserver(observer ApplyObserver) FSMOption {
	return func(f *kvFSM) {
		if observer == nil {
			return
		}
		f.applyObservers = append(f.applyObservers, observer)
	}
}

// beginDeferredApplyNotices starts holding observer notifications back
// until flushDeferredApplyNotices delivers them in order or
// discardDeferredApplyNotices drops them. Used while a batched Raft entry is
// staged, so observers never learn about a write that did not land.
func (f *kvFSM) beginDeferredApplyNotices() {
	f.deferApplyNotices = true
	f.deferredApplyNotices = f.deferredApplyNotices[:0]
}

func (f *kvFSM) discardDeferredApplyNotices() {
	f.deferApplyNotices = false
	f.deferredApplyNotices = nil
}

func (f *kvFSM) flushDeferredApplyNotices() {
	notices := f.deferredApplyNotices
	f.deferApplyNotices = false
	f.deferredApplyNotices = nil
	for _, notice := range notices {
		notice()
	}
}

// deferApplyNotice queues notice while notifications are deferred and
// reports whether it did; the caller delivers immediately otherwise.
func (f *kvFSM) deferApplyNotice(notice func()) bool {
	if !f.deferApplyNotices {
		return false
	}
	f.deferredApplyNotices = append(f.deferredApplyNotices, notice)
	return true
}

func (f *kvFSM) notifyApplyObservers(commitTS uint64, muts []*pb.Mutation) {
	if f.deferApplyNotice(func() { f.notifyApplyObservers(commitTS, muts) }) {
		return
	}
	f.observeAppliedCommitTS(commitTS)
	for _, mut := range muts {
		if mut == nil {
			continue
		}
		f.notifyApplyObserverAfterObserve(mut.Op, mut.Key)
	}
}

func (f *kvFSM) notifyApplyObserversForStoreMutations(commitTS uint64, muts []*store.KVPairMutation) {
	if f.deferApplyNotice(func() { f.notifyApplyObserversForStoreMutations(commitTS, muts) }) {
		return
	}
	f.observeAppliedCommitTS(commitTS)
	for _, mut := range muts {
		if mut == nil || isTxnInternalKey(mut.Key) {
			continue
		}
		switch mut.Op {
		case store.OpTypePut:
			f.notifyApplyObserverAfterObserve(pb.Op_PUT, mut.Key)
		case store.OpTypeDelete:
			f.notifyApplyObserverAfterObserve(pb.Op_DEL, mut.Key)
		}
	}
}

func (f *kvFSM) notifyApplyObserver(commitTS uint64, op pb.Op, key []byte) {
	if f.deferApplyNotice(func() { f.notifyApplyObserver(commitTS, op, key) }) {
		return
	}
	f.observeAppliedCommitTS(commitTS)
	f.notifyApplyObserverAfterObserve(op, key)
}

// observeAppliedCommitTS pins the HLC-4 strategy (c) handoff invariant before
// any observer can wake code that may allocate another persistence timestamp.
func (f *kvFSM) observeAppliedCommitTS(commitTS uint64) {
	if f.hlc != nil && commitTS > 0 {
		f.hlc.Observe(commitTS)
	}
}

func (f *kvFSM) notifyApplyObserverAfterObserve(op pb.Op, key []byte) {
	if len(f.applyObservers) == 0 {
		return
	}
	for _, observer := range f.applyObservers {
		if observer == nil {
			continue
		}
		f.notifyApplyObserverSafely(observer, op, key)
	}
}

func (f *kvFSM) notifyApplyObserverSafely(observer ApplyObserver, op pb.Op, key []byte) {
	defer func() {
		if r := recover(); r != nil && f.log != nil {
			f.log.Warn("apply observer panic", "panic", r)
		}
	}()
	observer.OnApply(op, key)
}
