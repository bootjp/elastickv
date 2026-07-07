package kv

import pb "github.com/bootjp/elastickv/proto"

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

func (f *kvFSM) notifyApplyObservers(commitTS uint64, muts []*pb.Mutation) {
	f.observeAppliedCommitTS(commitTS)
	for _, mut := range muts {
		if mut == nil {
			continue
		}
		f.notifyApplyObserverAfterObserve(mut.Op, mut.Key)
	}
}

func (f *kvFSM) notifyApplyObserver(commitTS uint64, op pb.Op, key []byte) {
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
