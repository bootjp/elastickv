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

func (f *kvFSM) notifyApplyObservers(muts []*pb.Mutation) {
	for _, mut := range muts {
		if mut == nil {
			continue
		}
		f.notifyApplyObserver(mut.Op, mut.Key)
	}
}

func (f *kvFSM) notifyApplyObserver(op pb.Op, key []byte) {
	if len(f.applyObservers) == 0 {
		return
	}
	for _, observer := range f.applyObservers {
		if observer == nil {
			continue
		}
		func() {
			defer func() {
				if r := recover(); r != nil && f.log != nil {
					f.log.Warn("apply observer panic", "panic", r)
				}
			}()
			observer.OnApply(op, key)
		}()
	}
}
