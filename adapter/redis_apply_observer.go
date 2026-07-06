package adapter

import (
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
)

// RedisApplyObserver wakes blocking Redis waiters from the FSM apply path.
// It is safe to share one instance between the FSM wiring and RedisServer.
type RedisApplyObserver struct {
	streamWaiters *keyWaiterRegistry
	zsetWaiters   *keyWaiterRegistry
}

var _ kv.ApplyObserver = (*RedisApplyObserver)(nil)

func NewRedisApplyObserver() *RedisApplyObserver {
	return &RedisApplyObserver{
		streamWaiters: newKeyWaiterRegistry(),
		zsetWaiters:   newKeyWaiterRegistry(),
	}
}

// WithRedisApplyObserver makes RedisServer use the same waiter registries that
// the FSM apply observer signals.
func WithRedisApplyObserver(observer *RedisApplyObserver) RedisServerOption {
	return func(r *RedisServer) {
		if observer == nil {
			return
		}
		observer.ensureRegistries()
		r.streamWaiters = observer.streamWaiters
		r.zsetWaiters = observer.zsetWaiters
	}
}

func (o *RedisApplyObserver) OnApply(op pb.Op, key []byte) {
	if o == nil || op != pb.Op_PUT {
		return
	}
	o.ensureRegistries()
	if userKey := streamApplyUserKey(key); userKey != nil {
		o.streamWaiters.Signal(userKey)
		return
	}
	if userKey := zsetApplyUserKey(key); userKey != nil {
		o.zsetWaiters.Signal(userKey)
	}
}

func (o *RedisApplyObserver) ensureRegistries() {
	if o.streamWaiters == nil {
		o.streamWaiters = newKeyWaiterRegistry()
	}
	if o.zsetWaiters == nil {
		o.zsetWaiters = newKeyWaiterRegistry()
	}
}

func streamApplyUserKey(key []byte) []byte {
	if !store.IsStreamEntryKey(key) {
		return nil
	}
	return store.ExtractStreamUserKeyFromEntry(key)
}

func zsetApplyUserKey(key []byte) []byte {
	switch {
	case store.IsZSetMemberKey(key):
		return store.ExtractZSetUserKeyFromMember(key)
	case store.IsZSetScoreKey(key):
		return store.ExtractZSetUserKeyFromScore(key)
	default:
		return nil
	}
}
