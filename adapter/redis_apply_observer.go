package adapter

import (
	"bytes"
	"sync"

	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
)

// RedisApplyObserver wakes blocking Redis waiters from the FSM apply path.
// It is safe to share one instance between the FSM wiring and RedisServer.
type RedisApplyObserver struct {
	initOnce      sync.Once
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
		streamWaiters, zsetWaiters := observer.registries()
		r.streamWaiters = streamWaiters
		r.zsetWaiters = zsetWaiters
	}
}

func (o *RedisApplyObserver) OnApply(op pb.Op, key []byte) {
	if o == nil || op != pb.Op_PUT {
		return
	}
	streamWaiters, zsetWaiters := o.registries()
	if userKey := streamApplyUserKey(key); userKey != nil {
		streamWaiters.Signal(userKey)
		return
	}
	if userKey := zsetApplyUserKey(key); userKey != nil {
		zsetWaiters.Signal(userKey)
	}
}

func (o *RedisApplyObserver) registries() (*keyWaiterRegistry, *keyWaiterRegistry) {
	o.initOnce.Do(func() {
		if o.streamWaiters == nil {
			o.streamWaiters = newKeyWaiterRegistry()
		}
		if o.zsetWaiters == nil {
			o.zsetWaiters = newKeyWaiterRegistry()
		}
	})
	return o.streamWaiters, o.zsetWaiters
}

func streamApplyUserKey(key []byte) []byte {
	if !store.IsStreamEntryKey(key) {
		return nil
	}
	return store.ExtractStreamUserKeyFromEntry(key)
}

func zsetApplyUserKey(key []byte) []byte {
	switch {
	case bytes.HasPrefix(key, []byte(redisZSetPrefix)):
		return key[len(redisZSetPrefix):]
	case store.IsZSetMemberKey(key):
		return store.ExtractZSetUserKeyFromMember(key)
	case store.IsZSetScoreKey(key):
		return store.ExtractZSetUserKeyFromScore(key)
	default:
		return nil
	}
}
