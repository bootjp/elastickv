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
	streamWaiters *keyWaiterRegistry
	zsetWaiters   *keyWaiterRegistry

	mu               sync.Mutex
	fastZSetApplyRef map[string]int
}

var _ kv.ApplyObserver = (*RedisApplyObserver)(nil)

func NewRedisApplyObserver() *RedisApplyObserver {
	return &RedisApplyObserver{
		streamWaiters:    newKeyWaiterRegistry(),
		zsetWaiters:      newKeyWaiterRegistry(),
		fastZSetApplyRef: map[string]int{},
	}
}

// WithRedisApplyObserver makes RedisServer use the same waiter registries that
// the FSM apply observer signals.
func WithRedisApplyObserver(observer *RedisApplyObserver) RedisServerOption {
	return func(r *RedisServer) {
		if observer == nil {
			return
		}
		r.applyObserver = observer
		r.streamWaiters = observer.streamWaiters
		r.zsetWaiters = observer.zsetWaiters
	}
}

func (o *RedisApplyObserver) OnApply(op pb.Op, key []byte) {
	if op != pb.Op_PUT {
		return
	}
	if userKey := streamApplyUserKey(key); userKey != nil {
		o.streamWaiters.Signal(userKey)
		return
	}
	if userKey := zsetApplyUserKey(key); userKey != nil {
		if o.fastZSetApply(userKey) {
			o.zsetWaiters.Signal(userKey)
			return
		}
		o.zsetWaiters.SignalFull(userKey)
	}
}

func (o *RedisApplyObserver) beginFastZSetApply(key []byte) func() {
	if o == nil {
		return func() {}
	}
	s := string(key)
	o.mu.Lock()
	if o.fastZSetApplyRef == nil {
		o.fastZSetApplyRef = map[string]int{}
	}
	o.fastZSetApplyRef[s]++
	o.mu.Unlock()
	return func() {
		o.mu.Lock()
		defer o.mu.Unlock()
		n := o.fastZSetApplyRef[s]
		if n <= 1 {
			delete(o.fastZSetApplyRef, s)
			return
		}
		o.fastZSetApplyRef[s] = n - 1
	}
}

func (o *RedisApplyObserver) fastZSetApply(key []byte) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.fastZSetApplyRef[string(key)] > 0
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
