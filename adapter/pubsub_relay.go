package adapter

import "sync"

// RedisPubSubRelay lets the internal gRPC service publish into the local Redis
// pubsub bus without depending on RedisServer startup order.
type RedisPubSubRelay struct {
	mu      sync.RWMutex
	publish func(channel, message []byte) int64
}

func NewRedisPubSubRelay() *RedisPubSubRelay {
	return &RedisPubSubRelay{}
}

func (r *RedisPubSubRelay) Bind(publish func(channel, message []byte) int64) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.publish = publish
	r.mu.Unlock()
}

func (r *RedisPubSubRelay) Publish(channel, message []byte) int64 {
	if r == nil {
		return 0
	}
	r.mu.RLock()
	publish := r.publish
	r.mu.RUnlock()
	if publish == nil {
		return 0
	}
	return publish(channel, message)
}
