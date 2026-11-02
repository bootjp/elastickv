package adapter

import "sync"

const (
	redisDefaultReplyBudgetBytes = 2 * redisMaxReplyBytes
	redisReplyAllocationSlack    = 128
)

func redisReplyCapacityCharge(capacity int) int {
	return capacity + redisReplyAllocationSlack
}

// Bounds live protected RESP backing capacities, including old+new capacity
// during growth. Parser/backend working sets and eventual GC/RSS are separate.
// This is receiver-wide, not per connection, IP, or request owner.
type redisReplyBudget struct {
	mu    sync.Mutex
	limit int
	used  int
	peak  int
}

func (b *redisReplyBudget) acquire(n int) bool {
	if b == nil {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	limit := b.limit
	if limit <= 0 {
		limit = redisDefaultReplyBudgetBytes
	}
	if n < 0 || n > limit-b.used {
		return false
	}
	b.used += n
	if b.used > b.peak {
		b.peak = b.used
	}
	return true
}

func (b *redisReplyBudget) release(n int) {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if n < 0 || n > b.used {
		panic("redis reply budget accounting underflow")
	}
	b.used -= n
}

func (r *RedisServer) newReplyBuffer() *redisReplyBuffer {
	b := &redisReplyBuffer{}
	if r != nil && r.requestAdmission != nil {
		b.budget = &r.replyBudget
	}
	return b
}
