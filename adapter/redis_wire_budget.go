package adapter

import "github.com/tidwall/redcon"

const (
	redisWireTxnBaseBytes     = 16 << 10
	redisQueueAllocationScale = 2
	redisQueueAllocationSlack = 8
)

// wireTxnMu protects a server-wide budget, independent of client IP or owner.
// Charge empty holders too, bounding otherwise unlimited empty MULTI owners.
// This bounds retained protected queues, not redcon's incoming parser buffers,
// EXEC working sets, reply buffers, or the Go runtime's eventual GC/RSS.
func (r *RedisServer) reserveWireTxnBytes(n int) bool {
	r.wireTxnMu.Lock()
	defer r.wireTxnMu.Unlock()
	limit := r.wireTxnBudgetLimit
	if limit <= 0 {
		limit = redisMaxReplyBytes
	}
	if n < 0 || n > limit-r.wireTxnBytes || r.baseRequestContext().Err() != nil {
		return false
	}
	r.wireTxnBytes += n
	return true
}

func (r *RedisServer) releaseWireTxnBytes(n int) {
	r.wireTxnMu.Lock()
	defer r.wireTxnMu.Unlock()
	if n < 0 || n > r.wireTxnBytes {
		panic("redis transaction budget accounting underflow")
	}
	r.wireTxnBytes -= n
}

// Explicit doubling makes the capacity allocation predictable BEFORE make or
// append. The 2x slot charge covers allocator rounding and the transient old
// backing array while copying into the new one (redcon.Command is 48 bytes on
// supported 64-bit platforms; redisQueuedCommandOverhead is conservatively 64).
func redisQueueCapacityGrowth(queue []redcon.Command) (capacity, charge int) {
	capacity = cap(queue)
	if len(queue) < capacity {
		return capacity, 0
	}
	capacity *= 2
	if capacity == 0 {
		capacity = 1
	}
	if capacity > redisMaxQueuedCommands {
		capacity = redisMaxQueuedCommands
	}
	return capacity, (capacity - cap(queue)) * redisQueuedCommandOverhead * redisQueueAllocationScale
}

// cloneCommand copies BOTH Raw and every argument, plus an argument slice.
// Conservatively allow 2x each allocation and small-object rounding, rather
// than charging only wire payload lengths. Saturate only the untrusted size
// estimate; the actual server reservation counter is always exact.
func queuedRedisCommandSize(cmd redcon.Command) int {
	if len(cmd.Args) > redisMaxReplyBytes/(redisQueuedArgOverhead*redisQueueAllocationScale) {
		return redisMaxReplyBytes + 1
	}
	size := redisQueuedCommandOverhead + len(cmd.Args)*redisQueuedArgOverhead*redisQueueAllocationScale
	size = redisQueueBytesCharge(size, len(cmd.Raw))
	for _, arg := range cmd.Args {
		size = redisQueueBytesCharge(size, len(arg))
	}
	return size
}

func redisQueueBytesCharge(size, length int) int {
	if length == 0 {
		return size
	}
	if size > redisMaxReplyBytes-redisQueueAllocationSlack || length > (redisMaxReplyBytes-size-redisQueueAllocationSlack)/redisQueueAllocationScale {
		return redisMaxReplyBytes + 1
	}
	return size + redisQueueAllocationScale*length + redisQueueAllocationSlack
}
