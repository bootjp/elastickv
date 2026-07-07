package adapter

import (
	"os"
	"runtime"
	"strconv"
	"strings"
)

const (
	redisHeavyCommandSlotsEnv     = "ELASTICKV_REDIS_HEAVY_COMMAND_SLOTS"
	defaultRedisHeavySlotCPUScale = 2
)

type redisHeavyCommandLimiter struct {
	slots chan struct{}
}

func newDefaultRedisHeavyCommandLimiter() *redisHeavyCommandLimiter {
	n := defaultRedisHeavySlotCPUScale * runtime.GOMAXPROCS(0)
	if raw := strings.TrimSpace(os.Getenv(redisHeavyCommandSlotsEnv)); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil {
			n = parsed
		}
	}
	return newRedisHeavyCommandLimiter(n)
}

func newRedisHeavyCommandLimiter(n int) *redisHeavyCommandLimiter {
	if n <= 0 {
		return nil
	}
	return &redisHeavyCommandLimiter{slots: make(chan struct{}, n)}
}

func (l *redisHeavyCommandLimiter) submit(fn func()) bool {
	if l == nil {
		fn()
		return true
	}
	select {
	case l.slots <- struct{}{}:
	default:
		return false
	}
	defer func() {
		<-l.slots
	}()
	fn()
	return true
}

func isRedisHeavyCommand(name string) bool {
	switch strings.ToUpper(name) {
	case cmdEval, cmdEvalSHA,
		cmdKeys, cmdScan,
		cmdHGetAll,
		cmdLRange,
		cmdSMembers,
		cmdXRead, cmdXRange, cmdXRevRange,
		cmdZRange, cmdZRangeByScore, cmdZRevRange, cmdZRevRangeByScore,
		cmdBZPopMin:
		return true
	default:
		return false
	}
}
