package adapter

import (
	"errors"
	"os"
	"runtime"
	"strconv"
	"strings"

	"github.com/tidwall/redcon"
)

const (
	redisHeavyCommandSlotsEnv     = "ELASTICKV_REDIS_HEAVY_COMMAND_SLOTS"
	defaultRedisHeavySlotCPUScale = 2
)

var errRedisHeavyCommandPoolFull = errors.New("BUSY server overloaded")

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
	switch name {
	case cmdEval, cmdEvalSHA,
		cmdDBSize,
		cmdKeys, cmdScan,
		cmdHGetAll,
		cmdLRange,
		cmdSMembers,
		cmdXRead, cmdXRange, cmdXRevRange,
		cmdZCount, cmdZPopMin,
		cmdZRange, cmdZRangeByScore, cmdZRevRange, cmdZRevRangeByScore,
		cmdZRem, cmdZRemRangeByRank, cmdZRemRangeByScore,
		cmdBZPopMin:
		return true
	default:
		return false
	}
}

func transactionHasHeavyCommand(queue []redcon.Command) bool {
	for _, cmd := range queue {
		if len(cmd.Args) == 0 {
			continue
		}
		if isRedisHeavyCommand(strings.ToUpper(string(cmd.Args[0]))) {
			return true
		}
	}
	return false
}
