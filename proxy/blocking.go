package proxy

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const blockingMultiPopMinArgs = 2

type blockingTimeoutBackend interface {
	DoWithTimeout(ctx context.Context, timeout time.Duration, args ...any) *redis.Cmd
}

func blockingCommandTimeout(cmd string, args [][]byte) time.Duration {
	switch strings.ToUpper(cmd) {
	case "BLPOP", "BRPOP", "BRPOPLPUSH", "BLMOVE", "BZPOPMIN", "BZPOPMAX":
		if len(args) == 0 {
			return 0
		}
		return parseBlockingSecondsArg(args[len(args)-1])
	case "BLMPOP":
		if len(args) < blockingMultiPopMinArgs {
			return 0
		}
		return parseBlockingSecondsArg(args[1])
	case "XREAD", "XREADGROUP":
		for i := 1; i+1 < len(args); i++ {
			if strings.EqualFold(string(args[i]), "BLOCK") {
				return parseBlockingMillisecondsArg(args[i+1])
			}
		}
	}
	return 0
}

func parseBlockingSecondsArg(raw []byte) time.Duration {
	seconds, err := strconv.ParseFloat(string(raw), 64)
	if err != nil || seconds < 0 {
		return 0
	}
	return time.Duration(seconds * float64(time.Second))
}

func parseBlockingMillisecondsArg(raw []byte) time.Duration {
	millis, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil || millis < 0 {
		return 0
	}
	return time.Duration(millis) * time.Millisecond
}
