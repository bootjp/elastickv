package proxy

import (
	"context"
	"fmt"
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

func blockingReplayCommand(cmd string, _ [][]byte, resp any) (string, []any, bool) {
	switch strings.ToUpper(cmd) {
	case "BLPOP":
		return blockingListPopReplay(1, resp)
	case "BRPOP":
		return blockingListPopReplay(-1, resp)
	case "BZPOPMIN", "BZPOPMAX":
		parts, ok := redisArray(resp)
		if !ok || len(parts) < 2 {
			return "", nil, false
		}
		key, keyOK := redisArg(parts[0])
		member, memberOK := redisArg(parts[1])
		if !keyOK || !memberOK {
			return "", nil, false
		}
		return "ZREM", []any{[]byte("ZREM"), key, member}, true
	default:
		return "", nil, false
	}
}

func blockingListPopReplay(count int64, resp any) (string, []any, bool) {
	parts, ok := redisArray(resp)
	if !ok || len(parts) < 2 {
		return "", nil, false
	}
	key, keyOK := redisArg(parts[0])
	value, valueOK := redisArg(parts[1])
	if !keyOK || !valueOK {
		return "", nil, false
	}
	return "LREM", []any{[]byte("LREM"), key, count, value}, true
}

func redisArray(v any) ([]any, bool) {
	switch x := v.(type) {
	case []any:
		return x, true
	case []string:
		out := make([]any, len(x))
		for i := range x {
			out[i] = x[i]
		}
		return out, true
	case [][]byte:
		out := make([]any, len(x))
		for i := range x {
			out[i] = x[i]
		}
		return out, true
	default:
		return nil, false
	}
}

func redisArg(v any) (any, bool) {
	switch x := v.(type) {
	case nil:
		return nil, false
	case []byte:
		return append([]byte(nil), x...), true
	case string:
		return []byte(x), true
	default:
		return []byte(fmt.Sprint(x)), true
	}
}
