package proxy

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	blockingMultiPopMinArgs        = 2
	blockingBLMPopMinArgs          = 5
	blockingBLMPopNumKeysArgIndex  = 2
	blockingBLMPopFirstKeyArgIndex = 3
	blockingListMoveMinArgs        = 3
	blockingBLMoveArgs             = 6
	blockingListPopReplayKeyCount  = int64(1)
	blockingListMoveReplayKeyCount = int64(2)
)

const (
	blockingListSideLeft  = "LEFT"
	blockingListSideRight = "RIGHT"
)

const blockingListMoveReplayScript = `
local removed = redis.call("LREM", KEYS[1], tonumber(ARGV[1]), ARGV[3])
if removed == 0 then
	return 0
end
if ARGV[2] == "LEFT" then
	return redis.call("LPUSH", KEYS[2], ARGV[3])
end
return redis.call("RPUSH", KEYS[2], ARGV[3])
`

const blockingListMultiPopReplayScript = `
local removed = 0
local count = tonumber(ARGV[1])
for i = 2, #ARGV do
	if redis.call("LREM", KEYS[1], count, ARGV[i]) > 0 then
		removed = removed + 1
	end
end
return removed
`

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

func blockingReplayCommand(cmd string, args [][]byte, resp any) (string, []any, bool) {
	switch strings.ToUpper(cmd) {
	case "BLPOP":
		return blockingListPopReplay(1, resp)
	case "BRPOP":
		return blockingListPopReplay(-1, resp)
	case "BRPOPLPUSH":
		return blockingListMoveReplay(args, resp, -1, blockingListSideLeft)
	case "BLMOVE":
		return blockingBLMoveReplay(args, resp)
	case "BLMPOP":
		return blockingBLMPopReplay(args, resp)
	case "BZPOPMIN", "BZPOPMAX":
		return blockingZSetPopReplay(resp)
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

func blockingZSetPopReplay(resp any) (string, []any, bool) {
	parts, ok := redisArray(resp)
	if !ok || len(parts) < blockingMultiPopMinArgs {
		return "", nil, false
	}
	key, keyOK := redisArg(parts[0])
	member, memberOK := redisArg(parts[1])
	if !keyOK || !memberOK {
		return "", nil, false
	}
	return "ZREM", []any{[]byte("ZREM"), key, member}, true
}

func blockingBLMoveReplay(args [][]byte, resp any) (string, []any, bool) {
	if len(args) < blockingBLMoveArgs {
		return "", nil, false
	}
	count, ok := blockingListPopCount(args[3])
	if !ok {
		return "", nil, false
	}
	to := strings.ToUpper(string(args[4]))
	if to != blockingListSideLeft && to != blockingListSideRight {
		return "", nil, false
	}
	return blockingListMoveReplay(args, resp, count, to)
}

func blockingBLMPopReplay(args [][]byte, resp any) (string, []any, bool) {
	numKeys, count, ok := parseBlockingBLMPopArgs(args)
	if !ok {
		return "", nil, false
	}
	key, values, ok := blockingBLMPopResponse(resp)
	if !ok || !blockingBLMPopKeyListed(args, numKeys, key) {
		return "", nil, false
	}

	replay := []any{
		[]byte("EVAL"),
		blockingListMultiPopReplayScript,
		blockingListPopReplayKeyCount,
		key,
		count,
	}
	for _, value := range values {
		arg, ok := redisArg(value)
		if !ok {
			return "", nil, false
		}
		replay = append(replay, arg)
	}
	return "EVAL", replay, true
}

func parseBlockingBLMPopArgs(args [][]byte) (int, int64, bool) {
	if len(args) < blockingBLMPopMinArgs {
		return 0, 0, false
	}
	numKeys, err := strconv.Atoi(string(args[blockingBLMPopNumKeysArgIndex]))
	if err != nil || numKeys <= 0 {
		return 0, 0, false
	}
	directionArgIndex := blockingBLMPopFirstKeyArgIndex + numKeys
	if directionArgIndex >= len(args) {
		return 0, 0, false
	}
	count, ok := blockingListPopCount(args[directionArgIndex])
	return numKeys, count, ok
}

func blockingBLMPopResponse(resp any) ([]byte, []any, bool) {
	parts, ok := redisArray(resp)
	if !ok || len(parts) != blockingMultiPopMinArgs {
		return nil, nil, false
	}
	key, ok := redisArgBytes(parts[0])
	if !ok {
		return nil, nil, false
	}
	values, ok := redisArray(parts[1])
	if !ok || len(values) == 0 {
		return nil, nil, false
	}
	return key, values, true
}

func blockingBLMPopKeyListed(args [][]byte, numKeys int, key []byte) bool {
	for i := 0; i < numKeys; i++ {
		if bytes.Equal(args[blockingBLMPopFirstKeyArgIndex+i], key) {
			return true
		}
	}
	return false
}

func blockingListPopCount(side []byte) (int64, bool) {
	switch strings.ToUpper(string(side)) {
	case blockingListSideLeft:
		return 1, true
	case blockingListSideRight:
		return -1, true
	default:
		return 0, false
	}
}

func blockingListMoveReplay(args [][]byte, resp any, count int64, to string) (string, []any, bool) {
	if len(args) < blockingListMoveMinArgs {
		return "", nil, false
	}
	value, ok := redisArg(resp)
	if !ok {
		return "", nil, false
	}
	source := append([]byte(nil), args[1]...)
	destination := append([]byte(nil), args[2]...)
	return "EVAL", []any{
		[]byte("EVAL"),
		blockingListMoveReplayScript,
		blockingListMoveReplayKeyCount,
		source,
		destination,
		count,
		[]byte(to),
		value,
	}, true
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

func redisArgBytes(v any) ([]byte, bool) {
	arg, ok := redisArg(v)
	if !ok {
		return nil, false
	}
	b, ok := arg.([]byte)
	return b, ok
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
