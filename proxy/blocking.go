package proxy

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const blockingMultiPopMinArgs = 2
const (
	elasticKVZRemFastCommand = "ELASTICKV.ZREMFAST"
	zremReplayCommand        = "ZREM"
)

type blockingTimeoutBackend interface {
	DoWithTimeout(ctx context.Context, timeout time.Duration, args ...any) *redis.Cmd
}

type readTimeoutBackend interface {
	DoWithReadTimeout(ctx context.Context, timeout time.Duration, args ...any) *redis.Cmd
}

type pipelineReadTimeoutBackend interface {
	PipelineWithReadTimeout(ctx context.Context, timeout time.Duration, cmds [][]any) ([]*redis.Cmd, error)
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

func shouldReplayBlockingToSecondary(cmd string, resp any) bool {
	if !blockingResponseHasEffect(resp) {
		return false
	}
	return !strings.EqualFold(cmd, "XREAD")
}

func secondaryBlockingReplay(cmd string, resp any, secondary Backend) (string, []any, bool) {
	switch strings.ToUpper(cmd) {
	case "BZPOPMIN", "BZPOPMAX":
		key, member, ok := zsetPopKeyMember(resp)
		if !ok {
			return "", nil, false
		}
		replayCmd := zremReplayCommandForSecondary(secondary)
		return replayCmd, []any{replayCmd, key, member}, true
	default:
		return "", nil, false
	}
}

func zremReplayCommandForSecondary(secondary Backend) string {
	if secondary != nil && strings.EqualFold(secondary.Name(), "elastickv") {
		return elasticKVZRemFastCommand
	}
	return zremReplayCommand
}

func zsetPopKeyMember(resp any) (any, any, bool) {
	arr, ok := resp.([]any)
	if !ok || len(arr) < 2 || arr[0] == nil || arr[1] == nil {
		return nil, nil, false
	}
	return arr[0], arr[1], true
}

func blockingResponseHasEffect(resp any) bool {
	if resp == nil {
		return false
	}
	arr, ok := resp.([]any)
	return !ok || len(arr) > 0
}
