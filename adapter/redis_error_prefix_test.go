package adapter

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
	"github.com/redis/go-redis/v9"
	"github.com/tidwall/redcon"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// captureConn is the minimal redcon.Conn surface these tests inspect.
type captureConn struct {
	redcon.Conn
	lastErr    string
	lastArray  int
	wroteArray bool
}

func (c *captureConn) WriteError(msg string) { c.lastErr = msg }
func (c *captureConn) WriteArray(count int) {
	c.lastArray = count
	c.wroteArray = true
}

// go-redis returns a redis.Error when it decodes an error frame from RESP.
type decodedRedisReply string

func (e decodedRedisReply) Error() string { return string(e) }
func (decodedRedisReply) RedisError()     {}

func TestIsRedisNotLeaderReply(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"decoded NOTLEADER", decodedRedisReply("NOTLEADER leader not found"), true},
		{"wrapped decoded NOTLEADER", errors.WithStack(decodedRedisReply("NOTLEADER leader not found")), true},
		{"plain text lookalike", errors.New("NOTLEADER leader not found"), false},
		{"suffix lookalike", decodedRedisReply("ERR key: NOTLEADER leader not found"), false},
		{"different code", decodedRedisReply("ERR not leader"), false},
		{"code extension", decodedRedisReply("NOTLEADER_EXTRA leader not found"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := isRedisNotLeaderReply(tc.err); got != tc.want {
				t.Fatalf("isRedisNotLeaderReply(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestIsTransientLeaderRedisError(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"adapter.ErrLeaderNotFound", ErrLeaderNotFound, true},
		{"adapter.ErrNotLeader", ErrNotLeader, true},
		{"kv.ErrLeaderNotFound", kv.ErrLeaderNotFound, true},
		{"kv.ErrLeaderProxyCircuitOpen", kv.ErrLeaderProxyCircuitOpen, true},
		{"wrapped kv.ErrLeaderProxyCircuitOpen", errors.WithStack(kv.ErrLeaderProxyCircuitOpen), true},
		{"lookalike circuit error", errors.New("leader proxy circuit open"), false},
		{"raftengine.ErrNotLeader", raftengine.ErrNotLeader, true},
		{"raftengine.ErrLeadershipLost", raftengine.ErrLeadershipLost, false},
		{"unknown outcome after leadership lost",
			fmt.Errorf("%w: %w", kv.ErrTransactionOutcomeUnknown, raftengine.ErrLeadershipLost), false},
		{"transport loss alongside leader signal",
			fmt.Errorf("%w: %w", io.EOF, ErrLeaderNotFound), false},
		{"raftengine.ErrLeadershipTransferInProgress",
			raftengine.ErrLeadershipTransferInProgress, true},
		{"wrapped with errors.WithStack",
			errors.WithStack(ErrLeaderNotFound), true},
		{"wrapped with errors.Wrapf",
			errors.Wrapf(kv.ErrLeaderNotFound, "verify leader"), true},
		{"wrapped with fmt.Errorf %w",
			fmt.Errorf("dispatch: %w", raftengine.ErrLeadershipLost), false},
		{"unrelated error", io.EOF, false},
		{"nil", nil, false},
		{"wrong-type-looking error", errors.New("WRONGTYPE op"), false},
		// A status message or a lookalike error is not a typed leader
		// rejection, even when its final words match a sentinel.
		{"grpc-wrapped leader not found",
			status.Error(codes.Unknown, "leader not found"), false},
		{"grpc-wrapped not leader",
			status.Error(codes.FailedPrecondition, "raft engine: not leader"), false},
		{"grpc-wrapped leadership lost",
			status.Error(codes.Aborted, "raft engine: leadership lost"), false},
		{"grpc-wrapped leadership transfer",
			status.Error(codes.Aborted, "raft engine: leadership transfer in progress"), false},
		{"grpc-wrapped leader proxy circuit open",
			status.Error(codes.Unavailable, "leader proxy circuit open"), false},
		{"lookalike text-only error",
			errors.New("rpc error: code = Unknown desc = leader not found"), false},
		{"user key embedding 'not leader' in the middle",
			errors.New("key: not leader: write conflict"), false},
		{"user key ending with leader phrase",
			errors.New("key: private: leader not found"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := isTransientLeaderRedisError(tc.err); got != tc.want {
				t.Fatalf("isTransientLeaderRedisError(%v) = %v, want %v",
					tc.err, got, tc.want)
			}
		})
	}
}

func TestWriteRedisError(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"transient leader error gains NOTLEADER prefix",
			ErrLeaderNotFound, "NOTLEADER leader not found"},
		{"wrapped transient leader error gains NOTLEADER prefix",
			errors.WithStack(kv.ErrLeaderNotFound),
			"NOTLEADER leader not found"},
		{"fmt.Errorf-wrapped raftengine error preserves wrapping in message",
			fmt.Errorf("verify leader: %w", raftengine.ErrNotLeader),
			"NOTLEADER verify leader: raft engine: not leader"},
		{"unknown outcome is not marked safe for leader retry",
			fmt.Errorf("%w: %w", kv.ErrTransactionOutcomeUnknown, raftengine.ErrLeadershipLost),
			"ERR operation outcome unknown"},
		{"unknown outcome does not expose its cause",
			fmt.Errorf("%w: %s", kv.ErrTransactionOutcomeUnknown, "private-key"),
			"ERR operation outcome unknown"},
		{"leadership loss after proposal is outcome unknown",
			fmt.Errorf("private-key: %w", raftengine.ErrLeadershipLost),
			redisOutcomeUnknownReply},
		{"transport loss wins over leader signal",
			fmt.Errorf("%w: %w", io.EOF, ErrLeaderNotFound),
			redisOutcomeUnknownReply},
		{"unrelated error untouched",
			errors.New("WRONGTYPE op"), "WRONGTYPE op"},
		{"lost response is outcome unknown",
			io.EOF, redisOutcomeUnknownReply},
		{"deadline after submission is outcome unknown",
			context.DeadlineExceeded, redisOutcomeUnknownReply},
		{"grpc status does not expose leader prose or claim safe retry",
			status.Error(codes.Unknown, "private-key: leader not found"),
			redisOutcomeUnknownReply},
		{"wrapped grpc status stays outcome unknown",
			errors.WithStack(status.Error(codes.Unavailable, "private-key: not leader")),
			redisOutcomeUnknownReply},
		{"decoded NOTLEADER is fixed and top level",
			decodedRedisReply("NOTLEADER private-key: leader not found"),
			redisNotLeaderReply},
		{"already NOTLEADER-prefixed error is not double-prefixed",
			errors.New("NOTLEADER leader not found"),
			"NOTLEADER leader not found"},
		// Regression: address-mapping gap errors (raft leader known
		// but raft→redis address missing in r.leaderRedis) must be
		// ERR-prefixed at the source so Carmine maps to :prefix :err
		// instead of :prefix :leader. This covers proxyDBSize / proxyDel /
		// proxyFlushDatabase / proxyFlushLegacy in redis_proxy.go +
		// proxyKeys / proxyLRange / proxyRPush / proxyLPush /
		// leaderClientForRedisUserKey / resolveLeaderRedisAddr in redis.go — all
		// errors.Newf'd with the same "ERR leader redis address unknown
		// for %s" prefix.
		{"leader-address-unknown config-gap is already ERR-prefixed",
			errors.Newf("ERR leader redis address unknown for %s", "n1"),
			"ERR leader redis address unknown for n1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := &captureConn{}
			writeRedisError(c, tc.err)
			if c.lastErr != tc.want {
				t.Fatalf("writeRedisError last reply = %q, want %q",
					c.lastErr, tc.want)
			}
		})
	}
}

func TestHandleProxyTxnError(t *testing.T) {
	t.Parallel()
	t.Run("grpc status is top-level outcome unknown", func(t *testing.T) {
		t.Parallel()
		c := &captureConn{}
		handled := handleProxyTxnError(c, status.Error(codes.Unknown, "private-key: leader not found"))
		if !handled {
			t.Fatal("handleProxyTxnError returned false")
		}
		if c.lastErr != redisOutcomeUnknownReply {
			t.Fatalf("last error = %q", c.lastErr)
		}
		if c.wroteArray {
			t.Fatalf("unexpected array reply %d", c.lastArray)
		}
	})
	t.Run("leadership loss after proposal is top-level outcome unknown", func(t *testing.T) {
		t.Parallel()
		c := &captureConn{}
		if !handleProxyTxnError(c, errors.WithStack(raftengine.ErrLeadershipLost)) {
			t.Fatal("handleProxyTxnError returned false")
		}
		if c.lastErr != redisOutcomeUnknownReply || c.wroteArray {
			t.Fatalf("reply = (%q, array=%v)", c.lastErr, c.wroteArray)
		}
	})
	t.Run("lost EXEC response is top-level outcome unknown", func(t *testing.T) {
		t.Parallel()
		c := &captureConn{}
		if !handleProxyTxnError(c, io.EOF) {
			t.Fatal("handleProxyTxnError returned false")
		}
		if c.lastErr != redisOutcomeUnknownReply || c.wroteArray {
			t.Fatalf("reply = (%q, array=%v)", c.lastErr, c.wroteArray)
		}
	})
}

func TestHandleProxyTxnAbort(t *testing.T) {
	t.Parallel()
	t.Run("redis transaction abort remains null array", func(t *testing.T) {
		t.Parallel()
		c := &captureConn{}
		handled := handleProxyTxnError(c, redis.TxFailedErr)
		if !handled {
			t.Fatal("handleProxyTxnError returned false")
		}
		if !c.wroteArray || c.lastArray != -1 {
			t.Fatalf("array reply = (%v, %d), want (-1)", c.wroteArray, c.lastArray)
		}
		if c.lastErr != "" {
			t.Fatalf("unexpected error reply %q", c.lastErr)
		}
	})
}

func TestHandleProxyTxnDecodedNotLeader(t *testing.T) {
	t.Parallel()
	c := &captureConn{}
	if !handleProxyTxnError(c, decodedRedisReply("NOTLEADER private-key: leader not found")) {
		t.Fatal("handleProxyTxnError returned false")
	}
	if c.lastErr != redisNotLeaderReply || c.wroteArray {
		t.Fatalf("reply = (%q, array=%v)", c.lastErr, c.wroteArray)
	}
}

func TestHandleProxyTxnDecodedOutcomeUnknown(t *testing.T) {
	t.Parallel()
	c := &captureConn{}
	if !handleProxyTxnError(c, decodedRedisReply(redisOutcomeUnknownReply)) {
		t.Fatal("handleProxyTxnError returned false")
	}
	if c.lastErr != redisOutcomeUnknownReply || c.wroteArray {
		t.Fatalf("reply = (%q, array=%v)", c.lastErr, c.wroteArray)
	}
}

func TestHandleProxyTxnTerminalExecError(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{
			name: "ambiguous route change typed",
			err:  errors.WithStack(errRedisExecRouteChangedAfterAmbiguousAttempt),
			want: errRedisExecRouteChangedAfterAmbiguousAttempt.Error(),
		},
		{
			name: "ambiguous route change decoded RESP",
			err:  errors.New(errRedisExecRouteChangedAfterAmbiguousAttempt.Error()),
			want: errRedisExecRouteChangedAfterAmbiguousAttempt.Error(),
		},
		{
			name: "split leader typed",
			err:  errors.WithStack(errRedisExecSplitShardLeaders),
			want: errRedisExecSplitShardLeaders.Error(),
		},
		{
			name: "split leader decoded RESP",
			err:  errors.New(errRedisExecSplitShardLeaders.Error()),
			want: errRedisExecSplitShardLeaders.Error(),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := &captureConn{}
			handled := handleProxyTxnError(c, tc.err)
			if !handled {
				t.Fatal("handleProxyTxnError returned false")
			}
			if c.lastErr != tc.want {
				t.Fatalf("last error = %q", c.lastErr)
			}
			if c.wroteArray {
				t.Fatalf("unexpected array reply %d", c.lastArray)
			}
		})
	}
}

func TestHandleProxyTxnHeavyCommandBusyError(t *testing.T) {
	t.Parallel()
	c := &captureConn{}
	handled := handleProxyTxnError(c, errors.New(errRedisHeavyCommandPoolFull.Error()))
	if !handled {
		t.Fatal("handleProxyTxnError returned false")
	}
	if c.lastErr != errRedisHeavyCommandPoolFull.Error() {
		t.Fatalf("last error = %q", c.lastErr)
	}
	if c.wroteArray {
		t.Fatalf("unexpected array reply %d", c.lastArray)
	}
}

func TestHandleProxyTxnCommandError(t *testing.T) {
	t.Parallel()
	t.Run("grpc command status is promoted to top-level outcome unknown", func(t *testing.T) {
		t.Parallel()
		cmd := redis.NewCmd(context.Background(), "LRANGE", "k", 0, -1)
		cmd.SetErr(status.Error(codes.FailedPrecondition, "private-key: raft engine: not leader"))
		c := &captureConn{}
		handled := handleProxyTxnCommandError(c, []*redis.Cmd{cmd})
		if !handled {
			t.Fatal("handleProxyTxnCommandError returned false")
		}
		if c.lastErr != redisOutcomeUnknownReply {
			t.Fatalf("last error = %q", c.lastErr)
		}
	})
	t.Run("leadership loss command is promoted to top-level outcome unknown", func(t *testing.T) {
		t.Parallel()
		cmd := redis.NewCmd(context.Background(), "SET", "k", "v")
		cmd.SetErr(errors.WithStack(raftengine.ErrLeadershipLost))
		c := &captureConn{}
		if !handleProxyTxnCommandError(c, []*redis.Cmd{cmd}) {
			t.Fatal("handleProxyTxnCommandError returned false")
		}
		if c.lastErr != redisOutcomeUnknownReply {
			t.Fatalf("last error = %q", c.lastErr)
		}
	})
	t.Run("lost command response is promoted to top-level outcome unknown", func(t *testing.T) {
		t.Parallel()
		cmd := redis.NewCmd(context.Background(), "SET", "k", "v")
		cmd.SetErr(io.EOF)
		c := &captureConn{}
		if !handleProxyTxnCommandError(c, []*redis.Cmd{cmd}) {
			t.Fatal("handleProxyTxnCommandError returned false")
		}
		if c.lastErr != redisOutcomeUnknownReply {
			t.Fatalf("last error = %q", c.lastErr)
		}
	})
}

func TestHandleProxyTxnCommandOtherError(t *testing.T) {
	t.Parallel()
	t.Run("ordinary command error stays in EXEC result array", func(t *testing.T) {
		t.Parallel()
		cmd := redis.NewCmd(context.Background(), "LRANGE", "k", 0, -1)
		cmd.SetErr(errors.New("WRONGTYPE Operation against a key holding the wrong kind of value"))
		c := &captureConn{}
		if handleProxyTxnCommandError(c, []*redis.Cmd{cmd}) {
			t.Fatal("ordinary command error was handled as terminal")
		}
		if c.lastErr != "" {
			t.Fatalf("unexpected error reply %q", c.lastErr)
		}
	})

	t.Run("ambiguous route change is promoted to top-level EXEC error", func(t *testing.T) {
		t.Parallel()

		for _, tc := range []struct {
			name string
			err  error
			want string
		}{
			{
				name: "ambiguous route change typed",
				err:  errors.WithStack(errRedisExecRouteChangedAfterAmbiguousAttempt),
				want: errRedisExecRouteChangedAfterAmbiguousAttempt.Error(),
			},
			{
				name: "ambiguous route change decoded RESP",
				err:  errors.New(errRedisExecRouteChangedAfterAmbiguousAttempt.Error()),
				want: errRedisExecRouteChangedAfterAmbiguousAttempt.Error(),
			},
			{
				name: "split leader typed",
				err:  errors.WithStack(errRedisExecSplitShardLeaders),
				want: errRedisExecSplitShardLeaders.Error(),
			},
			{
				name: "split leader decoded RESP",
				err:  errors.New(errRedisExecSplitShardLeaders.Error()),
				want: errRedisExecSplitShardLeaders.Error(),
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				cmd := redis.NewCmd(context.Background(), "SET", "k", "v")
				cmd.SetErr(tc.err)
				c := &captureConn{}
				handled := handleProxyTxnCommandError(c, []*redis.Cmd{cmd})
				if !handled {
					t.Fatal("handleProxyTxnCommandError returned false")
				}
				if c.lastErr != tc.want {
					t.Fatalf("last error = %q", c.lastErr)
				}
			})
		}
	})

}

func TestHandleProxyTxnCommandDecodedNotLeader(t *testing.T) {
	t.Parallel()
	cmd := redis.NewCmd(context.Background(), "SET", "k", "v")
	cmd.SetErr(decodedRedisReply("NOTLEADER private-key: leader not found"))
	c := &captureConn{}
	if !handleProxyTxnCommandError(c, []*redis.Cmd{cmd}) {
		t.Fatal("handleProxyTxnCommandError returned false")
	}
	if c.lastErr != redisNotLeaderReply || c.wroteArray {
		t.Fatalf("reply = (%q, array=%v)", c.lastErr, c.wroteArray)
	}
}

func TestHandleProxyTxnCommandDecodedOutcomeUnknown(t *testing.T) {
	t.Parallel()
	cmd := redis.NewCmd(context.Background(), "SET", "k", "v")
	cmd.SetErr(decodedRedisReply(redisOutcomeUnknownReply))
	c := &captureConn{}
	if !handleProxyTxnCommandError(c, []*redis.Cmd{cmd}) {
		t.Fatal("handleProxyTxnCommandError returned false")
	}
	if c.lastErr != redisOutcomeUnknownReply || c.wroteArray {
		t.Fatalf("reply = (%q, array=%v)", c.lastErr, c.wroteArray)
	}
}

func TestHandleProxyTxnCommandHeavyCommandBusyError(t *testing.T) {
	t.Parallel()
	cmd := redis.NewCmd(context.Background(), "LRANGE", "k", 0, -1)
	cmd.SetErr(errors.New(errRedisHeavyCommandPoolFull.Error()))
	c := &captureConn{}
	handled := handleProxyTxnCommandError(c, []*redis.Cmd{cmd})
	if !handled {
		t.Fatal("handleProxyTxnCommandError returned false")
	}
	if c.lastErr != errRedisHeavyCommandPoolFull.Error() {
		t.Fatalf("last error = %q", c.lastErr)
	}
}
