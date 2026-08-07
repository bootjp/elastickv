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
		{"raftengine.ErrNotLeader", raftengine.ErrNotLeader, true},
		{"raftengine.ErrLeadershipLost", raftengine.ErrLeadershipLost, true},
		{"raftengine.ErrLeadershipTransferInProgress",
			raftengine.ErrLeadershipTransferInProgress, true},
		{"wrapped with errors.WithStack",
			errors.WithStack(ErrLeaderNotFound), true},
		{"wrapped with errors.Wrapf",
			errors.Wrapf(kv.ErrLeaderNotFound, "verify leader"), true},
		{"wrapped with fmt.Errorf %w",
			fmt.Errorf("dispatch: %w", raftengine.ErrLeadershipLost), true},
		{"unrelated error", io.EOF, false},
		{"nil", nil, false},
		{"wrong-type-looking error", errors.New("WRONGTYPE op"), false},
		// Suffix-fallback regression cases. These cover the gRPC-
		// boundary path: when the coordinator forwards to a remote
		// leader and the remote returns ErrLeaderNotFound, gRPC
		// flattens it to "rpc error: code = X desc = leader not
		// found"; the typed sentinel chain is gone. The Jepsen
		// Redis workload (scheduled run 26035515694) saw workers
		// crash with `:prefix :rpc` because this case was not
		// classified as transient and the raw "rpc error: …"
		// reached Carmine.
		{"grpc-wrapped leader not found",
			errors.New("rpc error: code = Unknown desc = leader not found"), true},
		{"grpc-wrapped not leader",
			errors.New("rpc error: code = FailedPrecondition desc = raft engine: not leader"), true},
		{"grpc-wrapped leadership lost",
			errors.New("rpc error: code = Aborted desc = raft engine: leadership lost"), true},
		{"grpc-wrapped leadership transfer",
			errors.New("rpc error: code = Aborted desc = raft engine: leadership transfer in progress"), true},
		{"grpc-wrapped leader proxy circuit open",
			errors.New("rpc error: code = Unavailable desc = leader proxy circuit open"), true},
		// Suffix discipline: a user-controlled key in the middle
		// of the message must NOT trigger a false positive. The kv
		// suffix matcher pins this exact scenario; mirror it here.
		{"user key embedding 'not leader' in the middle",
			errors.New("key: not leader: write conflict"), false},
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
		{"unrelated error untouched",
			errors.New("WRONGTYPE op"), "WRONGTYPE op"},
		{"generic io.EOF untouched",
			io.EOF, io.EOF.Error()},
		// Suffix-fallback wire reply regression: the gRPC-wrapped
		// "rpc error: code = Unknown desc = leader not found" string
		// (the failure mode behind scheduled run 26035515694) must
		// gain a NOTLEADER prefix on the Redis wire so Carmine maps
		// it to `:prefix :notleader` and the upstream
		// jepsen-io/redis with-exceptions catch fires.
		{"grpc-wrapped leader-not-found gains NOTLEADER prefix",
			errors.New("rpc error: code = Unknown desc = leader not found"),
			"NOTLEADER rpc error: code = Unknown desc = leader not found"},
		{"already NOTLEADER-prefixed error is not double-prefixed",
			errors.New("NOTLEADER leader not found"),
			"NOTLEADER leader not found"},
		// Regression: address-mapping gap errors (raft leader known
		// but raft→redis address missing in r.leaderRedis) must be
		// ERR-prefixed at the source so Carmine maps to :prefix :err
		// instead of :prefix :leader. This covers proxyDBSize / proxyDel /
		// proxyFlushDatabase / proxyFlushLegacy in redis_proxy.go +
		// proxyKeys / proxyLRange / proxyRPush / proxyLPush /
		// leaderClientForKey / resolveLeaderRedisAddr in redis.go — all
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
	t.Run("transient leader error is top-level NOTLEADER", func(t *testing.T) {
		t.Parallel()
		c := &captureConn{}
		handled := handleProxyTxnError(c, errors.New("rpc error: code = Unknown desc = leader not found"))
		if !handled {
			t.Fatal("handleProxyTxnError returned false")
		}
		if c.lastErr != "NOTLEADER rpc error: code = Unknown desc = leader not found" {
			t.Fatalf("last error = %q", c.lastErr)
		}
		if c.wroteArray {
			t.Fatalf("unexpected array reply %d", c.lastArray)
		}
	})

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
	t.Run("transient command error is promoted to top-level NOTLEADER", func(t *testing.T) {
		t.Parallel()
		cmd := redis.NewCmd(context.Background(), "LRANGE", "k", 0, -1)
		cmd.SetErr(errors.New("rpc error: code = FailedPrecondition desc = raft engine: not leader"))
		c := &captureConn{}
		handled := handleProxyTxnCommandError(c, []*redis.Cmd{cmd})
		if !handled {
			t.Fatal("handleProxyTxnCommandError returned false")
		}
		if c.lastErr != "NOTLEADER rpc error: code = FailedPrecondition desc = raft engine: not leader" {
			t.Fatalf("last error = %q", c.lastErr)
		}
	})

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

// TestHasTransientLeaderSuffix_PinsSentinels closes the gap noted
// at kv/coordinator.go:529 ("A symmetric pin lives in the adapter
// test package"): the adapter's redisLeaderErrorPhrases set must
// stay in sync with the actual .Error() text of every transient-
// leader sentinel the suffix fallback is meant to catch. If a
// sentinel ever gets renamed (e.g. raftengine.ErrLeadershipLost
// becomes "raft engine: leadership lost (xyz)") the kv-side pin
// fails first, but without this adapter-side pin the adapter's
// phrase list could drift silently and the NOTLEADER classification
// would regress to the pre-PR-789 worker-crash failure mode.
//
// Each case calls hasTransientLeaderSuffix(sentinel.Error()) and
// asserts true. Wrapping (errors.Wrap / fmt.Errorf %w) is covered
// by TestIsTransientLeaderRedisError; this test pins the raw
// .Error() strings only.
func TestHasTransientLeaderSuffix_PinsSentinels(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		msg  string
	}{
		{"adapter.ErrLeaderNotFound", ErrLeaderNotFound.Error()},
		{"adapter.ErrNotLeader", ErrNotLeader.Error()},
		{"kv.ErrLeaderNotFound", kv.ErrLeaderNotFound.Error()},
		{"raftengine.ErrNotLeader", raftengine.ErrNotLeader.Error()},
		{"raftengine.ErrLeadershipLost", raftengine.ErrLeadershipLost.Error()},
		{"raftengine.ErrLeadershipTransferInProgress",
			raftengine.ErrLeadershipTransferInProgress.Error()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if !hasTransientLeaderSuffix(tc.msg) {
				t.Fatalf("hasTransientLeaderSuffix(%q) = false; "+
					"redisLeaderErrorPhrases is out of sync with %s — "+
					"a sentinel rename slipped through. Update the "+
					"phrase list in adapter/redis.go to match.", tc.msg, tc.name)
			}
		})
	}
}
