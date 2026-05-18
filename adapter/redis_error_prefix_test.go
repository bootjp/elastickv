package adapter

import (
	"fmt"
	"io"
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
	"github.com/tidwall/redcon"
)

// captureConn is the minimal redcon.Conn surface writeRedisError uses;
// only WriteError needs a real implementation.
type captureConn struct {
	redcon.Conn
	lastErr string
}

func (c *captureConn) WriteError(msg string) { c.lastErr = msg }

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
