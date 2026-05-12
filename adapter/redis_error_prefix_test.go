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
