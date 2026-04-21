package adapter

import (
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/redcon"
)

// sentinelErr is the error injected by the stub coordinator to simulate a
// LinearizableRead failure (e.g. leadership lost, context timeout).
var sentinelLinearizableErr = errors.New("linearizable read: not leader")

func newLuaTestServer(linearizableErr error) *RedisServer {
	return &RedisServer{
		store:       store.NewMVCCStore(),
		coordinator: &stubAdapterCoordinator{verifyLeaderErr: linearizableErr},
		scriptCache: map[string]string{},
	}
}

// TestEval_LinearizableReadFailure verifies that EVAL propagates a
// LinearizableRead error to the client as a Redis error reply.
func TestEval_LinearizableReadFailure(t *testing.T) {
	t.Parallel()

	r := newLuaTestServer(sentinelLinearizableErr)
	conn := &recordingConn{}
	r.eval(conn, redcon.Command{
		Args: [][]byte{
			[]byte("EVAL"),
			[]byte("return 1"),
			[]byte("0"),
		},
	})

	require.NotEmpty(t, conn.err, "EVAL must write an error when LinearizableRead fails")
	require.Contains(t, conn.err, sentinelLinearizableErr.Error())
}

// TestEvalSHA_LinearizableReadFailure verifies that EVALSHA propagates a
// LinearizableRead error when the script is cached.
func TestEvalSHA_LinearizableReadFailure(t *testing.T) {
	t.Parallel()

	r := newLuaTestServer(sentinelLinearizableErr)
	script := "return 1"
	sha := luaScriptSHA(script)
	r.scriptCache[sha] = script

	conn := &recordingConn{}
	r.evalsha(conn, redcon.Command{
		Args: [][]byte{
			[]byte("EVALSHA"),
			[]byte(sha),
			[]byte("0"),
		},
	})

	require.NotEmpty(t, conn.err, "EVALSHA must write an error when LinearizableRead fails")
	require.Contains(t, conn.err, sentinelLinearizableErr.Error())
}

// TestExecLuaCompat_LinearizableReadFailure verifies that the compat path
// (used by RENAME, RPOPLPUSH, LLEN, etc.) propagates LinearizableRead errors.
func TestExecLuaCompat_LinearizableReadFailure(t *testing.T) {
	t.Parallel()

	r := newLuaTestServer(sentinelLinearizableErr)
	conn := &recordingConn{}
	r.execLuaCompat(conn, cmdSet, [][]byte{[]byte("k"), []byte("v")})

	require.NotEmpty(t, conn.err, "execLuaCompat must write an error when LinearizableRead fails")
	require.Contains(t, conn.err, sentinelLinearizableErr.Error())
}

// TestEval_LinearizableReadSuccess verifies the happy path: when
// LinearizableRead succeeds, EVAL executes the script and returns a result.
func TestEval_LinearizableReadSuccess(t *testing.T) {
	t.Parallel()

	r := newLuaTestServer(nil) // no error
	conn := &recordingConn{}
	r.eval(conn, redcon.Command{
		Args: [][]byte{
			[]byte("EVAL"),
			[]byte("return 42"),
			[]byte("0"),
		},
	})

	require.Empty(t, conn.err, "EVAL must not write an error when LinearizableRead succeeds")
	require.Equal(t, int64(42), conn.int)
}
