package adapter

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/redcon"
)

func TestRedisExecutionBudgetRetainsThroughJoinAndPanic(t *testing.T) {
	t.Parallel()
	for _, exit := range []string{"success", "revoked", "panic"} {
		t.Run(exit, func(t *testing.T) {
			owner := &redisTestOwnership{ctx: t.Context()}
			s := redisOwnedTestServer(owner)
			t.Cleanup(func() { require.NoError(t, s.Close()) })
			s.requestAdmission = func(context.Context) (RequestOwnership, error) {
				return redisReplyJoinOwner{RequestOwnership: owner, join: func() {
					used, _ := redisReadExecutionUsage(s)
					require.Equal(t, 1, used, "backend slot must remain owned until child joins finish")
				}}, nil
			}
			s.route[cmdGet] = func(c redcon.Conn, _ redcon.Command) {
				c.WriteString("OK")
				switch exit {
				case "revoked":
					owner.revoked.Store(true)
				case "panic":
					panic("command panic")
				}
			}
			run := func() {
				s.handleRedisCommand(&redisOwnedTestConn{}, redcon.Command{Args: [][]byte{[]byte(cmdGet), []byte("key")}})
			}
			if exit == "panic" {
				require.Panics(t, run)
			} else {
				run()
			}
			used, peak := redisReadExecutionUsage(s)
			require.Zero(t, used)
			require.Equal(t, 1, peak)
			require.Equal(t, 1, owner.finishes)
			require.Equal(t, 1, owner.joins)
		})
	}
}

func TestRedisExecutionBudgetDefaultAndLegacy(t *testing.T) {
	t.Parallel()
	b := &redisProtectedExecutionBudget{}
	for range redisDefaultProtectedExecutions {
		require.True(t, b.acquire())
	}
	require.False(t, b.acquire())
	for range redisDefaultProtectedExecutions {
		b.release()
	}
	require.Panics(t, func() { b.release() })
	s := NewRedisServer(nil, "", nil, nil, nil, nil)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	s.executionBudget.limit = 1
	require.True(t, s.executionBudget.acquire())
	called := false
	s.route[cmdPing] = func(c redcon.Conn, _ redcon.Command) { called = true; c.WriteString("PONG") }
	s.handleRedisCommand(&redisOwnedTestConn{}, redcon.Command{Args: [][]byte{[]byte(cmdPing)}})
	require.True(t, called, "nil-hook legacy dispatch is not subject to the protected cap")
	used, _ := redisReadExecutionUsage(s)
	require.Equal(t, 1, used, "legacy did not acquire or release a protected slot")
	s.executionBudget.release()
}
