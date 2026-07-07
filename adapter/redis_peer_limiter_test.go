package adapter

import (
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	testPeerLimit = 2
)

func TestRedisPeerLimiterRejectsAndReleases(t *testing.T) {
	server := NewRedisServer(nil, "", nil, nil, nil, nil, WithRedisPerPeerConnectionLimit(testPeerLimit))
	c1 := &remoteCommandRecorder{remote: "192.168.0.64:10001"}
	c2 := &remoteCommandRecorder{remote: "192.168.0.64:10002"}
	c3 := &remoteCommandRecorder{remote: "192.168.0.64:10003"}

	require.True(t, server.acceptConn(c1))
	require.True(t, server.acceptConn(c2))
	require.False(t, server.acceptConn(c3))
	require.Len(t, c3.writes, 1)
	require.Equal(t, "error", c3.writes[0].op)
	require.Equal(t, redisPeerLimitError, c3.writes[0].s)

	server.closeConn(c1)
	require.True(t, server.acceptConn(c3))
	server.closeConn(c2)
	server.closeConn(c3)
	require.Empty(t, server.peerLimiter.active)
}

func TestRedisPeerLimiterCanBeDisabled(t *testing.T) {
	server := NewRedisServer(nil, "", nil, nil, nil, nil, WithRedisPerPeerConnectionLimit(0))
	for _, remote := range []string{"192.168.0.64:10001", "192.168.0.64:10002", "192.168.0.64:10003"} {
		require.True(t, server.acceptConn(&remoteCommandRecorder{remote: remote}))
	}
}

func TestRedisPeerKey(t *testing.T) {
	require.Equal(t, "192.168.0.64", redisPeerKey("192.168.0.64:6379"))
	require.Equal(t, "::1", redisPeerKey("[::1]:6379"))
	require.Equal(t, "proxy-without-port", redisPeerKey("proxy-without-port"))
	require.Equal(t, unknownRedisPeer, redisPeerKey(""))
}

type remoteCommandRecorder struct {
	commandRecorder
	remote string
}

func (c *remoteCommandRecorder) RemoteAddr() string {
	return c.remote
}
