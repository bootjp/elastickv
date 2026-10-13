package proxy

import (
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestParsedProxyRESP2StringTypes(t *testing.T) {
	for _, tc := range []struct {
		name string
		cmd  string
		args [][]byte
		resp any
		want []any
	}{
		{"get-status-looking-data", "GET", [][]byte{[]byte("GET"), []byte("k")}, "OK", []any{"BULKSTR:OK"}},
		{"nested-data", "LRANGE", [][]byte{[]byte("LRANGE"), []byte("k"), []byte("0"), []byte("-1")}, []any{"PONG"}, []any{1, "BULKSTR:PONG"}},
		{"type-status", "TYPE", [][]byte{[]byte("TYPE"), []byte("k")}, "none", []any{"STR:none"}},
		{"set-status", "SET", [][]byte{[]byte("SET"), []byte("k"), []byte("v")}, "OK", []any{"STR:OK"}},
		{"set-get-data", "SET", [][]byte{[]byte("SET"), []byte("k"), []byte("v"), []byte("GET")}, "OK", []any{"BULKSTR:OK"}},
		{"hmset-status", "HMSET", [][]byte{[]byte("HMSET"), []byte("k"), []byte("f"), []byte("v")}, "OK", []any{"STR:OK"}},
		{"bzpopmin-null-array", "BZPOPMIN", [][]byte{[]byte("BZPOPMIN"), []byte("k"), []byte("0.01")}, nil, []any{-1}},
		{"xread-null-array", "XREAD", [][]byte{[]byte("XREAD"), []byte("STREAMS"), []byte("k"), []byte("0")}, nil, []any{-1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := &mockRespWriter{}
			var err error
			if tc.resp == nil {
				err = redis.Nil
			}
			writeCommandResponse(w, tc.cmd, tc.args, tc.resp, err)
			require.Equal(t, tc.want, w.writes)
		})
	}
}

func TestParsedProxyCommandInfoFlagTypes(t *testing.T) {
	info := []any{[]any{
		"get", int64(2), []any{"readonly", "fast"}, int64(1), int64(1), int64(1),
		[]any{"@read"}, []any{}, []any{[]any{"flags", []any{"RO", "access"}}}, []any{},
	}}
	w := &mockRespWriter{}
	writeAdminResponse(w, "COMMAND", [][]byte{[]byte("COMMAND"), []byte("INFO"), []byte("GET")}, info, nil)
	require.Equal(t, []any{
		1, 10, "BULKSTR:get", int64(2), 2, "STR:readonly", "STR:fast",
		int64(1), int64(1), int64(1), 1, "STR:@read", 0,
		1, 2, "BULKSTR:flags", 2, "STR:RO", "STR:access", 0,
	}, w.writes)
}

func TestParsedProxyCommandGetKeysAndFlagsTypes(t *testing.T) {
	w := &mockRespWriter{}
	writeAdminResponse(w, "COMMAND", [][]byte{[]byte("COMMAND"), []byte("GETKEYSANDFLAGS"), []byte("GET"), []byte("k")}, []any{[]any{"k", []any{"RO", "access"}}}, nil)
	require.Equal(t, []any{1, 2, "BULKSTR:k", 2, "STR:RO", "STR:access"}, w.writes)
}

func TestParsedProxyTransactionKeepsElementTypes(t *testing.T) {
	w := &mockRespWriter{}
	queue := [][][]byte{
		{[]byte("SET"), []byte("k"), []byte("OK")},
		{[]byte("GET"), []byte("k")},
	}
	writeTxnResponse(w, queue, []any{"OK", "OK"}, nil)
	require.Equal(t, []any{2, "STR:OK", "BULKSTR:OK"}, w.writes)
}

func TestParsedProxyPingArity(t *testing.T) {
	p := &ProxyServer{}
	w := &commandDocsConn{}
	p.handleAdmin(w, "PING", [][]byte{[]byte("PING"), []byte("a"), []byte("b")})
	require.Equal(t, []any{"ERR:ERR wrong number of arguments for 'ping' command"}, w.writes)
}

func TestParsedProxyHello2(t *testing.T) {
	primary := newMockBackend("redis")
	primary.doFunc = makeCmd([]any{"server", "redis", "proto", int64(2)}, nil)
	d := NewDualWriter(primary, nil, ProxyConfig{Mode: ModeRedisOnly}, newTestMetrics(), newTestSentry(), testLogger)
	defer d.Close()
	p := &ProxyServer{dual: d}
	w := &commandDocsConn{}
	p.handleAdmin(w, "HELLO", [][]byte{[]byte("HELLO"), []byte("2")})
	require.Equal(t, []any{4, "BULKSTR:server", "BULKSTR:redis", "BULKSTR:proto", int64(2)}, w.writes)
	require.Len(t, primary.Calls(), 1)
}
