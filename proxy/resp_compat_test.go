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
		{"set-ifeq-get-value-status", "SET", [][]byte{[]byte("SET"), []byte("k"), []byte("v"), []byte("IFEQ"), []byte("GET")}, "OK", []any{"STR:OK"}},
		{"set-ifeq-get-value-and-get-option", "SET", [][]byte{[]byte("SET"), []byte("k"), []byte("v"), []byte("IFEQ"), []byte("GET"), []byte("GET")}, "OK", []any{"BULKSTR:OK"}},
		{"hmset-status", "HMSET", [][]byte{[]byte("HMSET"), []byte("k"), []byte("f"), []byte("v")}, "OK", []any{"STR:OK"}},
		{"mset-status", "MSET", [][]byte{[]byte("MSET"), []byte("k"), []byte("v")}, "OK", []any{"STR:OK"}},
		{"psetex-status", "PSETEX", [][]byte{[]byte("PSETEX"), []byte("k"), []byte("1000"), []byte("v")}, "OK", []any{"STR:OK"}},
		{"restore-status", "RESTORE", [][]byte{[]byte("RESTORE"), []byte("k"), []byte("0"), []byte("payload")}, "OK", []any{"STR:OK"}},
		{"pfmerge-status", "PFMERGE", [][]byte{[]byte("PFMERGE"), []byte("out"), []byte("in")}, "OK", []any{"STR:OK"}},
		{"xgroup-create-status", "XGROUP", [][]byte{[]byte("XGROUP"), []byte("CREATE"), []byte("stream"), []byte("group"), []byte("0"), []byte("MKSTREAM")}, "OK", []any{"STR:OK"}},
		{"xgroup-setid-status", "XGROUP", [][]byte{[]byte("XGROUP"), []byte("SETID"), []byte("stream"), []byte("group"), []byte("0")}, "OK", []any{"STR:OK"}},
		{"swapdb-status", "SWAPDB", [][]byte{[]byte("SWAPDB"), []byte("0"), []byte("1")}, "OK", []any{"STR:OK"}},
		{"watch-status", "WATCH", [][]byte{[]byte("WATCH"), []byte("k")}, "OK", []any{"STR:OK"}},
		{"unwatch-status", "UNWATCH", [][]byte{[]byte("UNWATCH")}, "OK", []any{"STR:OK"}},
		{"reset-status", "RESET", [][]byte{[]byte("RESET")}, "RESET", []any{"STR:RESET"}},
		{"save-status", "SAVE", [][]byte{[]byte("SAVE")}, "OK", []any{"STR:OK"}},
		{"bgsave-status", "BGSAVE", [][]byte{[]byte("BGSAVE")}, "Background saving started", []any{"STR:Background saving started"}},
		{"replicaof-status", "REPLICAOF", [][]byte{[]byte("REPLICAOF"), []byte("NO"), []byte("ONE")}, "OK", []any{"STR:OK"}},
		{"migrate-nokey-status", "MIGRATE", [][]byte{[]byte("MIGRATE"), []byte("127.0.0.1"), []byte("1"), []byte("missing"), []byte("0"), []byte("1000")}, "NOKEY", []any{"STR:NOKEY"}},
		{"readwrite-status", "READWRITE", [][]byte{[]byte("READWRITE")}, "OK", []any{"STR:OK"}},
		{"default-routed-module-status", "JSON.SET", [][]byte{[]byte("JSON.SET"), []byte("k"), []byte("$"), []byte("{}")}, "OK", []any{"STR:OK"}},
		{"config-set-status", "CONFIG", [][]byte{[]byte("CONFIG"), []byte("SET"), []byte("timeout"), []byte("0")}, "OK", []any{"STR:OK"}},
		{"acl-setuser-status", "ACL", [][]byte{[]byte("ACL"), []byte("SETUSER"), []byte("user"), []byte("OFF")}, "OK", []any{"STR:OK"}},
		{"client-tracking-status", "CLIENT", [][]byte{[]byte("CLIENT"), []byte("TRACKING"), []byte("ON")}, "OK", []any{"STR:OK"}},
		{"slowlog-reset-status", "SLOWLOG", [][]byte{[]byte("SLOWLOG"), []byte("RESET")}, "OK", []any{"STR:OK"}},
		{"memory-purge-status", "MEMORY", [][]byte{[]byte("MEMORY"), []byte("PURGE")}, "OK", []any{"STR:OK"}},
		{"acl-log-reset-status", "ACL", [][]byte{[]byte("ACL"), []byte("LOG"), []byte("RESET")}, "OK", []any{"STR:OK"}},
		{"script-debug-status", "SCRIPT", [][]byte{[]byte("SCRIPT"), []byte("DEBUG"), []byte("NO")}, "OK", []any{"STR:OK"}},
		{"cluster-addslots-status", "CLUSTER", [][]byte{[]byte("CLUSTER"), []byte("ADDSLOTS"), []byte("1")}, "OK", []any{"STR:OK"}},
		{"client-kill-legacy-status", "CLIENT", [][]byte{[]byte("CLIENT"), []byte("KILL"), []byte("127.0.0.1:1234")}, "OK", []any{"STR:OK"}},
		{"acl-dryrun-allowed-status", "ACL", [][]byte{[]byte("ACL"), []byte("DRYRUN"), []byte("default"), []byte("PING")}, "OK", []any{"STR:OK"}},
		{"acl-dryrun-denied-data", "ACL", [][]byte{[]byte("ACL"), []byte("DRYRUN"), []byte("restricted"), []byte("GET"), []byte("key")}, "User restricted has no permissions to run the 'get' command", []any{"BULKSTR:User restricted has no permissions to run the 'get' command"}},
		{"client-getname-data", "CLIENT", [][]byte{[]byte("CLIENT"), []byte("GETNAME")}, "OK", []any{"BULKSTR:OK"}},
		{"function-load-data", "FUNCTION", [][]byte{[]byte("FUNCTION"), []byte("LOAD"), []byte("code")}, "OK", []any{"BULKSTR:OK"}},
		{"slowlog-get-array", "SLOWLOG", [][]byte{[]byte("SLOWLOG"), []byte("GET")}, []any{"entry"}, []any{1, "BULKSTR:entry"}},
		{"cluster-bumpepoch-data", "CLUSTER", [][]byte{[]byte("CLUSTER"), []byte("BUMPEPOCH")}, "BUMPED", []any{"BULKSTR:BUMPED"}},
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

func TestParsedProxyTransactionPingTypes(t *testing.T) {
	w := &mockRespWriter{}
	queue := [][][]byte{
		{[]byte("PING")},
		{[]byte("PING"), []byte("PONG")},
	}
	writeTxnResponse(w, queue, []any{"PONG", "PONG"}, nil)
	require.Equal(t, []any{2, "STR:PONG", "BULKSTR:PONG"}, w.writes)
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

func TestParsedProxyAdminStatusReply(t *testing.T) {
	primary := newMockBackend("redis")
	primary.doFunc = makeCmd("OK", nil)
	d := NewDualWriter(primary, nil, ProxyConfig{Mode: ModeRedisOnly}, newTestMetrics(), newTestSentry(), testLogger)
	defer d.Close()
	p := &ProxyServer{dual: d}
	w := &commandDocsConn{}
	p.handleAdmin(w, "SLOWLOG", [][]byte{[]byte("SLOWLOG"), []byte("RESET")})
	require.Equal(t, []any{"STR:OK"}, w.writes)
	require.Len(t, primary.Calls(), 1)
}
