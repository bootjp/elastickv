package adapter

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/redcon"
)

func TestRedisRESP2PingAndSelect(t *testing.T) {
	server := &RedisServer{}
	for _, tc := range []struct {
		name string
		cmd  []string
		want commandRecorderEntry
	}{
		{"ping", []string{"PING"}, commandRecorderEntry{op: "string", s: "PONG"}},
		{"ping-message", []string{"PING", "OK"}, commandRecorderEntry{op: "bulk", s: "OK"}},
		{"ping-arity", []string{"PING", "a", "b"}, commandRecorderEntry{op: "error", s: "ERR wrong number of arguments for 'ping' command"}},
		{"select-zero", []string{"SELECT", "0"}, commandRecorderEntry{op: "string", s: "OK"}},
		{"select-nonzero", []string{"SELECT", "16"}, commandRecorderEntry{op: "error", s: "ERR DB index is out of range"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn := &commandRecorder{}
			args := make([][]byte, len(tc.cmd))
			for i, a := range tc.cmd {
				args[i] = []byte(a)
			}
			cmd := redcon.Command{Args: args}
			if strings.EqualFold(tc.cmd[0], "PING") {
				server.ping(conn, cmd)
			} else {
				server.selectDB(conn, cmd)
			}
			require.Equal(t, []commandRecorderEntry{tc.want}, conn.writes)
		})
	}
}

func TestRedisRESP2CommandMetadata(t *testing.T) {
	info := runCommand(t, "COMMAND", "INFO", "GET")
	require.Equal(t, commandRecorderEntry{op: "array", n: 10}, info.writes[1])
	require.Equal(t, "array", info.writes[4].op)
	for _, flag := range info.writes[5 : 5+info.writes[4].n] {
		require.Equal(t, "string", flag.op)
	}

	docs := runCommand(t, "COMMAND", "DOCS", "GET")
	require.Contains(t, docs.writes, commandRecorderEntry{op: "bulk", s: "group"})
	renameDocs := runCommand(t, "COMMAND", "DOCS", "RENAME")
	require.Contains(t, renameDocs.writes, commandRecorderEntry{op: "bulk", s: "generic"})

	unknown := runCommand(t, "COMMAND", "DOCS", "NO_SUCH_COMMAND")
	require.Equal(t, []commandRecorderEntry{{op: "array", n: 0}}, unknown.writes)
}

func TestRedisRESP2CommandGroups(t *testing.T) {
	// Redis 8.10.1 COMMAND DOCS groups for all 82 public adapter commands,
	// plus the two ElasticKV-only aliases.
	groups := map[string]string{
		"connection":   "client hello ping quit select",
		"generic":      "del exists expire keys pexpire pttl rename scan ttl type",
		"hash":         "hdel hexists hget hgetall hincrby hlen hmget hmset hset",
		"hyperloglog":  "pfadd pfcount",
		"list":         "lindex llen lpop lpos lpush lrange lrem lset ltrim rpop rpoplpush rpush",
		"pubsub":       "publish pubsub subscribe",
		"scripting":    "eval evalsha",
		"server":       "command dbsize flushall flushdb flushlegacy info",
		"set":          "sadd scard sismember smembers srem",
		"sorted-set":   "bzpopmin elastickv.zremfast zadd zcard zcount zincrby zpopmin zrange zrangebyscore zrem zremrangebyrank zremrangebyscore zrevrange zrevrangebyscore zscore",
		"stream":       "xadd xlen xrange xread xrevrange xtrim",
		"string":       "get getdel incr set setex setnx",
		"transactions": "discard exec multi",
	}
	want := make(map[string]string, len(redisCommandSpecs))
	for group, names := range groups {
		for _, name := range strings.Fields(names) {
			want[name] = group
		}
	}
	require.Len(t, want, len(redisCommandSpecs))
	for _, spec := range redisCommandSpecs {
		require.Equal(t, want[spec.Name], redisCommandGroup(spec.Name), spec.Name)
	}
}

func TestRedisRESP2NullArrayReplies(t *testing.T) {
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	for _, tc := range []struct {
		name string
		args []string
	}{
		{"xread-no-block", []string{"XREAD", "STREAMS", "resp2:missing", "0"}},
		{"xread-block-timeout", []string{"XREAD", "BLOCK", "10", "STREAMS", "resp2:missing", "0"}},
		{"bzpopmin-timeout", []string{"BZPOPMIN", "resp2:missing", "0.01"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dialer := &net.Dialer{Timeout: time.Second}
			conn, err := dialer.DialContext(context.Background(), "tcp", nodes[0].redisAddress)
			require.NoError(t, err)
			defer conn.Close()
			require.NoError(t, conn.SetDeadline(time.Now().Add(2*time.Second)))
			var command strings.Builder
			fmt.Fprintf(&command, "*%d\r\n", len(tc.args))
			for _, arg := range tc.args {
				fmt.Fprintf(&command, "$%d\r\n%s\r\n", len(arg), arg)
			}
			_, err = conn.Write([]byte(command.String()))
			require.NoError(t, err)
			line, err := bufio.NewReader(conn).ReadString('\n')
			require.NoError(t, err)
			require.Equal(t, "*-1\r\n", line)
		})
	}
}
