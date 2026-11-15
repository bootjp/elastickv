package adapter

import (
	"context"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/redcon"
)

func TestLeaderProxyDoesNotReplayAnAppliedCommandAfterLostReply(t *testing.T) {
	for _, blocking := range []bool{false, true} {
		t.Run(map[bool]string{false: "regular", true: "blocking"}[blocking], func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			var applied atomic.Int64
			done := make(chan struct{})
			go func() {
				defer close(done)
				_ = redcon.Serve(listener, func(conn redcon.Conn, cmd redcon.Command) {
					switch strings.ToUpper(string(cmd.Args[0])) {
					case "HELLO":
						conn.WriteError("ERR unknown command 'HELLO'")
					case "CLIENT":
						conn.WriteString("OK")
					case "RPUSH":
						if applied.Add(1) == 1 {
							// The leader applied the write, but its reply was lost.
							_ = conn.Close()
							return
						}
						conn.WriteInt64(applied.Load())
					default:
						conn.WriteError("ERR unexpected command")
					}
				}, nil, nil)
			}()
			srv := &RedisServer{leaderClients: make(map[string]*redis.Client)}
			var cli *redis.Client
			if blocking {
				cli = srv.getOrCreateBlockingLeaderClient(listener.Addr().String())
			} else {
				cli = srv.getOrCreateLeaderClient(listener.Addr().String())
			}
			t.Cleanup(func() {
				_ = cli.Close()
				_ = listener.Close()
				<-done
			})
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err = cli.Do(ctx, "RPUSH", "k", "26").Err()
			require.Error(t, err, "a lost reply must remain indeterminate")
			require.Equal(t, int64(1), applied.Load(), "the proxy must not replay a non-idempotent command")
		})
	}
}
