package adapter

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestRawGetSuccessLogging(t *testing.T) {
	st := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	ctx := t.Context()
	require.NoError(t, st.PutAt(ctx, []byte("present"), []byte("value"), 1, 0))
	require.NoError(t, st.PutAt(ctx, []byte("empty"), []byte{}, 1, 0))

	for _, level := range []slog.Level{slog.LevelInfo, slog.LevelWarn} {
		t.Run(level.String(), func(t *testing.T) {
			var logs bytes.Buffer
			coord := &grpcReadTelemetryCoordinator{}
			srv := NewGRPCServer(st, coord)
			srv.log = slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: level}))
			for _, tc := range []struct {
				key, value string
				exists     bool
			}{
				{"present", "value", true},
				{"empty", "", true},
				{"missing", "", false},
			} {
				logs.Reset()
				resp, err := srv.RawGet(ctx, &pb.RawGetRequest{Key: []byte(tc.key), Ts: 1})
				require.NoError(t, err)
				require.Equal(t, tc.exists, resp.GetExists())
				require.Equal(t, tc.value, string(resp.GetValue()))
				if level == slog.LevelInfo && tc.exists {
					var record map[string]string
					require.NoError(t, json.Unmarshal(logs.Bytes(), &record))
					require.Subset(t, record, map[string]string{
						"level": "INFO", "msg": "Get", "key": tc.key, "value": tc.value,
					})
				} else {
					require.Zero(t, logs.Len())
				}
			}
			require.Equal(t, [][]byte{[]byte("present"), []byte("empty"), []byte("missing")}, coord.keys)
		})
	}
}

// BenchmarkRawGetPebbleHit measures the local RawGet handler at its default Warn level.
func BenchmarkRawGetPebbleHit(b *testing.B) {
	for _, size := range []int{64, 1024, 16384} {
		b.Run(fmt.Sprintf("value_%d", size), func(b *testing.B) {
			st, err := store.NewPebbleStore(b.TempDir())
			require.NoError(b, err)
			b.Cleanup(func() { require.NoError(b, st.Close()) })
			srv := NewGRPCServer(st, nil)
			ctx := b.Context()
			value := bytes.Repeat([]byte("v"), size)
			var requests [64]*pb.RawGetRequest
			for i := range requests {
				key := fmt.Appendf(nil, "point-read-%02d", i)
				require.NoError(b, st.PutAt(ctx, key, value, 1, 0))
				requests[i] = &pb.RawGetRequest{Key: key, Ts: 1}
			}
			b.ReportAllocs()
			i := 0
			for b.Loop() {
				resp, err := srv.RawGet(ctx, requests[i%len(requests)])
				if err != nil || !resp.GetExists() || len(resp.GetValue()) != size {
					b.Fatalf("RawGet returned exists=%v bytes=%d err=%v", resp.GetExists(), len(resp.GetValue()), err)
				}
				i++
			}
		})
	}
}
