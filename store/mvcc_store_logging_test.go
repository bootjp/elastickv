package store

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPutAtLogging(t *testing.T) {
	for _, backend := range []string{"memory", "pebble"} {
		t.Run(backend, func(t *testing.T) {
			var out bytes.Buffer
			var level slog.LevelVar
			logger := slog.New(slog.NewJSONHandler(&out, &slog.HandlerOptions{Level: &level}))
			var st MVCCStore
			if backend == "memory" {
				st = NewMVCCStore(WithLogger(logger))
			} else {
				var err error
				st, err = NewPebbleStore(t.TempDir(), WithPebbleLogger(logger))
				require.NoError(t, err)
			}
			t.Cleanup(func() { require.NoError(t, st.Close()) })
			key := []byte("key")
			level.Set(slog.LevelWarn)
			require.NoError(t, st.PutAt(t.Context(), key, []byte("quiet"), 1, 0))
			require.Zero(t, out.Len())
			level.Set(slog.LevelInfo)
			require.NoError(t, st.PutAt(t.Context(), key, []byte("visible"), 2, 0))
			var record map[string]any
			require.NoError(t, json.Unmarshal(out.Bytes(), &record))
			require.Equal(t, "put_at", record["msg"])
			require.Equal(t, "key", record["key"])
			if backend == "memory" {
				require.Equal(t, "visible", record["value"])
				require.Equal(t, float64(2), record["commit_ts"])
			} else {
				require.NotContains(t, record, "value")
				require.Equal(t, float64(2), record["ts"])
			}
			got, err := st.GetAt(t.Context(), key, 1)
			require.NoError(t, err)
			require.Equal(t, []byte("quiet"), got)
		})
	}
}

// Use one key and timestamp so the version history stays the same size across iterations.
func BenchmarkMVCCPutAtLogging(b *testing.B) {
	for _, size := range []int{64, 1024, 16384} {
		b.Run(fmt.Sprintf("value_%d", size), func(b *testing.B) {
			st := NewMVCCStore()
			b.Cleanup(func() { require.NoError(b, st.Close()) })
			key, value := []byte("logging-benchmark-key"), bytes.Repeat([]byte("v"), size)
			ctx := b.Context()
			b.ReportAllocs()
			for b.Loop() {
				if err := st.PutAt(ctx, key, value, 1, 0); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
