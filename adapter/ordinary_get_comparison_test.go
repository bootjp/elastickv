package adapter

import (
	"bytes"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
)

func BenchmarkOrdinaryPointRead(b *testing.B) {
	for _, backend := range []string{"Memory", "Pebble"} {
		b.Run(backend, func(b *testing.B) {
			ctx := b.Context()
			st := store.NewMVCCStore()
			if backend == "Pebble" {
				var err error
				st, err = store.NewPebbleStore(b.TempDir())
				if err != nil {
					b.Fatal(err)
				}
			}
			b.Cleanup(func() { _ = st.Close() })
			key := []byte("key")
			if err := st.PutAt(ctx, key, bytes.Repeat([]byte{'x'}, 1024), 10, 0); err != nil {
				b.Fatal(err)
			}
			srv := NewGRPCServer(st, &stubAdapterCoordinator{})
			b.Run("Get", func(b *testing.B) {
				req := &pb.GetRequest{Key: key}
				b.ReportAllocs()
				for b.Loop() {
					if _, err := srv.Get(ctx, req); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("RawGet", func(b *testing.B) {
				req := &pb.RawGetRequest{Key: key, Ts: 10}
				b.ReportAllocs()
				for b.Loop() {
					if _, err := srv.RawGet(ctx, req); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

// BenchmarkOrdinaryPebbleGetAt isolates the bytes-only storage read from
// adapter dispatch and timestamp selection.
func BenchmarkOrdinaryPebbleGetAt(b *testing.B) {
	st, err := store.NewPebbleStore(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = st.Close() })
	ctx := b.Context()
	key := []byte("key")
	if err := st.PutAt(ctx, key, bytes.Repeat([]byte{'x'}, 1024), 10, 0); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		value, err := st.GetAt(ctx, key, 10)
		if err != nil || len(value) != 1024 {
			b.Fatalf("GetAt returned %d bytes, %v", len(value), err)
		}
	}
}
