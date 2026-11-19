package store

import "testing"

func BenchmarkMemoryMVCCHistory(b *testing.B) {
	const versions = 4096
	key := []byte("hot-key")
	st := newMVCCStore()
	b.Cleanup(func() { _ = st.Close() })
	ctx := b.Context()
	for ts := uint64(1); ts <= versions; ts++ {
		if err := st.PutAt(ctx, key, []byte("value"), ts, 0); err != nil {
			b.Fatal(err)
		}
	}
	for _, tc := range []struct {
		name string
		ts   uint64
	}{
		{"oldest", 1},
		{"middle", versions / 2},
		{"previous", versions - 1},
		{"latest", versions},
	} {
		b.Run("GetAt/"+tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := st.GetAt(ctx, key, tc.ts); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
	b.Run("VersionExistsAtOrBefore/before-first", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if exists, err := st.VersionExistsAtOrBefore(ctx, key, 0); err != nil || exists {
				b.Fatalf("exists=%t err=%v", exists, err)
			}
		}
	})
}
