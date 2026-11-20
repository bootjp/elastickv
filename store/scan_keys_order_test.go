package store

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAppendScanKeyResultOrderAndSeen(t *testing.T) {
	t.Parallel()
	const limit = 3
	var result [][]byte
	seen := make(map[string]struct{})
	for _, step := range []struct {
		key  string
		want []string
	}{
		{"a\x00\xff", []string{"a\x00\xff"}},
		{"a\xff", []string{"a\x00\xff", "a\xff"}},
		{"b", []string{"a\x00\xff", "a\xff", "b"}},
		{"a\x00", []string{"a\x00", "a\x00\xff", "a\xff"}},
		{"a", []string{"a", "a\x00", "a\x00\xff"}},
		{"a\x00", []string{"a", "a\x00", "a\x00\xff"}},
		{"b", []string{"a", "a\x00", "a\x00\xff"}},
		{"", []string{"", "a", "a\x00"}},
	} {
		key := []byte(step.key)
		result = appendScanKeyResult(result, seen, key, limit)
		for i := range key {
			key[i] ^= 0xff // The iterator may reuse its key buffer.
		}

		require.Len(t, result, len(step.want), "after %q", step.key)
		require.Len(t, seen, len(step.want), "after %q", step.key)
		for i, want := range step.want {
			require.Equal(t, []byte(want), result[i], "after %q", step.key)
			require.Contains(t, seen, want, "after %q", step.key)
		}
	}
}

func TestPebbleScanKeysAtTrimsAfterNestedPrefixDiscovery(t *testing.T) {
	t.Parallel()
	st, err := NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	ctx := t.Context()
	const ts = 10
	for _, key := range []string{"0", "a", "a\x00", "a\x00\x80"} {
		require.NoError(t, st.PutAt(ctx, []byte(key), []byte("v"), ts, 0))
	}
	// The timestamp suffix puts the extension before both prefixes on disk.
	got, err := st.ScanKeysAt(ctx, []byte("0"), []byte("b"), 3, ts)
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("0"), []byte("a"), []byte("a\x00")}, got)
}

func BenchmarkPebbleScanKeysAtOrdered(b *testing.B) {
	st, err := NewPebbleStore(b.TempDir())
	require.NoError(b, err)
	b.Cleanup(func() { require.NoError(b, st.Close()) })
	const ts = 10
	keys := make([][]byte, 1024)
	mutations := make([]*KVPairMutation, len(keys))
	for i := range keys {
		keys[i] = fmt.Appendf(nil, "key/%04d", i)
		mutations[i] = &KVPairMutation{Op: OpTypePut, Key: keys[i], Value: []byte("v")}
	}
	require.NoError(b, st.ApplyMutations(b.Context(), mutations, nil, 0, ts))
	start, end := []byte("key/"), []byte("key0")
	for _, limit := range []int{64, len(keys)} {
		b.Run(fmt.Sprintf("limit=%d", limit), func(b *testing.B) {
			ctx := b.Context()
			got, err := st.ScanKeysAt(ctx, start, end, limit, ts)
			require.NoError(b, err)
			require.Equal(b, keys[:limit], got)
			b.ReportAllocs()
			for b.Loop() {
				got, err = st.ScanKeysAt(ctx, start, end, limit, ts)
				if err != nil || len(got) != limit {
					b.Fatalf("ScanKeysAt returned %d keys, err = %v", len(got), err)
				}
			}
		})
	}
}
