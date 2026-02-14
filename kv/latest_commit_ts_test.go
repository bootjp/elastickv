package kv

import (
	"bytes"
	"context"
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

var ErrTestLatestCommitTS = errors.New("test latest commit ts")

type erroringLatestCommitStore struct {
	store.MVCCStore
	key []byte
}

func (s erroringLatestCommitStore) LatestCommitTS(ctx context.Context, key []byte) (uint64, bool, error) {
	if bytes.Equal(key, s.key) {
		return 0, false, ErrTestLatestCommitTS
	}
	return s.MVCCStore.LatestCommitTS(ctx, key)
}

func TestMaxLatestCommitTS_Empty(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ts, err := MaxLatestCommitTS(ctx, nil, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(0), ts)

	st := store.NewMVCCStore()
	ts, err = MaxLatestCommitTS(ctx, st, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(0), ts)

	ts, err = MaxLatestCommitTS(ctx, st, [][]byte{nil, {}})
	require.NoError(t, err)
	require.Equal(t, uint64(0), ts)
}

func TestMaxLatestCommitTS_SingleKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	require.NoError(t, st.PutAt(ctx, []byte("k1"), []byte("v1"), 10, 0))

	ts, err := MaxLatestCommitTS(ctx, st, [][]byte{[]byte("k1")})
	require.NoError(t, err)
	require.Equal(t, uint64(10), ts)
}

func TestMaxLatestCommitTS_DeduplicatesKeys(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("v1"), 10, 0))
	require.NoError(t, st.PutAt(ctx, []byte("b"), []byte("v2"), 20, 0))

	keys := [][]byte{
		[]byte("a"),
		[]byte("b"),
		[]byte("a"),
		[]byte("b"),
	}
	ts, err := MaxLatestCommitTS(ctx, st, keys)
	require.NoError(t, err)
	require.Equal(t, uint64(20), ts)
}

func TestMaxLatestCommitTS_ReturnsError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("v1"), 10, 0))

	wrapped := erroringLatestCommitStore{
		MVCCStore: st,
		key:       []byte("b"),
	}
	ts, err := MaxLatestCommitTS(ctx, wrapped, [][]byte{[]byte("a"), []byte("b")})
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrTestLatestCommitTS))
	require.Equal(t, uint64(0), ts)
}
