package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

type mockClock struct {
	ts uint64
}

func (m *mockClock) Now() uint64 { return m.ts }
func (m *mockClock) advanceMs(ms uint64) {
	m.ts += ms << hlcLogicalBits // HLC encodes milliseconds in high bits
}

func TestMVCCStore_PutWithTTL_Expires(t *testing.T) {
	ctx := context.Background()
	clock := &mockClock{ts: 0}
	st := NewMVCCStoreWithClock(clock)

	require.NoError(t, st.PutWithTTL(ctx, []byte("k"), []byte("v"), 1))

	v, err := st.Get(ctx, []byte("k"))
	require.NoError(t, err)
	require.Equal(t, []byte("v"), v)

	clock.advanceMs(1500) // 1.5s later
	_, err = st.Get(ctx, []byte("k"))
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestMVCCStore_ExpireExisting(t *testing.T) {
	ctx := context.Background()
	clock := &mockClock{ts: 0}
	st := NewMVCCStoreWithClock(clock)

	require.NoError(t, st.Put(ctx, []byte("k"), []byte("v")))
	require.NoError(t, st.Expire(ctx, []byte("k"), 1))

	clock.advanceMs(500)
	v, err := st.Get(ctx, []byte("k"))
	require.NoError(t, err)
	require.Equal(t, []byte("v"), v)

	clock.advanceMs(600) // total 1.1s
	_, err = st.Get(ctx, []byte("k"))
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestMVCCStore_TxnWithTTL(t *testing.T) {
	ctx := context.Background()
	clock := &mockClock{ts: 0}
	st := NewMVCCStoreWithClock(clock)

	require.NoError(t, st.TxnWithTTL(ctx, func(ctx context.Context, txn TTLTxn) error {
		return txn.PutWithTTL(ctx, []byte("k"), []byte("v"), 1)
	}))

	v, err := st.Get(ctx, []byte("k"))
	require.NoError(t, err)
	require.Equal(t, []byte("v"), v)

	clock.advanceMs(1100)
	_, err = st.Get(ctx, []byte("k"))
	require.ErrorIs(t, err, ErrKeyNotFound)
}
