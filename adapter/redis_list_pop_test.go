package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

// newListPopTestServer creates a minimal RedisServer backed by an in-memory
// store and a local (non-Raft) coordinator for unit tests.
func newListPopTestServer(t *testing.T) *RedisServer {
	t.Helper()
	st := store.NewMVCCStore()
	coord := newLocalAdapterCoordinator(st)
	return NewRedisServer(nil, "", st, coord, nil, nil)
}

func TestListPop_LPOPSingleItem(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	r := newListPopTestServer(t)
	key := []byte("lpop-single")

	_, err := r.listRPush(ctx, key, [][]byte{[]byte("a"), []byte("b"), []byte("c")})
	require.NoError(t, err)

	got, err := r.listPopClaim(ctx, key, 1, true)
	require.NoError(t, err)
	require.Equal(t, []string{"a"}, got)
}

func TestListPop_RPOPSingleItem(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	r := newListPopTestServer(t)
	key := []byte("rpop-single")

	_, err := r.listRPush(ctx, key, [][]byte{[]byte("x"), []byte("y"), []byte("z")})
	require.NoError(t, err)

	got, err := r.listPopClaim(ctx, key, 1, false)
	require.NoError(t, err)
	require.Equal(t, []string{"z"}, got)
}

func TestListPop_LPOPCount(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	r := newListPopTestServer(t)
	key := []byte("lpop-count")

	_, err := r.listRPush(ctx, key, [][]byte{[]byte("1"), []byte("2"), []byte("3"), []byte("4")})
	require.NoError(t, err)

	got, err := r.listPopClaim(ctx, key, 2, true)
	require.NoError(t, err)
	require.Equal(t, []string{"1", "2"}, got)
}

func TestListPop_RPOPCount(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	r := newListPopTestServer(t)
	key := []byte("rpop-count")

	_, err := r.listRPush(ctx, key, [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")})
	require.NoError(t, err)

	got, err := r.listPopClaim(ctx, key, 2, false)
	require.NoError(t, err)
	require.Equal(t, []string{"d", "c"}, got)
}

func TestListPop_LPOPMoreThanAvailableClamped(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	r := newListPopTestServer(t)
	key := []byte("lpop-clamp")

	_, err := r.listRPush(ctx, key, [][]byte{[]byte("only")})
	require.NoError(t, err)

	got, err := r.listPopClaim(ctx, key, 5, true)
	require.NoError(t, err)
	require.Equal(t, []string{"only"}, got)
}

func TestListPop_EmptyKeyReturnsNil(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	r := newListPopTestServer(t)
	key := []byte("nonexistent")

	got, err := r.listPopClaim(ctx, key, 1, true)
	require.NoError(t, err)
	require.Nil(t, got)
}

func TestListPop_LPOPDrainsListFully(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	r := newListPopTestServer(t)
	key := []byte("drain-left")

	_, err := r.listRPush(ctx, key, [][]byte{[]byte("p"), []byte("q"), []byte("r")})
	require.NoError(t, err)

	// Pop all three items one by one.
	for _, want := range []string{"p", "q", "r"} {
		got, popErr := r.listPopClaim(ctx, key, 1, true)
		require.NoError(t, popErr)
		require.Equal(t, []string{want}, got)
	}

	// List is now empty; next pop should return nil.
	got, err := r.listPopClaim(ctx, key, 1, true)
	require.NoError(t, err)
	require.Nil(t, got)
}

func TestListPop_RPOPDrainsListFully(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	r := newListPopTestServer(t)
	key := []byte("drain-right")

	_, err := r.listRPush(ctx, key, [][]byte{[]byte("a"), []byte("b"), []byte("c")})
	require.NoError(t, err)

	// Pop all three from the tail.
	for _, want := range []string{"c", "b", "a"} {
		got, popErr := r.listPopClaim(ctx, key, 1, false)
		require.NoError(t, popErr)
		require.Equal(t, []string{want}, got)
	}

	got, err := r.listPopClaim(ctx, key, 1, false)
	require.NoError(t, err)
	require.Nil(t, got)
}

func TestListPop_LPOPThenRPOPAlternating(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	r := newListPopTestServer(t)
	key := []byte("alternating")

	_, err := r.listRPush(ctx, key, [][]byte{[]byte("1"), []byte("2"), []byte("3"), []byte("4")})
	require.NoError(t, err)

	// LPOP → "1"
	got, err := r.listPopClaim(ctx, key, 1, true)
	require.NoError(t, err)
	require.Equal(t, []string{"1"}, got)

	// RPOP → "4"
	got, err = r.listPopClaim(ctx, key, 1, false)
	require.NoError(t, err)
	require.Equal(t, []string{"4"}, got)

	// LPOP → "2"
	got, err = r.listPopClaim(ctx, key, 1, true)
	require.NoError(t, err)
	require.Equal(t, []string{"2"}, got)

	// RPOP → "3"
	got, err = r.listPopClaim(ctx, key, 1, false)
	require.NoError(t, err)
	require.Equal(t, []string{"3"}, got)

	// Empty now.
	got, err = r.listPopClaim(ctx, key, 1, true)
	require.NoError(t, err)
	require.Nil(t, got)
}

// TestListPop_ClaimKeyPreventsDoublePopSameSeq verifies that after an item is
// popped (a claim key is written), a second pop at the same OCC readTS produces
// a write-write conflict and retries, returning the next item rather than the
// already-claimed one.
func TestListPop_ClaimKeyLeftAfterPop(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := store.NewMVCCStore()
	coord := newLocalAdapterCoordinator(st)
	r := NewRedisServer(nil, "", st, coord, nil, nil)
	key := []byte("claim-check")

	_, err := r.listRPush(ctx, key, [][]byte{[]byte("item0"), []byte("item1")})
	require.NoError(t, err)

	// Pop the first item.
	got, err := r.listPopClaim(ctx, key, 1, true)
	require.NoError(t, err)
	require.Equal(t, []string{"item0"}, got)

	// Verify the claim key for seq=0 (Head=0) was written.
	readTS := st.LastCommitTS()
	claimKey := store.ListClaimKey(key, 0)
	_, getErr := st.GetAt(ctx, claimKey, readTS)
	require.NoError(t, getErr, "claim key for seq=0 should exist after LPOP")
}
