package adapter

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

// TestS3Server_AdminListBuckets_EmptyReturnsEmptySlice covers the
// "no buckets at all" case so the admin handler can rely on getting
// an empty slice — not nil — and produce a stable `[]` JSON shape.
func TestS3Server_AdminListBuckets_EmptyReturnsEmptySlice(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)

	got, err := server.AdminListBuckets(context.Background())
	require.NoError(t, err)
	require.NotNil(t, got, "must return non-nil slice for empty state so the admin JSON shape is `[]`")
	require.Empty(t, got)
}

// TestS3Server_AdminListBuckets_ReflectsCreatedBuckets confirms the
// SigV4-bypass admin path sees the same buckets a normal SigV4
// CreateBucket flow produced. The two views share loadBucketMetaAt
// + the metadata-prefix scan, so any drift here is an encoding bug
// in summaryFromBucketMeta — exactly the regression the test pins.
func TestS3Server_AdminListBuckets_ReflectsCreatedBuckets(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)

	for _, name := range []string{"alpha", "bravo", "charlie"} {
		rec := httptest.NewRecorder()
		req := newS3TestRequest(http.MethodPut, "/"+name, nil)
		server.handle(rec, req)
		require.Equal(t, http.StatusOK, rec.Code, "create %s", name)
	}

	got, err := server.AdminListBuckets(context.Background())
	require.NoError(t, err)
	require.Len(t, got, 3)
	// ScanAt produces metadata-prefix order (lexicographic by
	// escaped name); summaryFromBucketMeta preserves that.
	require.Equal(t, "alpha", got[0].Name)
	require.Equal(t, "bravo", got[1].Name)
	require.Equal(t, "charlie", got[2].Name)
	for _, b := range got {
		require.Equal(t, s3AclPrivate, b.ACL,
			"unspecified ACL must default to private (matches createBucket)")
		require.NotZero(t, b.CreatedAtHLC, "creation HLC must be populated")
		require.NotZero(t, b.Generation, "generation must be populated")
	}
}

// TestS3Server_AdminDescribeBucket_Existing returns the populated
// summary with ACL / region preserved through the bridge, and
// (nil, false, nil) for a missing name. The handler depends on the
// (nil, false, nil) shape to differentiate "not found" from a
// storage failure without sniffing sentinels.
func TestS3Server_AdminDescribeBucket_Existing(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)

	rec := httptest.NewRecorder()
	req := newS3TestRequest(http.MethodPut, "/orders", nil)
	req.Header.Set("x-amz-acl", s3AclPublicRead)
	server.handle(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	got, exists, err := server.AdminDescribeBucket(context.Background(), "orders")
	require.NoError(t, err)
	require.True(t, exists)
	require.NotNil(t, got)
	require.Equal(t, "orders", got.Name)
	require.Equal(t, s3AclPublicRead, got.ACL,
		"explicit x-amz-acl must round-trip through the admin describe path")
	require.NotZero(t, got.CreatedAtHLC)
}

func TestS3Server_AdminDescribeBucket_Missing(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)

	got, exists, err := server.AdminDescribeBucket(context.Background(), "no-such-bucket")
	require.NoError(t, err)
	require.False(t, exists)
	require.Nil(t, got)
}

// TestS3Server_AdminListBuckets_PaginatesPastSinglePage pins the
// fix for the truncation bug Codex P1 / Claude Issue 1 / Gemini
// flagged on PR #658: AdminListBuckets must walk the metadata
// prefix until exhausted, not stop at adminBucketScanPage. The
// test exceeds the per-iteration page by 100 buckets (1100 total)
// so a regression that re-introduces a single-call ScanAt would
// silently drop the tail and the assertion fails.
//
// Total bucket count (1100) is small enough to keep the test
// O(seconds) on the in-memory MVCC store. Names are zero-padded to
// 4 digits so lexicographic order matches numeric order — the test
// pins both the count AND the ordering contract.
func TestS3Server_AdminListBuckets_PaginatesPastSinglePage(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)

	const total = adminBucketScanPage + 100
	for i := range total {
		name := fmt.Sprintf("bucket-%04d", i)
		rec := httptest.NewRecorder()
		req := newS3TestRequest(http.MethodPut, "/"+name, nil)
		server.handle(rec, req)
		require.Equal(t, http.StatusOK, rec.Code, "create %s", name)
	}

	got, err := server.AdminListBuckets(context.Background())
	require.NoError(t, err)
	require.Len(t, got, total,
		"AdminListBuckets must continue past adminBucketScanPage; truncating here is the regression")
	require.Equal(t, "bucket-0000", got[0].Name)
	require.Equal(t, fmt.Sprintf("bucket-%04d", total-1), got[total-1].Name)
}
