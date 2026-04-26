package adapter

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
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

// fullAdminPrincipal returns a Full-role principal so write-path
// adapter tests can dispatch without standing up the HTTP layer.
// Mirrors fullAdminPrincipal in dynamodb_admin_test.go (kept
// distinct so the S3 tests can diverge if the role model splits).
func fullAdminBucketsPrincipal() AdminPrincipal {
	return AdminPrincipal{AccessKey: "AKIA_FULL", Role: AdminRoleFull}
}

// readOnlyAdminBucketsPrincipal mirrors the above for the
// read-only role.
func readOnlyAdminBucketsPrincipal() AdminPrincipal {
	return AdminPrincipal{AccessKey: "AKIA_RO", Role: AdminRoleReadOnly}
}

func TestS3Server_AdminCreateBucket_HappyPath(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)

	got, err := server.AdminCreateBucket(context.Background(),
		fullAdminBucketsPrincipal(), "public-assets", s3AclPublicRead)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, "public-assets", got.Name)
	require.Equal(t, s3AclPublicRead, got.ACL)
	require.NotZero(t, got.CreatedAtHLC)
	require.NotZero(t, got.Generation)
	require.Equal(t, "AKIA_FULL", got.Owner,
		"AdminCreateBucket must persist the principal access key as the bucket owner")

	// Round-trip: AdminDescribeBucket should see what we just stored.
	round, exists, err := server.AdminDescribeBucket(context.Background(), "public-assets")
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, s3AclPublicRead, round.ACL)
}

func TestS3Server_AdminCreateBucket_DefaultsACL(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)

	got, err := server.AdminCreateBucket(context.Background(),
		fullAdminBucketsPrincipal(), "private-assets", "")
	require.NoError(t, err)
	require.Equal(t, s3AclPrivate, got.ACL,
		"empty ACL must default to private (matches the SigV4 path)")
}

func TestS3Server_AdminCreateBucket_RejectsReadOnly(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)

	got, err := server.AdminCreateBucket(context.Background(),
		readOnlyAdminBucketsPrincipal(), "any", s3AclPrivate)
	require.ErrorIs(t, err, ErrAdminForbidden)
	require.Nil(t, got)
}

func TestS3Server_AdminCreateBucket_RejectsInvalidACL(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)

	got, err := server.AdminCreateBucket(context.Background(),
		fullAdminBucketsPrincipal(), "any", "public-write")
	require.ErrorIs(t, err, ErrAdminInvalidACL)
	require.Nil(t, got)
}

func TestS3Server_AdminCreateBucket_RejectsInvalidBucketName(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)

	got, err := server.AdminCreateBucket(context.Background(),
		fullAdminBucketsPrincipal(), "BAD_NAME", s3AclPrivate)
	require.ErrorIs(t, err, ErrAdminInvalidBucketName)
	require.Nil(t, got)
}

func TestS3Server_AdminCreateBucket_AlreadyExists(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)

	_, err := server.AdminCreateBucket(context.Background(),
		fullAdminBucketsPrincipal(), "duplicate", s3AclPrivate)
	require.NoError(t, err)

	_, err = server.AdminCreateBucket(context.Background(),
		fullAdminBucketsPrincipal(), "duplicate", s3AclPrivate)
	require.ErrorIs(t, err, ErrAdminBucketAlreadyExists)
}

func TestS3Server_AdminPutBucketAcl_RoundTrips(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)

	_, err := server.AdminCreateBucket(context.Background(),
		fullAdminBucketsPrincipal(), "orders", s3AclPrivate)
	require.NoError(t, err)

	err = server.AdminPutBucketAcl(context.Background(),
		fullAdminBucketsPrincipal(), "orders", s3AclPublicRead)
	require.NoError(t, err)

	got, _, err := server.AdminDescribeBucket(context.Background(), "orders")
	require.NoError(t, err)
	require.Equal(t, s3AclPublicRead, got.ACL)
}

func TestS3Server_AdminPutBucketAcl_MissingBucket(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)

	err := server.AdminPutBucketAcl(context.Background(),
		fullAdminBucketsPrincipal(), "missing", s3AclPublicRead)
	require.ErrorIs(t, err, ErrAdminBucketNotFound)
}

func TestS3Server_AdminPutBucketAcl_RejectsReadOnly(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)

	err := server.AdminPutBucketAcl(context.Background(),
		readOnlyAdminBucketsPrincipal(), "any", s3AclPrivate)
	require.ErrorIs(t, err, ErrAdminForbidden)
}

func TestS3Server_AdminDeleteBucket_HappyPath(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)

	_, err := server.AdminCreateBucket(context.Background(),
		fullAdminBucketsPrincipal(), "to-delete", s3AclPrivate)
	require.NoError(t, err)

	err = server.AdminDeleteBucket(context.Background(),
		fullAdminBucketsPrincipal(), "to-delete")
	require.NoError(t, err)

	_, exists, err := server.AdminDescribeBucket(context.Background(), "to-delete")
	require.NoError(t, err)
	require.False(t, exists)
}

func TestS3Server_AdminDeleteBucket_MissingBucket(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)

	err := server.AdminDeleteBucket(context.Background(),
		fullAdminBucketsPrincipal(), "no-such")
	require.ErrorIs(t, err, ErrAdminBucketNotFound)
}

func TestS3Server_AdminDeleteBucket_RejectsReadOnly(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)

	err := server.AdminDeleteBucket(context.Background(),
		readOnlyAdminBucketsPrincipal(), "any")
	require.ErrorIs(t, err, ErrAdminForbidden)
}

func TestS3Server_AdminDeleteBucket_RejectsNonEmpty(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)

	_, err := server.AdminCreateBucket(context.Background(),
		fullAdminBucketsPrincipal(), "with-objects", s3AclPrivate)
	require.NoError(t, err)

	// Place an object via the SigV4 path so the deletion path sees
	// a non-empty bucket. Reusing the existing handler avoids
	// reaching into the storage layer's encoding directly.
	rec := httptest.NewRecorder()
	req := newS3TestRequest(http.MethodPut, "/with-objects/file.txt",
		strings.NewReader("hello"))
	req.Header.Set("Content-Type", "text/plain")
	server.handle(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	err = server.AdminDeleteBucket(context.Background(),
		fullAdminBucketsPrincipal(), "with-objects")
	require.ErrorIs(t, err, ErrAdminBucketNotEmpty)
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
