package adapter

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/kv"
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

// TestS3Server_AdminDeleteBucket_SweepsOrphansAcrossAllPerBucketPrefixes
// is the regression test for the AdminDeleteBucket TOCTOU race
// (design doc 2026_04_28_proposed_admin_delete_bucket_safety_net.md;
// coderabbitai 🔴/🟠 on PR #669). The race lands when a concurrent
// PutObject inserts data between AdminDeleteBucket's empty-probe
// scan (at readTS) and its commit (at a later commitTS). Without
// the DEL_PREFIX safety net, the BucketMetaKey delete commits but
// the concurrent write's manifest, blob chunks, upload metadata,
// upload parts, GC entries, and route key all survive — orphaned
// under a now-deleted bucket meta with no API visibility.
//
// This test plants orphan keys directly in the store across the
// 5 non-manifest per-bucket prefixes (the empty-probe only scans
// the manifest prefix; orphans in the other 5 are exactly what
// can leak through the race window). It then calls
// AdminDeleteBucket and asserts every per-bucket prefix is empty
// at a post-commit readTS. The manifest prefix is covered
// indirectly by the symmetric assertion (the safety net wipes
// it whether or not orphans landed there).
//
// Without the fix, the assertions for upload-meta / upload-part /
// blob / gc / route fail because AdminDeleteBucket's commit only
// touched BucketMetaKey. With the fix, the DEL_PREFIX ops in the
// same OperationGroup tombstone every per-bucket prefix at the
// shared commitTS.
func TestS3Server_AdminDeleteBucket_SweepsOrphansAcrossAllPerBucketPrefixes(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	coord := newLocalAdapterCoordinator(st)
	server := NewS3Server(nil, "", st, coord, nil)
	ctx := context.Background()

	const bucket = "race-target"
	summary, err := server.AdminCreateBucket(ctx,
		fullAdminBucketsPrincipal(), bucket, s3AclPrivate)
	require.NoError(t, err)
	gen := summary.Generation
	require.NotZero(t, gen)

	// Plant orphan keys across the 5 non-manifest per-bucket
	// prefixes. Each entry is what a concurrent PutObject (or its
	// in-flight multipart upload state) would leave behind if it
	// committed in the AdminDeleteBucket race window. The values
	// are arbitrary — DEL_PREFIX tombstones key-by-key at the
	// commit timestamp, so the body content does not matter for
	// the assertion.
	const (
		objectName = "race/object.bin"
		uploadID   = "upload-race"
	)
	planted := []*kv.Elem[kv.OP]{
		{Op: kv.Put, Key: s3keys.UploadMetaKey(bucket, gen, objectName, uploadID), Value: []byte("orphan-upload-meta")},
		{Op: kv.Put, Key: s3keys.UploadPartKey(bucket, gen, objectName, uploadID, 1), Value: []byte("orphan-part")},
		{Op: kv.Put, Key: s3keys.BlobKey(bucket, gen, objectName, uploadID, 1, 0), Value: []byte("orphan-chunk")},
		{Op: kv.Put, Key: s3keys.GCUploadKey(bucket, gen, objectName, uploadID), Value: []byte("orphan-gc")},
		{Op: kv.Put, Key: s3keys.RouteKey(bucket, gen, objectName), Value: []byte("orphan-route")},
	}
	_, err = coord.Dispatch(ctx, &kv.OperationGroup[kv.OP]{Elems: planted})
	require.NoError(t, err)

	// Sanity check: every planted key is visible BEFORE the delete.
	postPlantTS := coord.Clock().Next()
	for _, elem := range planted {
		got, err := st.GetAt(ctx, elem.Key, postPlantTS)
		require.NoError(t, err)
		require.NotNil(t, got, "planted key %q must be visible before AdminDeleteBucket", string(elem.Key))
	}

	// Empty-probe sees the bucket as empty (no manifest keys
	// planted) so the delete proceeds and the safety net runs.
	require.NoError(t, server.AdminDeleteBucket(ctx,
		fullAdminBucketsPrincipal(), bucket))

	// After the delete commits, every per-bucket prefix must be
	// empty at any post-commit readTS. ScanAt at the latest clock
	// tick covers all visible commits.
	postDeleteTS := coord.Clock().Next()
	prefixes := []struct {
		name   string
		prefix []byte
	}{
		{"object_manifest", s3keys.ObjectManifestPrefixForBucket(bucket, gen)},
		{"upload_meta", s3keys.UploadMetaPrefixForBucket(bucket, gen)},
		{"upload_part", s3keys.UploadPartPrefixForBucket(bucket, gen)},
		{"blob", s3keys.BlobPrefixForBucket(bucket, gen)},
		{"gc_upload", s3keys.GCUploadPrefixForBucket(bucket, gen)},
		{"route", s3keys.RoutePrefixForBucket(bucket, gen)},
	}
	for _, p := range prefixes {
		t.Run(p.name, func(t *testing.T) {
			kvs, err := st.ScanAt(ctx, p.prefix, prefixScanEnd(p.prefix), 100, postDeleteTS)
			require.NoError(t, err)
			require.Empty(t, kvs,
				"AdminDeleteBucket must sweep the %s prefix; orphans here mean the DEL_PREFIX safety net regressed", p.name)
		})
	}

	// BucketMetaKey is also gone (existing contract; pinned here
	// alongside the new prefix assertions so a refactor that
	// replaces the Del with the wrong shape still triggers a
	// failure). MVCCStore returns ErrKeyNotFound for tombstoned
	// keys; we match on that explicitly so a future refactor that
	// changes the absence shape still fails the assertion.
	_, err = st.GetAt(ctx, s3keys.BucketMetaKey(bucket), postDeleteTS)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}

// TestS3Server_AdminDeleteBucket_BucketGenerationKeySurvives pins
// the orphan-isolation property the design doc relies on:
// AdminDeleteBucket must NOT delete BucketGenerationKey. Re-creating
// a bucket with the same name bumps the generation, and any blobs
// or manifests that ever escaped under the old generation prefix
// stay invisible to the new bucket. Removing the generation key
// would lose that property — a regression here would let a
// recreate land back at generation=1 and accidentally pick up
// pre-existing orphans.
func TestS3Server_AdminDeleteBucket_BucketGenerationKeySurvives(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	coord := newLocalAdapterCoordinator(st)
	server := NewS3Server(nil, "", st, coord, nil)
	ctx := context.Background()

	const bucket = "gen-survives"
	summary, err := server.AdminCreateBucket(ctx,
		fullAdminBucketsPrincipal(), bucket, s3AclPrivate)
	require.NoError(t, err)
	originalGen := summary.Generation

	require.NoError(t, server.AdminDeleteBucket(ctx,
		fullAdminBucketsPrincipal(), bucket))

	// BucketGenerationKey must still be readable after delete.
	postDeleteTS := coord.Clock().Next()
	got, err := st.GetAt(ctx, s3keys.BucketGenerationKey(bucket), postDeleteTS)
	require.NoError(t, err)
	require.NotNil(t, got,
		"BucketGenerationKey must NOT be deleted; orphan isolation across recreate depends on it")

	// Re-create lands at a strictly higher generation.
	recreated, err := server.AdminCreateBucket(ctx,
		fullAdminBucketsPrincipal(), bucket, s3AclPrivate)
	require.NoError(t, err)
	require.Greater(t, recreated.Generation, originalGen,
		"recreate must bump generation; if it ever lands back at the original generation, "+
			"orphan isolation breaks and old pre-existing data could surface in the new bucket")
}
