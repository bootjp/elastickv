package adminbridge

import (
	"context"
	"io"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/internal/admin"
	"github.com/cockroachdb/errors"
)

// NewBucketsSource is the S3 counterpart of NewDynamoTablesSource.
// Returns nil when s3Server is nil so a build that ships without the
// S3 adapter (--s3Address empty) silently disables the
// /admin/api/v1/s3/buckets routes.
func NewBucketsSource(s3Server *adapter.S3Server) admin.BucketsSource {
	if s3Server == nil {
		return nil
	}
	return &bucketsBridge{server: s3Server}
}

// bucketsBridge re-shapes the adapter's AdminBucketSummary DTO into
// the admin package's BucketSummary, threading through the
// (result, present, error) tuple semantics for the describe path.
// CreatedAtHLC is formatted into the SPA's expected ISO-8601 string
// here rather than in the adapter — formatting is a UI concern, not
// a storage one.
type bucketsBridge struct {
	server *adapter.S3Server
}

func (b *bucketsBridge) AdminListBuckets(ctx context.Context) ([]admin.BucketSummary, error) {
	rows, err := b.server.AdminListBuckets(ctx)
	if err != nil {
		// Wrap with the bridge frame so an operator debugging a 500
		// from /admin/api/v1/s3/buckets sees the bridge in the error
		// chain (Claude Issue 5 on PR #658). cockroachdb/errors
		// already preserves the adapter's stack trace; this just
		// adds the call-site context.
		return nil, errors.Wrap(err, "admin buckets bridge: list")
	}
	out := make([]admin.BucketSummary, len(rows))
	for i, r := range rows {
		out[i] = bucketSummaryFromAdapter(r)
	}
	return out, nil
}

func (b *bucketsBridge) AdminDescribeBucket(ctx context.Context, name string) (*admin.BucketSummary, bool, error) {
	row, exists, err := b.server.AdminDescribeBucket(ctx, name)
	if err != nil {
		return nil, false, errors.Wrapf(err, "admin buckets bridge: describe %q", name)
	}
	if !exists || row == nil {
		return nil, false, nil
	}
	summary := bucketSummaryFromAdapter(*row)
	return &summary, true, nil
}

func (b *bucketsBridge) AdminCreateBucket(ctx context.Context, principal admin.AuthPrincipal, in admin.CreateBucketRequest) (*admin.BucketSummary, error) {
	row, err := b.server.AdminCreateBucket(ctx, convertAdminPrincipal(principal), in.BucketName, in.ACL)
	if err != nil {
		return nil, translateAdminBucketsError(err)
	}
	if row == nil {
		// AdminCreateBucket guarantees a non-nil summary on success;
		// nil here would be an adapter regression. Surface as a typed
		// error so the handler logs it as 500 rather than panicking
		// on the de-reference at the call site.
		return nil, errors.New("admin buckets bridge: adapter returned nil summary on create success")
	}
	summary := bucketSummaryFromAdapter(*row)
	return &summary, nil
}

func (b *bucketsBridge) AdminPutBucketAcl(ctx context.Context, principal admin.AuthPrincipal, name, acl string) error {
	if err := b.server.AdminPutBucketAcl(ctx, convertAdminPrincipal(principal), name, acl); err != nil {
		return translateAdminBucketsError(err)
	}
	return nil
}

func (b *bucketsBridge) AdminDeleteBucket(ctx context.Context, principal admin.AuthPrincipal, name string) error {
	if err := b.server.AdminDeleteBucket(ctx, convertAdminPrincipal(principal), name); err != nil {
		return translateAdminBucketsError(err)
	}
	return nil
}

// ---------- Phase 3b object-level bridge ----------

func (b *bucketsBridge) AdminListObjects(ctx context.Context, principal admin.AuthPrincipal, bucket string, opts admin.AdminListObjectsOptions) (admin.AdminObjectListing, error) {
	out, err := b.server.AdminListObjects(ctx, convertAdminPrincipal(principal), bucket, adapter.AdminListObjectsOptions{
		Prefix:            opts.Prefix,
		Delimiter:         opts.Delimiter,
		ContinuationToken: opts.ContinuationToken,
		MaxKeys:           opts.MaxKeys,
	})
	if err != nil {
		return admin.AdminObjectListing{}, translateAdminObjectsError(err)
	}
	return adminObjectListingFromAdapter(out), nil
}

func (b *bucketsBridge) AdminGetObject(ctx context.Context, principal admin.AuthPrincipal, bucket, key string) (io.ReadCloser, admin.AdminObject, error) {
	body, meta, err := b.server.AdminGetObject(ctx, convertAdminPrincipal(principal), bucket, key)
	if err != nil {
		return nil, admin.AdminObject{}, translateAdminObjectsError(err)
	}
	return body, adminObjectFromAdapter(meta), nil
}

func (b *bucketsBridge) AdminPutObject(ctx context.Context, principal admin.AuthPrincipal, bucket, key string, body io.Reader, contentType string) error {
	if err := b.server.AdminPutObject(ctx, convertAdminPrincipal(principal), bucket, key, body, contentType); err != nil {
		return translateAdminObjectsError(err)
	}
	return nil
}

func (b *bucketsBridge) AdminDeleteObject(ctx context.Context, principal admin.AuthPrincipal, bucket, key string) error {
	if err := b.server.AdminDeleteObject(ctx, convertAdminPrincipal(principal), bucket, key); err != nil {
		return translateAdminObjectsError(err)
	}
	return nil
}

// adminObjectFromAdapter / adminObjectListingFromAdapter translate
// the adapter's wire shape into the admin package's. The two are
// field-for-field isomorphic so the build breaks here on drift.
func adminObjectFromAdapter(in adapter.AdminObject) admin.AdminObject {
	return admin.AdminObject{
		Key:          in.Key,
		Size:         in.Size,
		ContentType:  in.ContentType,
		ETag:         in.ETag,
		LastModified: in.LastModified,
		StorageClass: in.StorageClass,
	}
}

func adminObjectListingFromAdapter(in adapter.AdminObjectListing) admin.AdminObjectListing {
	out := admin.AdminObjectListing{
		CommonPrefixes:        in.CommonPrefixes,
		NextContinuationToken: in.NextContinuationToken,
	}
	if in.Objects != nil {
		out.Objects = make([]admin.AdminObject, len(in.Objects))
		for i, o := range in.Objects {
			out.Objects[i] = adminObjectFromAdapter(o)
		}
	}
	return out
}

// translateAdminObjectsError maps the adapter's object-tier admin
// error vocabulary onto the admin-package sentinels the handler
// matches against. Mirrors translateAdminBucketsError but covers the
// object-only sentinels (UploadTooLarge, InvalidObjectKey,
// ObjectNotFound) that the bucket-tier path never sees.
func translateAdminObjectsError(err error) error {
	if err == nil {
		return nil
	}
	for _, m := range adminObjectsSentinels {
		if errors.Is(err, m.from) {
			return m.to
		}
	}
	switch {
	case isAdminObjectsValidationError(err):
		// Wrap the adapter's text so the handler's ErrObjectsValidation
		// branch surfaces a useful message rather than the bare
		// sentinel string. Same pattern translateAdminBucketsError
		// uses for ErrAdminInvalidBucketName via *ValidationError.
		return errors.Wrap(admin.ErrObjectsValidation, err.Error())
	case isLeaderChurnError(err):
		return admin.ErrObjectsNotLeader
	default:
		return err //nolint:wrapcheck // forwarded so the handler logs but does not surface it.
	}
}

// adminObjectsSentinels pairs each adapter object-tier sentinel with the
// admin sentinel the HTTP handler matches on.
var adminObjectsSentinels = []struct{ from, to error }{
	{from: adapter.ErrAdminForbidden, to: admin.ErrObjectsForbidden},
	{from: adapter.ErrAdminNotLeader, to: admin.ErrObjectsNotLeader},
	{from: adapter.ErrAdminBucketNotFound, to: admin.ErrObjectsBucketNotFound},
	{from: adapter.ErrAdminObjectNotFound, to: admin.ErrObjectsNotFound},
	{from: adapter.ErrAdminUploadTooLarge, to: admin.ErrObjectsUploadTooLarge},
	{from: adapter.ErrAdminUploadThrottled, to: admin.ErrObjectsUploadThrottled},
}

// isAdminObjectsValidationError reports whether err is one of the adapter's
// object-tier request validation sentinels (answered with 400).
func isAdminObjectsValidationError(err error) bool {
	return errors.Is(err, adapter.ErrAdminInvalidBucketName) ||
		errors.Is(err, adapter.ErrAdminInvalidObjectKey) ||
		errors.Is(err, adapter.ErrAdminInvalidContinuationToken)
}

func bucketSummaryFromAdapter(in adapter.AdminBucketSummary) admin.BucketSummary {
	return admin.BucketSummary{
		Name:       in.Name,
		ACL:        in.ACL,
		CreatedAt:  admin.FormatBucketCreatedAt(in.CreatedAtHLC),
		Generation: in.Generation,
		Region:     in.Region,
		Owner:      in.Owner,
	}
}

// translateAdminBucketsError maps the adapter's S3 admin error
// vocabulary onto the admin-package sentinels the HTTP handler
// matches against. Mirrors translateAdminTablesError on the Dynamo
// side: structured failures (forbidden, not-leader, validation,
// already-exists, etc.) become typed sentinels; everything else
// is forwarded as-is and answered with 500 + a sanitised body.
func translateAdminBucketsError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, adapter.ErrAdminForbidden):
		return admin.ErrBucketsForbidden
	case errors.Is(err, adapter.ErrAdminNotLeader):
		return admin.ErrBucketsNotLeader
	case errors.Is(err, adapter.ErrAdminBucketAlreadyExists):
		return admin.ErrBucketsAlreadyExists
	case errors.Is(err, adapter.ErrAdminBucketNotFound):
		return admin.ErrBucketsNotFound
	case errors.Is(err, adapter.ErrAdminBucketNotEmpty):
		return admin.ErrBucketsNotEmpty
	case errors.Is(err, adapter.ErrAdminInvalidBucketName),
		errors.Is(err, adapter.ErrAdminInvalidACL):
		// Surface the adapter's wrapped message via *ValidationError
		// so the HTTP handler emits 400 invalid_request with a
		// useful explanation instead of leaking raw err.Error().
		return &admin.ValidationError{Message: err.Error()}
	case isLeaderChurnError(err):
		// Mid-dispatch leadership churn looks like an internal error
		// from the kv coordinator; mapping it to the Bucket-side
		// not-leader sentinel keeps the SPA's retry contract intact.
		return admin.ErrBucketsNotLeader
	default:
		return err //nolint:wrapcheck // forwarded so the handler logs but does not surface it.
	}
}
