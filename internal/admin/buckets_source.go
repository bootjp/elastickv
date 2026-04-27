package admin

import (
	"context"
	"errors"
)

// BucketsSource is the in-process surface the admin S3 handler
// dispatches into. It mirrors TablesSource on the Dynamo side
// (Section 3.2 of the admin design): defining the contract here lets
// the bridge in main_admin.go translate adapter errors into the
// admin-package vocabulary without the adapter package importing
// internal/admin.
//
// Read methods (List / Describe) are SigV4-bypass equivalents of
// the corresponding HTTP handlers. Write methods (Create / Delete /
// PutACL) additionally enforce a leader check and a Full-role
// principal check inside the adapter — duplicating the role check
// the handler already did defends against a follower-forwarded
// call smuggling a downgraded principal past the leader's view
// (Section 3.2 "認可の真実は常に adapter 側").
type BucketsSource interface {
	// AdminListBuckets returns every bucket this server knows about,
	// in stable lexicographic order. The empty list is a valid
	// response — the handler returns `{"buckets":[]}` rather than
	// 404 so the SPA can distinguish "no buckets yet" from "S3
	// admin not configured" (the latter shape is a 404 from the
	// router fallthrough).
	AdminListBuckets(ctx context.Context) ([]BucketSummary, error)
	// AdminDescribeBucket returns the metadata snapshot for name.
	// The triple (result, present, error) lets the handler emit a
	// 404 for missing buckets without sniffing sentinels; storage
	// failures still surface via the error return.
	AdminDescribeBucket(ctx context.Context, name string) (*BucketSummary, bool, error)
	// AdminCreateBucket creates a bucket atomically with its
	// initial ACL. Returns the freshly-stored summary on success;
	// ErrBucketsAlreadyExists / ErrBucketsForbidden /
	// ErrBucketsNotLeader / *ValidationError on the structured
	// failure paths.
	AdminCreateBucket(ctx context.Context, principal AuthPrincipal, in CreateBucketRequest) (*BucketSummary, error)
	// AdminPutBucketAcl updates the canned ACL on an existing
	// bucket. Returns ErrBucketsNotFound when the bucket is missing
	// and the same role / leader / validation sentinels as Create.
	AdminPutBucketAcl(ctx context.Context, principal AuthPrincipal, name, acl string) error
	// AdminDeleteBucket removes a bucket if it is empty. Returns
	// ErrBucketsNotFound for a missing bucket and ErrBucketsNotEmpty
	// when objects remain (the dashboard cannot force a recursive
	// delete; the SPA renders the 409 with a hint to clean up first).
	AdminDeleteBucket(ctx context.Context, principal AuthPrincipal, name string) error
}

// BucketSummary is the bucket-level DTO the SPA receives. The JSON
// shape matches the design doc Section 4.1 / web/admin's
// `S3Bucket` interface — bucket_name + acl + created_at — plus
// generation/region/owner for operators inspecting via curl.
//
// CreatedAt is an ISO-8601 string (UTC, second precision). The
// adapter persists it as an HLC; the handler formats. Producing
// the formatted string here rather than in the SPA keeps timezone
// rendering server-side and prevents drift between the two SPA
// pages that surface buckets (S3List + S3Detail).
type BucketSummary struct {
	Name       string `json:"bucket_name"`
	ACL        string `json:"acl,omitempty"`
	CreatedAt  string `json:"created_at,omitempty"`
	Generation uint64 `json:"generation,omitempty"`
	Region     string `json:"region,omitempty"`
	Owner      string `json:"owner,omitempty"`
}

// CreateBucketRequest is the JSON body shape for POST
// /admin/api/v1/s3/buckets per design Section 4.2. ACL is optional;
// when omitted, the adapter defaults to "private", matching the
// SigV4 createBucket path's behaviour for a missing x-amz-acl
// header.
type CreateBucketRequest struct {
	BucketName string `json:"bucket_name"`
	ACL        string `json:"acl,omitempty"`
}

// PutBucketACLRequest is the JSON body shape for PUT
// /admin/api/v1/s3/buckets/{name}/acl. Single field so the body
// stays simple, but kept as a typed struct so a future "owner
// override" or "grants" extension does not require revisiting the
// wire format.
type PutBucketACLRequest struct {
	ACL string `json:"acl"`
}

// ErrBucketsForbidden is returned when the principal lacks the
// role required for the operation. Maps to 403. Kept as its own
// sentinel (rather than reusing ErrTablesForbidden) so a future
// per-resource role model can diverge without breaking either
// handler's match list.
var ErrBucketsForbidden = errors.New("admin buckets: principal lacks required role")

// ErrBucketsNotLeader is returned when the local node is not the
// Raft leader for the S3 group. The HTTP handler maps this to
// 503 + Retry-After: 1 today; the next slice's AdminForward
// integration will catch this as the trigger to forward.
var ErrBucketsNotLeader = errors.New("admin buckets: local node is not the raft leader")

// ErrBucketsNotFound is returned when DELETE / DESCRIBE / PutACL
// targets a bucket that does not exist. AdminDescribeBucket's
// (nil, false, nil) tuple is the preferred signal for the read
// path; this sentinel covers the write paths.
var ErrBucketsNotFound = errors.New("admin buckets: bucket does not exist")

// ErrBucketsAlreadyExists is returned when CreateBucket targets
// a name already in use. Maps to 409 Conflict.
var ErrBucketsAlreadyExists = errors.New("admin buckets: bucket already exists")

// ErrBucketsNotEmpty is returned when DeleteBucket targets a
// bucket that still has objects. Maps to 409 Conflict — the
// dashboard cannot force a recursive delete; the SPA must guide
// the operator to clean up first. Mirrors the SigV4 deleteBucket
// path's BucketNotEmpty response.
var ErrBucketsNotEmpty = errors.New("admin buckets: bucket is not empty")
