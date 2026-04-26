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
// All methods are read-only in this slice. Write methods
// (AdminCreateBucket, AdminPutBucketAcl, AdminDeleteBucket) ship in
// the next slice with AdminForward integration so a follower can
// hand them off to the leader transparently.
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

// ErrBucketsForbidden is returned when the principal lacks the
// role required for the operation. Maps to 403. Kept as its own
// sentinel (rather than reusing ErrTablesForbidden) so a future
// per-resource role model can diverge without breaking either
// handler's match list.
var ErrBucketsForbidden = errors.New("admin buckets: principal lacks required role")

// ErrBucketsNotLeader is returned when the local node is not the
// Raft leader for the S3 group. Read-only methods do NOT return
// this — list / describe are leader-agnostic in this slice. Kept
// here so the next slice's write methods can wire it without
// adding a new sentinel.
var ErrBucketsNotLeader = errors.New("admin buckets: local node is not the raft leader")

// ErrBucketsNotFound is returned when DELETE / DESCRIBE / a
// follow-up read targets a bucket that does not exist. The triple
// (nil, false, nil) is the preferred signal for the read path;
// this sentinel covers the future write paths only.
var ErrBucketsNotFound = errors.New("admin buckets: bucket does not exist")

// ErrBucketsAlreadyExists is returned when CREATE targets a name
// that is already in use. Maps to 409. Reserved for the next slice.
var ErrBucketsAlreadyExists = errors.New("admin buckets: bucket already exists")
