package adapter

import (
	"context"
	"sort"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/cockroachdb/errors"
)

// AdminBucketSummary is the bucket-level information the admin
// dashboard surfaces. It deliberately projects only the fields the
// dashboard needs so the package's wire-format types
// (s3BucketMeta, s3ListBucketsResult) stay internal.
//
// CreatedAtHLC is the same physical-time-bearing HLC the bucket
// metadata persists; the admin HTTP handler formats it for the SPA.
// ACL is the canned-ACL string ("private" / "public-read") — the
// admin layer does not expand it into the AWS ACL XML grant tree
// because the dashboard renders the canned form directly.
type AdminBucketSummary struct {
	Name         string
	ACL          string
	CreatedAtHLC uint64
	Generation   uint64
	Region       string
	Owner        string
}

// AdminListBuckets returns every S3-style bucket this server knows
// about, in lexicographic order (the metadata-prefix scan natural
// ordering). Intended for the in-process admin listener as the
// SigV4-free counterpart to the listBuckets HTTP handler; both
// share the same underlying ScanAt so the two views cannot drift.
//
// Returns an empty slice (not nil) when no buckets exist so JSON
// callers see `[]` instead of `null`.
func (s *S3Server) AdminListBuckets(ctx context.Context) ([]AdminBucketSummary, error) {
	readTS := s.readTS()
	readPin := s.pinReadTS(readTS)
	defer readPin.Release()
	kvs, err := s.store.ScanAt(ctx,
		[]byte(s3keys.BucketMetaPrefix),
		prefixScanEnd([]byte(s3keys.BucketMetaPrefix)),
		s3MaxKeys, readTS)
	if err != nil {
		return nil, errors.Wrap(err, "admin list buckets: scan metadata")
	}
	out := make([]AdminBucketSummary, 0, len(kvs))
	for _, kvp := range kvs {
		bucket, ok := s3keys.ParseBucketMetaKey(kvp.Key)
		if !ok {
			continue
		}
		meta, err := decodeS3BucketMeta(kvp.Value)
		if err != nil {
			return nil, errors.Wrapf(err, "admin list buckets: decode metadata for %q", bucket)
		}
		if meta == nil {
			continue
		}
		out = append(out, summaryFromBucketMeta(bucket, meta))
	}
	// ScanAt returns metadata-prefix order which is already
	// lexicographic by escaped name. The escape preserves byte
	// ordering for the ASCII bucket-name alphabet, so a final
	// sort is a defensive no-op rather than a correction — kept
	// to lock the contract in case the encoding changes.
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// AdminDescribeBucket returns the bucket-level snapshot for name.
// The triple (result, present, error) lets admin callers
// distinguish a genuine "not found" from a storage error without
// sniffing sentinels — when the bucket is missing the function
// returns (nil, false, nil), mirroring AdminDescribeTable's
// contract on the Dynamo side.
//
// Like AdminListBuckets this is a read-only path that bypasses
// SigV4. The HTTP admin handler enforces session + CSRF + role at
// the boundary; the adapter trusts the caller for authentication
// (Section 3.2's exception for read-only paths).
func (s *S3Server) AdminDescribeBucket(ctx context.Context, name string) (*AdminBucketSummary, bool, error) {
	readTS := s.readTS()
	readPin := s.pinReadTS(readTS)
	defer readPin.Release()
	meta, exists, err := s.loadBucketMetaAt(ctx, name, readTS)
	if err != nil {
		return nil, false, errors.Wrapf(err, "admin describe bucket %q", name)
	}
	if !exists || meta == nil {
		return nil, false, nil
	}
	summary := summaryFromBucketMeta(name, meta)
	return &summary, true, nil
}

// summaryFromBucketMeta projects the on-disk metadata into the
// admin DTO. Pulled out so list and describe both produce the same
// shape, including the empty-string defaults for optional fields —
// the SPA depends on these being present even when blank.
func summaryFromBucketMeta(name string, meta *s3BucketMeta) AdminBucketSummary {
	return AdminBucketSummary{
		Name:         name,
		ACL:          meta.Acl,
		CreatedAtHLC: meta.CreatedAtHLC,
		Generation:   meta.Generation,
		Region:       meta.Region,
		Owner:        meta.Owner,
	}
}
