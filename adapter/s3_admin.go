package adapter

import (
	"bytes"
	"context"
	"sort"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

// adminBucketScanPage is the per-iteration ScanAt page size used by
// AdminListBuckets. Smaller than s3MaxKeys is unnecessary —
// ScanAt's per-call memory budget is already this size on the
// SigV4 listBuckets path — but we use it as a named constant so the
// loop's intent is explicit. AdminListBuckets accumulates pages
// until the prefix is exhausted, so the total returned size is
// bounded by the cluster's bucket count rather than this knob.
const adminBucketScanPage = 1000

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
// SigV4-free counterpart to the listBuckets HTTP handler.
//
// Unlike the SigV4 path (which intentionally caps each call at
// s3MaxKeys = 1000 because the AWS API is page-based), the admin
// dashboard's pagination is implemented at the handler layer, which
// expects this method to return the full set. We loop the per-page
// ScanAt until the metadata prefix is exhausted — same pattern as
// scanAllByPrefixAt on the Dynamo side (Codex P1 + Claude Issue 1
// on PR #658).
//
// Returns an empty slice (not nil) when no buckets exist so JSON
// callers see `[]` instead of `null`.
func (s *S3Server) AdminListBuckets(ctx context.Context) ([]AdminBucketSummary, error) {
	readTS := s.readTS()
	readPin := s.pinReadTS(readTS)
	defer readPin.Release()
	prefix := []byte(s3keys.BucketMetaPrefix)
	end := prefixScanEnd(prefix)
	start := bytes.Clone(prefix)
	out := make([]AdminBucketSummary, 0, adminBucketScanPage)
	for {
		kvs, err := s.store.ScanAt(ctx, start, end, adminBucketScanPage, readTS)
		if err != nil {
			return nil, errors.Wrap(err, "admin list buckets: scan metadata")
		}
		if len(kvs) == 0 {
			break
		}
		appended, halt, err := appendAdminBucketSummaries(out, kvs, prefix)
		if err != nil {
			return nil, err
		}
		out = appended
		if halt {
			// A key outside the metadata prefix means the
			// table-of-contents layout changed mid-scan; returning
			// what we have is safer than fabricating a summary
			// from an unrelated key.
			return finaliseAdminBucketList(out), nil
		}
		if len(kvs) < adminBucketScanPage {
			break
		}
		start = nextScanCursor(kvs[len(kvs)-1].Key)
		if end != nil && bytes.Compare(start, end) > 0 {
			break
		}
	}
	return finaliseAdminBucketList(out), nil
}

// appendAdminBucketSummaries projects one ScanAt page into the
// accumulating result slice. Returns the extended slice plus a halt
// flag the caller uses to short-circuit when ScanAt yielded a key
// outside the bucket-meta prefix (a defensive check that should not
// trigger in practice but locks the contract). Splitting this out
// keeps AdminListBuckets under the cyclomatic ceiling without
// hiding the per-row decode + skip logic.
func appendAdminBucketSummaries(out []AdminBucketSummary, kvs []*store.KVPair, prefix []byte) ([]AdminBucketSummary, bool, error) {
	for _, kvp := range kvs {
		if !bytes.HasPrefix(kvp.Key, prefix) {
			return out, true, nil
		}
		bucket, ok := s3keys.ParseBucketMetaKey(kvp.Key)
		if !ok {
			continue
		}
		meta, err := decodeS3BucketMeta(kvp.Value)
		if err != nil {
			return nil, false, errors.Wrapf(err, "admin list buckets: decode metadata for %q", bucket)
		}
		if meta == nil {
			continue
		}
		out = append(out, summaryFromBucketMeta(bucket, meta))
	}
	return out, false, nil
}

// finaliseAdminBucketList sorts the accumulated summaries and
// returns the result. Pulled out because the scan loop has two
// early-exit branches (prefix-mismatch defensive return + the
// natural end-of-prefix exit) and both must guarantee
// lexicographic ordering — one place to enforce it is safer than
// two near-identical sort.Slice calls.
func finaliseAdminBucketList(out []AdminBucketSummary) []AdminBucketSummary {
	// ScanAt yields metadata-prefix order, which is already
	// lexicographic by escaped name on the ASCII bucket-name
	// alphabet. The final sort is defensive against a future
	// key-encoding change rather than a correction today.
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
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
