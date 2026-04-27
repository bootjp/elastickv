package adapter

import (
	"bytes"
	"context"
	"sort"
	"strings"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/kv"
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

// Sentinel errors the admin write methods return so the bridge in
// main_admin.go can translate them into admin-package vocabulary
// without sniffing strings. Named separately from
// ErrAdminTableAlreadyExists / ErrAdminTableNotFound on the Dynamo
// side so a future per-resource role / status divergence does not
// require renaming both packages' callers.
var (
	// ErrAdminBucketAlreadyExists signals that AdminCreateBucket
	// targeted a name already in use. Maps to 409 Conflict.
	ErrAdminBucketAlreadyExists = errors.New("s3 admin: bucket already exists")
	// ErrAdminBucketNotFound signals that AdminDeleteBucket /
	// AdminPutBucketAcl targeted a missing bucket. Maps to 404.
	ErrAdminBucketNotFound = errors.New("s3 admin: bucket not found")
	// ErrAdminBucketNotEmpty signals that AdminDeleteBucket targeted
	// a bucket that still has objects. Maps to 409 Conflict to match
	// the SigV4 path's BucketNotEmpty response (the dashboard cannot
	// force a recursive delete; the operator must clean up first).
	ErrAdminBucketNotEmpty = errors.New("s3 admin: bucket is not empty")
	// ErrAdminInvalidBucketName signals that AdminCreateBucket got
	// a name that does not satisfy validateS3BucketName. Maps to 400.
	ErrAdminInvalidBucketName = errors.New("s3 admin: invalid bucket name")
	// ErrAdminInvalidACL signals that the ACL string did not pass
	// validateS3CannedAcl. Maps to 400 (the SigV4 path returns 501
	// NotImplemented for unsupported canned ACLs, but the admin API
	// is documented as private/public-read only and rejecting other
	// values as invalid input is a more useful contract for the
	// dashboard).
	ErrAdminInvalidACL = errors.New("s3 admin: invalid ACL")
)

// AdminCreateBucket creates a bucket on behalf of the admin
// dashboard. The principal MUST be re-validated by the caller (the
// admin HTTP handler does this against the live RoleStore); this
// method enforces the authorisation invariant a second time so a
// follower-forwarded call cannot smuggle a read-only principal past
// the check on the leader side (Section 3.2 "認可の真実は常に
// adapter 側").
//
// The transaction is atomic: bucket meta + generation + ACL all land
// in a single OperationGroup, mirroring the SigV4 createBucket path.
// On success returns the freshly-stored summary; on conflict returns
// ErrAdminBucketAlreadyExists; on a non-leader / non-full-role / bad
// input returns the corresponding sentinel.
func (s *S3Server) AdminCreateBucket(ctx context.Context, principal AdminPrincipal, name, acl string) (*AdminBucketSummary, error) {
	if !principal.Role.canWrite() {
		return nil, ErrAdminForbidden
	}
	if !s.isVerifiedS3Leader() {
		return nil, ErrAdminNotLeader
	}
	if err := validateS3BucketName(name); err != nil {
		return nil, errors.Wrapf(ErrAdminInvalidBucketName, "%s", err.Error())
	}
	acl = adminCanonicalACL(acl)
	if err := validateS3CannedAcl(acl); err != nil {
		return nil, errors.Wrapf(ErrAdminInvalidACL, "%s", err.Error())
	}

	var summary *AdminBucketSummary
	err := s.retryS3Mutation(ctx, func() error {
		out, err := s.adminCreateBucketTxn(ctx, principal, name, acl)
		if err != nil {
			return err
		}
		summary = out
		return nil
	})
	if err != nil {
		return nil, err //nolint:wrapcheck // sentinel errors propagate as-is; structured errors are already wrapped above.
	}
	return summary, nil
}

// adminCreateBucketTxn is the per-attempt body retryS3Mutation
// invokes. Pulled out so AdminCreateBucket stays under the
// cyclomatic ceiling without hiding the bucket-existence /
// generation / commit-ts dance — every step has a meaningful
// error path that the wrapping retry harness needs to see.
func (s *S3Server) adminCreateBucketTxn(ctx context.Context, principal AdminPrincipal, name, acl string) (*AdminBucketSummary, error) {
	readTS := s.readTS()
	startTS := s.txnStartTS(readTS)
	readPin := s.pinReadTS(readTS)
	defer readPin.Release()

	existing, exists, err := s.loadBucketMetaAt(ctx, name, readTS)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if exists && existing != nil {
		return nil, ErrAdminBucketAlreadyExists
	}
	nextGeneration, err := s.nextBucketGenerationAt(ctx, name, readTS)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	commitTS, err := s.nextTxnCommitTS(startTS)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	meta := &s3BucketMeta{
		BucketName:   name,
		Generation:   nextGeneration,
		CreatedAtHLC: commitTS,
		Region:       s.effectiveRegion(),
		Owner:        principal.AccessKey,
		Acl:          acl,
	}
	body, err := encodeS3BucketMeta(meta)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	_, err = s.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
		IsTxn:    true,
		StartTS:  startTS,
		CommitTS: commitTS,
		Elems: []*kv.Elem[kv.OP]{
			{Op: kv.Put, Key: s3keys.BucketMetaKey(name), Value: body},
			{Op: kv.Put, Key: s3keys.BucketGenerationKey(name), Value: encodeS3Generation(nextGeneration)},
		},
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	out := summaryFromBucketMeta(name, meta)
	return &out, nil
}

// AdminPutBucketAcl swaps the canned ACL on an existing bucket.
// Same authorisation contract as AdminCreateBucket. Mutates only
// the meta.Acl field; generation is preserved so existing object
// references stay valid.
func (s *S3Server) AdminPutBucketAcl(ctx context.Context, principal AdminPrincipal, name, acl string) error {
	if !principal.Role.canWrite() {
		return ErrAdminForbidden
	}
	if !s.isVerifiedS3Leader() {
		return ErrAdminNotLeader
	}
	acl = adminCanonicalACL(acl)
	if err := validateS3CannedAcl(acl); err != nil {
		return errors.Wrapf(ErrAdminInvalidACL, "%s", err.Error())
	}

	err := s.retryS3Mutation(ctx, func() error {
		readTS := s.readTS()
		startTS := s.txnStartTS(readTS)
		readPin := s.pinReadTS(readTS)
		defer readPin.Release()

		meta, exists, err := s.loadBucketMetaAt(ctx, name, readTS)
		if err != nil {
			return errors.WithStack(err)
		}
		if !exists || meta == nil {
			return ErrAdminBucketNotFound
		}
		meta.Acl = acl
		body, err := encodeS3BucketMeta(meta)
		if err != nil {
			return errors.WithStack(err)
		}
		_, err = s.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
			IsTxn:   true,
			StartTS: startTS,
			Elems: []*kv.Elem[kv.OP]{
				{Op: kv.Put, Key: s3keys.BucketMetaKey(name), Value: body},
			},
		})
		return errors.WithStack(err)
	})
	if err != nil {
		return err //nolint:wrapcheck // sentinel errors propagate as-is.
	}
	return nil
}

// AdminDeleteBucket removes a bucket if it is empty. Same
// authorisation contract as the other admin write methods. The
// bucket-must-be-empty rule mirrors the SigV4 deleteBucket path —
// the dashboard cannot force a recursive delete, by design.
//
// Known orphan-race limitation (coderabbitai 🔴 / 🟠 on PR #669):
// the empty-bucket probe (ScanAt with limit=1 on
// ObjectManifestPrefixForBucket) reads at readTS but the
// subsequent BucketMetaKey delete only carries that single point
// key in its ReadKeys set. A concurrent PutObject that inserts a
// manifest key in the scanned prefix between readTS and the
// delete's commitTS will not conflict — the OCC validator only
// inspects keys that appear in ReadKeys, and there is no
// ReadRanges mechanism today. The object's manifest key survives
// under a now-deleted bucket meta and becomes orphaned.
//
// This race exists pre-existing in the SigV4 path
// (adapter/s3.go:deleteBucket — same shape, same limitation), so
// AdminDeleteBucket inherits the contract; closing the gap
// requires either (a) bumping BucketGenerationKey on every
// PutObject so it can serve as an OCC token in this read set, or
// (b) extending OperationGroup with ReadRanges and teaching the
// FSM to validate range emptiness atomically with commit. Both
// are larger changes outside this PR's scope; tracked in
// docs/design/2026_04_24_partial_admin_dashboard.md under the
// Outstanding open items section. Operators concerned about the
// orphan window today should pause writes against the target
// bucket before issuing the admin delete.
func (s *S3Server) AdminDeleteBucket(ctx context.Context, principal AdminPrincipal, name string) error {
	if !principal.Role.canWrite() {
		return ErrAdminForbidden
	}
	if !s.isVerifiedS3Leader() {
		return ErrAdminNotLeader
	}

	err := s.retryS3Mutation(ctx, func() error {
		readTS := s.readTS()
		startTS := s.txnStartTS(readTS)
		readPin := s.pinReadTS(readTS)
		defer readPin.Release()

		meta, exists, err := s.loadBucketMetaAt(ctx, name, readTS)
		if err != nil {
			return errors.WithStack(err)
		}
		if !exists || meta == nil {
			return ErrAdminBucketNotFound
		}
		start := s3keys.ObjectManifestPrefixForBucket(name, meta.Generation)
		kvs, err := s.store.ScanAt(ctx, start, prefixScanEnd(start), 1, readTS)
		if err != nil {
			return errors.WithStack(err)
		}
		if len(kvs) > 0 {
			return ErrAdminBucketNotEmpty
		}
		_, err = s.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
			IsTxn:   true,
			StartTS: startTS,
			Elems: []*kv.Elem[kv.OP]{
				{Op: kv.Del, Key: s3keys.BucketMetaKey(name)},
			},
		})
		return errors.WithStack(err)
	})
	if err != nil {
		return err //nolint:wrapcheck // sentinel errors propagate as-is.
	}
	return nil
}

// adminCanonicalACL normalises an empty input to the canned
// "private" default. The SigV4 createBucket / putBucketAcl paths
// apply the same default after trimming the x-amz-acl header.
// Pulled out so the admin write methods do not silently accept a
// blank string and create / mutate with whatever validateS3CannedAcl
// happens to allow on its empty branch.
func adminCanonicalACL(acl string) string {
	trimmed := strings.TrimSpace(acl)
	if trimmed == "" {
		return s3AclPrivate
	}
	return trimmed
}
