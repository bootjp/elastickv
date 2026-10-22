package adapter

import (
	"context"
	"strings"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

// s3ObjectListingRequest is one ListObjectsV2-style page request. The SigV4
// handler and AdminListObjects both build one and call scanS3ObjectListing,
// then map the page and its errors onto their own wire formats.
type s3ObjectListingRequest struct {
	Bucket            string
	Prefix            string
	Delimiter         string
	ContinuationToken string
	// StartAfter positions the first page after this object key. It is
	// ignored when ContinuationToken is set; only SigV4 exposes it.
	StartAfter string
	// MaxKeys bounds objects plus common prefixes on the page. Callers clamp
	// it to their own limits first.
	MaxKeys int
}

// s3ListedObject is one object row of a listing page.
type s3ListedObject struct {
	Key      string
	Manifest *s3ObjectManifest
}

// s3ObjectListingPage is one page of a listing. Objects and CommonPrefixes
// keep scan order within each list. NextContinuationToken is set when the
// page filled up before the scan reached the end of the prefix.
type s3ObjectListingPage struct {
	Objects               []s3ListedObject
	CommonPrefixes        []string
	Truncated             bool
	NextContinuationToken string
}

// Listing failures the callers translate. Anything else is an internal error.
var (
	errS3ListingMalformedToken = errors.New("malformed continuation token")
	errS3ListingTokenMismatch  = errors.New("continuation token does not match request")
	errS3ListingTokenExpired   = errors.New("continuation token readTS has been compacted")
	errS3ListingNoSuchBucket   = errors.New("bucket not found")
)

// scanS3ObjectListing runs one listing page: it decodes and validates the
// continuation token, reads the bucket metadata and scans the object
// manifests at the token's read timestamp (or a fresh one for a first page),
// collapses keys under Delimiter into common prefixes, and encodes the next
// continuation token.
//
// A token whose read timestamp has been compacted fails with
// errS3ListingTokenExpired whether the bucket-metadata load or the scan hits
// the compaction first.
func (s *S3Server) scanS3ObjectListing(ctx context.Context, req s3ObjectListingRequest) (s3ObjectListingPage, error) {
	// Decode the token first so metadata is loaded at the same snapshot the
	// scan uses.
	token, err := decodeS3ContinuationToken(req.ContinuationToken)
	if err != nil {
		return s3ObjectListingPage{}, errors.WithStack(
			errors.Mark(errors.Wrap(err, "decode continuation token"), errS3ListingMalformedToken))
	}
	readTS := s.readTS()
	if token != nil && token.ReadTS != 0 {
		readTS = token.ReadTS
	}
	generation, err := s.s3ListingGeneration(ctx, req, token, readTS)
	if err != nil {
		return s3ObjectListingPage{}, err
	}

	readPin := s.pinReadTS(readTS)
	defer readPin.Release()

	scan := &s3ObjectListingScan{
		req:        req,
		generation: generation,
		readTS:     readTS,
		resumed:    token != nil,
		seen:       map[string]struct{}{},
	}
	if err := s.runS3ObjectListingScan(ctx, scan, scan.start(token)); err != nil {
		return s3ObjectListingPage{}, err
	}
	return scan.page, nil
}

// s3ListingGeneration loads the bucket metadata at readTS and checks the
// continuation token, if any, against the request and the bucket's current
// generation.
func (s *S3Server) s3ListingGeneration(ctx context.Context, req s3ObjectListingRequest, token *s3ContinuationToken, readTS uint64) (uint64, error) {
	meta, exists, err := s.loadBucketMetaAt(ctx, req.Bucket, readTS)
	if err != nil {
		if token != nil && errors.Is(err, store.ErrReadTSCompacted) {
			return 0, errors.WithStack(errS3ListingTokenExpired)
		}
		return 0, err
	}
	if !exists || meta == nil {
		return 0, errors.WithStack(errS3ListingNoSuchBucket)
	}
	if token != nil && !token.resumes(req, meta.Generation) {
		return 0, errors.WithStack(errS3ListingTokenMismatch)
	}
	return meta.Generation, nil
}

// resumes reports whether the token was issued for this request against the
// bucket's current generation.
func (t *s3ContinuationToken) resumes(req s3ObjectListingRequest, generation uint64) bool {
	return t.Bucket == req.Bucket && t.Generation == generation &&
		t.Prefix == req.Prefix && t.Delimiter == req.Delimiter
}

// s3ObjectListingScan accumulates one listing page.
type s3ObjectListingScan struct {
	req        s3ObjectListingRequest
	generation uint64
	readTS     uint64
	resumed    bool
	seen       map[string]struct{}
	emitted    int
	page       s3ObjectListingPage
}

func (s *S3Server) runS3ObjectListingScan(ctx context.Context, scan *s3ObjectListingScan, start []byte) error {
	end := prefixScanEnd(s3keys.ObjectManifestScanStart(scan.req.Bucket, scan.generation, scan.req.Prefix))
	cursor := start
	for scan.emitted < scan.req.MaxKeys {
		pageLimit := min(s3ListPageSize, scan.req.MaxKeys-scan.emitted)
		kvs, err := s.store.ScanAt(ctx, cursor, end, pageLimit, scan.readTS)
		if err != nil {
			if scan.resumed && errors.Is(err, store.ErrReadTSCompacted) {
				return errors.WithStack(errS3ListingTokenExpired)
			}
			return errors.WithStack(err)
		}
		if len(kvs) == 0 {
			return nil
		}
		for _, kvp := range kvs {
			full, err := scan.add(kvp)
			if err != nil || full {
				return err
			}
		}
		cursor = nextScanCursor(kvs[len(kvs)-1].Key)
		if len(kvs) < pageLimit {
			return nil
		}
	}
	return nil
}

// start returns the first manifest key to scan: just past the token's last
// key when resuming (whose common prefix, if any, was already emitted), just
// past StartAfter, or the start of the prefix.
func (l *s3ObjectListingScan) start(token *s3ContinuationToken) []byte {
	switch {
	case token != nil:
		if token.LastCommonPrefix != "" {
			l.seen[token.LastCommonPrefix] = struct{}{}
		}
		return nextScanCursor(s3keys.ObjectManifestKey(l.req.Bucket, token.Generation, token.LastKey))
	case l.req.StartAfter != "":
		return nextScanCursor(s3keys.ObjectManifestKey(l.req.Bucket, l.generation, l.req.StartAfter))
	default:
		return s3keys.ObjectManifestScanStart(l.req.Bucket, l.generation, l.req.Prefix)
	}
}

// add folds one manifest row into the page and reports whether the page is
// full.
func (l *s3ObjectListingScan) add(kvp *store.KVPair) (bool, error) {
	_, generation, objectKey, ok := s3keys.ParseObjectManifestKey(kvp.Key)
	if !ok || generation != l.generation || !strings.HasPrefix(objectKey, l.req.Prefix) {
		return false, nil
	}
	if commonPrefix, ok := l.commonPrefix(objectKey); ok {
		if _, seen := l.seen[commonPrefix]; seen {
			return false, nil
		}
		l.seen[commonPrefix] = struct{}{}
		l.page.CommonPrefixes = append(l.page.CommonPrefixes, commonPrefix)
		return l.emit(objectKey, commonPrefix), nil
	}
	manifest, err := decodeS3ObjectManifest(kvp.Value)
	if err != nil {
		return false, err
	}
	l.page.Objects = append(l.page.Objects, s3ListedObject{Key: objectKey, Manifest: manifest})
	return l.emit(objectKey, ""), nil
}

// commonPrefix returns the pseudo-directory objectKey collapses into, if the
// request has a delimiter and the key contains it after the prefix.
func (l *s3ObjectListingScan) commonPrefix(objectKey string) (string, bool) {
	if l.req.Delimiter == "" {
		return "", false
	}
	suffix := strings.TrimPrefix(objectKey, l.req.Prefix)
	idx := strings.Index(suffix, l.req.Delimiter)
	if idx < 0 {
		return "", false
	}
	return l.req.Prefix + suffix[:idx+len(l.req.Delimiter)], true
}

// emit counts one row. When the page fills up it records the continuation
// token that resumes after that row.
func (l *s3ObjectListingScan) emit(objectKey, commonPrefix string) bool {
	l.emitted++
	if l.emitted < l.req.MaxKeys {
		return false
	}
	l.page.Truncated = true
	l.page.NextContinuationToken = encodeS3ContinuationToken(l.req.Bucket, l.generation,
		l.req.Prefix, l.req.Delimiter, objectKey, commonPrefix, l.readTS)
	return true
}
