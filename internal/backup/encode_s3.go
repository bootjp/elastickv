package backup

import (
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/cockroachdb/errors"
)

// encode_s3.go is the Phase 0b S3 reverse encoder — the inverse of the
// S3 decoder in s3.go (type S3Encoder, which turns internal !s3|* records
// into an s3/<bucket>/ dump tree). Design:
// docs/design/2026_05_25_proposed_snapshot_logical_encoder.md §"S3".
//
// This slice (M4-1) covers BUCKET metadata: it reconstructs the
// !s3|bucket|meta| record (JSON s3LiveBucketMeta) and the
// !s3|bucket|gen| generation counter (8-byte big-endian) from each
// bucket's _bucket.json. Object bodies (re-chunked into !s3|blob| +
// !s3|obj|head| manifests) and multipart uploads land in follow-up slices.
//
// Generation: a uniform s3RestoreGeneration is stamped into the counter
// (and, in later slices, every object/blob key), matching the Option-B
// decision the DynamoDB encoder uses (design §"generation counters").
// The dump carries no generation field, so the original value is not
// preserved; this is internally consistent for a single-cluster restore.
// created_at_hlc is likewise not represented in the dump and is restored
// as 0.

// s3RestoreGeneration is the uniform generation stamped into the bucket
// generation counter and (in later slices) every object/blob key, so a
// restored bucket is internally consistent (Option B; see file header).
const s3RestoreGeneration uint64 = 1

// s3BucketFormatVersion is the only _bucket.json format_version the
// encoder accepts; a newer version fails closed rather than being parsed
// under the v1 schema (mirrors the DynamoDB schema reader's gate).
const s3BucketFormatVersion uint32 = 1

// ErrS3EncodeInvalidBucket is returned when a _bucket.json cannot be
// parsed into the expected shape (or carries an empty bucket name).
var ErrS3EncodeInvalidBucket = errors.New("backup: s3 encode invalid _bucket.json")

// ErrS3EncodeNotRegular is returned when a dump file (here _bucket.json)
// is not a regular file — a symlink, FIFO, device, or directory.
var ErrS3EncodeNotRegular = errors.New("backup: s3 dump file is not a regular file")

// S3RecordEncoder reconstructs the internal S3 keyspace from the decoded
// s3/ directory tree. (Named distinctly from the decoder's S3Encoder in
// s3.go, which despite its name turns records INTO the dump tree.)
type S3RecordEncoder struct {
	inRoot string
}

// NewS3RecordEncoder constructs an encoder rooted at <inRoot>/s3/.
func NewS3RecordEncoder(inRoot string) *S3RecordEncoder {
	return &S3RecordEncoder{inRoot: inRoot}
}

func (e *S3RecordEncoder) s3Dir() string {
	return filepath.Join(e.inRoot, "s3")
}

// Encode walks s3/<bucket>/ and stages each bucket's meta + generation
// counter records on b. A missing s3/ directory is not an error.
func (e *S3RecordEncoder) Encode(b *snapshotBuilder) error {
	dir := e.s3Dir()
	if err := lstatDumpDir(dir); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() { _ = root.Close() }()
	entries, err := readRootDirEntries(root)
	if err != nil {
		return err
	}
	for _, ent := range entries {
		name := ent.Name()
		if !ent.IsDir() {
			// Top-level entries under s3/ must be bucket directories.
			// A regular file or symlink here means the dump is
			// malformed or partially truncated — silently skipping
			// would let the encoder publish a partial .fsm with
			// the affected bucket omitted (codex P2 v31 #904; the
			// manifest's empty S3 scope from populateAdapterScopes
			// cannot otherwise distinguish missing bucket from
			// dumped-empty bucket).
			//
			// Reserved-prefix entries that start with "_" (e.g.
			// _incomplete_uploads, _orphans) are handled by their
			// own dedicated paths and are NOT top-level buckets;
			// the fail-closed should not catch them. Today the
			// reverse encoder doesn't emit those subtrees at all
			// (covered by ErrEncodeUnsupportedS3IncompleteUploads /
			// ErrEncodeUnsupportedS3Orphans) so any "_*" entry here
			// would have been rejected upstream — but skip them
			// here too for forward compat.
			if strings.HasPrefix(name, "_") {
				continue
			}
			return errors.Wrapf(ErrS3EncodeNotRegular,
				"s3/%s is not a directory (mode=%s); top-level entries under s3/ must be bucket directories",
				name, ent.Type())
		}
		if err := e.encodeBucket(b, root, name); err != nil {
			return err
		}
	}
	return nil
}

// encodeBucket reads one <bucket>/_bucket.json and emits its
// !s3|bucket|meta| record and !s3|bucket|gen| counter.
func (e *S3RecordEncoder) encodeBucket(b *snapshotBuilder, root *os.Root, bucketDir string) error {
	pub, err := e.readBucketMeta(root, bucketDir)
	if err != nil {
		return err
	}
	if pub.Name == "" {
		return errors.Wrapf(ErrS3EncodeInvalidBucket, "%s/_bucket.json: empty bucket name", bucketDir)
	}
	// s3PublicBucket carries Versioning, PolicyJSONString, and
	// CreationTimeISO, but the internal s3LiveBucketMeta record has no
	// counterpart for them — they are dump-only metadata the live store
	// never persisted — so they are intentionally not restored (the
	// decoder likewise leaves CreatedAtHLC out of the public struct).
	live := s3LiveBucketMeta{
		BucketName:   pub.Name,
		Generation:   s3RestoreGeneration,
		CreatedAtHLC: 0,
		Owner:        pub.Owner,
		Region:       pub.Region,
		Acl:          pub.ACL,
	}
	metaVal, err := json.Marshal(live)
	if err != nil {
		return errors.WithStack(err)
	}
	if err := b.Add(s3keys.BucketMetaKey(pub.Name), metaVal, 0); err != nil {
		return err
	}
	var genRaw [8]byte
	binary.BigEndian.PutUint64(genRaw[:], s3RestoreGeneration)
	if err := b.Add(s3keys.BucketGenerationKey(pub.Name), genRaw[:], 0); err != nil {
		return err
	}
	return e.encodeBucketObjects(b, root, bucketDir, pub.Name)
}

// readBucketMeta opens <bucket>/_bucket.json within root (symlink-escape /
// FIFO / hard-link safe, like the DynamoDB schema reader) and decodes the
// public bucket projection.
func (e *S3RecordEncoder) readBucketMeta(root *os.Root, bucketDir string) (s3PublicBucket, error) {
	rel := filepath.Join(bucketDir, "_bucket.json")
	linfo, err := root.Lstat(rel)
	if err != nil {
		return s3PublicBucket{}, errors.WithStack(err)
	}
	if !linfo.Mode().IsRegular() {
		return s3PublicBucket{}, errors.Wrapf(ErrS3EncodeNotRegular, "%s (mode=%s)", rel, linfo.Mode())
	}
	if err := refuseHardLink(linfo, rel); err != nil {
		return s3PublicBucket{}, err
	}
	f, err := root.Open(rel)
	if err != nil {
		return s3PublicBucket{}, errors.WithStack(err)
	}
	defer func() { _ = f.Close() }()
	var pub s3PublicBucket
	if err := decodeOneJSON(f, &pub); err != nil {
		return s3PublicBucket{}, errors.Wrapf(ErrS3EncodeInvalidBucket, "%s: %v", rel, err)
	}
	if pub.FormatVersion != s3BucketFormatVersion {
		return s3PublicBucket{}, errors.Wrapf(ErrS3EncodeInvalidBucket,
			"%s: unsupported format_version %d", rel, pub.FormatVersion)
	}
	return pub, nil
}
