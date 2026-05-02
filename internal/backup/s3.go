package backup

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/cockroachdb/errors"
)

// Snapshot prefixes the S3 encoder dispatches on. Mirror
// internal/s3keys/keys.go so a renamed prefix surfaces at compile
// time via the dispatch tests.
const (
	S3BucketMetaPrefix     = s3keys.BucketMetaPrefix
	S3BucketGenPrefix      = s3keys.BucketGenerationPrefix
	S3ObjectManifestPrefix = s3keys.ObjectManifestPrefix
	S3UploadMetaPrefix     = s3keys.UploadMetaPrefix
	S3UploadPartPrefix     = s3keys.UploadPartPrefix
	S3BlobPrefix           = s3keys.BlobPrefix
	S3GCUploadPrefix       = s3keys.GCUploadPrefix
	S3RoutePrefix          = s3keys.RoutePrefix
)

// S3MetaSuffixReserved is the sidecar suffix per the design doc. A user
// S3 object key whose suffix matches this is rejected at dump time
// unless WithRenameCollisions is on.
const S3MetaSuffixReserved = ".elastickv-meta.json"

// S3LeafDataSuffix renames the shorter of two S3 keys when the longer
// would force its parent to be a directory. Recorded in KEYMAP.jsonl.
const S3LeafDataSuffix = ".elastickv-leaf-data"

var (
	// ErrS3InvalidBucketMeta is returned when a !s3|bucket|meta value
	// fails JSON decoding.
	ErrS3InvalidBucketMeta = errors.New("backup: invalid !s3|bucket|meta value")
	// ErrS3InvalidManifest is returned when a !s3|obj|head value fails
	// JSON decoding.
	ErrS3InvalidManifest = errors.New("backup: invalid !s3|obj|head value")
	// ErrS3MalformedKey is returned when an S3 key cannot be parsed
	// for its structural components.
	ErrS3MalformedKey = errors.New("backup: malformed S3 key")
	// ErrS3MetaSuffixCollision is returned when a user object key
	// collides with the reserved S3MetaSuffixReserved suffix.
	ErrS3MetaSuffixCollision = errors.New("backup: user S3 object key collides with reserved sidecar suffix")
	// ErrS3IncompleteBlobChunks is returned when a manifest declares
	// N chunks for some part but the snapshot did not contain all N.
	// Without this guard a partial / racy snapshot would silently
	// emit a truncated body. Codex P1 #729.
	ErrS3IncompleteBlobChunks = errors.New("backup: incomplete blob chunks for manifest-declared part")
)

// verifyChunkCompleteness checks every (partNo, partVersion) entry in
// declaredParts has exactly the set of chunkNo values {0, 1, …,
// chunk_count-1} present in chunks. Chunks are expected at chunkNo in
// [0, chunk_count); a missing index in that range surfaces as
// ErrS3IncompleteBlobChunks rather than letting the assembler emit a
// truncated body.
//
// We track the actual set of seen chunk indexes (not just count and
// maxIndex) because count + maxIndex alone admits false positives:
// for declared chunk_count=3, observed `{0, 2, 2}` produces count=3
// and maxIndex=2 but is missing chunkNo=1, which would silently
// assemble a corrupted body. Codex P1 round 12.
//
// declaredParts == nil means "no contract to verify" — used by tests
// that pre-date the manifest-parts feature; production callers
// always populate it via HandleObjectManifest.
func verifyChunkCompleteness(chunks []s3ChunkKey, declaredParts map[s3PartKey]s3DeclaredPart) error {
	if declaredParts == nil {
		return nil
	}
	got := make(map[s3PartKey]map[uint64]struct{}, len(declaredParts))
	for _, k := range chunks {
		pk := s3PartKey{partNo: k.partNo, partVersion: k.partVersion}
		if got[pk] == nil {
			got[pk] = make(map[uint64]struct{})
		}
		got[pk][k.chunkNo] = struct{}{}
	}
	for pk, want := range declaredParts {
		if want.chunkCount == 0 {
			continue
		}
		seen := got[pk]
		if uint64(len(seen)) != want.chunkCount { //nolint:gosec // bounded
			return errors.Wrapf(ErrS3IncompleteBlobChunks,
				"partNo=%d partVersion=%d declared chunks=%d, observed unique=%d",
				pk.partNo, pk.partVersion, want.chunkCount, len(seen))
		}
		for i := uint64(0); i < want.chunkCount; i++ {
			if _, ok := seen[i]; !ok {
				return errors.Wrapf(ErrS3IncompleteBlobChunks,
					"partNo=%d partVersion=%d declared chunks=%d, missing chunkNo=%d",
					pk.partNo, pk.partVersion, want.chunkCount, i)
			}
		}
	}
	return nil
}

// S3Encoder emits per-bucket _bucket.json + assembled object bodies +
// .elastickv-meta.json sidecars + KEYMAP.jsonl, per the Phase 0
// design (docs/design/2026_04_29_proposed_snapshot_logical_decoder.md).
//
// Lifecycle: Handle* per record, Finalize once. Records arrive in
// snapshot lex order:
//
//	!s3|blob|*           (b)  -- written to a per-(bucket,object)
//	                            scratch chunk pool
//	!s3|bucket|gen|*     (bg) -- ignored (operational counter)
//	!s3|bucket|meta|*    (bm) -- buffered until Finalize
//	!s3|gc|upload|*      (g)  -- ignored (in-flight cleanup state)
//	!s3|obj|head|*       (o)  -- buffered until Finalize
//	!s3|upload|meta|*    (um) -- excluded by default; opt in via
//	                            WithIncludeIncompleteUploads
//	!s3|upload|part|*    (up) -- same
//	!s3route|*           (r)  -- ignored (control plane)
//
// Object body assembly happens at Finalize: for each object manifest,
// the encoder enumerates parts in PartNo order and chunks in ChunkNo
// order, concatenates the matching blob chunks (which were
// pre-spilled to scratch files as they arrived), and writes the
// assembled body to <outRoot>/s3/<bucket>/<object> with the metadata
// sidecar at <object>.elastickv-meta.json.
//
// Memory: O(num_objects + num_buckets) buffered metadata. Per-blob
// payloads are streamed to disk as they arrive — never held in memory.
type S3Encoder struct {
	outRoot                  string
	scratchRoot              string
	includeIncompleteUploads bool
	includeOrphans           bool
	renameCollisions         bool

	buckets map[string]*s3BucketState
	warn    func(event string, fields ...any)
}

type s3BucketState struct {
	name string
	meta *s3PublicBucket
	// activeGen is the bucket's current generation, captured from the
	// bucket-meta record. Used at flush time to suppress objects
	// belonging to older incarnations of the same bucket name (Codex
	// P2 #521). Zero means "no bucket meta seen yet"; in that state
	// every object flushes (the prior orphan-warning path covers it).
	activeGen uint64
	objects   map[string]*s3ObjectState // keyed by "object\x00generation"
	// keymap / keymapFile / keymapDir are lazily set on the first
	// rename. KeymapWriter.Close only flushes the bufio buffer, so
	// the *os.File is tracked separately to be closed at finalize —
	// otherwise a dump that produces keymaps for many buckets
	// accumulates descriptors until EMFILE (Codex P1 round 9).
	keymap     *KeymapWriter
	keymapFile *os.File
	keymapDir  string
	// incompleteUploadsJL is opened lazily on the first
	// !s3|upload|meta or !s3|upload|part record under
	// --include-incomplete-uploads, then reused for every subsequent
	// record in the same bucket and closed in flushBucket. Without
	// this caching, the prior code re-opened (truncating!) the file
	// on every record, leaving only the last record on disk and
	// silently losing forensic data — flagged as Codex P2 #318.
	incompleteUploadsJL *jsonlFile
}

type s3ObjectState struct {
	bucket     string
	generation uint64
	object     string
	// uploadID is the manifest's `upload_id`. Set by HandleObjectManifest;
	// consumed by assembleObjectBody to filter chunkPaths so a stale
	// upload's blob chunks (still in the snapshot during a delete/retry
	// window) cannot be merged into the active body — Codex P1 #500,
	// Gemini HIGH #106/#476/#504.
	uploadID string
	// declaredParts maps each manifest-declared (partNo, partVersion)
	// to the metadata the assembler needs to validate completeness
	// (chunk_count). When non-nil, the body assembler restricts
	// chunkPaths to entries matching the keys AND verifies every
	// chunk index in [0, chunk_count) is present — Codex P1 #619
	// (filter) + #729 (completeness). nil means "no filter".
	declaredParts map[s3PartKey]s3DeclaredPart
	// scratchDirCreated avoids the per-blob MkdirAll syscall flagged
	// by Gemini MEDIUM #285. The scratch directory for this object is
	// created exactly once on the first HandleBlob call.
	scratchDirCreated bool
	manifest          *s3PublicManifest
	// chunkPaths maps (uploadID, partNo, chunkNo, partVersion) ->
	// scratch path.
	chunkPaths map[s3ChunkKey]string
}

type s3ChunkKey struct {
	uploadID    string
	partNo      uint64
	chunkNo     uint64
	partVersion uint64
}

// s3PartKey is the manifest-declared part identifier: a (partNo,
// partVersion) tuple. ChunkNo is excluded because the manifest's
// per-part chunk_count drives how many chunks to expect per part;
// that count is stored on the s3DeclaredPart value, not in the key.
type s3PartKey struct {
	partNo      uint64
	partVersion uint64
}

// s3DeclaredPart captures what the manifest claims for a part: its
// expected chunk_count. assembleObjectBody verifies that one chunk
// per (partNo, partVersion, chunkNo) in [0, chunk_count) actually
// arrived; a missing chunk surfaces as ErrS3IncompleteBlobChunks
// rather than a silently-truncated body (Codex P1 #729).
type s3DeclaredPart struct {
	chunkCount uint64
}

// s3PublicBucket is the dump-format projection of s3BucketMeta.
type s3PublicBucket struct {
	FormatVersion    uint32 `json:"format_version"`
	Name             string `json:"name"`
	CreationTimeISO  string `json:"creation_time_iso,omitempty"`
	Owner            string `json:"owner,omitempty"`
	Region           string `json:"region,omitempty"`
	ACL              string `json:"acl,omitempty"`
	Versioning       string `json:"versioning,omitempty"`
	PolicyJSONString string `json:"policy_json,omitempty"`
}

// s3PublicManifest is the dump-format sidecar projection of
// s3ObjectManifest. The dump strips internal fields (UploadID,
// LastModifiedHLC, the per-part ETag/chunk arrays) that are
// implementation detail and surfaces only what S3's HEAD/GET
// expose to clients.
type s3PublicManifest struct {
	FormatVersion      uint32            `json:"format_version"`
	ETag               string            `json:"etag,omitempty"`
	SizeBytes          int64             `json:"size_bytes"`
	LastModified       string            `json:"last_modified,omitempty"`
	ContentType        string            `json:"content_type,omitempty"`
	ContentEncoding    string            `json:"content_encoding,omitempty"`
	CacheControl       string            `json:"cache_control,omitempty"`
	ContentDisposition string            `json:"content_disposition,omitempty"`
	UserMetadata       map[string]string `json:"user_metadata,omitempty"`
}

// s3LiveManifest mirrors the live adapter/s3.go s3ObjectManifest
// just enough to decode the JSON value. Fields the dump format
// drops are still parsed (so unknown-fields default-tolerance is
// preserved) but elided from the public sidecar.
// formatHLCAsRFC3339Nano renders the millisecond half of an HLC
// (the upper 48 bits, see kv/hlc.go) as an RFC3339Nano UTC string
// for the `last_modified` sidecar field. Restore tools compare
// these timestamps to S3 HEAD `Last-Modified` semantics, which is
// millisecond-resolution UTC. HLC zero (no last_modified_hlc on
// the live record) maps to "" so omitempty drops the field rather
// than emitting "1970-01-01T00:00:00Z" — which would mislead
// consumers about the object's age. Codex P2 round 9.
func formatHLCAsRFC3339Nano(hlc uint64) string {
	if hlc == 0 {
		return ""
	}
	ms := int64(hlc >> hlcLogicalBitsForBackupS3) //nolint:gosec // bit-shift is safe; HLC is bounded
	return time.UnixMilli(ms).UTC().Format(time.RFC3339Nano)
}

// hlcLogicalBitsForBackupS3 mirrors kv/hlc.go:hlcLogicalBits. We keep
// the literal here (and in a single place via this name) rather than
// importing the kv package because the backup package is meant to
// stay decoupled from the live cluster's internals.
const hlcLogicalBitsForBackupS3 = 16

type s3LiveManifest struct {
	UploadID           string            `json:"upload_id"`
	ETag               string            `json:"etag"`
	SizeBytes          int64             `json:"size_bytes"`
	LastModifiedHLC    uint64            `json:"last_modified_hlc"`
	ContentType        string            `json:"content_type"`
	ContentEncoding    string            `json:"content_encoding"`
	CacheControl       string            `json:"cache_control"`
	ContentDisposition string            `json:"content_disposition"`
	UserMetadata       map[string]string `json:"user_metadata"`
	Parts              []s3LivePart      `json:"parts"`
}

type s3LivePart struct {
	PartNo      uint64   `json:"part_no"`
	ETag        string   `json:"etag"`
	SizeBytes   int64    `json:"size_bytes"`
	ChunkCount  uint64   `json:"chunk_count"`
	ChunkSizes  []uint64 `json:"chunk_sizes"`
	PartVersion uint64   `json:"part_version"`
}

// NewS3Encoder constructs an encoder rooted at <outRoot>/s3/. Blob
// chunks are spilled to <scratchRoot>/s3/ as they arrive and assembled
// into final object bodies at Finalize. The caller owns scratchRoot;
// it must exist and be writable. A common choice is os.TempDir() under
// the dump runner — the encoder removes its scratch subtree on
// Close().
func NewS3Encoder(outRoot, scratchRoot string) *S3Encoder {
	return &S3Encoder{
		outRoot:     outRoot,
		scratchRoot: filepath.Join(scratchRoot, "s3"),
		buckets:     make(map[string]*s3BucketState),
	}
}

// WithIncludeIncompleteUploads routes !s3|upload|meta|/!s3|upload|part|
// records to s3/<bucket>/_incomplete_uploads/. Default is to skip them.
func (s *S3Encoder) WithIncludeIncompleteUploads(on bool) *S3Encoder {
	s.includeIncompleteUploads = on
	return s
}

// WithIncludeOrphans surfaces blob chunks that have no matching
// manifest under s3/<bucket>/_orphans/. Default skips them.
func (s *S3Encoder) WithIncludeOrphans(on bool) *S3Encoder {
	s.includeOrphans = on
	return s
}

// WithRenameCollisions opts in to renaming user objects that collide
// with the reserved S3MetaSuffixReserved suffix. Default rejects.
func (s *S3Encoder) WithRenameCollisions(on bool) *S3Encoder {
	s.renameCollisions = on
	return s
}

// WithWarnSink wires a structured warning sink.
func (s *S3Encoder) WithWarnSink(fn func(event string, fields ...any)) *S3Encoder {
	s.warn = fn
	return s
}

// HandleBucketMeta decodes and parks a !s3|bucket|meta record.
func (s *S3Encoder) HandleBucketMeta(key, value []byte) error {
	bucketName, ok := s3keys.ParseBucketMetaKey(key)
	if !ok {
		return errors.Wrapf(ErrS3MalformedKey, "bucket meta key: %q", key)
	}
	var live s3LiveBucketMeta
	if err := json.Unmarshal(value, &live); err != nil {
		return errors.Wrap(ErrS3InvalidBucketMeta, err.Error())
	}
	st := s.bucketState(bucketName)
	st.meta = &s3PublicBucket{
		FormatVersion: 1,
		Name:          bucketName,
		Owner:         live.Owner,
		Region:        live.Region,
		ACL:           live.Acl,
	}
	st.activeGen = live.Generation
	return nil
}

type s3LiveBucketMeta struct {
	BucketName   string `json:"bucket_name"`
	Generation   uint64 `json:"generation"`
	CreatedAtHLC uint64 `json:"created_at_hlc"`
	Owner        string `json:"owner"`
	Region       string `json:"region"`
	Acl          string `json:"acl"`
}

// HandleObjectManifest decodes and parks an !s3|obj|head record. The
// manifest's UploadID and Parts list drive the Finalize-time blob
// assembly.
func (s *S3Encoder) HandleObjectManifest(key, value []byte) error {
	bucket, gen, object, ok := s3keys.ParseObjectManifestKey(key)
	if !ok {
		return errors.Wrapf(ErrS3MalformedKey, "manifest key: %q", key)
	}
	var live s3LiveManifest
	if err := json.Unmarshal(value, &live); err != nil {
		return errors.Wrap(ErrS3InvalidManifest, err.Error())
	}
	st := s.objectState(bucket, gen, object)
	st.manifest = &s3PublicManifest{
		FormatVersion:      1,
		ETag:               live.ETag,
		SizeBytes:          live.SizeBytes,
		LastModified:       formatHLCAsRFC3339Nano(live.LastModifiedHLC),
		ContentType:        live.ContentType,
		ContentEncoding:    live.ContentEncoding,
		CacheControl:       live.CacheControl,
		ContentDisposition: live.ContentDisposition,
		UserMetadata:       live.UserMetadata,
	}
	// Capture the manifest's uploadID so assembleObjectBody can
	// filter blob chunks belonging to other (stale or in-flight)
	// upload attempts. Also capture the manifest's declared
	// (partNo, partVersion) set so the assembler restricts itself
	// to canonically-declared parts — older partVersions left
	// behind by overwrite-then-async-cleanup must NOT be merged
	// into the body (Codex P1 #619).
	st.uploadID = live.UploadID
	st.declaredParts = make(map[s3PartKey]s3DeclaredPart, len(live.Parts))
	for _, p := range live.Parts {
		st.declaredParts[s3PartKey{partNo: p.PartNo, partVersion: p.PartVersion}] = s3DeclaredPart{
			chunkCount: p.ChunkCount,
		}
	}
	st.chunkPaths = ensureChunkPaths(st.chunkPaths)
	return nil
}

// HandleBlob spills a !s3|blob| record to a per-chunk scratch file
// and registers it under the (bucket, object, gen, uploadID, partNo,
// chunkNo, partVersion) routing key. EncodeSegment percent-encodes
// `/` so a multi-segment object key like `../../tmp/pwn` collapses
// into one filename, but a literal `..` (or `.`) survives unchanged
// because both `.` chars are RFC3986-unreserved. Without explicit
// validation, a crafted bucket+object pair like `bucket="..",
// object=".."` would resolve to filepath.Join(scratchRoot, "..",
// "..") = the parent of scratchRoot, letting writeFileAtomic
// land outside the decoder's controlled directory before
// safeJoinUnderRoot ever runs at output time. Codex P1 round 11.
func (s *S3Encoder) HandleBlob(key, value []byte) error {
	bucket, gen, object, uploadID, partNo, chunkNo, partVersion, ok := s3keys.ParseBlobKey(key)
	if !ok {
		return errors.Wrapf(ErrS3MalformedKey, "blob key: %q", key)
	}
	st := s.objectState(bucket, gen, object)
	dir, err := scratchDirForBlob(s.scratchRoot, bucket, object)
	if err != nil {
		return err
	}
	if !st.scratchDirCreated {
		if err := os.MkdirAll(dir, 0o755); err != nil { //nolint:mnd // 0755 == standard dir mode
			return errors.WithStack(err)
		}
		st.scratchDirCreated = true
	}
	path := filepath.Join(dir, blobScratchName(uploadID, partNo, chunkNo, partVersion))
	if err := writeFileAtomic(path, value); err != nil {
		return err
	}
	st.chunkPaths = ensureChunkPaths(st.chunkPaths)
	st.chunkPaths[s3ChunkKey{uploadID: uploadID, partNo: partNo, chunkNo: chunkNo, partVersion: partVersion}] = path
	return nil
}

// scratchDirForBlob builds the per-(bucket,object) scratch path and
// validates it stays under scratchRoot. A bucket or object name of
// `.` / `..` would let `filepath.Join` resolve out of scratchRoot
// before anything else gets a chance to refuse the key. Reject the
// dot-component case at the encoder boundary so the spill-to-disk
// step inherits the same containment invariant the final output
// path enforces via safeJoinUnderRoot.
func scratchDirForBlob(scratchRoot, bucket, object string) (string, error) {
	for _, seg := range [...]string{bucket, object} {
		switch seg {
		case ".", "..":
			return "", errors.Wrapf(ErrS3MalformedKey,
				"bucket or object key %q is a dot segment (would escape scratch root)", seg)
		case "":
			return "", errors.Wrapf(ErrS3MalformedKey,
				"bucket or object key is empty (cannot construct scratch path)")
		}
	}
	return filepath.Join(scratchRoot, EncodeSegment([]byte(bucket)), EncodeSegment([]byte(object))), nil
}

// HandleIncompleteUpload routes !s3|upload|meta|/!s3|upload|part|
// records to <bucket>/_incomplete_uploads/records.jsonl when the
// include flag is on; otherwise drops them.
//
// The output writer is opened once per bucket on the first record and
// cached on s3BucketState. Re-opening per record (the prior
// implementation) used create/truncate semantics, so each call wiped
// the file and only the last record survived — Codex P2 #318 / Gemini
// HIGH+MEDIUM #318.
func (s *S3Encoder) HandleIncompleteUpload(prefix string, key, value []byte) error {
	if !s.includeIncompleteUploads {
		return nil
	}
	bucket, _, _, _, _, ok := parseUploadFamily(prefix, key)
	if !ok {
		return errors.Wrapf(ErrS3MalformedKey, "upload-family key: %q", key)
	}
	// Reject dot-segment / empty bucket names BEFORE the
	// filesystem join — same fix as flushBucket (round 6) but for
	// the --include-incomplete-uploads code path, which runs
	// during HandleIncompleteUpload and so escapes outRoot before
	// Finalize's bucket-name guard ever runs. EncodeSegment
	// preserves "." and "..", so a malformed snapshot upload
	// record with bucket="." or ".." would otherwise let
	// `filepath.Join` collapse the s3/ subtree and write
	// records.jsonl outside the dump root. Codex P1 round 13
	// (PR #716, follow-up).
	switch bucket {
	case ".", "..", "":
		return errors.Wrapf(ErrS3MalformedKey,
			"bucket name %q in upload-family key is empty or a dot segment", bucket)
	}
	b := s.bucketState(bucket)
	if b.incompleteUploadsJL == nil {
		dir := filepath.Join(s.outRoot, "s3", EncodeSegment([]byte(bucket)), "_incomplete_uploads")
		if err := os.MkdirAll(dir, 0o755); err != nil { //nolint:mnd // 0755 == standard dir mode
			return errors.WithStack(err)
		}
		jl, err := openJSONL(filepath.Join(dir, "records.jsonl"))
		if err != nil {
			return err
		}
		b.incompleteUploadsJL = jl
	}
	rec := struct {
		Prefix   string `json:"prefix"`
		KeyB64   []byte `json:"key"`
		ValueB64 []byte `json:"value"`
	}{Prefix: prefix, KeyB64: key, ValueB64: value}
	if err := b.incompleteUploadsJL.enc.Encode(rec); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// HandleIgnored is a no-op for prefixes the encoder explicitly drops
// (!s3|bucket|gen|, !s3|gc|upload|, !s3route|). Exposed so the master
// pipeline can dispatch all !s3|* prefixes uniformly without
// special-casing.
func (s *S3Encoder) HandleIgnored(_, _ []byte) error { return nil }

// Finalize assembles every object body, writes its sidecar, flushes
// per-bucket _bucket.json, and removes the scratch tree.
func (s *S3Encoder) Finalize() error {
	defer func() { _ = os.RemoveAll(s.scratchRoot) }()
	var firstErr error
	for _, b := range s.buckets {
		if err := s.flushBucket(b); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (s *S3Encoder) flushBucket(b *s3BucketState) error {
	// Reject bucket-name dot segments before the filesystem join.
	// `EncodeSegment(".") == "."` and `EncodeSegment("..") == ".."`
	// (both are RFC3986-unreserved), so without this guard a
	// crafted bucket meta record with name="." or ".." would let
	// `filepath.Join` collapse `<outRoot>/s3/.` back to
	// `<outRoot>/s3` or resolve `<outRoot>/s3/..` up to outRoot
	// itself — `_bucket.json` would land outside the s3/ subtree
	// and clobber dump-root files. Codex P1 round 13 (PR #716,
	// finding originally surfaced on the merged-in s3.go).
	switch b.name {
	case ".", "..", "":
		return errors.Wrapf(ErrS3MalformedKey,
			"bucket name %q is empty or a dot segment (would escape s3/ subtree)", b.name)
	}
	bucketDir := filepath.Join(s.outRoot, "s3", EncodeSegment([]byte(b.name)))
	if err := os.MkdirAll(bucketDir, 0o755); err != nil { //nolint:mnd // 0755 == standard dir mode
		return errors.WithStack(err)
	}
	if b.meta != nil {
		if err := writeFileAtomic(filepath.Join(bucketDir, "_bucket.json"), mustMarshalIndent(b.meta)); err != nil {
			return err
		}
	}
	staleCount, err := s.flushBucketObjects(b, bucketDir)
	if err != nil {
		return err
	}
	if staleCount > 0 {
		s.emitWarn("s3_stale_generation_objects",
			"bucket", b.name,
			"active_generation", b.activeGen,
			"stale_count", staleCount,
			"hint", "stale-gen objects excluded; restore would otherwise emit them under the new bucket")
	}
	// closeJSONL errors must surface — they are the canonical "data
	// did not flush to disk" signal for a writable resource (Gemini
	// MEDIUM #318).
	if err := closeBucketKeymap(b); err != nil {
		return err
	}
	if b.incompleteUploadsJL != nil {
		if err := closeJSONL(b.incompleteUploadsJL); err != nil {
			return err
		}
	}
	return nil
}

// flushBucketObjects walks the bucket's object set, routes stale-gen
// objects to the orphan path (under --include-orphans) or drops them
// with a warning counter, and flushes active-gen objects normally.
// Split out of flushBucket to keep cyclomatic complexity within the
// package cap.
func (s *S3Encoder) flushBucketObjects(b *s3BucketState, bucketDir string) (int, error) {
	// Pre-compute the set of "directory prefixes" required by the
	// union of active-gen object keys: for an object "a/b/c" the
	// directory prefixes "a" and "a/b" are mandatory parent dirs on
	// the filesystem. An object whose key IS one of those prefixes
	// (e.g., bucket holds both "a/b" and "a/b/c") cannot share the
	// natural path with the longer key — POSIX requires that path
	// be a directory. The design's documented strategy is to rename
	// the shorter key to "<key>.elastickv-leaf-data" and record the
	// rename in KEYMAP.jsonl so restore can reverse it. Codex P1
	// #615.
	dirPrefixes := s.computeDirPrefixes(b)
	objectKeys := s.computeActiveGenObjectKeys(b)
	stale := 0
	for _, obj := range b.objects {
		// Suppress objects from older bucket incarnations: when a
		// bucket is deleted and recreated the generation bumps, but
		// snapshots taken mid-cleanup can still carry the previous
		// generation's manifests + chunks. Routing both to the same
		// natural path is non-deterministic last-write-wins (Codex
		// P2 #521). When a bucket-meta record is present, only its
		// active generation flushes.
		if b.activeGen != 0 && obj.generation != b.activeGen {
			stale++
			if s.includeOrphans {
				if err := s.flushOrphanObject(b, bucketDir, obj); err != nil {
					return stale, err
				}
			}
			continue
		}
		needsLeafDataRename := dirPrefixes[obj.object]
		if err := s.flushObjectWithCollision(b, bucketDir, obj, needsLeafDataRename, objectKeys); err != nil {
			return stale, err
		}
	}
	return stale, nil
}

// computeActiveGenObjectKeys returns the set of every active-gen
// object key in the bucket. resolveObjectFilename consults this set
// so a rename target (`.user-data` or `.elastickv-leaf-data`) that
// happens to match a real object key surfaces an error instead of
// silently merging two distinct objects onto one filesystem path
// (Codex P1 round 9).
func (s *S3Encoder) computeActiveGenObjectKeys(b *s3BucketState) map[string]bool {
	out := make(map[string]bool, len(b.objects))
	for _, obj := range b.objects {
		if b.activeGen != 0 && obj.generation != b.activeGen {
			continue
		}
		out[obj.object] = true
	}
	return out
}

// computeDirPrefixes returns the set of directory prefixes the union
// of active-gen object keys requires. For object key "a/b/c" the
// prefixes are {"a", "a/b"}. The set is consulted at flush time to
// detect file-vs-directory collisions.
func (s *S3Encoder) computeDirPrefixes(b *s3BucketState) map[string]bool {
	out := make(map[string]bool)
	for _, obj := range b.objects {
		if b.activeGen != 0 && obj.generation != b.activeGen {
			continue
		}
		key := obj.object
		// Walk parent directories: split on "/" and accumulate.
		for i := 0; i < len(key); i++ {
			if key[i] != '/' {
				continue
			}
			out[key[:i]] = true
		}
	}
	return out
}

// closeBucketKeymap closes the per-bucket KEYMAP.jsonl writer (if
// opened) and removes the file when no rename was recorded. The
// *os.File is closed separately because KeymapWriter.Close only
// flushes its bufio buffer; without explicit fd close, dumps that
// produce keymaps for many buckets leak descriptors until EMFILE
// (Codex P1 round 9).
func closeBucketKeymap(b *s3BucketState) error {
	if b.keymap == nil {
		return nil
	}
	flushErr := b.keymap.Close()
	var closeErr error
	if b.keymapFile != nil {
		closeErr = b.keymapFile.Close()
	}
	if flushErr == nil && closeErr != nil {
		flushErr = errors.WithStack(closeErr)
	}
	if b.keymap.Count() == 0 && b.keymapDir != "" {
		_ = os.Remove(filepath.Join(b.keymapDir, "KEYMAP.jsonl"))
	}
	return flushErr
}

func (s *S3Encoder) flushObjectWithCollision(b *s3BucketState, bucketDir string, obj *s3ObjectState, needsLeafDataRename bool, objectKeys map[string]bool) error {
	if obj.manifest == nil {
		return s.flushOrphanObject(b, bucketDir, obj)
	}
	objectName, kind, err := s.resolveObjectFilename(b, obj, needsLeafDataRename, objectKeys)
	if err != nil {
		return err
	}
	bodyPath, err := safeJoinUnderRoot(bucketDir, objectName)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(bodyPath), 0o755); err != nil { //nolint:mnd // 0755 == standard dir mode
		return errors.WithStack(err)
	}
	if err := assembleObjectBody(bodyPath, obj); err != nil {
		return err
	}
	sidecar := bodyPath + S3MetaSuffixReserved
	if err := writeFileAtomic(sidecar, mustMarshalIndent(obj.manifest)); err != nil {
		return err
	}
	if kind != "" {
		if err := s.recordKeymap(b, bucketDir, objectName, []byte(obj.object), kind); err != nil {
			return err
		}
	}
	return nil
}

// flushOrphanObject handles objects with chunks but no manifest. By
// default they emit only a warning. With --include-orphans on, the
// chunks are written under <bucket>/_orphans/<encoded-object>/ as
// per-chunk .bin files so the operator can recover bytes manually
// (Gemini MEDIUM #386).
func (s *S3Encoder) flushOrphanObject(b *s3BucketState, bucketDir string, obj *s3ObjectState) error {
	s.emitWarn("s3_orphan_chunks",
		"bucket", b.name,
		"object", obj.object,
		"chunks", len(obj.chunkPaths),
		"hint", "blob chunks present but no !s3|obj|head record matched")
	if !s.includeOrphans {
		return nil
	}
	if len(obj.chunkPaths) == 0 {
		return nil
	}
	if obj.object == "." || obj.object == ".." || obj.object == "" {
		return errors.Wrapf(ErrS3MalformedKey,
			"orphan object key %q is a dot segment (would escape orphan dir)", obj.object)
	}
	// Include the generation in the orphan path. Without this,
	// two stale generations of the same object key sharing
	// (uploadID, partNo, chunkNo, partVersion) — possible during
	// delete/recreate cleanup windows where the live system
	// recycled identifiers — would overwrite one another in
	// `_orphans/<encoded-object>/`, silently dropping forensic
	// data the operator opted in to preserve via
	// --include-orphans. Codex P2 round 13 (PR #716).
	genDir := fmt.Sprintf("gen-%d", obj.generation)
	dir := filepath.Join(bucketDir, "_orphans", EncodeSegment([]byte(obj.object)), genDir)
	if err := os.MkdirAll(dir, 0o755); err != nil { //nolint:mnd // 0755 == standard dir mode
		return errors.WithStack(err)
	}
	for k, scratchPath := range obj.chunkPaths {
		out := filepath.Join(dir, blobScratchName(k.uploadID, k.partNo, k.chunkNo, k.partVersion))
		body, err := os.ReadFile(scratchPath) //nolint:gosec // scratchPath composed from scratch root
		if err != nil {
			return errors.WithStack(err)
		}
		if err := writeFileAtomic(out, body); err != nil {
			return err
		}
	}
	return nil
}

// safeJoinUnderRoot composes <root>/<rel> and asserts the result is
// still rooted under <root>. S3 object keys are user-controlled and
// can contain "..", absolute paths, NUL bytes, or "." segments;
// without this guard a key like "../etc/passwd" would escape the
// dump tree and overwrite host files (Codex P1 #425).
//
// We refuse keys whose path-segment components include "." or ".."
// rather than filepath.Clean'ing them. S3 treats those bytes
// literally — `aws s3 put-object` accepts a key like "a/../b" as
// distinct from "b" — so collapsing them via filepath.Clean would
// silently merge two distinct user keys into one output file
// (Codex P2 #497). Operators with such keys must rename them in
// S3, then re-take the dump; the spec's rename-collisions path
// does not currently cover this.
//
// NUL bytes are also refused: POSIX cannot represent them in a
// path component, and they have no legitimate meaning in S3 keys
// transmitted over HTTP.
//
// Backslashes are refused for the same reason: filepath.Join treats
// '\' as a separator on Windows, so a key like `a\..\b` would bypass
// the '/'-based dot-segment scan below and normalise to `b`,
// silently merging two distinct S3 keys (Codex P1 round 6). Dumps
// must produce identical output regardless of the host OS, so we
// refuse '\' on every platform; operators with such keys must
// rename them in S3 first.
func safeJoinUnderRoot(root, rel string) (string, error) {
	if rel == "" {
		return "", errors.Wrap(ErrS3MalformedKey, "empty object name")
	}
	if strings.ContainsRune(rel, 0) {
		return "", errors.Wrapf(ErrS3MalformedKey, "object name contains NUL: %q", rel)
	}
	if strings.ContainsRune(rel, '\\') {
		return "", errors.Wrapf(ErrS3MalformedKey,
			"object name contains backslash %q (treated as a separator on Windows; rename in S3 first)", rel)
	}
	// Split on "/" and inspect every segment. S3 treats "a/", "a",
	// and "a//b" as three distinct keys, but filepath.Join collapses
	// them onto one filesystem path; without explicit rejection,
	// distinct user keys would silently overwrite each other at
	// finalize (Codex P1 #614).
	//
	// Empty segments are rejected wherever they appear — including
	// the leading position. A leading "/" produces an initial empty
	// segment (segs[0] == "") which filepath.Join would otherwise
	// strip, collapsing "/a" onto the same output path as "a".
	// Because S3 treats those as two distinct keys, last-flush wins
	// and silently overwrites the other (Codex P1 round 5).
	segs := strings.Split(rel, "/")
	for _, seg := range segs {
		switch seg {
		case ".", "..":
			return "", errors.Wrapf(ErrS3MalformedKey,
				"object name has dot segment %q (S3 treats it literally; rename in S3 first)", rel)
		case "":
			return "", errors.Wrapf(ErrS3MalformedKey,
				"object name has empty path segment %q", rel)
		}
	}
	cleanRoot := filepath.Clean(root)
	// Use filepath.Join here — its only behavioural change vs. raw
	// concatenation after the dot-segment guard above is normalising
	// a leading "/" off `rel` (which is what we want: absolute-path
	// keys collapse safely under bucketDir).
	joined := filepath.Join(cleanRoot, rel)
	rootSep := cleanRoot + string(filepath.Separator)
	if joined != cleanRoot && !strings.HasPrefix(joined, rootSep) {
		return "", errors.Wrapf(ErrS3MalformedKey,
			"object name %q escapes bucket directory", rel)
	}
	return joined, nil
}

// resolveObjectFilename returns the relative path of the assembled
// body within the bucket directory, plus the keymap "kind" when a
// rename took place ("" when the object writes at its natural path).
//
// needsLeafDataRename is set by the caller when another active-gen
// object's key would force this object's natural path to be a
// directory (e.g., bucket holds both "a/b" and "a/b/c"). The shorter
// key is renamed to "<key>.elastickv-leaf-data" and recorded in
// KEYMAP.jsonl with KindS3LeafData. Codex P1 #615.
//
// objectKeys is the set of every active-gen object key in the bucket
// (including obj.object itself). Both rename strategies — meta-suffix
// `.user-data` and leaf-data `.elastickv-leaf-data` — must refuse to
// emit if their target collides with an existing real object key in
// the same bucket: otherwise two distinct keys would map to one
// filesystem path and finalize would last-flush-wins one of them
// without a KEYMAP record that could reverse the merge. Codex P1
// round 9.
func (s *S3Encoder) resolveObjectFilename(b *s3BucketState, obj *s3ObjectState, needsLeafDataRename bool, objectKeys map[string]bool) (string, string, error) {
	if strings.HasSuffix(obj.object, S3MetaSuffixReserved) {
		if !s.renameCollisions {
			return "", "", errors.Wrapf(ErrS3MetaSuffixCollision,
				"bucket %q object %q", b.name, obj.object)
		}
		target := obj.object + ".user-data"
		if objectKeys[target] {
			return "", "", errors.Wrapf(ErrS3MetaSuffixCollision,
				"bucket %q object %q rename target %q is also a real object key (rename in S3 first)",
				b.name, obj.object, target)
		}
		return target, KindMetaCollision, nil
	}
	if needsLeafDataRename {
		target := obj.object + S3LeafDataSuffix
		if objectKeys[target] {
			return "", "", errors.Wrapf(ErrS3MetaSuffixCollision,
				"bucket %q object %q leaf-data rename target %q is also a real object key (rename in S3 first)",
				b.name, obj.object, target)
		}
		return target, KindS3LeafData, nil
	}
	// Object path taken at face value. Path-traversal sanitisation
	// runs in safeJoinUnderRoot, downstream of this function, where
	// the bucket-directory root is in scope.
	return obj.object, "", nil
}

func (s *S3Encoder) recordKeymap(b *s3BucketState, bucketDir, encodedFilename string, original []byte, kind string) error {
	if b.keymap == nil {
		// openSidecarFile (per-platform) refuses both symlinks and
		// hard-link clobber attacks. The previous os.Create here
		// followed both, leaving an arbitrary-write primitive if a
		// stale prior run or local adversary placed a link at the
		// path. Codex P2 round 9.
		f, err := openSidecarFile(filepath.Join(bucketDir, "KEYMAP.jsonl"))
		if err != nil {
			return err
		}
		b.keymap = NewKeymapWriter(f)
		b.keymapFile = f
		b.keymapDir = bucketDir
	}
	return b.keymap.WriteOriginal(encodedFilename, original, kind)
}

func (s *S3Encoder) emitWarn(event string, fields ...any) {
	if s.warn != nil {
		s.warn(event, fields...)
	}
}

func (s *S3Encoder) bucketState(name string) *s3BucketState {
	if st, ok := s.buckets[name]; ok {
		return st
	}
	st := &s3BucketState{name: name, objects: make(map[string]*s3ObjectState)}
	s.buckets[name] = st
	return st
}

func (s *S3Encoder) objectState(bucket string, gen uint64, object string) *s3ObjectState {
	b := s.bucketState(bucket)
	key := object + "\x00" + uint64Hex(gen)
	if st, ok := b.objects[key]; ok {
		return st
	}
	st := &s3ObjectState{bucket: bucket, generation: gen, object: object}
	b.objects[key] = st
	return st
}

// assembleObjectBody concatenates the blob chunks per the manifest's
// (PartNo, ChunkNo) order into outPath. The encoder buffers chunks on
// disk during the scan, so this copy walk is bounded by the object's
// size — no all-in-memory step.
//
// We re-decode the live manifest from the chunkPaths' uploadID rather
// than threading it through s3PublicManifest because the public
// sidecar deliberately drops the internal upload metadata.
func assembleObjectBody(outPath string, obj *s3ObjectState) error {
	tmp, err := os.CreateTemp(filepath.Dir(outPath), ".obj.tmp-*")
	if err != nil {
		return errors.WithStack(err)
	}
	tmpPath := tmp.Name()
	defer func() {
		if _, statErr := os.Stat(tmpPath); statErr == nil {
			_ = os.Remove(tmpPath)
		}
	}()
	// Filter chunks by the manifest's uploadID AND its declared
	// (partNo, partVersion) set. A snapshot taken during
	// delete/recreate, retry-after-failed-CompleteUpload, or
	// part-overwrite-before-cleanup can legitimately contain blob
	// chunks for multiple upload attempts and/or multiple part
	// versions under the same (bucket, generation, object). Mixing
	// them produces corrupted bytes — Codex P1 #500 (uploadID),
	// Codex P1 #619 (partVersion). The manifest is the single source
	// of truth; only its uploadID + declaredParts make it into the
	// assembled body.
	chunks := filterChunksForManifest(obj.chunkPaths, obj.uploadID, obj.declaredParts)
	if err := verifyChunkCompleteness(chunks, obj.declaredParts); err != nil {
		_ = tmp.Close()
		return err
	}
	for _, k := range chunks {
		path := obj.chunkPaths[k]
		if err := appendFile(tmp, path); err != nil {
			if closeErr := tmp.Close(); closeErr != nil {
				return errors.Wrap(err, "tmp.Close after appendFile failure: "+closeErr.Error())
			}
			return err
		}
	}
	if err := tmp.Close(); err != nil {
		return errors.WithStack(err)
	}
	if err := os.Rename(tmpPath, outPath); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// filterChunksForManifest returns the chunk keys belonging to
// manifestUploadID AND whose (partNo, partVersion) appears in
// declaredParts. Returned keys are sorted by (partNo, partVersion,
// chunkNo) for byte-deterministic body assembly.
//
// An empty manifestUploadID and a nil declaredParts both mean "no
// filter" — used by tests that pre-date these features. Production
// callers always pass non-empty/non-nil values via
// HandleObjectManifest.
func filterChunksForManifest(m map[s3ChunkKey]string, manifestUploadID string, declaredParts map[s3PartKey]s3DeclaredPart) []s3ChunkKey {
	keys := make([]s3ChunkKey, 0, len(m))
	for k := range m {
		if manifestUploadID != "" && k.uploadID != manifestUploadID {
			continue
		}
		if declaredParts != nil {
			declared, ok := declaredParts[s3PartKey{partNo: k.partNo, partVersion: k.partVersion}]
			if !ok {
				continue
			}
			// Symmetric with verifyChunkCompleteness's
			// `want.chunkCount == 0 → skip` branch: a
			// manifest part declaring zero chunks must not
			// contribute any bytes to the assembled body,
			// even if stray chunks for that (partNo,
			// partVersion) exist on disk. Without this
			// filter the assembler would silently merge those
			// stray chunks while the completeness check
			// passed, producing a body that violates the
			// declared-part contract. CodeRabbit Major round
			// 13 (PR #716).
			if declared.chunkCount == 0 {
				continue
			}
		}
		keys = append(keys, k)
	}
	sort.SliceStable(keys, func(i, j int) bool {
		a, b := keys[i], keys[j]
		switch {
		case a.partNo != b.partNo:
			return a.partNo < b.partNo
		case a.partVersion != b.partVersion:
			return a.partVersion < b.partVersion
		default:
			return a.chunkNo < b.chunkNo
		}
	})
	return keys
}

func appendFile(dst io.Writer, srcPath string) error {
	f, err := os.Open(srcPath) //nolint:gosec // srcPath composed from scratch root
	if err != nil {
		return errors.WithStack(err)
	}
	defer f.Close()
	if _, err := io.Copy(dst, f); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func ensureChunkPaths(m map[s3ChunkKey]string) map[s3ChunkKey]string {
	if m == nil {
		return make(map[s3ChunkKey]string)
	}
	return m
}

func parseUploadFamily(prefix string, key []byte) (bucket string, generation uint64, object string, uploadID string, partNo uint64, ok bool) {
	switch prefix {
	case S3UploadPartPrefix:
		return s3keys.ParseUploadPartKey(key)
	case S3UploadMetaPrefix:
		// Parse via prefix arithmetic: same shape as upload-part minus
		// the partNo trailer. ParseUploadPartKey would reject the
		// shorter form, so we accept it heuristically here. Phase 0a
		// only needs the bucket for routing.
		out := key[len(S3UploadMetaPrefix):]
		if len(out) == 0 {
			return "", 0, "", "", 0, false
		}
		return decodeBucketSegmentForRouting(out)
	}
	return "", 0, "", "", 0, false
}

func decodeBucketSegmentForRouting(rest []byte) (string, uint64, string, string, uint64, bool) {
	// We only need the bucket for routing; the rest is passed through
	// as opaque bytes.
	for i := 0; i < len(rest); i++ {
		if rest[i] == 0x00 && i+1 < len(rest) && rest[i+1] == 0x01 {
			return string(rest[:i]), 0, "", "", 0, true
		}
	}
	return "", 0, "", "", 0, false
}

func uint64Hex(v uint64) string {
	const hexDigits = "0123456789abcdef"
	const u64HexLen = 16
	out := make([]byte, u64HexLen)
	for i := u64HexLen - 1; i >= 0; i-- {
		out[i] = hexDigits[v&0xF] //nolint:mnd // 0xF == low-nibble mask
		v >>= 4                   //nolint:mnd // 4 == nibble width
	}
	return string(out)
}

func blobScratchName(uploadID string, partNo, chunkNo, partVersion uint64) string {
	return EncodeSegment([]byte(uploadID)) + "_" + uint64Hex(partNo) + "_" + uint64Hex(chunkNo) + "_" + uint64Hex(partVersion) + ".bin"
}
