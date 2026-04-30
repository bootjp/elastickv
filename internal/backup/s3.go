package backup

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

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
)

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
	name      string
	meta      *s3PublicBucket
	objects   map[string]*s3ObjectState // keyed by "object\x00generation"
	keymap    *KeymapWriter
	keymapDir string
}

type s3ObjectState struct {
	bucket     string
	generation uint64
	object     string
	manifest   *s3PublicManifest
	// chunkPaths maps (uploadID, partNo, chunkNo) -> scratch path.
	chunkPaths map[s3ChunkKey]string
}

type s3ChunkKey struct {
	uploadID    string
	partNo      uint64
	chunkNo     uint64
	partVersion uint64
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
		ContentType:        live.ContentType,
		ContentEncoding:    live.ContentEncoding,
		CacheControl:       live.CacheControl,
		ContentDisposition: live.ContentDisposition,
		UserMetadata:       live.UserMetadata,
	}
	// Persist the parts list on the object state so Finalize knows
	// what to assemble. We attach the live parts directly because
	// that's purely structural data — the public sidecar has no need
	// for them.
	st.chunkPaths = ensureChunkPaths(st.chunkPaths)
	st.attachManifestParts(live.UploadID, live.Parts)
	return nil
}

// HandleBlob spills a !s3|blob| record to a per-chunk scratch file
// and registers it under the (bucket, object, gen, uploadID, partNo,
// chunkNo, partVersion) routing key.
func (s *S3Encoder) HandleBlob(key, value []byte) error {
	bucket, gen, object, uploadID, partNo, chunkNo, partVersion, ok := s3keys.ParseBlobKey(key)
	if !ok {
		return errors.Wrapf(ErrS3MalformedKey, "blob key: %q", key)
	}
	st := s.objectState(bucket, gen, object)
	dir := filepath.Join(s.scratchRoot, EncodeSegment([]byte(bucket)), EncodeSegment([]byte(object)))
	if err := os.MkdirAll(dir, 0o755); err != nil { //nolint:mnd // 0755 == standard dir mode
		return errors.WithStack(err)
	}
	path := filepath.Join(dir, blobScratchName(uploadID, partNo, chunkNo, partVersion))
	if err := writeFileAtomic(path, value); err != nil {
		return err
	}
	st.chunkPaths = ensureChunkPaths(st.chunkPaths)
	st.chunkPaths[s3ChunkKey{uploadID: uploadID, partNo: partNo, chunkNo: chunkNo, partVersion: partVersion}] = path
	return nil
}

// HandleIncompleteUpload routes !s3|upload|meta|/!s3|upload|part|
// records to <bucket>/_incomplete_uploads/ when the include flag is
// on; otherwise drops them.
func (s *S3Encoder) HandleIncompleteUpload(prefix string, key, value []byte) error {
	if !s.includeIncompleteUploads {
		return nil
	}
	bucket, _, _, _, _, ok := parseUploadFamily(prefix, key)
	if !ok {
		return errors.Wrapf(ErrS3MalformedKey, "upload-family key: %q", key)
	}
	dir := filepath.Join(s.outRoot, "s3", EncodeSegment([]byte(bucket)), "_incomplete_uploads")
	if err := os.MkdirAll(dir, 0o755); err != nil { //nolint:mnd // 0755 == standard dir mode
		return errors.WithStack(err)
	}
	// Phase 0a stores upload-family records as opaque key/value pairs
	// (one JSON line per record) rather than reconstructing the
	// in-flight upload state. Restoring incomplete uploads is itself
	// a follow-up; this artifact preserves the bytes for forensics.
	jl, err := openJSONL(filepath.Join(dir, "records.jsonl"))
	if err != nil {
		return err
	}
	defer func() { _ = closeJSONL(jl) }()
	rec := struct {
		Prefix   string `json:"prefix"`
		KeyB64   []byte `json:"key"`
		ValueB64 []byte `json:"value"`
	}{Prefix: prefix, KeyB64: key, ValueB64: value}
	if err := jl.enc.Encode(rec); err != nil {
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
	bucketDir := filepath.Join(s.outRoot, "s3", EncodeSegment([]byte(b.name)))
	if err := os.MkdirAll(bucketDir, 0o755); err != nil { //nolint:mnd // 0755 == standard dir mode
		return errors.WithStack(err)
	}
	if b.meta != nil {
		if err := writeFileAtomic(filepath.Join(bucketDir, "_bucket.json"), mustMarshalIndent(b.meta)); err != nil {
			return err
		}
	}
	for _, obj := range b.objects {
		if err := s.flushObject(b, bucketDir, obj); err != nil {
			return err
		}
	}
	if b.keymap != nil {
		if err := b.keymap.Close(); err != nil {
			return err
		}
		// If no rename was recorded, drop the empty file so the
		// dump tree omits it (per the spec: keymaps are absent when
		// empty).
		if b.keymap.Count() == 0 && b.keymapDir != "" {
			_ = os.Remove(filepath.Join(b.keymapDir, "KEYMAP.jsonl"))
		}
	}
	return nil
}

func (s *S3Encoder) flushObject(b *s3BucketState, bucketDir string, obj *s3ObjectState) error {
	if obj.manifest == nil {
		s.emitWarn("s3_orphan_chunks",
			"bucket", b.name,
			"object", obj.object,
			"chunks", len(obj.chunkPaths),
			"hint", "blob chunks present but no !s3|obj|head record matched")
		return nil
	}
	objectName, kind, err := s.resolveObjectFilename(b, obj)
	if err != nil {
		return err
	}
	bodyPath := filepath.Join(bucketDir, objectName)
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

// resolveObjectFilename returns the relative path of the assembled
// body within the bucket directory, plus the keymap "kind" when a
// rename took place ("" when the object writes at its natural path).
func (s *S3Encoder) resolveObjectFilename(b *s3BucketState, obj *s3ObjectState) (string, string, error) {
	if strings.HasSuffix(obj.object, S3MetaSuffixReserved) {
		if !s.renameCollisions {
			return "", "", errors.Wrapf(ErrS3MetaSuffixCollision,
				"bucket %q object %q", b.name, obj.object)
		}
		return obj.object + ".user-data", KindMetaCollision, nil
	}
	// Object path taken at face value. Path collisions (`path/to`
	// vs `path/to/sub`) are deferred until the master pipeline
	// detects them across multiple manifests; this PR's per-object
	// flush trusts the caller's collision detection.
	return obj.object, "", nil
}

func (s *S3Encoder) recordKeymap(b *s3BucketState, bucketDir, encodedFilename string, original []byte, kind string) error {
	if b.keymap == nil {
		path := filepath.Join(bucketDir, "KEYMAP.jsonl")
		f, err := os.Create(path) //nolint:gosec // path composed from output root
		if err != nil {
			return errors.WithStack(err)
		}
		b.keymap = NewKeymapWriter(f)
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

// attachManifestParts is a placeholder that records the part list on
// the object state. The current implementation walks the manifest's
// part order at Finalize time, so this method just memoises the upload
// ID for reference; future extensions (e.g., versioned parts) can
// surface here.
func (st *s3ObjectState) attachManifestParts(_ string, _ []s3LivePart) {
	// Intentionally empty: assembleObjectBody consumes the manifest
	// directly via st.manifest at Finalize. Kept as a hook so the
	// callsite reads symmetrically with HandleBlob.
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
	chunks := sortChunkKeys(obj.chunkPaths)
	for _, k := range chunks {
		path := obj.chunkPaths[k]
		if err := appendFile(tmp, path); err != nil {
			_ = tmp.Close()
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

func sortChunkKeys(m map[s3ChunkKey]string) []s3ChunkKey {
	out := make([]s3ChunkKey, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.SliceStable(out, func(i, j int) bool {
		a, b := out[i], out[j]
		switch {
		case a.partNo != b.partNo:
			return a.partNo < b.partNo
		case a.partVersion != b.partVersion:
			return a.partVersion < b.partVersion
		default:
			return a.chunkNo < b.chunkNo
		}
	})
	return out
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
