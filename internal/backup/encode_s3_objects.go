package backup

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/cockroachdb/errors"
)

// encode_s3_objects.go is the M4-2 slice of the S3 reverse encoder: object
// bodies. For each object in the dump it re-chunks the body into
// !s3|blob| records and rebuilds the !s3|obj|head| manifest from the
// sidecar — the inverse of the decoder's assembleObjectBody +
// HandleObjectManifest.
//
// Scope of this slice (M4-2a): objects that map to a natural nested path
// (s3/<bucket>/a/b/c). File-vs-directory collisions (the decoder renames
// the shorter key to <key>.elastickv-leaf-data and records it in
// KEYMAP.jsonl) are NOT yet reversed — a dump carrying KEYMAP.jsonl or a
// .elastickv-leaf-data file fails closed rather than emit a wrong key.
//
// Reconstruction choices (Option-B style, consistent with the generation
// decision; the dump does not carry these):
//   - uploadID: the decoder drops the manifest's uploadID, so a uniform
//     s3RestoreUploadID is synthesized. The manifest's uploadID only has
//     to match its own blob keys' uploadID for assembleObjectBody to find
//     the chunks (the object key already disambiguates objects), so a
//     fixed value is sufficient for a single-cluster restore.
//   - Every object is emitted as a SINGLE re-chunked part (partNo=1,
//     partVersion=0). Chunk boundaries are not load-bearing — the decoder
//     concatenates chunks in (partNo, chunkNo) order — so a body that was
//     originally multipart still reassembles byte-identically, and the
//     object etag is taken verbatim from the sidecar (no recompute).
//   - last_modified_hlc is reconstructed from the sidecar's RFC3339
//     LastModified (physical half only; the logical bits were discarded on
//     decode). Absent/unparseable timestamps restore as 0.

// s3ChunkSize mirrors adapter/s3.go: object bodies are re-split into
// 1 MiB blob chunks.
const s3ChunkSize = 1 << 20

// s3RestorePartNo / s3RestoreUploadID are the synthesized single-part
// identifiers stamped into every restored object's blob keys and manifest
// (see file header).
const (
	s3RestorePartNo   uint64 = 1
	s3RestoreUploadID        = "elastickv-restore"
)

// ErrS3EncodeUnsupportedCollision is returned when the dump carries an
// object-name collision artifact (KEYMAP.jsonl or a .elastickv-leaf-data
// file) that this slice does not yet reverse — failing closed avoids
// emitting a record under the wrong object key.
var ErrS3EncodeUnsupportedCollision = errors.New("backup: s3 encode object-name collision not yet supported")

// s3ManifestFormatVersion is the only object .elastickv-meta.json
// format_version the encoder accepts (a dedicated manifest constant rather
// than reusing the bucket one, though both are currently 1).
const s3ManifestFormatVersion uint32 = 1

// ErrS3EncodeInvalidManifest is returned when an object's
// .elastickv-meta.json sidecar cannot be parsed.
var ErrS3EncodeInvalidManifest = errors.New("backup: s3 encode invalid object sidecar")

// encodeBucketObjects walks a bucket's object tree and stages each
// object's manifest + blob records.
//
// Object-name collisions: the decoder writes a top-level KEYMAP.jsonl
// recording shorter keys renamed to <key>.elastickv-leaf-data. M4-2a does
// not reverse those renames, so a collision-tracker KEYMAP.jsonl fails
// closed. It is distinguished from a legitimate user object named
// "KEYMAP.jsonl" (which the decoder also emits verbatim) by the absence of
// a companion .elastickv-meta.json sidecar — the tracker has none. The
// .elastickv-leaf-data suffix is NOT special-cased: a real object whose
// key ends in it has a sidecar and round-trips normally, and any actual
// collision is already gated by the tracker check above (codex P1 #845).
func (e *S3RecordEncoder) encodeBucketObjects(b *snapshotBuilder, root *os.Root, bucketDir, bucketName string) error {
	keymapRel := filepath.Join(bucketDir, "KEYMAP.jsonl")
	present, err := rootEntryExists(root, keymapRel)
	if err != nil {
		return err
	}
	if present {
		hasSidecar, err := rootEntryExists(root, keymapRel+S3MetaSuffixReserved)
		if err != nil {
			return err
		}
		if !hasSidecar {
			return errors.Wrapf(ErrS3EncodeUnsupportedCollision,
				"%s: collision-rename KEYMAP.jsonl present", bucketDir)
		}
	}
	return e.walkObjects(b, root, bucketDir, bucketName, "")
}

// rootEntryExists reports whether rel exists within root (via Lstat, so it
// does not follow a final symlink).
func rootEntryExists(root *os.Root, rel string) (bool, error) {
	_, err := root.Lstat(rel)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, errors.WithStack(err)
	}
	return true, nil
}

// walkObjects recursively descends bucketDir, treating each non-sidecar
// regular file as an object body whose key is its path relative to
// bucketDir. The reserved top-level entries (_bucket.json,
// _incomplete_uploads/, _orphans/) are skipped.
func (e *S3RecordEncoder) walkObjects(b *snapshotBuilder, root *os.Root, bucketDir, bucketName, rel string) error {
	entries, err := readRootSubdirEntries(root, filepath.Join(bucketDir, rel))
	if err != nil {
		return err
	}
	for _, ent := range entries {
		childRel := filepath.Join(rel, ent.Name())
		if err := e.walkObjectEntry(b, root, bucketDir, bucketName, rel, childRel, ent); err != nil {
			return err
		}
	}
	return nil
}

// walkObjectEntry classifies one directory entry: reserved skips,
// sidecars, collision artifacts (fail closed), sub-directories (recurse),
// or an object body.
func (e *S3RecordEncoder) walkObjectEntry(b *snapshotBuilder, root *os.Root, bucketDir, bucketName, rel, childRel string, ent os.DirEntry) error {
	name := ent.Name()
	if ent.IsDir() {
		// TODO(M4-2b): _incomplete_uploads/ and _orphans/ are the decoder's
		// reserved dump dirs and are skipped here, but a user object key
		// literally prefixed with "_incomplete_uploads/" or "_orphans/"
		// would also land here and be silently dropped. The robust fix
		// (distinguishing reserved dumps from user keys, like the
		// KEYMAP-tracker disambiguation) is deferred to the collision slice.
		if rel == "" && (name == "_incomplete_uploads" || name == "_orphans") {
			return nil
		}
		return e.walkObjects(b, root, bucketDir, bucketName, childRel)
	}
	switch {
	case rel == "" && name == "_bucket.json":
		return nil
	case strings.HasSuffix(name, S3MetaSuffixReserved):
		return nil // sidecar, handled with its body
	default:
		// Any other regular file (including a user object literally named
		// "KEYMAP.jsonl" or ending in .elastickv-leaf-data) is an object
		// body; encodeObject reads its sidecar and fails closed if absent.
		// A sidecar-less collision-tracker KEYMAP.jsonl was already gated
		// in encodeBucketObjects.
		return e.encodeObject(b, root, bucketDir, bucketName, childRel)
	}
}

// encodeObject reads one object body + sidecar and stages its blob chunks
// and manifest. The object key is the body's path relative to bucketDir.
func (e *S3RecordEncoder) encodeObject(b *snapshotBuilder, root *os.Root, bucketDir, bucketName, objRel string) error {
	objectKey := filepath.ToSlash(objRel)
	bodyRel := filepath.Join(bucketDir, objRel)
	body, err := readRootBodyFile(root, bodyRel)
	if err != nil {
		return err
	}
	sidecar, err := e.readObjectSidecar(root, bodyRel+S3MetaSuffixReserved)
	if err != nil {
		return err
	}
	return e.emitObject(b, bucketName, objectKey, body, sidecar)
}

// readRootBodyFile reads an object body within root with a PRE-open Lstat
// guard: a symlink / FIFO / device / directory is refused BEFORE root.Open,
// so a reader-less FIFO planted at an object path cannot block the open
// (readRootFile's post-open check happens only after the blocking Open).
// This matches the guard readObjectSidecar / readBucketMeta use.
func readRootBodyFile(root *os.Root, rel string) ([]byte, error) {
	linfo, err := root.Lstat(rel)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if !linfo.Mode().IsRegular() {
		return nil, errors.Wrapf(ErrS3EncodeNotRegular, "%s (mode=%s)", rel, linfo.Mode())
	}
	if err := refuseHardLink(linfo, rel); err != nil {
		return nil, err
	}
	f, err := root.Open(rel)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer func() { _ = f.Close() }()
	body, err := io.ReadAll(f)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return body, nil
}

// readObjectSidecar opens <body>.elastickv-meta.json within root (symlink
// / FIFO / hard-link safe) and decodes the public manifest projection.
func (e *S3RecordEncoder) readObjectSidecar(root *os.Root, rel string) (s3PublicManifest, error) {
	linfo, err := root.Lstat(rel)
	if err != nil {
		return s3PublicManifest{}, errors.WithStack(err)
	}
	if !linfo.Mode().IsRegular() {
		return s3PublicManifest{}, errors.Wrapf(ErrS3EncodeNotRegular, "%s (mode=%s)", rel, linfo.Mode())
	}
	if err := refuseHardLink(linfo, rel); err != nil {
		return s3PublicManifest{}, err
	}
	f, err := root.Open(rel)
	if err != nil {
		return s3PublicManifest{}, errors.WithStack(err)
	}
	defer func() { _ = f.Close() }()
	var pub s3PublicManifest
	if err := decodeOneJSON(f, &pub); err != nil {
		return s3PublicManifest{}, errors.Wrapf(ErrS3EncodeInvalidManifest, "%s: %v", rel, err)
	}
	if pub.FormatVersion != s3ManifestFormatVersion {
		return s3PublicManifest{}, errors.Wrapf(ErrS3EncodeInvalidManifest,
			"%s: unsupported format_version %d", rel, pub.FormatVersion)
	}
	return pub, nil
}

// emitObject re-chunks the body into blob records and builds the manifest.
func (e *S3RecordEncoder) emitObject(b *snapshotBuilder, bucketName, objectKey string, body []byte, sidecar s3PublicManifest) error {
	// The body file IS the object, so its length must match the manifest's
	// declared size; a mismatch means a corrupt/inconsistent dump and fails
	// closed rather than restoring an object whose size_bytes lies.
	if sidecar.SizeBytes != int64(len(body)) {
		return errors.Wrapf(ErrS3EncodeInvalidManifest,
			"%s: size_bytes %d != body length %d", objectKey, sidecar.SizeBytes, len(body))
	}
	chunkSizes, err := e.addObjectBlobs(b, bucketName, objectKey, body)
	if err != nil {
		return err
	}
	live := s3LiveManifest{
		UploadID:           s3RestoreUploadID,
		ETag:               sidecar.ETag,
		SizeBytes:          sidecar.SizeBytes,
		LastModifiedHLC:    parseRFC3339NanoAsHLC(sidecar.LastModified),
		ContentType:        sidecar.ContentType,
		ContentEncoding:    sidecar.ContentEncoding,
		CacheControl:       sidecar.CacheControl,
		ContentDisposition: sidecar.ContentDisposition,
		UserMetadata:       sidecar.UserMetadata,
	}
	if len(chunkSizes) > 0 {
		live.Parts = []s3LivePart{{
			PartNo:      s3RestorePartNo,
			ETag:        sidecar.ETag,
			SizeBytes:   sidecar.SizeBytes,
			ChunkCount:  uint64(len(chunkSizes)),
			ChunkSizes:  chunkSizes,
			PartVersion: 0,
		}}
	}
	val, err := json.Marshal(live)
	if err != nil {
		return errors.WithStack(err)
	}
	return b.Add(s3keys.ObjectManifestKey(bucketName, s3RestoreGeneration, objectKey), val, 0)
}

// addObjectBlobs splits body into s3ChunkSize chunks, stages each as a
// !s3|blob| record, and returns the per-chunk byte lengths for the
// manifest. A zero-length body yields no chunks (and no part).
func (e *S3RecordEncoder) addObjectBlobs(b *snapshotBuilder, bucketName, objectKey string, body []byte) ([]uint64, error) {
	var sizes []uint64
	var chunkNo uint64
	for start := 0; start < len(body); start += s3ChunkSize {
		end := start + s3ChunkSize
		if end > len(body) {
			end = len(body)
		}
		chunk := body[start:end]
		key := s3keys.BlobKey(bucketName, s3RestoreGeneration, objectKey, s3RestoreUploadID, s3RestorePartNo, chunkNo)
		if err := b.Add(key, chunk, 0); err != nil {
			return nil, err
		}
		sizes = append(sizes, uint64(len(chunk)))
		chunkNo++
	}
	return sizes, nil
}

// parseRFC3339NanoAsHLC inverts formatHLCAsRFC3339Nano: it recovers the
// physical (millisecond) half of the HLC from the sidecar timestamp,
// shifted into place with zero logical bits. An empty or unparseable
// value (cosmetic metadata, not load-bearing) yields 0.
func parseRFC3339NanoAsHLC(s string) uint64 {
	if s == "" {
		return 0
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return 0
	}
	ms := t.UnixMilli()
	if ms < 0 {
		return 0
	}
	return uint64(ms) << hlcLogicalBitsForBackupS3
}
