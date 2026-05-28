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

// ErrS3EncodeReservedPrefixCollision is returned when a user object key
// collides with a reserved dump-control directory (_incomplete_uploads/
// or _orphans/) — the decoder writes such keys at their natural path
// without renaming, so the encoder cannot disambiguate them from the
// dump's own payload. Codex P1 #842 follow-up: failing closed prevents
// silently dropping the entire user-object subtree.
var ErrS3EncodeReservedPrefixCollision = errors.New("backup: s3 encode user object key collides with reserved dump directory")

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
	tracker, err := e.isKeymapCollisionTracker(root, bucketDir)
	if err != nil {
		return err
	}
	if tracker {
		return errors.Wrapf(ErrS3EncodeUnsupportedCollision,
			"%s: collision-rename KEYMAP.jsonl present", bucketDir)
	}
	return e.walkObjects(b, root, bucketDir, bucketName, "")
}

// isKeymapCollisionTracker reports whether the bucket's top-level
// KEYMAP.jsonl is the decoder's collision-rename tracker. The tracker is a
// regular FILE with no companion sidecar. It is NOT:
//   - a user object literally named KEYMAP.jsonl (which has a sidecar), or
//   - a directory holding objects under the "KEYMAP.jsonl/" key prefix
//     (a directory, not a file).
func (e *S3RecordEncoder) isKeymapCollisionTracker(root *os.Root, bucketDir string) (bool, error) {
	keymapRel := filepath.Join(bucketDir, "KEYMAP.jsonl")
	linfo, err := root.Lstat(keymapRel)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, errors.WithStack(err)
	}
	if !linfo.Mode().IsRegular() {
		return false, nil // a directory (KEYMAP.jsonl/ key prefix) or other
	}
	hasSidecar, err := rootEntryExists(root, keymapRel+S3MetaSuffixReserved)
	if err != nil {
		return false, err
	}
	return !hasSidecar, nil
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

// reservedDirHoldsSidecar reports whether dirRel (a reserved dump dir like
// _incomplete_uploads/ or _orphans/) contains any *.elastickv-meta.json
// sidecar at any depth. Sidecar presence is the disambiguator: the
// decoder's reserved-dir payload (records.jsonl, orphan .bin chunks) has
// no sidecars; a user object whose key prefix collides with the reserved
// name does. Missing directory is not an error.
func reservedDirHoldsSidecar(root *os.Root, dirRel string) (bool, error) {
	entries, err := readRootSubdirEntries(root, dirRel)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	for _, ent := range entries {
		childRel := filepath.Join(dirRel, ent.Name())
		if ent.IsDir() {
			has, err := reservedDirHoldsSidecar(root, childRel)
			if err != nil {
				return false, err
			}
			if has {
				return true, nil
			}
			continue
		}
		if strings.HasSuffix(ent.Name(), S3MetaSuffixReserved) {
			return true, nil
		}
	}
	return false, nil
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
		return e.walkObjectSubdir(b, root, bucketDir, bucketName, rel, childRel, name)
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

// walkObjectSubdir handles a directory entry encountered during the
// object walk. The reserved top-level names _incomplete_uploads/ and
// _orphans/ are skipped — but only if they hold the decoder's own
// payload (records.jsonl / orphan .bin chunks, none of which carry a
// .elastickv-meta.json sidecar). A real S3 object whose key prefix
// collides with these names (resolveObjectFilename does not rename
// those) also lands here; its sidecar makes that detectable, and we
// fail closed rather than silently drop the user data (codex P1 #842
// follow-up).
func (e *S3RecordEncoder) walkObjectSubdir(b *snapshotBuilder, root *os.Root, bucketDir, bucketName, rel, childRel, name string) error {
	if rel == "" && (name == "_incomplete_uploads" || name == "_orphans") {
		hasUserObject, err := reservedDirHoldsSidecar(root, filepath.Join(bucketDir, childRel))
		if err != nil {
			return err
		}
		if hasUserObject {
			return errors.Wrapf(ErrS3EncodeReservedPrefixCollision,
				"%s/%s: rename the colliding S3 key before backup", bucketDir, name)
		}
		return nil
	}
	return e.walkObjects(b, root, bucketDir, bucketName, childRel)
}

// encodeObject reads one object's sidecar, streams its body into blob
// records, and stages the manifest. The object key is the body's path
// relative to bucketDir.
func (e *S3RecordEncoder) encodeObject(b *snapshotBuilder, root *os.Root, bucketDir, bucketName, objRel string) error {
	objectKey := filepath.ToSlash(objRel)
	bodyRel := filepath.Join(bucketDir, objRel)
	sidecar, err := e.readObjectSidecar(root, bodyRel+S3MetaSuffixReserved)
	if err != nil {
		return err
	}
	chunkSizes, err := e.streamObjectBlobs(b, root, bodyRel, bucketName, objectKey, sidecar.SizeBytes)
	if err != nil {
		return err
	}
	return e.addObjectManifest(b, bucketName, objectKey, sidecar, chunkSizes)
}

// openRootRegular opens rel within root with a PRE-open Lstat guard (a
// symlink / FIFO / device / directory is refused BEFORE root.Open, so a
// reader-less FIFO planted at an object path cannot block the open) plus a
// hard-link refusal, returning the open file and its size.
func openRootRegular(root *os.Root, rel string) (*os.File, int64, error) {
	linfo, err := root.Lstat(rel)
	if err != nil {
		return nil, 0, errors.WithStack(err)
	}
	if !linfo.Mode().IsRegular() {
		return nil, 0, errors.Wrapf(ErrS3EncodeNotRegular, "%s (mode=%s)", rel, linfo.Mode())
	}
	if err := refuseHardLink(linfo, rel); err != nil {
		return nil, 0, err
	}
	f, err := root.Open(rel)
	if err != nil {
		return nil, 0, errors.WithStack(err)
	}
	return f, linfo.Size(), nil
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

// streamObjectBlobs streams the object body in s3ChunkSize reads, staging
// each chunk as a !s3|blob| record, and returns the per-chunk byte lengths
// for the manifest. The body is never fully buffered in RAM (only one
// chunk at a time), so a multi-GiB object does not blow up the encoder
// (codex P1 #845). The declared size is validated against the file's
// stat size before streaming — the body file IS the object, so a mismatch
// is a corrupt dump and fails closed. A zero-length body yields no chunks.
func (e *S3RecordEncoder) streamObjectBlobs(b *snapshotBuilder, root *os.Root, bodyRel, bucketName, objectKey string, declaredSize int64) ([]uint64, error) {
	f, size, err := openRootRegular(root, bodyRel)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	if size != declaredSize {
		return nil, errors.Wrapf(ErrS3EncodeInvalidManifest,
			"%s: size_bytes %d != body length %d", objectKey, declaredSize, size)
	}
	var sizes []uint64
	var chunkNo uint64
	buf := make([]byte, s3ChunkSize)
	for {
		n, rerr := io.ReadFull(f, buf)
		if n > 0 {
			chunk := buf[:n]
			key := s3keys.BlobKey(bucketName, s3RestoreGeneration, objectKey, s3RestoreUploadID, s3RestorePartNo, chunkNo)
			// b.Add copies the value (encodeMVCCValue appends), so reusing
			// buf across iterations is safe.
			if err := b.Add(key, chunk, 0); err != nil {
				return nil, err
			}
			sizes = append(sizes, uint64(len(chunk)))
			chunkNo++
		}
		if errors.Is(rerr, io.EOF) || errors.Is(rerr, io.ErrUnexpectedEOF) {
			return sizes, nil
		}
		if rerr != nil {
			return nil, errors.WithStack(rerr)
		}
	}
}

// addObjectManifest builds the !s3|obj|head| manifest from the sidecar and
// the staged chunk sizes.
func (e *S3RecordEncoder) addObjectManifest(b *snapshotBuilder, bucketName, objectKey string, sidecar s3PublicManifest, chunkSizes []uint64) error {
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
