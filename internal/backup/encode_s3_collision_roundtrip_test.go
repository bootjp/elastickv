package backup

import (
	"bytes"
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/cockroachdb/errors"
)

// encode_s3_collision_roundtrip_test.go is the M4-2b commit C suite:
// end-to-end round-trips covering the 8 tests called out in design
// doc PR #913 §"Test plan". Unit-level loader / validator tests live
// alongside their helper in encode_s3_collision_test.go.

// writeS3LeafDataRename writes both the renamed body
// (<originalKey>.elastickv-leaf-data) and its sidecar under the
// bucket, mirroring what the decoder produces when two object keys
// collide on a file/dir boundary. Caller is responsible for writing
// any colliding "subtree" objects (e.g. path/to/sub).
func writeS3LeafDataRename(t *testing.T, root, bucket, originalKey string, body, sidecar []byte) {
	t.Helper()
	writeS3Object(t, root, bucket, originalKey+S3LeafDataSuffix, body, sidecar)
}

// writeS3MetaCollisionRename writes the meta-collision rename
// target (<some-non-colliding-name>) with its sidecar; the keymap
// records the original .elastickv-meta.json-suffixed user key.
func writeS3MetaCollisionRename(t *testing.T, root, bucket, renamedKey string, body, sidecar []byte) {
	t.Helper()
	writeS3Object(t, root, bucket, renamedKey, body, sidecar)
}

// collisionTestBucket is the fixed bucket name every commit-C
// collision round-trip test uses. Kept as a constant since the
// unparam lint would otherwise flag the parameter as always equal
// to "b".
const collisionTestBucket = "b"

// assertS3ManifestEmittedUnder verifies that the encoder emitted a
// !s3|obj|head|<bucket>|<originalKey> manifest record (i.e. the
// original S3 key was recovered from the keymap rather than leaking
// the renamed on-disk filename) and that no record exists under the
// disk-form key. Tests the byte-shape contract directly without
// re-running the decoder, which would re-apply collision renames
// and obscure the assertion.
func assertS3ManifestEmittedUnder(t *testing.T, fsm []byte, originalKey, mustNotKey string) {
	t.Helper()
	entries, _, err := DecodeLiveEntries(bytes.NewReader(fsm))
	if err != nil {
		t.Fatalf("DecodeLiveEntries: %v", err)
	}
	wantKey := s3keys.ObjectManifestKey(collisionTestBucket, s3RestoreGeneration, originalKey)
	dontWantKey := s3keys.ObjectManifestKey(collisionTestBucket, s3RestoreGeneration, mustNotKey)
	var found bool
	for _, e := range entries {
		if bytes.Equal(e.UserKey, wantKey) {
			found = true
		}
		if bytes.Equal(e.UserKey, dontWantKey) {
			t.Errorf("emitted manifest for renamed disk key %q (should be recovered to %q)", mustNotKey, originalKey)
		}
	}
	if !found {
		t.Fatalf("no manifest record under recovered original key %q", originalKey)
	}
}

// TestS3EncodeRoundTripsLeafDataCollision verifies that a bucket
// holding both <key>.elastickv-leaf-data (the renamed shorter key)
// and <key>/sub (the longer key) is encoded such that the emitted
// !s3|obj|head|<bucket>|<orig> records are keyed by the recovered
// original S3 keys (no .elastickv-leaf-data suffix leak), and the
// longer-key record stays under its unchanged path.
func TestS3EncodeRoundTripsLeafDataCollision(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	const bucket = "b"
	const (
		shorterKey = "path/to"
		longerKey  = "path/to/sub"
	)
	writeS3Bucket(t, in, bucket, []byte(`{"format_version":1,"name":"b"}`))
	shorterBody := []byte("shorter-key body")
	writeS3LeafDataRename(t, in, bucket, shorterKey, shorterBody,
		s3ObjectSidecar("etag-shorter", int64(len(shorterBody)), "text/plain", ""))
	longerBody := []byte("longer-key subobject body")
	writeS3Object(t, in, bucket, longerKey, longerBody,
		s3ObjectSidecar("etag-longer", int64(len(longerBody)), "text/plain", ""))
	writeS3KeymapTracker(t, in, []KeymapRecord{
		leafRecord(shorterKey+S3LeafDataSuffix, shorterKey),
	})

	fsm := encodeS3Tree(t, in)
	assertS3ManifestEmittedUnder(t, fsm, shorterKey, shorterKey+S3LeafDataSuffix)
	assertS3ManifestEmittedUnder(t, fsm, longerKey, "")
}

// TestS3EncodeRoundTripsMetaCollision verifies that a meta-suffix
// rename (the decoder's --rename-collisions=true variant for a user
// object whose key naturally ends in .elastickv-meta.json) is
// reversed: the keymap-recorded original key is recovered. Asserts
// at the emitted-record level since a full decoder round-trip would
// reject the .elastickv-meta.json-suffixed key unless invoked with
// --rename-collisions=true.
func TestS3EncodeRoundTripsMetaCollision(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	const bucket = "b"
	const (
		renamedKey  = "report.user-data"
		originalKey = "report" + S3MetaSuffixReserved
	)
	writeS3Bucket(t, in, bucket, []byte(`{"format_version":1,"name":"b"}`))
	body := []byte("user object whose key happens to look like a sidecar")
	writeS3MetaCollisionRename(t, in, bucket, renamedKey, body,
		s3ObjectSidecar("etag-meta", int64(len(body)), "application/json", ""))
	writeS3KeymapTracker(t, in, []KeymapRecord{
		metaRecord(renamedKey, originalKey),
	})

	fsm := encodeS3Tree(t, in)
	assertS3ManifestEmittedUnder(t, fsm, originalKey, renamedKey)
}

// TestS3EncodeRejectsKeymapTargetingReservedSubtree pins gate
// "keymap original key cannot target _orphans or _incomplete_uploads"
// at the encoder integration level (the unit version is in
// encode_s3_collision_test.go).
func TestS3EncodeRejectsKeymapTargetingReservedSubtree(t *testing.T) {
	t.Parallel()
	for _, reserved := range []string{"_orphans", "_incomplete_uploads"} {
		t.Run(reserved, func(t *testing.T) {
			t.Parallel()
			in := t.TempDir()
			const bucket = "b"
			writeS3Bucket(t, in, bucket, []byte(`{"format_version":1,"name":"b"}`))
			writeS3Object(t, in, bucket, "renamed-body",
				[]byte("body"), s3ObjectSidecar("e", 4, "text/plain", ""))
			writeS3KeymapTracker(t, in, []KeymapRecord{
				leafRecord("renamed-body", reserved+"/forbidden/key"),
			})
			b := newSnapshotBuilder(s3EncTS)
			err := NewS3RecordEncoder(in).Encode(b)
			if !errors.Is(err, ErrS3EncodeInvalidBucket) {
				t.Fatalf("Encode err = %v, want wrap of ErrS3EncodeInvalidBucket", err)
			}
		})
	}
}

// TestS3EncodeAcceptsKeymapWithUserUnderscoreKey pins that codex P2
// v913 v1's narrow-reserved-prefix-rule allows a legitimate user
// key whose first segment starts with _ but is NOT one of the two
// named reserved subtrees (_orphans / _incomplete_uploads).
func TestS3EncodeAcceptsKeymapWithUserUnderscoreKey(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	const bucket = "b"
	const (
		renamedDisk = "renamed-underscore-key"
		originalKey = "_foo/legitimate-user-key"
	)
	writeS3Bucket(t, in, bucket, []byte(`{"format_version":1,"name":"b"}`))
	body := []byte("user object under a single underscore prefix")
	writeS3Object(t, in, bucket, renamedDisk, body,
		s3ObjectSidecar("etag-u", int64(len(body)), "text/plain", ""))
	writeS3KeymapTracker(t, in, []KeymapRecord{
		// Use KindMetaCollision so the encoded value bypasses the
		// .elastickv-leaf-data suffix check. The point of this test
		// is the reserved-root validation, not the suffix check.
		// metaRecord requires the original to end in
		// .elastickv-meta.json - use a Kind=SHAFallback record with
		// an arbitrary encoded value to isolate the reserved-root
		// gate from per-Kind suffix gates.
		{
			Encoded:     renamedDisk,
			OriginalB64: base64.RawURLEncoding.EncodeToString([]byte(originalKey)),
			Kind:        KindSHAFallback,
		},
	})

	fsm := encodeS3Tree(t, in)
	assertS3ManifestEmittedUnder(t, fsm, originalKey, renamedDisk)
}

// TestS3EncodeRejectsOrphanKeymapEntry pins fail-closed at the
// integration level when KEYMAP.jsonl references an encoded segment
// that has no on-disk file (e.g. the operator removed the renamed
// body but kept the keymap).
func TestS3EncodeRejectsOrphanKeymapEntry(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	const bucket = "b"
	writeS3Bucket(t, in, bucket, []byte(`{"format_version":1,"name":"b"}`))
	writeS3KeymapTracker(t, in, []KeymapRecord{
		leafRecord("ghost"+S3LeafDataSuffix, "ghost"),
	})
	b := newSnapshotBuilder(s3EncTS)
	err := NewS3RecordEncoder(in).Encode(b)
	if !errors.Is(err, ErrS3EncodeInvalidBucket) {
		t.Fatalf("Encode err = %v, want wrap of ErrS3EncodeInvalidBucket", err)
	}
}

// TestS3EncodeRejectsDuplicateKeymapEntry pins fail-closed at the
// integration level when KEYMAP.jsonl has two records with the same
// Encoded value (S3 decoder writes one record per rename — a dup
// means a corrupt dump). Loader-level coverage is in
// TestLoadBucketKeymap_DuplicateEncoded.
func TestS3EncodeRejectsDuplicateKeymapEntry(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	const bucket = "b"
	writeS3Bucket(t, in, bucket, []byte(`{"format_version":1,"name":"b"}`))
	writeS3Object(t, in, bucket, "x"+S3LeafDataSuffix,
		[]byte("body"), s3ObjectSidecar("e", 4, "text/plain", ""))
	writeS3KeymapTracker(t, in, []KeymapRecord{
		leafRecord("x"+S3LeafDataSuffix, "x"),
		leafRecord("x"+S3LeafDataSuffix, "elsewhere"),
	})
	b := newSnapshotBuilder(s3EncTS)
	err := NewS3RecordEncoder(in).Encode(b)
	if !errors.Is(err, ErrS3EncodeInvalidBucket) {
		t.Fatalf("Encode err = %v, want wrap of ErrS3EncodeInvalidBucket", err)
	}
}

// TestS3EncodeRejectsMalformedKeymapJSON pins fail-closed at the
// integration level when KEYMAP.jsonl has an unparseable line.
func TestS3EncodeRejectsMalformedKeymapJSON(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	const bucket = "b"
	writeS3Bucket(t, in, bucket, []byte(`{"format_version":1,"name":"b"}`))
	bucketDir := filepath.Join(in, "s3", EncodeSegment([]byte(bucket)))
	if err := os.WriteFile(filepath.Join(bucketDir, "KEYMAP.jsonl"),
		[]byte("not-json\n"), 0o600); err != nil {
		t.Fatalf("write KEYMAP.jsonl: %v", err)
	}
	b := newSnapshotBuilder(s3EncTS)
	err := NewS3RecordEncoder(in).Encode(b)
	if !errors.Is(err, ErrS3EncodeInvalidBucket) {
		t.Fatalf("Encode err = %v, want wrap of ErrS3EncodeInvalidBucket", err)
	}
}

// TestS3EncodeTrackerWithDirectorySidecar pins codex P2 PR #928. A
// user key prefix that happens to be `KEYMAP.jsonl.elastickv-meta.json/`
// creates a directory at the sidecar path. Earlier code treated ANY
// entry at the sidecar path as "the sidecar exists" and skipped the
// keymap-loading branch, mis-classifying the tracker file as a user
// object. The fix requires the sidecar to be a regular file before
// the tracker can be bypassed.
func TestS3EncodeTrackerWithDirectorySidecar(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	writeS3Bucket(t, in, collisionTestBucket, []byte(`{"format_version":1,"name":"b"}`))
	bucketDir := filepath.Join(in, "s3", EncodeSegment([]byte(collisionTestBucket)))
	// Tracker file (no companion regular-file sidecar).
	writeS3KeymapTracker(t, in, []KeymapRecord{
		leafRecord("path/to"+S3LeafDataSuffix, "path/to"),
	})
	// User-key subtree whose prefix happens to be the sidecar name.
	// This creates a DIRECTORY at <bucket>/KEYMAP.jsonl.elastickv-meta.json/.
	if err := os.MkdirAll(filepath.Join(bucketDir, "KEYMAP.jsonl"+S3MetaSuffixReserved), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	// The renamed leaf-data body for the tracker entry.
	body := []byte("shorter-key body")
	writeS3LeafDataRename(t, in, collisionTestBucket, "path/to", body,
		s3ObjectSidecar("etag-1", int64(len(body)), "text/plain", ""))
	// path/to/sub to make the leaf-data rename plausible.
	longerBody := []byte("longer-key subobject body")
	writeS3Object(t, in, collisionTestBucket, "path/to/sub", longerBody,
		s3ObjectSidecar("etag-2", int64(len(longerBody)), "text/plain", ""))

	fsm := encodeS3Tree(t, in)
	// The tracker classification stood despite the directory at the
	// sidecar path, so the keymap loaded and "path/to" recovered.
	assertS3ManifestEmittedUnder(t, fsm, "path/to", "path/to"+S3LeafDataSuffix)
}

// TestS3EncodeMissingKeymapIsValidNoCollisionDump pins that the
// M4-2a happy path is unchanged: a bucket without KEYMAP.jsonl
// continues to encode every object at its on-disk path.
func TestS3EncodeMissingKeymapIsValidNoCollisionDump(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	const bucket = "b"
	writeS3Bucket(t, in, bucket, []byte(`{"format_version":1,"name":"b"}`))
	body := []byte("body without any keymap")
	writeS3Object(t, in, bucket, "plain/key", body,
		s3ObjectSidecar("etag-plain", int64(len(body)), "text/plain", ""))
	gotBody, _ := decodeS3Object(t, encodeS3Tree(t, in), bucket, "plain/key")
	if !bytes.Equal(gotBody, body) {
		t.Fatalf("no-keymap object body = %q, want %q", gotBody, body)
	}
}
