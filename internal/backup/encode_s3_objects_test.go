package backup

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/errors"
)

// writeS3Object writes an object body + its .elastickv-meta.json sidecar
// under <root>/s3/<EncodeSegment(bucket)>/<objKey path>.
func writeS3Object(t *testing.T, root, bucket, objKey string, body, sidecar []byte) {
	t.Helper()
	base := filepath.Join(root, "s3", EncodeSegment([]byte(bucket)), filepath.FromSlash(objKey))
	if err := os.MkdirAll(filepath.Dir(base), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(base, body, 0o600); err != nil {
		t.Fatalf("WriteFile body: %v", err)
	}
	if err := os.WriteFile(base+S3MetaSuffixReserved, sidecar, 0o600); err != nil {
		t.Fatalf("WriteFile sidecar: %v", err)
	}
}

// decodeS3Object decodes fsm bytes and returns the reassembled body and
// parsed sidecar for one object.
func decodeS3Object(t *testing.T, fsm []byte, bucket, objKey string) ([]byte, s3PublicManifest) {
	t.Helper()
	out := t.TempDir()
	if _, err := DecodeSnapshot(bytes.NewReader(fsm), DecodeOptions{
		OutRoot:  out,
		Adapters: AdapterSet{S3: true},
	}); err != nil {
		t.Fatalf("DecodeSnapshot: %v", err)
	}
	base := filepath.Join(out, "s3", EncodeSegment([]byte(bucket)), filepath.FromSlash(objKey))
	body, err := os.ReadFile(base)
	if err != nil {
		t.Fatalf("read decoded body %q: %v", objKey, err)
	}
	scData, err := os.ReadFile(base + S3MetaSuffixReserved)
	if err != nil {
		t.Fatalf("read decoded sidecar: %v", err)
	}
	var sc s3PublicManifest
	if err := json.Unmarshal(scData, &sc); err != nil {
		t.Fatalf("unmarshal decoded sidecar: %v", err)
	}
	return body, sc
}

func s3ObjectSidecar(etag string, size int64, contentType, lastModified string) []byte {
	m := s3PublicManifest{
		FormatVersion: 1,
		ETag:          etag,
		SizeBytes:     size,
		ContentType:   contentType,
		LastModified:  lastModified,
	}
	out, _ := json.Marshal(m)
	return out
}

// TestS3EncodeObjectRoundTrip runs the gold-standard round-trip for a
// small single-chunk object: body + sidecar survive encode -> real decode.
func TestS3EncodeObjectRoundTrip(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	const bucket = "obj-rt"
	writeS3Bucket(t, in, bucket, []byte(`{"format_version":1,"name":"obj-rt"}`))
	body := []byte("hello world")
	writeS3Object(t, in, bucket, "greeting.txt", body,
		s3ObjectSidecar("etag-1", int64(len(body)), "text/plain", "2024-01-02T03:04:05Z"))

	gotBody, gotSC := decodeS3Object(t, encodeS3Tree(t, in), bucket, "greeting.txt")
	if !bytes.Equal(gotBody, body) {
		t.Fatalf("body = %q, want %q", gotBody, body)
	}
	if gotSC.ETag != "etag-1" || gotSC.SizeBytes != int64(len(body)) || gotSC.ContentType != "text/plain" {
		t.Fatalf("sidecar = %+v", gotSC)
	}
	if gotSC.LastModified != "2024-01-02T03:04:05Z" {
		t.Fatalf("last_modified = %q, want round-tripped 2024-01-02T03:04:05Z", gotSC.LastModified)
	}
}

// TestS3EncodeObjectMultiChunkRoundTrip pins blob re-chunking: a body
// larger than s3ChunkSize is split into multiple !s3|blob| records and
// reassembled byte-identically by the decoder.
func TestS3EncodeObjectMultiChunkRoundTrip(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	const bucket = "obj-mc"
	writeS3Bucket(t, in, bucket, []byte(`{"format_version":1,"name":"obj-mc"}`))
	// 2.5 chunks worth, with a position-dependent pattern so a mis-ordered
	// or dropped chunk is detected.
	body := make([]byte, s3ChunkSize*2+512)
	for i := range body {
		body[i] = byte(i % 251)
	}
	writeS3Object(t, in, bucket, "big.bin", body,
		s3ObjectSidecar("etag-big", int64(len(body)), "application/octet-stream", ""))

	gotBody, _ := decodeS3Object(t, encodeS3Tree(t, in), bucket, "big.bin")
	if !bytes.Equal(gotBody, body) {
		t.Fatalf("multi-chunk body mismatch: got %d bytes, want %d", len(gotBody), len(body))
	}
}

// TestS3EncodeEmptyObjectRoundTrip pins the zero-byte object (no blobs, no
// part) round-trip.
func TestS3EncodeEmptyObjectRoundTrip(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	const bucket = "obj-empty"
	writeS3Bucket(t, in, bucket, []byte(`{"format_version":1,"name":"obj-empty"}`))
	writeS3Object(t, in, bucket, "empty", []byte{}, s3ObjectSidecar("etag-0", 0, "", ""))

	gotBody, _ := decodeS3Object(t, encodeS3Tree(t, in), bucket, "empty")
	if len(gotBody) != 0 {
		t.Fatalf("empty object body = %d bytes, want 0", len(gotBody))
	}
}

// TestS3EncodeNestedObjectKeyRoundTrip pins that an object key with "/"
// (stored as a nested path) round-trips with its slash-separated key.
func TestS3EncodeNestedObjectKeyRoundTrip(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	const bucket = "obj-nested"
	writeS3Bucket(t, in, bucket, []byte(`{"format_version":1,"name":"obj-nested"}`))
	body := []byte("nested body")
	writeS3Object(t, in, bucket, "a/b/c.txt", body, s3ObjectSidecar("e", int64(len(body)), "text/plain", ""))

	gotBody, _ := decodeS3Object(t, encodeS3Tree(t, in), bucket, "a/b/c.txt")
	if !bytes.Equal(gotBody, body) {
		t.Fatalf("nested object body = %q, want %q", gotBody, body)
	}
}

// TestReadRootBodyFileRejectsNonRegular pins the PRE-open guard on the
// object body open: a non-regular target (directory stand-in for a
// symlink/FIFO) is refused before the open, so a planted FIFO cannot block
// the encoder (gemini security-high on PR #845).
func TestOpenRootRegularRejectsNonRegular(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "body"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatalf("OpenRoot: %v", err)
	}
	defer func() { _ = root.Close() }()
	if _, _, err := openRootRegular(root, "body"); !errors.Is(err, ErrS3EncodeNotRegular) {
		t.Fatalf("openRootRegular err = %v, want ErrS3EncodeNotRegular", err)
	}
}

// TestS3EncodeRejectsSizeMismatch pins fail-closed when the sidecar's
// size_bytes disagrees with the actual object body length (corrupt dump).
func TestS3EncodeRejectsSizeMismatch(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	const bucket = "b"
	writeS3Bucket(t, in, bucket, []byte(`{"format_version":1,"name":"b"}`))
	writeS3Object(t, in, bucket, "obj", []byte("hello"), s3ObjectSidecar("e", 999, "text/plain", ""))
	b := newSnapshotBuilder(s3EncTS)
	if err := NewS3RecordEncoder(in).Encode(b); !errors.Is(err, ErrS3EncodeInvalidManifest) {
		t.Fatalf("Encode err = %v, want ErrS3EncodeInvalidManifest", err)
	}
}

// TestReadObjectSidecarRejectsNonRegular pins the pre-open guard on the
// sidecar read.
func TestReadObjectSidecarRejectsNonRegular(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "sc"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatalf("OpenRoot: %v", err)
	}
	defer func() { _ = root.Close() }()
	e := NewS3RecordEncoder(dir)
	if _, err := e.readObjectSidecar(root, "sc"); !errors.Is(err, ErrS3EncodeNotRegular) {
		t.Fatalf("readObjectSidecar err = %v, want ErrS3EncodeNotRegular", err)
	}
}

// TestParseRFC3339NanoAsHLC pins the three silent-zero paths and the
// sub-millisecond precision loss of the HLC reconstruction.
func TestParseRFC3339NanoAsHLC(t *testing.T) {
	t.Parallel()
	if got := parseRFC3339NanoAsHLC(""); got != 0 {
		t.Fatalf("empty = %d, want 0", got)
	}
	if got := parseRFC3339NanoAsHLC("not-a-time"); got != 0 {
		t.Fatalf("invalid = %d, want 0", got)
	}
	if got := parseRFC3339NanoAsHLC("1969-12-31T23:59:59Z"); got != 0 {
		t.Fatalf("pre-epoch = %d, want 0", got)
	}
	if got := parseRFC3339NanoAsHLC("2024-01-02T03:04:05Z"); got == 0 {
		t.Fatalf("happy path = 0, want non-zero")
	}
	// Sub-millisecond precision is lost (truncated to ms), so the nanosecond
	// form encodes identically to the millisecond form.
	withNanos := parseRFC3339NanoAsHLC("2024-01-02T03:04:05.000000001Z")
	withoutNanos := parseRFC3339NanoAsHLC("2024-01-02T03:04:05Z")
	if withNanos != withoutNanos {
		t.Fatalf("sub-ms precision not truncated: %d != %d", withNanos, withoutNanos)
	}
}

// TestS3EncodeKeymapDirPrefixObjectRoundTrip pins that object keys under a
// "KEYMAP.jsonl/" prefix (where KEYMAP.jsonl is a directory, not the
// collision-tracker file) round-trip rather than being mistaken for the
// tracker (codex P1 #845).
func TestS3EncodeKeymapDirPrefixObjectRoundTrip(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	const bucket = "obj-kmdir"
	writeS3Bucket(t, in, bucket, []byte(`{"format_version":1,"name":"obj-kmdir"}`))
	body := []byte("under keymap dir")
	writeS3Object(t, in, bucket, "KEYMAP.jsonl/foo", body, s3ObjectSidecar("e", int64(len(body)), "text/plain", ""))

	gotBody, _ := decodeS3Object(t, encodeS3Tree(t, in), bucket, "KEYMAP.jsonl/foo")
	if !bytes.Equal(gotBody, body) {
		t.Fatalf("object under KEYMAP.jsonl/ prefix body = %q, want %q", gotBody, body)
	}
}

// TestS3EncodeKeymapObjectRoundTrip pins that a legitimate user object
// named "KEYMAP.jsonl" (one with a companion sidecar) round-trips, rather
// than being mistaken for the collision tracker (codex P1 #845).
func TestS3EncodeKeymapObjectRoundTrip(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	const bucket = "obj-keymap"
	writeS3Bucket(t, in, bucket, []byte(`{"format_version":1,"name":"obj-keymap"}`))
	body := []byte("user keymap object")
	writeS3Object(t, in, bucket, "KEYMAP.jsonl", body, s3ObjectSidecar("e", int64(len(body)), "application/json", ""))

	gotBody, _ := decodeS3Object(t, encodeS3Tree(t, in), bucket, "KEYMAP.jsonl")
	if !bytes.Equal(gotBody, body) {
		t.Fatalf("KEYMAP.jsonl object body = %q, want %q", gotBody, body)
	}
}

// TestS3EncodeLeafDataObjectRoundTrip pins that a legitimate object key
// ending in .elastickv-leaf-data (with a sidecar, no collision) round-trips
// rather than being rejected as a rename artifact (codex P1 #845).
func TestS3EncodeLeafDataObjectRoundTrip(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	const bucket = "obj-leaf"
	writeS3Bucket(t, in, bucket, []byte(`{"format_version":1,"name":"obj-leaf"}`))
	body := []byte("leaf-data named object")
	objKey := "foo" + S3LeafDataSuffix
	writeS3Object(t, in, bucket, objKey, body, s3ObjectSidecar("e", int64(len(body)), "application/octet-stream", ""))

	gotBody, _ := decodeS3Object(t, encodeS3Tree(t, in), bucket, objKey)
	if !bytes.Equal(gotBody, body) {
		t.Fatalf("leaf-data-named object body = %q, want %q", gotBody, body)
	}
}

// TestS3EncodeRejectsUserObjectUnderReservedDir pins fail-closed when a
// user S3 object key collides with the reserved dump-control directories
// _incomplete_uploads/ or _orphans/. The decoder writes such keys at their
// natural path (resolveObjectFilename does not rename them), so the encoder
// must distinguish them from the dump's own payload — sidecar presence
// inside the reserved dir is the signal. Without this guard the entire
// user-object subtree would be silently dropped (codex P1 #842 follow-up).
func TestS3EncodeRejectsUserObjectUnderReservedDir(t *testing.T) {
	t.Parallel()
	for _, dir := range []string{"_incomplete_uploads", "_orphans"} {
		t.Run(dir, func(t *testing.T) {
			t.Parallel()
			in := t.TempDir()
			const bucket = "b"
			writeS3Bucket(t, in, bucket, []byte(`{"format_version":1,"name":"b"}`))
			body := []byte("user object colliding with reserved prefix")
			writeS3Object(t, in, bucket, dir+"/path/to/key", body,
				s3ObjectSidecar("e", int64(len(body)), "text/plain", ""))

			b := newSnapshotBuilder(s3EncTS)
			if err := NewS3RecordEncoder(in).Encode(b); !errors.Is(err, ErrS3EncodeReservedPrefixCollision) {
				t.Fatalf("Encode err = %v, want ErrS3EncodeReservedPrefixCollision", err)
			}
		})
	}
}

// TestS3EncodeSkipsLegitimateReservedDumpDirs pins that a benign
// _incomplete_uploads/records.jsonl or _orphans/<obj>/gen-N/*.bin payload
// (no sidecars) is silently skipped, NOT treated as a collision. The
// reserved-prefix guard must only fire when sidecar-bearing user objects
// are present.
func TestS3EncodeSkipsLegitimateReservedDumpDirs(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	const bucket = "b"
	writeS3Bucket(t, in, bucket, []byte(`{"format_version":1,"name":"b"}`))
	bucketDir := filepath.Join(in, "s3", EncodeSegment([]byte(bucket)))
	// records.jsonl: no sidecar
	iu := filepath.Join(bucketDir, "_incomplete_uploads")
	if err := os.MkdirAll(iu, 0o755); err != nil {
		t.Fatalf("MkdirAll iu: %v", err)
	}
	if err := os.WriteFile(filepath.Join(iu, "records.jsonl"), []byte("{}\n"), 0o600); err != nil {
		t.Fatalf("write records.jsonl: %v", err)
	}
	// orphan chunk .bin: no sidecar
	od := filepath.Join(bucketDir, "_orphans", EncodeSegment([]byte("ghost")), "gen-1")
	if err := os.MkdirAll(od, 0o755); err != nil {
		t.Fatalf("MkdirAll od: %v", err)
	}
	if err := os.WriteFile(filepath.Join(od, "0.bin"), []byte("x"), 0o600); err != nil {
		t.Fatalf("write orphan bin: %v", err)
	}
	// And one legitimate object outside the reserved dirs so Encode has work to do
	body := []byte("ok")
	writeS3Object(t, in, bucket, "real-object", body,
		s3ObjectSidecar("e", int64(len(body)), "text/plain", ""))

	gotBody, _ := decodeS3Object(t, encodeS3Tree(t, in), bucket, "real-object")
	if !bytes.Equal(gotBody, body) {
		t.Fatalf("real-object body = %q, want %q", gotBody, body)
	}
}
