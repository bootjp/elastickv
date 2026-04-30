package backup

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/cockroachdb/errors"
)

func newS3Encoder(t *testing.T) (*S3Encoder, string) {
	t.Helper()
	out := t.TempDir()
	scratch := t.TempDir()
	return NewS3Encoder(out, scratch), out
}

func encodeS3BucketMetaValue(t *testing.T, m map[string]any) []byte {
	t.Helper()
	body, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return body
}

func encodeS3ManifestValue(t *testing.T, m map[string]any) []byte {
	t.Helper()
	body, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return body
}

// emitObject is the minimal happy-path fixture: bucket meta + a
// single-part single-chunk object with its body.
func emitObject(t *testing.T, enc *S3Encoder, bucket string, gen uint64, object string, body []byte, contentType string) {
	t.Helper()
	if err := enc.HandleBucketMeta(s3keys.BucketMetaKey(bucket), encodeS3BucketMetaValue(t, map[string]any{
		"bucket_name": bucket, "generation": gen, "owner": "alice", "region": "us-east-1",
	})); err != nil {
		t.Fatalf("HandleBucketMeta: %v", err)
	}
	uploadID := "u-1"
	manifest := map[string]any{
		"upload_id":    uploadID,
		"etag":         "\"deadbeef\"",
		"size_bytes":   int64(len(body)),
		"content_type": contentType,
		"parts": []map[string]any{
			{"part_no": 1, "etag": "\"x\"", "size_bytes": int64(len(body)), "chunk_count": 1},
		},
	}
	if err := enc.HandleObjectManifest(s3keys.ObjectManifestKey(bucket, gen, object), encodeS3ManifestValue(t, manifest)); err != nil {
		t.Fatalf("HandleObjectManifest: %v", err)
	}
	if err := enc.HandleBlob(s3keys.BlobKey(bucket, gen, object, uploadID, 1, 0), body); err != nil {
		t.Fatalf("HandleBlob: %v", err)
	}
}

func readJSONFile[T any](t *testing.T, path string, into *T) {
	t.Helper()
	body, err := os.ReadFile(path) //nolint:gosec // test path
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if err := json.Unmarshal(body, into); err != nil {
		t.Fatalf("unmarshal %s: %v", path, err)
	}
}

func TestS3_BucketMetaAndSingleObjectAssembly(t *testing.T) {
	t.Parallel()
	enc, root := newS3Encoder(t)
	body := []byte("hello-world")
	emitObject(t, enc, "photos", 7, "2026/04/img.jpg", body, "image/jpeg")
	if err := enc.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(root, "s3", "photos", "2026/04/img.jpg")) //nolint:gosec // test path
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("body mismatch: %q vs %q", got, body)
	}
	var pm s3PublicManifest
	readJSONFile(t, filepath.Join(root, "s3", "photos", "2026/04/img.jpg.elastickv-meta.json"), &pm)
	if pm.ContentType != "image/jpeg" {
		t.Fatalf("content_type = %q", pm.ContentType)
	}
	if pm.SizeBytes != int64(len(body)) {
		t.Fatalf("size = %d", pm.SizeBytes)
	}
	var pb s3PublicBucket
	readJSONFile(t, filepath.Join(root, "s3", "photos", "_bucket.json"), &pb)
	if pb.Region != "us-east-1" {
		t.Fatalf("region = %q", pb.Region)
	}
}

func TestS3_MultipartObjectAssemblesInPartChunkOrder(t *testing.T) {
	t.Parallel()
	enc, root := newS3Encoder(t)
	bucket := "logs"
	gen := uint64(1)
	object := "app.log"
	uploadID := "u-mp"
	if err := enc.HandleBucketMeta(s3keys.BucketMetaKey(bucket), encodeS3BucketMetaValue(t, map[string]any{
		"bucket_name": bucket, "generation": gen,
	})); err != nil {
		t.Fatal(err)
	}
	if err := enc.HandleObjectManifest(s3keys.ObjectManifestKey(bucket, gen, object), encodeS3ManifestValue(t, map[string]any{
		"upload_id": uploadID, "size_bytes": 11, "parts": []map[string]any{
			{"part_no": 1, "size_bytes": 5, "chunk_count": 2},
			{"part_no": 2, "size_bytes": 6, "chunk_count": 1},
		},
	})); err != nil {
		t.Fatal(err)
	}
	// Insert chunks out of order; assembly must respect (partNo, chunkNo).
	if err := enc.HandleBlob(s3keys.BlobKey(bucket, gen, object, uploadID, 2, 0), []byte("WORLD!")); err != nil {
		t.Fatal(err)
	}
	if err := enc.HandleBlob(s3keys.BlobKey(bucket, gen, object, uploadID, 1, 1), []byte("lo")); err != nil {
		t.Fatal(err)
	}
	if err := enc.HandleBlob(s3keys.BlobKey(bucket, gen, object, uploadID, 1, 0), []byte("hel")); err != nil {
		t.Fatal(err)
	}
	if err := enc.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(root, "s3", bucket, object)) //nolint:gosec
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "helloWORLD!" {
		t.Fatalf("body = %q want %q", got, "helloWORLD!")
	}
}

func TestS3_OrphanChunksWarn(t *testing.T) {
	t.Parallel()
	enc, _ := newS3Encoder(t)
	var events []string
	enc.WithWarnSink(func(e string, _ ...any) { events = append(events, e) })
	if err := enc.HandleBlob(s3keys.BlobKey("ghost", 1, "lost.bin", "u", 1, 0), []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := enc.Finalize(); err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0] != "s3_orphan_chunks" {
		t.Fatalf("events = %v", events)
	}
}

func TestS3_MetaSuffixCollisionRejectedByDefault(t *testing.T) {
	t.Parallel()
	enc, _ := newS3Encoder(t)
	emitObject(t, enc, "b", 1, "evil.elastickv-meta.json", []byte("payload"), "")
	err := enc.Finalize()
	if !errors.Is(err, ErrS3MetaSuffixCollision) {
		t.Fatalf("err = %v want ErrS3MetaSuffixCollision", err)
	}
}

func TestS3_MetaSuffixCollisionRenamesUnderFlag(t *testing.T) {
	t.Parallel()
	enc, root := newS3Encoder(t)
	enc.WithRenameCollisions(true)
	emitObject(t, enc, "b", 1, "evil.elastickv-meta.json", []byte("payload"), "")
	if err := enc.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	want := filepath.Join(root, "s3", "b", "evil.elastickv-meta.json.user-data")
	if _, err := os.Stat(want); err != nil {
		t.Fatalf("renamed body not found at %s: %v", want, err)
	}
	// KEYMAP must record the rename.
	keymapPath := filepath.Join(root, "s3", "b", "KEYMAP.jsonl")
	body, err := os.ReadFile(keymapPath) //nolint:gosec
	if err != nil {
		t.Fatalf("read keymap: %v", err)
	}
	var rec KeymapRecord
	if err := json.Unmarshal(bytes.TrimRight(body, "\n"), &rec); err != nil {
		t.Fatalf("unmarshal keymap: %v", err)
	}
	if rec.Kind != KindMetaCollision {
		t.Fatalf("kind = %q", rec.Kind)
	}
	orig, err := rec.Original()
	if err != nil {
		t.Fatal(err)
	}
	if string(orig) != "evil.elastickv-meta.json" {
		t.Fatalf("original = %q", orig)
	}
}

func TestS3_RejectsMalformedManifestJSON(t *testing.T) {
	t.Parallel()
	enc, _ := newS3Encoder(t)
	err := enc.HandleObjectManifest(s3keys.ObjectManifestKey("b", 1, "o"), []byte("not-json"))
	if !errors.Is(err, ErrS3InvalidManifest) {
		t.Fatalf("err = %v", err)
	}
}

func TestS3_RejectsMalformedBucketMetaJSON(t *testing.T) {
	t.Parallel()
	enc, _ := newS3Encoder(t)
	err := enc.HandleBucketMeta(s3keys.BucketMetaKey("b"), []byte("not-json"))
	if !errors.Is(err, ErrS3InvalidBucketMeta) {
		t.Fatalf("err = %v", err)
	}
}

func TestS3_HandleIgnored_NoOp(t *testing.T) {
	t.Parallel()
	enc, _ := newS3Encoder(t)
	if err := enc.HandleIgnored([]byte("!s3|gc|upload|whatever"), []byte("opaque")); err != nil {
		t.Fatalf("HandleIgnored should be a no-op, err=%v", err)
	}
}

func TestS3_IncludeIncompleteUploadsBuffersRecords(t *testing.T) {
	t.Parallel()
	enc, root := newS3Encoder(t)
	enc.WithIncludeIncompleteUploads(true)
	if err := enc.HandleBucketMeta(s3keys.BucketMetaKey("b"), encodeS3BucketMetaValue(t, map[string]any{
		"bucket_name": "b", "generation": 1,
	})); err != nil {
		t.Fatal(err)
	}
	uploadKey := s3keys.UploadMetaKey("b", 1, "obj", "u-1")
	if err := enc.HandleIncompleteUpload(S3UploadMetaPrefix, uploadKey, []byte("payload")); err != nil {
		t.Fatal(err)
	}
	if err := enc.Finalize(); err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(root, "s3", "b", "_incomplete_uploads", "records.jsonl")
	if _, err := os.Stat(want); err != nil {
		t.Fatalf("expected incomplete-uploads file: %v", err)
	}
}

func TestS3_DefaultDoesNotEmitIncompleteUploads(t *testing.T) {
	t.Parallel()
	enc, root := newS3Encoder(t)
	if err := enc.HandleBucketMeta(s3keys.BucketMetaKey("b"), encodeS3BucketMetaValue(t, map[string]any{
		"bucket_name": "b", "generation": 1,
	})); err != nil {
		t.Fatal(err)
	}
	uploadKey := s3keys.UploadMetaKey("b", 1, "obj", "u-1")
	if err := enc.HandleIncompleteUpload(S3UploadMetaPrefix, uploadKey, []byte("payload")); err != nil {
		t.Fatal(err)
	}
	if err := enc.Finalize(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "s3", "b", "_incomplete_uploads")); !os.IsNotExist(err) {
		t.Fatalf("expected no _incomplete_uploads dir without flag, stat err=%v", err)
	}
}

func TestS3_VersionedBlobAssembledByPartVersionOrder(t *testing.T) {
	t.Parallel()
	enc, root := newS3Encoder(t)
	bucket := "v"
	gen := uint64(1)
	object := "obj"
	uploadID := "u"
	if err := enc.HandleBucketMeta(s3keys.BucketMetaKey(bucket), encodeS3BucketMetaValue(t, map[string]any{
		"bucket_name": bucket, "generation": gen,
	})); err != nil {
		t.Fatal(err)
	}
	if err := enc.HandleObjectManifest(s3keys.ObjectManifestKey(bucket, gen, object), encodeS3ManifestValue(t, map[string]any{
		"upload_id": uploadID, "size_bytes": 6, "parts": []map[string]any{
			{"part_no": 1, "size_bytes": 6, "chunk_count": 1, "part_version": 9},
		},
	})); err != nil {
		t.Fatal(err)
	}
	// Versioned blob — partVersion encoded into the key.
	if err := enc.HandleBlob(s3keys.VersionedBlobKey(bucket, gen, object, uploadID, 1, 0, 9), []byte("vBlobX")); err != nil {
		t.Fatal(err)
	}
	if err := enc.Finalize(); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(root, "s3", bucket, object)) //nolint:gosec
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "vBlobX" {
		t.Fatalf("body = %q", got)
	}
}
