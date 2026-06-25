package backup

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/cockroachdb/errors"
)

const s3EncTS uint64 = 0x0001_8F1A_2B3C_00CD

// writeS3Bucket writes a _bucket.json under <root>/s3/<EncodeSegment(name)>/.
func writeS3Bucket(t *testing.T, root, name string, body []byte) {
	t.Helper()
	path := filepath.Join(root, "s3", EncodeSegment([]byte(name)), "_bucket.json")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
}

// encodeS3Tree runs the S3 reverse encoder over inRoot and returns the
// EKVPBBL1 bytes.
func encodeS3Tree(t *testing.T, inRoot string) []byte {
	t.Helper()
	b := newSnapshotBuilder(s3EncTS)
	if err := NewS3RecordEncoder(inRoot).Encode(b); err != nil {
		t.Fatalf("S3RecordEncoder.Encode: %v", err)
	}
	var buf bytes.Buffer
	if _, err := b.WriteTo(&buf); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	return buf.Bytes()
}

// TestS3EncodeBucketMetaRoundTrip runs the gold-standard directory
// round-trip: a _bucket.json is reconstructed into the !s3|bucket|meta|
// record and recovered by the real decoder into an equivalent _bucket.json.
func TestS3EncodeBucketMetaRoundTrip(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	const bucket = "mybucket"
	input := []byte(`{"format_version":1,"name":"mybucket","owner":"acct-1","region":"us-east-1","acl":"private"}`)
	writeS3Bucket(t, in, bucket, input)

	out := t.TempDir()
	if _, err := DecodeSnapshot(bytes.NewReader(encodeS3Tree(t, in)), DecodeOptions{
		OutRoot:  out,
		Adapters: AdapterSet{S3: true},
	}); err != nil {
		t.Fatalf("DecodeSnapshot: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(out, "s3", EncodeSegment([]byte(bucket)), "_bucket.json"))
	if err != nil {
		t.Fatalf("read decoded _bucket.json: %v", err)
	}
	var got s3PublicBucket
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal decoded bucket: %v", err)
	}
	if got.Name != bucket || got.Owner != "acct-1" || got.Region != "us-east-1" || got.ACL != "private" {
		t.Fatalf("round-tripped bucket = %+v", got)
	}
}

// TestS3EncodeBucketRecordsLayout pins the emitted record bytes: the
// !s3|bucket|meta| value is JSON with generation = s3RestoreGeneration,
// and the !s3|bucket|gen| counter is the 8-byte big-endian generation.
func TestS3EncodeBucketRecordsLayout(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	const bucket = "b1"
	writeS3Bucket(t, in, bucket, []byte(`{"format_version":1,"name":"b1","owner":"o","region":"r","acl":"private"}`))

	entries, _, err := DecodeLiveEntries(bytes.NewReader(encodeS3Tree(t, in)))
	if err != nil {
		t.Fatalf("DecodeLiveEntries: %v", err)
	}
	meta := findEntry(entries, s3keys.BucketMetaKey(bucket))
	gen := findEntry(entries, s3keys.BucketGenerationKey(bucket))
	if meta == nil || gen == nil {
		t.Fatalf("missing records: meta=%v gen=%v", meta != nil, gen != nil)
	}
	var live s3LiveBucketMeta
	if err := json.Unmarshal(meta.UserValue, &live); err != nil {
		t.Fatalf("unmarshal meta value: %v", err)
	}
	if live.BucketName != bucket || live.Generation != s3RestoreGeneration {
		t.Fatalf("meta value = %+v, want name=%s gen=%d", live, bucket, s3RestoreGeneration)
	}
	if len(gen.UserValue) != 8 || binary.BigEndian.Uint64(gen.UserValue) != s3RestoreGeneration {
		t.Fatalf("gen counter = %x, want 8-byte BE %d", gen.UserValue, s3RestoreGeneration)
	}
}

// findEntry returns the first live entry with the given user key, or nil.
func findEntry(entries []RoundTripEntry, key []byte) *RoundTripEntry {
	for i := range entries {
		if bytes.Equal(entries[i].UserKey, key) {
			return &entries[i]
		}
	}
	return nil
}

// TestS3EncodeRejectsUnknownFormatVersion pins the format gate: a
// _bucket.json with an unsupported format_version fails closed rather than
// being silently parsed under the v1 schema — mirrors the DynamoDB schema
// reader's format_version gate.
func TestS3EncodeRejectsUnknownFormatVersion(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	writeS3Bucket(t, in, "b", []byte(`{"format_version":99,"name":"b"}`))
	b := newSnapshotBuilder(s3EncTS)
	if err := NewS3RecordEncoder(in).Encode(b); !errors.Is(err, ErrS3EncodeInvalidBucket) {
		t.Fatalf("Encode err = %v, want ErrS3EncodeInvalidBucket", err)
	}
}

// TestS3EncodeMissingDirIsNoop pins that an absent s3/ dir is a no-op.
func TestS3EncodeMissingDirIsNoop(t *testing.T) {
	t.Parallel()
	b := newSnapshotBuilder(s3EncTS)
	if err := NewS3RecordEncoder(t.TempDir()).Encode(b); err != nil {
		t.Fatalf("Encode on empty tree: %v", err)
	}
	if b.Len() != 0 {
		t.Fatalf("entries = %d, want 0", b.Len())
	}
}

// TestS3EncodeRejectsNameDirMismatch pins #35: when the on-disk bucket
// dir doesn't match EncodeSegment([]byte(_bucket.json.name)), the
// encoder fails closed before emitting any keys. Otherwise a
// hand-edited dump could end up with bucket-meta records keyed by
// the JSON's name but object bytes pulled from a different
// filesystem subtree — silent name/dir consistency violation.
func TestS3EncodeRejectsNameDirMismatch(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	// Plant the dump under s3/wrong-dir/_bucket.json with name="real".
	// EncodeSegment("real") is "real" (unreserved chars), so dir != name.
	path := filepath.Join(in, "s3", "wrong-dir", "_bucket.json")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(path,
		[]byte(`{"format_version":1,"name":"real"}`), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	b := newSnapshotBuilder(s3EncTS)
	err := NewS3RecordEncoder(in).Encode(b)
	if !errors.Is(err, ErrS3EncodeInvalidBucket) {
		t.Fatalf("Encode err = %v, want ErrS3EncodeInvalidBucket for name/dir mismatch", err)
	}
}

// TestS3EncodeRejectsNonDirectoryBucketEntry pins codex P2 v32 #904:
// when an entry directly under s3/ is a regular file or symlink
// rather than a bucket directory, the encoder must fail closed with
// ErrS3EncodeNotRegular rather than silently skipping (which would
// publish a partial .fsm with the affected bucket omitted; the
// manifest's deferred-enumeration empty S3 scope cannot otherwise
// flag the missing data). Reserved-prefix `_*` entries (e.g.
// `_incomplete_uploads`) are explicitly tolerated because they're
// handled by dedicated paths.
func TestS3EncodeRejectsNonDirectoryBucketEntry(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	if err := os.MkdirAll(filepath.Join(in, "s3"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	// Plant a regular file where a bucket directory should be.
	if err := os.WriteFile(filepath.Join(in, "s3", "stray.txt"), []byte("oops"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	b := newSnapshotBuilder(s3EncTS)
	err := NewS3RecordEncoder(in).Encode(b)
	if !errors.Is(err, ErrS3EncodeNotRegular) {
		t.Fatalf("Encode err = %v, want errors.Is ErrS3EncodeNotRegular", err)
	}
}

// TestS3EncodeIgnoresReservedPrefixEntry pins that codex P2 v32's
// fail-closed for non-directory top-level entries does NOT fire on
// reserved-prefix entries (those starting with "_"). The reverse
// encoder's unsupported-features guard handles those subtrees
// separately via ErrEncodeUnsupportedS3IncompleteUploads /
// ErrEncodeUnsupportedS3Orphans.
func TestS3EncodeIgnoresReservedPrefixEntry(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	if err := os.MkdirAll(filepath.Join(in, "s3"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	// Reserved-prefix file (e.g., a marker the operator left).
	if err := os.WriteFile(filepath.Join(in, "s3", "_marker"), []byte("x"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	b := newSnapshotBuilder(s3EncTS)
	if err := NewS3RecordEncoder(in).Encode(b); err != nil {
		t.Errorf("Encode err = %v, want nil (reserved-prefix entries should be skipped)", err)
	}
}

// TestS3EncodeRejectsNonRegularBucketMeta pins the pre-open guard: a
// _bucket.json that is a directory is refused with ErrS3EncodeNotRegular.
func TestS3EncodeRejectsNonRegularBucketMeta(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	if err := os.MkdirAll(filepath.Join(in, "s3", "b", "_bucket.json"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	b := newSnapshotBuilder(s3EncTS)
	if err := NewS3RecordEncoder(in).Encode(b); !errors.Is(err, ErrS3EncodeNotRegular) {
		t.Fatalf("Encode err = %v, want ErrS3EncodeNotRegular", err)
	}
}
