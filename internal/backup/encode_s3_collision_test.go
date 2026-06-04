package backup

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
)

// writeS3KeymapTracker writes <root>/s3/<EncodeSegment(bucket)>/KEYMAP.jsonl
// with the given records and NO companion .elastickv-meta.json
// sidecar, so isKeymapCollisionTracker classifies it as a tracker.
func writeS3KeymapTracker(t *testing.T, root, bucket string, records []KeymapRecord) {
	t.Helper()
	bucketDir := filepath.Join(root, "s3", EncodeSegment([]byte(bucket)))
	if err := os.MkdirAll(bucketDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	f, err := os.Create(filepath.Join(bucketDir, "KEYMAP.jsonl"))
	if err != nil {
		t.Fatalf("Create KEYMAP.jsonl: %v", err)
	}
	defer func() { _ = f.Close() }()
	w := NewKeymapWriter(f)
	for _, rec := range records {
		if err := w.Write(rec); err != nil {
			t.Fatalf("Write %+v: %v", rec, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// loadKeymapForBucketB is a thin test wrapper around the new loader
// that opens the standard test bucket "b" via os.OpenRoot the same
// way Encode does. Bucket name is hardcoded since every loader test
// uses the same fixture name.
func loadKeymapForBucketB(t *testing.T, inRoot string) (map[string]KeymapRecord, error) {
	t.Helper()
	enc := NewS3RecordEncoder(inRoot)
	r, err := os.OpenRoot(filepath.Join(inRoot, "s3"))
	if err != nil {
		t.Fatalf("OpenRoot: %v", err)
	}
	defer func() { _ = r.Close() }()
	return enc.loadBucketKeymap(r, EncodeSegment([]byte("b")))
}

func leafRecord(encoded, originalSlashKey string) KeymapRecord {
	return KeymapRecord{
		Encoded:     encoded,
		OriginalB64: base64.RawURLEncoding.EncodeToString([]byte(originalSlashKey)),
		Kind:        KindS3LeafData,
	}
}

func metaRecord(encoded, originalSlashKey string) KeymapRecord {
	return KeymapRecord{
		Encoded:     encoded,
		OriginalB64: base64.RawURLEncoding.EncodeToString([]byte(originalSlashKey)),
		Kind:        KindMetaCollision,
	}
}

// TestLoadBucketKeymap_HappyPath verifies that three valid records
// (one per accepted Kind) load into a map keyed by Encoded.
func TestLoadBucketKeymap_HappyPath(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	const bucket = "b"
	records := []KeymapRecord{
		leafRecord("path/to"+S3LeafDataSuffix, "path/to"),
		metaRecord("renamed.user-data", "real"+S3MetaSuffixReserved),
		{
			Encoded:     "shorthash__truncated",
			OriginalB64: base64.RawURLEncoding.EncodeToString([]byte("very-long-original-key")),
			Kind:        KindSHAFallback,
		},
	}
	writeS3KeymapTracker(t, in, bucket, records)

	got, err := loadKeymapForBucketB(t, in)
	if err != nil {
		t.Fatalf("loadBucketKeymap: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("loaded %d records, want 3", len(got))
	}
	for _, rec := range records {
		if loaded, ok := got[rec.Encoded]; !ok {
			t.Errorf("missing record for encoded %q", rec.Encoded)
		} else if loaded.Kind != rec.Kind {
			t.Errorf("record %q: kind = %q, want %q", rec.Encoded, loaded.Kind, rec.Kind)
		}
	}
}

// TestLoadBucketKeymap_DuplicateEncoded pins the divergence from
// LoadKeymap's last-wins behavior: a duplicate Encoded value means
// the S3 decoder wrote two distinct rename targets for the same
// on-disk name, which is a corrupt dump the encoder cannot
// disambiguate. Claude v913 v2 documented this contract.
func TestLoadBucketKeymap_DuplicateEncoded(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	writeS3KeymapTracker(t, in, "b", []KeymapRecord{
		leafRecord("path/to"+S3LeafDataSuffix, "path/to"),
		leafRecord("path/to"+S3LeafDataSuffix, "elsewhere"),
	})
	_, err := loadKeymapForBucketB(t, in)
	if !errors.Is(err, ErrInvalidKeymapRecord) {
		t.Fatalf("err = %v, want wrap of ErrInvalidKeymapRecord", err)
	}
	if !strings.Contains(err.Error(), "duplicate encoded segment") {
		t.Fatalf("err = %v, want duplicate-encoded message", err)
	}
}

// TestLoadBucketKeymap_MalformedJSON pins that a corrupted JSONL
// line surfaces ErrInvalidKeymapRecord (via the KeymapReader, not a
// loader-side check — same outcome either way for the operator).
func TestLoadBucketKeymap_MalformedJSON(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	const bucket = "b"
	bucketDir := filepath.Join(in, "s3", EncodeSegment([]byte(bucket)))
	if err := os.MkdirAll(bucketDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(filepath.Join(bucketDir, "KEYMAP.jsonl"),
		[]byte("{this is not json}\n"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	_, err := loadKeymapForBucketB(t, in)
	if !errors.Is(err, ErrInvalidKeymapRecord) {
		t.Fatalf("err = %v, want ErrInvalidKeymapRecord", err)
	}
}

// TestLoadBucketKeymap_MissingFile pins that a missing
// KEYMAP.jsonl returns (nil, nil) rather than failing. The caller
// (isKeymapCollisionTracker) is the precondition gate; this
// behavior is defensive against an interleaving racy unlink.
func TestLoadBucketKeymap_MissingFile(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	bucketDir := filepath.Join(in, "s3", EncodeSegment([]byte("b")))
	if err := os.MkdirAll(bucketDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	got, err := loadKeymapForBucketB(t, in)
	if err != nil {
		t.Fatalf("loadBucketKeymap on missing KEYMAP.jsonl: %v", err)
	}
	if got != nil {
		t.Fatalf("got = %v, want nil for missing keymap", got)
	}
}

// TestValidateKeymapRecord_KindLeafSuffix pins gate "KindS3LeafData
// encoded must end in .elastickv-leaf-data".
func TestValidateKeymapRecord_KindLeafSuffix(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		encoded string
		wantErr bool
	}{
		{"valid suffix", "path/to" + S3LeafDataSuffix, false},
		{"missing suffix", "path/to", true},
		{"wrong suffix", "path/to.user-data", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			rec := leafRecord(c.encoded, "path/to")
			err := validateKeymapRecord(rec)
			if c.wantErr && !errors.Is(err, ErrInvalidKeymapRecord) {
				t.Fatalf("err = %v, want ErrInvalidKeymapRecord", err)
			}
			if !c.wantErr && err != nil {
				t.Fatalf("unexpected err = %v", err)
			}
		})
	}
}

// TestValidateKeymapRecord_KindMetaSuffix pins gate
// "KindMetaCollision original must end in .elastickv-meta.json".
func TestValidateKeymapRecord_KindMetaSuffix(t *testing.T) {
	t.Parallel()
	good := metaRecord("renamed.user-data", "real"+S3MetaSuffixReserved)
	if err := validateKeymapRecord(good); err != nil {
		t.Fatalf("valid record: unexpected err = %v", err)
	}
	bad := metaRecord("renamed.user-data", "real-without-suffix")
	if err := validateKeymapRecord(bad); !errors.Is(err, ErrInvalidKeymapRecord) {
		t.Fatalf("err = %v, want ErrInvalidKeymapRecord", err)
	}
}

// TestValidateKeymapRecord_UnknownKind pins the forward-compat
// guard: an unrecognized Kind value fails closed rather than
// silently passing.
func TestValidateKeymapRecord_UnknownKind(t *testing.T) {
	t.Parallel()
	rec := KeymapRecord{
		Encoded:     "x",
		OriginalB64: base64.RawURLEncoding.EncodeToString([]byte("orig")),
		Kind:        "future-rename-kind",
	}
	err := validateKeymapRecord(rec)
	if !errors.Is(err, ErrInvalidKeymapRecord) {
		t.Fatalf("err = %v, want ErrInvalidKeymapRecord", err)
	}
}

// TestValidateKeymapReservedRoot covers the boundary cases codex P2
// v913 v1 caught: the whole `_` namespace is NOT reserved, only
// _orphans/ and _incomplete_uploads/ at top level.
func TestValidateKeymapReservedRoot(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		key     string
		wantErr bool
	}{
		{"_orphans top-level", "_orphans/x", true},
		{"_incomplete_uploads top-level", "_incomplete_uploads/x", true},
		{"_orphans bare (no slash, first segment matches)", "_orphans", true},
		{"_foo (legit user key with leading underscore)", "_foo", false},
		{"_foo/bar", "_foo/bar", false},
		{"_orphansFoo (longer name, NOT reserved)", "_orphansFoo/x", false},
		{"nested _orphans (only top-level reserved)", "nested/_orphans/x", false},
		{"plain user key", "path/to", false},
		{"empty key", "", false}, // skip case; reserved check only fires on non-empty
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			rec := KeymapRecord{
				Encoded:     "anything",
				OriginalB64: base64.RawURLEncoding.EncodeToString([]byte(c.key)),
				Kind:        KindS3LeafData,
			}
			err := validateKeymapReservedRoot(rec, c.key)
			if c.wantErr && !errors.Is(err, ErrInvalidKeymapRecord) {
				t.Fatalf("err = %v, want ErrInvalidKeymapRecord", err)
			}
			if !c.wantErr && err != nil {
				t.Fatalf("unexpected err = %v", err)
			}
		})
	}
}

// verifyKeymapTargetsExistCase runs one verifyKeymapTargetsExist
// invocation against a freshly-opened os.Root so the resource
// lifetime stays tied to the subtest (parent-level defer + parallel
// subtests would close the root before the subtests run).
func verifyKeymapTargetsExistCase(t *testing.T, in string, km map[string]KeymapRecord) error {
	t.Helper()
	enc := NewS3RecordEncoder(in)
	r, err := os.OpenRoot(filepath.Join(in, "s3"))
	if err != nil {
		t.Fatalf("OpenRoot: %v", err)
	}
	defer func() { _ = r.Close() }()
	return enc.verifyKeymapTargetsExist(r, EncodeSegment([]byte("b")), km)
}

// TestVerifyKeymapTargetsExist pins that a keymap record citing an
// on-disk file that doesn't exist surfaces as a wrapped
// ErrInvalidKeymapRecord, not as a later missing-sidecar error.
// Run once at load time so the operator sees the keymap
// inconsistency directly.
func TestVerifyKeymapTargetsExist(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	bucketDir := filepath.Join(in, "s3", EncodeSegment([]byte("b")))
	if err := os.MkdirAll(filepath.Join(bucketDir, "path"), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(filepath.Join(bucketDir, "path", "to"+S3LeafDataSuffix), []byte("body"), 0o600); err != nil {
		t.Fatalf("WriteFile body: %v", err)
	}

	t.Run("present", func(t *testing.T) {
		t.Parallel()
		km := map[string]KeymapRecord{
			"path/to" + S3LeafDataSuffix: leafRecord("path/to"+S3LeafDataSuffix, "path/to"),
		}
		if err := verifyKeymapTargetsExistCase(t, in, km); err != nil {
			t.Fatalf("verifyKeymapTargetsExist: %v", err)
		}
	})

	t.Run("orphan record", func(t *testing.T) {
		t.Parallel()
		km := map[string]KeymapRecord{
			"missing/file" + S3LeafDataSuffix: leafRecord("missing/file"+S3LeafDataSuffix, "missing/file"),
		}
		err := verifyKeymapTargetsExistCase(t, in, km)
		if !errors.Is(err, ErrInvalidKeymapRecord) {
			t.Fatalf("err = %v, want ErrInvalidKeymapRecord", err)
		}
		if !strings.Contains(err.Error(), "orphan keymap record") {
			t.Fatalf("err message = %v, want orphan-keymap-record", err)
		}
	})
}

// TestResolveObjectKeyFromRel covers the lookup contract: nil
// keymap is the no-collision case (just slash-convert); a hit
// returns decoded Original; a miss returns the slash-form rel.
func TestResolveObjectKeyFromRel(t *testing.T) {
	t.Parallel()
	keymap := map[string]KeymapRecord{
		"path/to" + S3LeafDataSuffix: leafRecord("path/to"+S3LeafDataSuffix, "path/to"),
	}
	cases := []struct {
		name   string
		keymap map[string]KeymapRecord
		objRel string
		want   string
	}{
		{"nil keymap, no collision", nil, "path/to", "path/to"},
		{"empty keymap, no collision", map[string]KeymapRecord{}, "path/to", "path/to"},
		{"hit on leaf-data rename", keymap, "path/to" + S3LeafDataSuffix, "path/to"},
		{"miss returns rel as-is", keymap, "other/key", "other/key"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			got, err := resolveObjectKeyFromRel(c.keymap, c.objRel)
			if err != nil {
				t.Fatalf("resolveObjectKeyFromRel: %v", err)
			}
			if got != c.want {
				t.Fatalf("got %q, want %q", got, c.want)
			}
		})
	}
}
