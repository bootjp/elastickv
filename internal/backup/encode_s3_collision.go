package backup

import (
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/errors"
)

// encode_s3_collision.go is the M4-2b helper that inverts the S3
// decoder's collision-rename writes. The decoder writes a per-bucket
// KEYMAP.jsonl with one record per renamed object (KindS3LeafData,
// KindMetaCollision, KindSHAFallback); this file is the read-side that
// turns that file back into a map[encoded-rel-path]KeymapRecord the
// object walk consults to recover original S3 keys.
//
// Design: docs/design/2026_06_03_proposed_s3_collision_reversal.md.
//
// File-system safety: loadBucketKeymap re-runs the read-side
// Lstat → refuseHardLink → Open sequence rather than reusing
// OpenSidecarFile. OpenSidecarFile is write-side
// (O_WRONLY|O_CREATE|O_TRUNC on Windows/fallback, O_WRONLY|O_CREATE
// + Truncate(0) on Unix) and would erase KEYMAP.jsonl before the
// reader could see it. Codex P2 v913 v3.
//
// Duplicate-key contract: this loader DIVERGES from LoadKeymap
// (keymap.go:245) which is last-wins. The S3 decoder's recordKeymap
// writes exactly one entry per renamed object, so a duplicate
// `Encoded` value in a tracker file means the dump is corrupt — the
// loader fails closed. Claude v913 v2 caught this contract mismatch.

// keymapReservedRoots is the closed set of first-path-component
// names disallowed as the head of a KEYMAP `Original` key. Only
// _orphans/ and _incomplete_uploads/ are dump-control reserved
// subtrees; legitimate user object keys like "_foo" or "_foo/bar"
// remain valid. Codex P2 v913 v1.
var keymapReservedRoots = map[string]struct{}{
	"_orphans":            {},
	"_incomplete_uploads": {},
}

// keymapAllowedKinds is the closed set of Kind values M4-2b honors.
// A Kind outside this set fails closed so a hand-edited dump that
// injects a novel discriminator cannot silently bypass invariants
// (forward-compat guard, claude v913 v2).
var keymapAllowedKinds = map[string]struct{}{
	KindS3LeafData:    {},
	KindMetaCollision: {},
	KindSHAFallback:   {},
}

// loadBucketKeymap reads <bucketDir>/KEYMAP.jsonl into a
// map[Encoded]KeymapRecord. Precondition: the caller has already
// verified via isKeymapCollisionTracker that the file exists, is a
// regular file, and has no companion .elastickv-meta.json sidecar
// (sidecar-paired KEYMAP.jsonl is a legitimate user object handled
// by the normal object walk; codex P2 v913 v2).
//
// All four invariants in the design doc's "Error contract" section
// fire from here — duplicate Encoded, malformed JSON, suffix-
// mismatch per Kind, reserved-root, and unknown Kind. Orphan-record
// detection (Encoded with no on-disk body) happens in
// verifyKeymapTargetsExist after the load, because it requires the
// full keymap to be parsed first.
func (e *S3RecordEncoder) loadBucketKeymap(root *os.Root, bucketDir string) (map[string]KeymapRecord, error) {
	keymapRel := filepath.Join(bucketDir, "KEYMAP.jsonl")
	// Read-side Lstat + refuseHardLink + Open. NOT OpenSidecarFile
	// (write-side; truncates before read). Mirrors openRootRegular
	// (encode_s3_objects.go:260) but tolerates os.ErrNotExist as a
	// no-op return (defensive — caller's precondition already
	// covered the Lstat once, but interleaved with our re-Lstat the
	// file could vanish; treat that as "no records" rather than
	// failing closed).
	linfo, err := root.Lstat(keymapRel)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if !linfo.Mode().IsRegular() {
		return nil, errors.Wrapf(ErrS3EncodeNotRegular, "%s (mode=%s)", keymapRel, linfo.Mode())
	}
	if err := refuseHardLink(linfo, keymapRel); err != nil {
		return nil, err
	}
	f, err := root.Open(keymapRel)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer func() { _ = f.Close() }()

	return readKeymapStrict(f, keymapRel)
}

// readKeymapStrict iterates a KEYMAP.jsonl stream and assembles the
// map, failing closed on duplicate Encoded values (vs LoadKeymap's
// last-wins) and on every per-record invariant. Split out of
// loadBucketKeymap so the parent stays under the cyclop limit.
func readKeymapStrict(f io.Reader, keymapRel string) (map[string]KeymapRecord, error) {
	out := make(map[string]KeymapRecord)
	rd := NewKeymapReader(f)
	for {
		rec, ok, err := rd.Next()
		if err != nil {
			return nil, err
		}
		if !ok {
			return out, nil
		}
		if _, dup := out[rec.Encoded]; dup {
			return nil, errors.Wrapf(ErrInvalidKeymapRecord,
				"%s: duplicate encoded segment %q (S3 decoder writes one record per rename; a duplicate means the dump is corrupt)",
				keymapRel, rec.Encoded)
		}
		if err := validateKeymapRecord(rec); err != nil {
			return nil, errors.Wrapf(err, "%s", keymapRel)
		}
		out[rec.Encoded] = rec
	}
}

// validateKeymapRecord enforces the per-record invariants of the
// design doc's "Error contract" section: Kind must be in the closed
// set; KindS3LeafData entries must end in S3LeafDataSuffix;
// KindMetaCollision entries' original key must end in
// S3MetaSuffixReserved; no entry's original key may target a
// reserved top-level dump subtree.
func validateKeymapRecord(rec KeymapRecord) error {
	if _, ok := keymapAllowedKinds[rec.Kind]; !ok {
		return errors.Wrapf(ErrInvalidKeymapRecord,
			"unknown kind %q for encoded segment %q", rec.Kind, rec.Encoded)
	}
	if err := validateKeymapEncodedPath(rec.Encoded); err != nil {
		return err
	}
	originalBytes, err := rec.Original()
	if err != nil {
		return err
	}
	original := string(originalBytes)
	// S3 object keys cannot be empty; reject an empty decoded
	// original here so the operator sees the keymap problem at load
	// time rather than at object-body assembly. Gemini medium PR #928.
	if original == "" {
		return errors.Wrapf(ErrInvalidKeymapRecord,
			"empty original key for encoded segment %q (kind=%q)", rec.Encoded, rec.Kind)
	}
	if err := validateKeymapReservedRoot(rec, original); err != nil {
		return err
	}
	return validateKeymapKindSuffix(rec, original)
}

// validateKeymapEncodedPath rejects rec.Encoded values that would
// escape the bucket sub-root or carry empty segments. The lookup
// site (resolveObjectKeyFromRel) joins this value under bucketDir
// when calling root.Lstat, and ".." would silently break out of the
// bucket. Gemini medium PR #928.
func validateKeymapEncodedPath(encoded string) error {
	for _, seg := range strings.Split(encoded, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return errors.Wrapf(ErrInvalidKeymapRecord,
				"encoded path %q contains invalid segment %q", encoded, seg)
		}
	}
	return nil
}

// validateKeymapKindSuffix enforces the Kind-specific suffix
// invariants: KindS3LeafData encoded values must end in
// S3LeafDataSuffix; KindMetaCollision original values must end in
// S3MetaSuffixReserved; KindSHAFallback has no extra invariant
// today (forward-compat).
func validateKeymapKindSuffix(rec KeymapRecord, original string) error {
	switch rec.Kind {
	case KindS3LeafData:
		if !strings.HasSuffix(rec.Encoded, S3LeafDataSuffix) {
			return errors.Wrapf(ErrInvalidKeymapRecord,
				"KindS3LeafData encoded %q missing %q suffix", rec.Encoded, S3LeafDataSuffix)
		}
	case KindMetaCollision:
		if !strings.HasSuffix(original, S3MetaSuffixReserved) {
			return errors.Wrapf(ErrInvalidKeymapRecord,
				"KindMetaCollision original %q missing %q suffix", original, S3MetaSuffixReserved)
		}
	case KindSHAFallback:
		// Design §"KindSHAFallback policy for S3": currently
		// reserved for the (not-yet-implemented) case where a
		// single S3 key segment exceeds EncodeSegment's 240-byte
		// filesystem ceiling. The decoder does not emit it for S3
		// today, but if a future decoder does, the encoder's
		// full-rel-path lookup handles it transparently. No
		// additional invariant beyond the kind+reserved-root
		// checks above.
	}
	return nil
}

// validateKeymapReservedRoot rejects an original key whose first
// slash-split path component is exactly one of the dump-control
// reserved subtrees (_orphans, _incomplete_uploads). Codex P2 v913 v1:
// the whole `_` namespace is NOT reserved, only those two named
// subtrees. Legit user keys like "_foo" or "_foo/bar" or
// "nested/_orphans/x" pass.
func validateKeymapReservedRoot(rec KeymapRecord, original string) error {
	if original == "" {
		return nil
	}
	firstSegment, _, _ := strings.Cut(original, "/")
	if _, reserved := keymapReservedRoots[firstSegment]; reserved {
		return errors.Wrapf(ErrInvalidKeymapRecord,
			"original key %q targets reserved dump subtree %q (encoded=%q, kind=%q)",
			original, firstSegment, rec.Encoded, rec.Kind)
	}
	return nil
}

// verifyKeymapTargetsExist scans the loaded keymap and confirms each
// record's Encoded value points to an actual on-disk file under
// bucketDir. Orphan records (Encoded with no body) surface here
// rather than as a missing-sidecar error from the walker, so the
// operator sees the keymap inconsistency directly. O(N) Lstats per
// bucket, run once at load time.
func (e *S3RecordEncoder) verifyKeymapTargetsExist(root *os.Root, bucketDir string, km map[string]KeymapRecord) error {
	for encoded := range km {
		// Encoded is a bucket-relative path in slash-form (that's
		// what the decoder's recordKeymap stored; see s3.go:728
		// — the call site passes resolveObjectFilename's output,
		// which is the slash-form S3 key + optional rename
		// suffix). Convert to filepath form before Lstat.
		rel := filepath.Join(bucketDir, filepath.FromSlash(encoded))
		_, err := root.Lstat(rel)
		if errors.Is(err, os.ErrNotExist) {
			return errors.Wrapf(ErrInvalidKeymapRecord,
				"orphan keymap record: encoded %q has no on-disk file under %s", encoded, bucketDir)
		}
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

// resolveObjectKeyFromRel maps an on-disk bucket-relative path to
// the original S3 object key. The keymap is the loaded
// per-bucket map (nil for buckets with no KEYMAP.jsonl); a hit
// returns the decoded original key bytes as a string, a miss
// returns the slash-form of objRel itself (the no-collision case
// M4-2a already covered).
//
// The lookup uses the slash-form of objRel because the decoder's
// recordKeymap call site (s3.go:728) stored that exact form as
// the Encoded field (the encodedFilename argument is the
// resolveObjectFilename return, which is plain slash-form S3
// path + optional suffix).
func resolveObjectKeyFromRel(keymap map[string]KeymapRecord, objRel string) (string, error) {
	slashRel := filepath.ToSlash(objRel)
	if keymap == nil {
		return slashRel, nil
	}
	rec, ok := keymap[slashRel]
	if !ok {
		return slashRel, nil
	}
	originalBytes, err := rec.Original()
	if err != nil {
		return "", err
	}
	return string(originalBytes), nil
}
