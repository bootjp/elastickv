// Package backup implements the per-adapter logical-backup format defined in
// docs/design/2026_04_29_proposed_snapshot_logical_decoder.md (Phase 0) and
// reused by docs/design/2026_04_29_proposed_logical_backup.md (Phase 1).
//
// This file owns the filename encoding rules for non-S3 segments. S3 object
// keys preserve their `/` separators (and so are not transformed by EncodeSegment);
// every other adapter scope encodes user-supplied bytes through this path.
//
// Encoding rules (see "Filename encoding" in the Phase 0 doc):
//
//   - Bytes in the unreserved set [A-Za-z0-9._-] pass through.
//   - Every other byte is rendered as %HH (uppercase hex), like
//     application/x-www-form-urlencoded but applied to every non-allowlisted byte.
//   - If the encoded result exceeds maxSegmentBytes (240), the segment is
//     replaced with <sha256-hex-prefix-32>__<truncated-original> and the full
//     original bytes must be recorded in KEYMAP.jsonl by the caller.
//   - Binary DynamoDB partition / sort keys take a separate "b64.<base64url>"
//     path so a binary key never collides with a string key whose hex encoding
//     happens to look like base64. EncodeBinarySegment emits that form.
package backup

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"strings"

	"github.com/cockroachdb/errors"
)

const (
	// maxSegmentBytes is the maximum length of a single encoded path segment
	// before the SHA-fallback kicks in. Chosen to leave headroom under the
	// common NAME_MAX of 255: two-character percent escapes can grow a 240-byte
	// raw segment to 720 encoded bytes in the worst case, but any segment
	// large enough to overflow NAME_MAX after expansion takes the SHA-fallback
	// path before the encoded length is examined.
	maxSegmentBytes = 240

	// shaFallbackHexPrefixBytes is the number of hex characters of SHA-256
	// embedded in the SHA-fallback prefix. 32 hex chars == 128 bits of
	// hash-prefix entropy — enough to make accidental collision negligible
	// for any single scope.
	shaFallbackHexPrefixBytes = 32

	// shaFallbackTruncatedSuffixBytes is the number of leading bytes of the
	// raw segment retained (after percent-encoding) in the SHA-fallback
	// rendering. Total encoded segment is then at most:
	//
	//   shaFallbackHexPrefixBytes + len("__") + 3*shaFallbackTruncatedSuffixBytes
	//
	// = 32 + 2 + 3*64 = 226 bytes  (under the 240 ceiling).
	//
	// The truncated suffix is purely a human-recognisability aid; it does
	// NOT carry enough information to reverse the original bytes — that is
	// what KEYMAP.jsonl is for.
	shaFallbackTruncatedSuffixBytes = 64

	// binaryPrefix marks a DynamoDB B-attribute segment encoded as base64url.
	binaryPrefix = "b64."

	// shaFallbackSeparator separates the SHA-256 prefix from the truncated
	// original bytes. Two underscores rather than one because single
	// underscores are common in user keys; doubled is much rarer and so
	// the boundary is unambiguous.
	shaFallbackSeparator = "__"
)

// ErrInvalidEncodedSegment is returned by DecodeSegment when its input is
// neither a valid percent-encoded segment, a binary-prefixed segment, nor a
// SHA-fallback segment.
var ErrInvalidEncodedSegment = errors.New("backup: invalid encoded filename segment")

// ErrShaFallbackNeedsKeymap is returned by DecodeSegment when its input is a
// SHA-fallback segment. The segment cannot be reversed to its original bytes
// from the filename alone — the caller must consult KEYMAP.jsonl.
var ErrShaFallbackNeedsKeymap = errors.New("backup: filename uses SHA fallback; consult KEYMAP.jsonl")

// EncodeSegment encodes a single user-supplied path segment for use as a
// filename component. It is the inverse of DecodeSegment for non-fallback
// inputs.
//
// The encoding is deterministic and idempotent given the same input.
func EncodeSegment(raw []byte) string {
	encoded := percentEncode(raw)
	if len(encoded) <= maxSegmentBytes {
		return encoded
	}
	return shaFallback(raw)
}

// EncodeBinarySegment encodes a DynamoDB B-attribute (binary) segment as
// "b64.<base64url-no-padding>" so that binary keys never collide with string
// keys whose hex-encoding happens to look like base64.
//
// b64-encoded segments take the SHA fallback if they exceed maxSegmentBytes
// after the base64 expansion (~4/3 of the raw length).
func EncodeBinarySegment(raw []byte) string {
	enc := binaryPrefix + base64.RawURLEncoding.EncodeToString(raw)
	if len(enc) <= maxSegmentBytes {
		return enc
	}
	return shaFallback(raw)
}

// DecodeSegment is the inverse of EncodeSegment for percent-encoded and
// binary-prefixed inputs. SHA-fallback inputs return ErrShaFallbackNeedsKeymap
// so the caller knows to consult KEYMAP.jsonl rather than treat the partial
// suffix as the original key.
func DecodeSegment(seg string) ([]byte, error) {
	if isShaFallback(seg) {
		return nil, errors.WithStack(ErrShaFallbackNeedsKeymap)
	}
	if strings.HasPrefix(seg, binaryPrefix) {
		raw, err := base64.RawURLEncoding.DecodeString(seg[len(binaryPrefix):])
		if err != nil {
			return nil, errors.Wrap(ErrInvalidEncodedSegment, err.Error())
		}
		return raw, nil
	}
	return percentDecode(seg)
}

// IsShaFallback reports whether seg uses the SHA-prefix-and-truncated-original
// form. Such segments cannot be reversed without KEYMAP.jsonl.
func IsShaFallback(seg string) bool {
	return isShaFallback(seg)
}

// IsBinarySegment reports whether seg is a base64-url encoded binary segment
// emitted by EncodeBinarySegment.
func IsBinarySegment(seg string) bool {
	return strings.HasPrefix(seg, binaryPrefix)
}

func percentEncode(raw []byte) string {
	// Worst case: every byte expands to %HH (3 bytes). Pre-allocate.
	var b strings.Builder
	b.Grow(len(raw) * 3) //nolint:mnd // 3 == len("%HH"), local idiom
	for _, c := range raw {
		if isUnreserved(c) {
			b.WriteByte(c)
			continue
		}
		b.WriteByte('%')
		b.WriteByte(hexUpper(c >> 4))   //nolint:mnd // 4 == nibble width
		b.WriteByte(hexUpper(c & 0x0F)) //nolint:mnd // 0x0F == low-nibble mask
	}
	return b.String()
}

func percentDecode(seg string) ([]byte, error) {
	out := make([]byte, 0, len(seg))
	for i := 0; i < len(seg); i++ {
		c := seg[i]
		if c != '%' {
			if !isUnreserved(c) {
				return nil, errors.Wrapf(ErrInvalidEncodedSegment,
					"unexpected raw byte 0x%02x at offset %d", c, i)
			}
			out = append(out, c)
			continue
		}
		if i+2 >= len(seg) { //nolint:mnd // 2 == hex digit count after %
			return nil, errors.Wrapf(ErrInvalidEncodedSegment,
				"truncated percent escape at offset %d", i)
		}
		const (
			hiNibbleOff = 1
			loNibbleOff = 2
		)
		hi, ok := unhex(seg[i+hiNibbleOff])
		if !ok {
			return nil, errors.Wrapf(ErrInvalidEncodedSegment,
				"non-hex digit 0x%02x at offset %d", seg[i+hiNibbleOff], i+hiNibbleOff)
		}
		lo, ok := unhex(seg[i+loNibbleOff])
		if !ok {
			return nil, errors.Wrapf(ErrInvalidEncodedSegment,
				"non-hex digit 0x%02x at offset %d", seg[i+loNibbleOff], i+loNibbleOff)
		}
		out = append(out, (hi<<4)|lo) //nolint:mnd // 4 == nibble width
		i += loNibbleOff              // skip the two consumed hex digits
	}
	return out, nil
}

func shaFallback(raw []byte) string {
	sum := sha256.Sum256(raw)
	hashHex := hex.EncodeToString(sum[:])[:shaFallbackHexPrefixBytes]
	suffix := raw
	if len(suffix) > shaFallbackTruncatedSuffixBytes {
		suffix = suffix[:shaFallbackTruncatedSuffixBytes]
	}
	return hashHex + shaFallbackSeparator + percentEncode(suffix)
}

func isShaFallback(seg string) bool {
	if len(seg) < shaFallbackHexPrefixBytes+len(shaFallbackSeparator) {
		return false
	}
	for i := 0; i < shaFallbackHexPrefixBytes; i++ {
		if _, ok := unhex(seg[i]); !ok {
			return false
		}
	}
	return seg[shaFallbackHexPrefixBytes:shaFallbackHexPrefixBytes+len(shaFallbackSeparator)] == shaFallbackSeparator
}

// isUnreserved is the RFC3986 unreserved set: ALPHA / DIGIT / "-" / "." / "_".
// "~" is excluded because it has caused interop problems with older shells and
// the additional safety is not worth the rare benefit.
func isUnreserved(c byte) bool {
	switch {
	case c >= 'A' && c <= 'Z':
		return true
	case c >= 'a' && c <= 'z':
		return true
	case c >= '0' && c <= '9':
		return true
	case c == '-', c == '.', c == '_':
		return true
	}
	return false
}

func hexUpper(nibble byte) byte {
	if nibble < 10 { //nolint:mnd // 10 == decimal/hex boundary
		return '0' + nibble
	}
	return 'A' + (nibble - 10) //nolint:mnd // 10 == decimal/hex boundary
}

func unhex(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true //nolint:mnd // 10 == decimal/hex boundary
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, true //nolint:mnd // 10 == decimal/hex boundary
	}
	return 0, false
}
