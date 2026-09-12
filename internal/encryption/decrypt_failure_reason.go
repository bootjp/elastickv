package encryption

import "github.com/cockroachdb/errors"

// The design doc's §9.2 `reason` label values for
// elastickv_encryption_decrypt_failures_total. The set is closed: the
// counter is a paging-grade signal, so an unclassifiable error must
// still be counted (under DecryptFailureReasonUnknown) rather than
// dropped, and must never mint a fresh label value at runtime — an
// attacker-influenced label would be an unbounded-cardinality vector
// against the metrics endpoint.
//
// These constants live beside the sentinels they classify so a new
// decrypt-path error and its reason label are edited in one place.
const (
	// DecryptFailureReasonTagMismatch is a GCM authentication
	// failure: ciphertext, AAD, or the bound header fields were
	// altered, or the wrong DEK is loaded. Never benign.
	DecryptFailureReasonTagMismatch = "tag_mismatch"

	// DecryptFailureReasonUnknownKeyID is an envelope naming a
	// key_id absent from the keystore — typically a DEK retired
	// before every value under it was rewritten (§5.4).
	DecryptFailureReasonUnknownKeyID = "unknown_key_id"

	// DecryptFailureReasonTruncated is an envelope shorter than
	// header+tag: storage truncation or a partial write.
	DecryptFailureReasonTruncated = "truncated"

	// DecryptFailureReasonBadVersion is an unknown envelope version
	// or an undefined flag bit for the version — a downgrade to an
	// older binary, or on-disk corruption of the header byte.
	DecryptFailureReasonBadVersion = "bad_version"

	// DecryptFailureReasonUnknown is the catch-all for a decrypt
	// failure that matches no sentinel above. It exists so the
	// counter never silently drops a failure; a non-zero rate here
	// means the decrypt path grew an error class this mapping has
	// not been taught yet.
	DecryptFailureReasonUnknown = "unknown"
)

// DecryptFailureReason maps a storage/raft decrypt-path error to its
// §9.2 `reason` label. It reports ok=false for a nil error so callers
// cannot accidentally count a success.
//
// Order matters where an error could carry two marks: ErrBadNonceSize
// and ErrReservedKeyID are reported as bad_version rather than minting
// labels outside the doc's set, because both mean the envelope header
// decoded into a shape this build rejects.
func DecryptFailureReason(err error) (string, bool) {
	switch {
	case err == nil:
		return "", false
	case errors.Is(err, ErrIntegrity):
		return DecryptFailureReasonTagMismatch, true
	case errors.Is(err, ErrUnknownKeyID):
		return DecryptFailureReasonUnknownKeyID, true
	case errors.Is(err, ErrEnvelopeShort):
		return DecryptFailureReasonTruncated, true
	case errors.Is(err, ErrEnvelopeVersion),
		errors.Is(err, ErrEnvelopeFlag),
		errors.Is(err, ErrBadNonceSize),
		errors.Is(err, ErrReservedKeyID):
		return DecryptFailureReasonBadVersion, true
	default:
		return DecryptFailureReasonUnknown, true
	}
}
