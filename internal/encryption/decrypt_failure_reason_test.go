package encryption_test

import (
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestDecryptFailureReasonClassifiesEveryDecryptPathSentinel pins the
// §9.2 reason label for each sentinel the decrypt path can produce.
// The errors arrive wrapped, exactly as the store sees them, because
// the classifier must traverse cockroachdb wrapping to be useful.
func TestDecryptFailureReasonClassifiesEveryDecryptPathSentinel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "gcm tag mismatch",
			err:  errors.Wrap(errors.WithSecondaryError(encryption.ErrIntegrity, errors.New("open")), "store: decrypt value"),
			want: encryption.DecryptFailureReasonTagMismatch,
		},
		{
			name: "retired or absent dek",
			err:  errors.Wrapf(encryption.ErrUnknownKeyID, "key_id=%d", 7),
			want: encryption.DecryptFailureReasonUnknownKeyID,
		},
		{
			name: "envelope shorter than header+tag",
			err:  errors.Wrap(encryption.ErrEnvelopeShort, "store: decode envelope"),
			want: encryption.DecryptFailureReasonTruncated,
		},
		{
			name: "unknown envelope version",
			err:  errors.Wrap(encryption.ErrEnvelopeVersion, "store: decode envelope"),
			want: encryption.DecryptFailureReasonBadVersion,
		},
		{
			name: "undefined flag bit for version",
			err:  errors.Wrap(encryption.ErrEnvelopeFlag, "store: decode envelope"),
			want: encryption.DecryptFailureReasonBadVersion,
		},
		{
			name: "nonce field wrong width",
			err:  errors.Wrap(encryption.ErrBadNonceSize, "store: decrypt value"),
			want: encryption.DecryptFailureReasonBadVersion,
		},
		{
			name: "reserved key id sentinel",
			err:  errors.Wrap(encryption.ErrReservedKeyID, "store: decrypt value"),
			want: encryption.DecryptFailureReasonBadVersion,
		},
		{
			name: "unclassified error still counted",
			err:  errors.New("something the mapping has not been taught"),
			want: encryption.DecryptFailureReasonUnknown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, ok := encryption.DecryptFailureReason(tt.err)
			require.True(t, ok, "a non-nil decrypt error must always be counted")
			require.Equal(t, tt.want, got)
		})
	}
}

// TestDecryptFailureReasonRejectsNil guards the one case that must not
// produce a label: counting a success would make the paging-grade
// counter fire on healthy traffic.
func TestDecryptFailureReasonRejectsNil(t *testing.T) {
	t.Parallel()

	reason, ok := encryption.DecryptFailureReason(nil)
	require.False(t, ok)
	require.Empty(t, reason)
}
