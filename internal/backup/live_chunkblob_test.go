package backup

import (
	"testing"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/stretchr/testify/require"
)

// A content-addressed chunk blob has no bucket in its key, so it cannot be
// classified into a backup scope. Returning "not scoped" for it would drop it
// from the stream while its manifest and chunk references still go out,
// producing a dump the decoder cannot finalize because S3Encoder resolves each
// reference only from payloads delivered to HandleChunkBlob. The live path
// fails closed instead.
func TestScopeForKeyFailsClosedOnOffloadedChunkBlob(t *testing.T) {
	t.Parallel()

	var digest [32]byte
	for i := range digest {
		digest[i] = byte(i)
	}
	scope, scoped, err := ScopeForKey(s3keys.ChunkBlobKey(digest))
	require.ErrorIs(t, err, ErrScopeOffloadedChunkBlob)
	require.False(t, scoped)
	require.Equal(t, Scope{}, scope)
}

// The chunk reference is bucket-scoped and stays streamable: only the blob
// payload is unrepresentable, so the guard must not swallow its sibling.
func TestScopeForKeyStillScopesChunkRef(t *testing.T) {
	t.Parallel()

	scope, scoped, err := ScopeForKey(s3keys.ChunkRefKey("bucket", 0, "object", "upload", 0, 0))
	require.NoError(t, err)
	require.True(t, scoped)
	require.Equal(t, "s3", scope.Adapter)
	require.Equal(t, "bucket", scope.Name)
}
