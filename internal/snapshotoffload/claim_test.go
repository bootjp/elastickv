package snapshotoffload

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestObjectClaimsAreExclusiveAndReusable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		store func(*testing.T) ObjectClaimStore
	}{
		{
			name: "local",
			store: func(t *testing.T) ObjectClaimStore {
				return newTestLocalStore(t, filepath.Join(t.TempDir(), "objects"))
			},
		},
		{
			name: "s3",
			store: func(t *testing.T) ObjectClaimStore {
				return newTestS3Store(t, newFakeS3Client())
			},
		},
	}

	for _, tc := range tests {

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			store := tc.store(t)
			const key = "cluster-a/v1/payloads/sha256/aa/target.fsm"

			first, err := store.AcquireObjectClaim(ctx, key)
			require.NoError(t, err)

			_, err = store.AcquireObjectClaim(ctx, key)
			require.ErrorIs(t, err, ErrObjectClaimed)

			require.NoError(t, first.Release(ctx))
			second, err := store.AcquireObjectClaim(ctx, key)
			require.NoError(t, err)
			require.NoError(t, second.Release(ctx))
		})
	}
}
