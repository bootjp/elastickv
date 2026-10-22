package snapshotoffload

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

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

type alwaysClaimedStore struct {
	attempts atomic.Int32
}

func (s *alwaysClaimedStore) AcquireObjectClaim(context.Context, string) (ObjectClaim, error) {
	s.attempts.Add(1)
	return nil, ErrObjectClaimed
}

func TestAcquireObjectClaimWaitingIsBounded(t *testing.T) {
	t.Parallel()

	store := &alwaysClaimedStore{}
	started := time.Now()
	_, err := acquireObjectClaimWithin(context.Background(), store, "claimed", 40*time.Millisecond)
	require.ErrorIs(t, err, ErrObjectClaimed)
	require.Less(t, time.Since(started), 500*time.Millisecond)
	require.GreaterOrEqual(t, store.attempts.Load(), int32(1))
}

type deadlineClaim struct {
	releases *atomic.Int32
}

func (c deadlineClaim) Release(ctx context.Context) error {
	c.releases.Add(1)
	<-ctx.Done()
	return ctx.Err()
}

type immediateClaim struct {
	releases *atomic.Int32
}

func (c immediateClaim) Release(context.Context) error {
	c.releases.Add(1)
	return nil
}

func TestRetentionClaimCleanupDoesNotSerializeBehindSlowRelease(t *testing.T) {
	t.Parallel()

	var fastReleases atomic.Int32
	var slowReleases atomic.Int32
	plan := retentionPlan{payloadClaims: []claimedPayload{
		{claim: deadlineClaim{releases: &slowReleases}},
		{claim: immediateClaim{releases: &fastReleases}},
		{claim: immediateClaim{releases: &fastReleases}},
	}}
	done := make(chan error, 1)
	go func() {
		done <- releaseRetentionClaimsWithin(context.Background(), plan, 200*time.Millisecond)
	}()
	require.Eventually(t, func() bool { return fastReleases.Load() == 2 }, 100*time.Millisecond, time.Millisecond)
	require.Error(t, <-done)
	require.Equal(t, int32(1), slowReleases.Load())
}

func TestRetentionClaimCleanupUsesOneDeadline(t *testing.T) {
	t.Parallel()

	var releases atomic.Int32
	plan := retentionPlan{
		manifestClaims: map[string]claimedManifest{
			"m1": {claim: deadlineClaim{releases: &releases}},
			"m2": {claim: deadlineClaim{releases: &releases}},
		},
		payloadClaims: []claimedPayload{
			{claim: deadlineClaim{releases: &releases}},
		},
	}
	started := time.Now()
	err := releaseRetentionClaimsWithin(context.Background(), plan, 40*time.Millisecond)
	require.Error(t, err)
	require.Less(t, time.Since(started), 500*time.Millisecond)
	require.Equal(t, int32(3), releases.Load())
}
