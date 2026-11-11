package raftadmission

import (
	"context"
	"crypto/sha256"
	"math"
	"sync"
	"testing"

	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/stretchr/testify/require"
)

func reviewRotationRun(t *testing.T) (*Arbiter, *RotationRun, raftbootstrap.Digest) {
	t.Helper()
	a := newAdmission(t, true)
	projection := sha256.Sum256([]byte("reviewed installed projection"))
	publishRotationProjection(t, a, projection)
	run, err := a.BeginRotation(t.Context(), sha256.Sum256([]byte("reviewed recipe")))
	require.NoError(t, err)
	t.Cleanup(func() { _ = run.Finish(ErrClosed) })
	return a, run, projection
}

func TestRotationReviewCompletionRejectsChangedUsedGroup(t *testing.T) {
	t.Parallel()
	a, run, projection := reviewRotationRun(t)
	_, err := a.BindProposal(run.Context(), 2, []byte{byte(Rotation)})
	require.NoError(t, err)
	require.NoError(t, run.Finish(nil))
	ticket, err := a.EncryptionTicket()
	require.NoError(t, err)
	observeAdmission(t, a, 2, 2)
	require.ErrorIs(t, a.CompleteRotation(run, ticket, projection), ErrStale)
	publishAdmission(t, a, 2, 2)
	fresh, err := a.EncryptionTicket()
	require.NoError(t, err)
	require.ErrorIs(t, a.CompleteRotation(run, fresh, projection), ErrStale,
		"recovery must not refresh the run's original used-group generation")
	loadAdmissionCatalog(t, a)
	require.NotEqual(t, peersession.DataReady, a.Stage(2))
}

func TestRotationReviewRejectsUnreconciledTarget(t *testing.T) {
	t.Parallel()
	a := newAdmission(t, true)
	publishRotationProjection(t, a, sha256.Sum256([]byte("keys")))
	observeAdmission(t, a, 2, 2)
	require.Equal(t, peersession.Reconciling, a.Stage(2))
	run, err := a.BeginRotation(t.Context(), sha256.Sum256([]byte("recipe")))
	require.NoError(t, err)
	t.Cleanup(func() { _ = run.Finish(ErrClosed) })
	_, err = a.BindProposal(run.Context(), 2, []byte{byte(Rotation)})
	require.ErrorIs(t, err, ErrStale)
	// Even a matching seal cannot replace target recovery at the terminal gate.
	sealed := context.WithValue(run.Context(), payloadKey{}, boundPayload{
		operation: run.op, group: 2, digest: sha256.Sum256([]byte{byte(Rotation)}),
	})
	err = a.WithNative(sealed, 2, func(scope raftengine.NativeScope) error {
		return scope.Check(sealed, raftengine.NativeRequest{Operation: raftengine.NativeProposal,
			PayloadDigest: sha256.Sum256([]byte{byte(Rotation)})})
	})
	require.ErrorIs(t, err, ErrStale)
	publishAdmission(t, a, 2, 2)
	_, err = a.BindProposal(run.Context(), 2, []byte{byte(Rotation)})
	require.ErrorIs(t, err, ErrStale, "target recovery cannot renew the original run")
}

func TestRotationReviewFinishPreservesFirstResult(t *testing.T) {
	t.Parallel()
	for _, failed := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "failure"}[failed], func(t *testing.T) {
			a, run, projection := reviewRotationRun(t)
			var original error
			if failed {
				original = ErrClosed
			}
			require.ErrorIs(t, run.Finish(original), original)
			results := make(chan error, 16)
			var workers sync.WaitGroup
			for range 16 {
				workers.Go(func() { results <- run.Finish(nil) })
			}
			workers.Wait()
			close(results)
			for result := range results {
				require.ErrorIs(t, result, original)
			}
			require.ErrorIs(t, run.Finish(ErrStale), original)
			ticket, err := a.EncryptionTicket()
			require.NoError(t, err)
			err = a.CompleteRotation(run, ticket, projection)
			if failed {
				require.ErrorIs(t, err, ErrStale)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRotationReviewOverflowFailsClosed(t *testing.T) {
	t.Parallel()
	for _, field := range []string{"sequence", "permission", "projection"} {
		t.Run(field, func(t *testing.T) {
			a := newAdmission(t, true)
			publishRotationProjection(t, a, sha256.Sum256([]byte("keys")))
			a.mu.Lock()
			switch field {
			case "sequence":
				a.rotation.sequence = math.MaxUint64
			case "permission":
				a.permissionGeneration = math.MaxUint64
			case "projection":
				a.encryption.generation = math.MaxUint64
			}
			a.mu.Unlock()
			run, err := a.BeginRotation(t.Context(), sha256.Sum256([]byte("recipe")))
			require.ErrorIs(t, err, ErrClosed)
			require.Nil(t, run)
			a.mu.Lock()
			fatal, active, operations := a.fatal, a.rotation.active, len(a.operations)
			a.mu.Unlock()
			require.ErrorIs(t, fatal, ErrClosed)
			require.Nil(t, active)
			require.Zero(t, operations)
			_, err = a.EncryptionTicket()
			require.Error(t, err)
			require.Equal(t, peersession.Reconciling, a.Stage(1))
		})
	}
}
