package raftadmission

import (
	"context"
	"crypto/sha256"
	"math"
	"testing"

	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/stretchr/testify/require"
)

func observeRotationRole(t *testing.T, a *Arbiter, term uint64, role raftengine.State) {
	t.Helper()
	require.NoError(t, a.WithNative(t.Context(), 1, func(scope raftengine.NativeScope) error {
		scope.Observe(raftengine.NativeState{Term: term, Role: role, LeaderID: 1, ConfigurationIndex: 1})
		return nil
	}))
}

func TestRotationFinishPreservesFirstOutcome(t *testing.T) {
	t.Parallel()
	a := newAdmission(t, true)
	publishRotationProjection(t, a, sha256.Sum256([]byte("keys")))
	run, err := a.BeginRotation(t.Context(), sha256.Sum256([]byte("recipe")))
	require.NoError(t, err)
	require.NoError(t, run.Finish(nil))
	require.NoError(t, run.Finish(ErrClosed), "later cleanup cannot rewrite the joined result")
}

func TestRotationGenerationOverflowLatchesClosed(t *testing.T) {
	t.Parallel()
	for _, counter := range []string{"sequence", "permission", "projection"} {
		t.Run(counter, func(t *testing.T) {
			a := newAdmission(t, true)
			publishRotationProjection(t, a, sha256.Sum256([]byte("keys")))
			a.mu.Lock()
			switch counter {
			case "sequence":
				a.rotation.sequence = math.MaxUint64
			case "permission":
				a.permissionGeneration = math.MaxUint64
			case "projection":
				a.encryption.generation = math.MaxUint64
			}
			a.mu.Unlock()
			_, err := a.BeginRotation(t.Context(), sha256.Sum256([]byte("recipe")))
			require.Error(t, err)
			_, err = a.RotationStatus()
			require.Error(t, err)
			require.False(t, a.LocalPermissionReady())
		})
	}
}

func publishRotationProjection(t *testing.T, a *Arbiter, digest raftbootstrap.Digest) EncryptionTicket {
	t.Helper()
	ticket, err := a.EncryptionTicket()
	require.NoError(t, err)
	require.NoError(t, a.PublishEncryption(ticket, digest))
	return ticket
}

func completeRotationFixture(t *testing.T, a *Arbiter, recipe, projection raftbootstrap.Digest) {
	t.Helper()
	run, err := a.BeginRotation(t.Context(), recipe)
	require.NoError(t, err)
	t.Cleanup(func() { _ = run.Finish(ErrClosed) })
	require.NoError(t, run.Finish(nil))
	ticket, err := a.EncryptionTicket()
	require.NoError(t, err)
	require.NoError(t, a.CompleteRotation(run, ticket, projection))
}

func TestRotationLeaderCompletionHasOwnedGeneration(t *testing.T) {
	t.Parallel()
	a := newAdmission(t, true)
	digest := sha256.Sum256([]byte("verified local key projection"))
	publishRotationProjection(t, a, digest)
	_, _, err := a.Begin(t.Context(), Rotation)
	require.Error(t, err, "generic callers cannot mint rotation authority")
	loadAdmissionCatalog(t, a)
	require.Equal(t, peersession.IdentityReady, a.Stage(1))
	completeRotationFixture(t, a, sha256.Sum256([]byte("recipe")), digest)
	require.Equal(t, peersession.DataReady, a.Stage(1))
	ctx, op, err := a.Begin(t.Context(), Data)
	require.NoError(t, err)
	defer op.CancelAndJoin()
	bound, err := a.BindProposal(ctx, 2, []byte{byte(Data)})
	require.NoError(t, err)
	require.NoError(t, a.WithNative(t.Context(), 1, func(scope raftengine.NativeScope) error {
		scope.Observe(raftengine.NativeState{Term: 2, Role: raftengine.StateFollower, LeaderID: 9, ConfigurationIndex: 1})
		other := nativeScope{owner: a, group: 2}
		require.Error(t, other.Check(bound, raftengine.NativeRequest{Operation: raftengine.NativeProposal, PayloadDigest: sha256.Sum256([]byte{byte(Data)})}), "cross-group enqueue must close before callbacks")
		return nil
	}))
	require.Error(t, a.ValidateResult(ctx))
}

func TestRotationFollowerDoesNotFabricateLeaderCompletion(t *testing.T) {
	t.Parallel()
	a := newAdmission(t, true)
	projection := sha256.Sum256([]byte("registered local wrappers"))
	publishRotationProjection(t, a, projection)
	recipe := sha256.Sum256([]byte("exact immutable recipe"))
	run, err := a.BeginRotation(t.Context(), recipe)
	require.NoError(t, err)
	defer func() { _ = run.Finish(ErrClosed) }()
	observeRotationRole(t, a, 2, raftengine.StateFollower)
	require.Error(t, run.Finish(nil))
	publishAdmission(t, a, 1, 2)
	ticket := publishRotationProjection(t, a, projection)
	require.Error(t, a.CompleteRotation(run, ticket, projection))
	require.Error(t, a.PublishFollowerPermission(ticket, projection), "retirement alone must not resolve responsibility")
	require.Error(t, a.ResolveRotationResponsibility(ticket, sha256.Sum256([]byte("wrong recipe")), projection))
	require.NoError(t, a.ResolveRotationResponsibility(ticket, recipe, projection))
	require.Equal(t, peersession.IdentityReady, a.Stage(1), "reconciliation alone is not F")
	require.NoError(t, a.PublishFollowerPermission(ticket, projection))
	loadAdmissionCatalog(t, a)
	require.Equal(t, peersession.DataReady, a.Stage(1))
	observeRotationRole(t, a, 3, raftengine.StateLeader)
	require.NotEqual(t, peersession.DataReady, a.Stage(1), "F cannot become leader completion")
	require.Error(t, a.PublishFollowerPermission(ticket, projection))
}

func TestRotationProjectionChangeRetainsStaleHandles(t *testing.T) {
	t.Parallel()
	a := newAdmission(t, true)
	projection := sha256.Sum256([]byte("old keys"))
	publishRotationProjection(t, a, projection)
	completeRotationFixture(t, a, sha256.Sum256([]byte("recipe")), projection)
	loadAdmissionCatalog(t, a)
	ctx, op, err := a.Begin(t.Context(), Allocator)
	require.NoError(t, err)
	defer op.CancelAndJoin()
	old, err := a.EncryptionTicket()
	require.NoError(t, err)
	a.InvalidateEncryption()
	require.Error(t, a.PublishEncryption(old, projection))
	require.Error(t, a.ValidateResult(ctx))
	require.Equal(t, peersession.IdentityReady, a.Stage(1))
	op.CancelAndJoin()
	newProjection := sha256.Sum256([]byte("new keys"))
	publishRotationProjection(t, a, newProjection)
	require.Equal(t, peersession.IdentityReady, a.Stage(1), "old completion cannot authorize changed keys")
	completeRotationFixture(t, a, sha256.Sum256([]byte("reconciled recipe")), newProjection)
	require.Equal(t, peersession.DataReady, a.Stage(1))
	require.Error(t, a.ValidateResult(ctx), "reopening never refreshes a stale allocator")
}

func TestRotationCompletionRejectsCancelledParentAndFatalLatch(t *testing.T) {
	t.Parallel()
	a := newAdmission(t, true)
	projection := sha256.Sum256([]byte("keys"))
	publishRotationProjection(t, a, projection)
	parent, cancel := context.WithCancel(t.Context())
	run, err := a.BeginRotation(parent, sha256.Sum256([]byte("recipe")))
	require.NoError(t, err)
	defer func() { _ = run.Finish(ErrClosed) }()
	require.NoError(t, run.Finish(nil))
	ticket, err := a.EncryptionTicket()
	require.NoError(t, err)
	cancel()
	require.Error(t, a.CompleteRotation(run, ticket, projection))
	a.LatchFatal(ErrClosed)
	require.Error(t, a.PublishEncryption(ticket, projection))
	require.Error(t, a.ResolveRotationResponsibility(ticket, run.recipe, projection))
	require.Error(t, a.PublishFollowerPermission(ticket, projection))
	require.Equal(t, peersession.Reconciling, a.Stage(1))
}

func TestEncryptionMutationCannotRepublishCoherentOldImage(t *testing.T) {
	t.Parallel()
	a := newAdmission(t, true)
	projection := sha256.Sum256([]byte("old coherent image"))
	publishRotationProjection(t, a, projection)
	completeRotationFixture(t, a, sha256.Sum256([]byte("recipe")), projection)
	loadAdmissionCatalog(t, a)
	old, err := a.EncryptionTicket()
	require.NoError(t, err)
	end := a.BeginEncryptionMutation()
	require.Error(t, a.PublishEncryption(old, projection))
	_, err = a.EncryptionTicket()
	require.Error(t, err, "a still-coherent old image during mutation is not settled")
	_, err = a.EncryptionFence()
	require.Error(t, err)
	require.False(t, a.LocalPermissionReady())
	end()
	end()
	ticket, err := a.EncryptionTicket()
	require.NoError(t, err)
	require.NoError(t, a.PublishEncryption(ticket, projection))
	require.False(t, a.LocalPermissionReady(), "settling cannot reuse the old leader completion")
}
