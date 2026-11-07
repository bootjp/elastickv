package raftadmission

import (
	"context"
	"crypto/sha256"
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/stretchr/testify/require"
)

func readyExternalRotation(t *testing.T) *Arbiter {
	t.Helper()
	a := newAdmission(t, true)
	projection := sha256.Sum256([]byte("keys"))
	publishRotationProjection(t, a, projection)
	loadAdmissionCatalog(t, a)
	completeRotationFixture(t, a, sha256.Sum256([]byte("startup")), projection)
	require.True(t, a.LocalPermissionReady())
	return a
}

func TestExternalRotationTransfersOriginalOwner(t *testing.T) {
	t.Parallel()
	a := readyExternalRotation(t)
	request, err := a.CaptureRotationRequest(t.Context())
	require.NoError(t, err)
	run, err := a.BeginRotationFromRequest(request, sha256.Sum256([]byte("external")))
	require.NoError(t, err)
	defer func() { _ = run.Finish(ErrClosed) }()
	require.Error(t, request.Context().Err())
	require.NoError(t, run.Context().Err(), "transfer retains caller lifetime, not cancelled planning lifetime")
	require.False(t, a.LocalPermissionReady())
	require.Error(t, a.ValidateResult(request.Context()))
	_, err = a.BeginRotationFromRequest(request, sha256.Sum256([]byte("retry")))
	require.Error(t, err, "transferred request cannot be refreshed")
}

func TestExternalRotationRejectsInvalidatedRequest(t *testing.T) {
	t.Parallel()
	for _, cause := range []string{"term", "encryption", "caller"} {
		t.Run(cause, func(t *testing.T) {
			a := readyExternalRotation(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			request, err := a.CaptureRotationRequest(ctx)
			require.NoError(t, err)
			defer request.CancelAndJoin()
			switch cause {
			case "term":
				observeRotationRole(t, a, 2, raftengine.StateLeader)
			case "encryption":
				a.InvalidateEncryption()
			case "caller":
				cancel()
			}
			_, err = a.BeginRotationFromRequest(request, sha256.Sum256([]byte("external")))
			require.Error(t, err)
			state, err := a.RotationStatus()
			require.NoError(t, err)
			require.False(t, state.Running)
		})
	}
}

func TestExternalRotationRequiresLoadedCatalog(t *testing.T) {
	t.Parallel()
	a := readyExternalRotation(t)
	a.mu.Lock()
	a.groups[a.config.DefaultGroup].loaded = false
	a.mu.Unlock()
	_, err := a.CaptureRotationRequest(t.Context())
	require.Error(t, err, "external admin requests cannot use identity/control-only readiness")
}
