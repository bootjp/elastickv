package raftadmission

import (
	"testing"

	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/stretchr/testify/require"
)

func TestRecoveryPublicationCleanupRunsOutsideAdmission(t *testing.T) {
	a := newAdmission(t, false)
	ticket, err := a.Ticket(1)
	require.NoError(t, err)
	cleaned := false
	err = a.PublishRecoveryWith(ticket, admissionPolicy(t, 1), RegistrationNotConfigured,
		func(stage peersession.Stage, generation uint64) (func(), error) {
			require.Equal(t, peersession.IdentityReady, stage)
			require.Equal(t, uint64(2), generation)
			return func() {
				_, err := a.Ticket(1)
				require.NoError(t, err, "physical cleanup may reenter A")
				cleaned = true
			}, nil
		})
	require.NoError(t, err)
	require.True(t, cleaned)
	require.ErrorIs(t, a.MarkCatalogLoaded(ticket), ErrStale)
}

func TestPublicationFailureClosesRatherThanRetainsAuthority(t *testing.T) {
	a := newAdmission(t, false)
	loadAdmissionCatalog(t, a)
	err := a.PublishStage(1, func(peersession.Stage, uint64) (func(), error) {
		return nil, ErrClosed
	})
	require.ErrorIs(t, err, ErrClosed)
	require.Equal(t, peersession.Reconciling, a.Stage(1))
}

func TestGenerationOverflowCannotRefreshAuthority(t *testing.T) {
	a := newAdmission(t, false)
	a.mu.Lock()
	a.groups[1].generation = ^uint64(0)
	a.mu.Unlock()
	a.Suspend(1)
	require.Equal(t, peersession.Reconciling, a.Stage(1))
	_, err := a.Ticket(1)
	require.ErrorIs(t, err, ErrClosed)
}

func TestSessionGenerationOverflowCannotPublish(t *testing.T) {
	a := newAdmission(t, false)
	a.mu.Lock()
	a.groups[1].sessionGeneration = ^uint64(0)
	a.mu.Unlock()
	ticket, err := a.Ticket(1)
	require.NoError(t, err)
	err = a.PublishRecoveryWith(ticket, admissionPolicy(t, 1), RegistrationNotConfigured,
		func(peersession.Stage, uint64) (func(), error) {
			t.Fatal("wrapped session generation must not be published")
			return nil, nil
		})
	require.ErrorIs(t, err, ErrClosed)
	require.Equal(t, peersession.Reconciling, a.Stage(1))
}

func TestStageGenerationOverflowCancelsExistingOwners(t *testing.T) {
	a := newAdmission(t, false)
	loadAdmissionCatalog(t, a)
	ctx, op, err := a.Begin(t.Context(), Data)
	require.NoError(t, err)
	defer op.CancelAndJoin()
	a.mu.Lock()
	a.groups[1].sessionGeneration = ^uint64(0)
	a.mu.Unlock()
	err = a.PublishStage(1, func(peersession.Stage, uint64) (func(), error) {
		t.Fatal("wrapped session generation must not be published")
		return nil, nil
	})
	require.ErrorIs(t, err, ErrClosed)
	select {
	case <-ctx.Done():
	default:
		t.Fatal("fatal publication overflow left an owned operation running")
	}
	require.ErrorIs(t, a.ValidateResult(ctx), ErrStale)
}
