package server

import (
	"context"
	stderrors "errors"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/clusterbootstrap"
	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/bootjp/elastickv/internal/raftadmission"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestBootstrapCatalogDeferExactTypedClassification(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		err   error
		stale bool
	}{
		{"nil", nil, false},
		{"admission stale", raftadmission.ErrStale, true},
		{"wrapped admission stale", errors.Wrap(raftadmission.ErrStale, "catalog publication"), true},
		{"version stale", distribution.ErrEngineSnapshotVersionStale, true},
		{"wrapped version stale", errors.Wrap(distribution.ErrEngineSnapshotVersionStale, "catalog apply"), true},
		{"standard stale text is not authority", stderrors.New(raftadmission.ErrStale.Error()), false},
		{"standard version stale text is not authority", stderrors.New(distribution.ErrEngineSnapshotVersionStale.Error()), false},
		{"stale text is not authority", errors.New(raftadmission.ErrStale.Error()), false},
		{"version stale text is not authority", errors.New(distribution.ErrEngineSnapshotVersionStale.Error()), false},
		{"corrupt version", distribution.ErrCatalogInvalidVersionRecord, false},
		{"route overlap", distribution.ErrEngineSnapshotRouteOverlap, false},
		{"closed", raftadmission.ErrClosed, false},
		{"inconsistent policy", raftbootstrap.ErrInconsistent, false},
		{"cancelled observation", context.Canceled, false},
		{"expired observation", context.DeadlineExceeded, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.stale, clusterbootstrap.BootstrapCatalogObservationStale(tc.err))
			domain := bootstrapIngressFixture(t).Domain
			result := clusterbootstrap.DeferBootstrapCatalogObservation(t.Context(), domain, 1, tc.err)
			if tc.stale {
				require.NoError(t, result)
			} else {
				require.Equal(t, tc.err, result, "nonstale errors must propagate unchanged")
			}
			require.Equal(t, peersession.ControlReady, domain.Admission.Stage(1))
		})
	}
}

func TestBootstrapCatalogDeferHealthyStaleNeverPublishesLoaded(t *testing.T) {
	t.Parallel()
	for _, observation := range []error{raftadmission.ErrStale, distribution.ErrEngineSnapshotVersionStale} {
		t.Run(observation.Error(), func(t *testing.T) {
			domain := bootstrapIngressFixture(t).Domain
			before, err := domain.Admission.Ticket(1)
			require.NoError(t, err)
			require.NoError(t, clusterbootstrap.DeferBootstrapCatalogObservation(t.Context(), domain, 1, errors.Wrap(observation, "old catalog read")))
			after, err := domain.Admission.Ticket(1)
			require.NoError(t, err)
			require.Equal(t, before, after, "deferral neither refreshes nor invalidates native authority")
			require.Equal(t, peersession.ControlReady, domain.Admission.Stage(1))
			_, finish, err := domain.Admission.Enter(t.Context(), raftadmission.Data, 1)
			if finish != nil {
				t.Cleanup(func() { _ = finish(raftadmission.ErrClosed) })
			}
			require.ErrorIs(t, err, raftadmission.ErrClosed, "no catalog-loaded permission may be minted")
		})
	}
}

func TestBootstrapCatalogDeferRequiresLiveContextAndHealthyTicket(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		prepare func(*testing.T, *clusterbootstrap.BootstrapDomain) (context.Context, *clusterbootstrap.BootstrapDomain, uint64)
		cause   error
	}{
		{"cancelled context", func(t *testing.T, d *clusterbootstrap.BootstrapDomain) (context.Context, *clusterbootstrap.BootstrapDomain, uint64) {
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			return ctx, d, 1
		}, context.Canceled},
		{"expired context", func(t *testing.T, d *clusterbootstrap.BootstrapDomain) (context.Context, *clusterbootstrap.BootstrapDomain, uint64) {
			ctx, cancel := context.WithDeadline(t.Context(), time.Unix(0, 0))
			t.Cleanup(cancel)
			return ctx, d, 1
		}, context.DeadlineExceeded},
		{"latched fatal", func(t *testing.T, d *clusterbootstrap.BootstrapDomain) (context.Context, *clusterbootstrap.BootstrapDomain, uint64) {
			d.Admission.LatchFatal(errors.New("fatal native recovery"))
			return t.Context(), d, 1
		}, raftadmission.ErrClosed},
		{"closed arbiter", func(t *testing.T, d *clusterbootstrap.BootstrapDomain) (context.Context, *clusterbootstrap.BootstrapDomain, uint64) {
			d.Admission.Close()
			return t.Context(), d, 1
		}, raftadmission.ErrClosed},
		{"unknown group", func(t *testing.T, d *clusterbootstrap.BootstrapDomain) (context.Context, *clusterbootstrap.BootstrapDomain, uint64) {
			return t.Context(), d, 2
		}, raftadmission.ErrClosed},
		{"missing domain", func(t *testing.T, _ *clusterbootstrap.BootstrapDomain) (context.Context, *clusterbootstrap.BootstrapDomain, uint64) {
			return t.Context(), nil, 1
		}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, observation := range []error{raftadmission.ErrStale, distribution.ErrEngineSnapshotVersionStale} {
				domain := bootstrapIngressFixture(t).Domain
				ctx, domain, id := tc.prepare(t, domain)
				result := clusterbootstrap.DeferBootstrapCatalogObservation(ctx, domain, id, observation)
				require.ErrorIs(t, result, observation, "failed deferral retains the original typed observation")
				if tc.cause != nil {
					require.ErrorIs(t, result, tc.cause)
				}
			}
		})
	}
}
