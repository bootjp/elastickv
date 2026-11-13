package server

import (
	"context"
	"flag"
	"testing"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/nodeauth"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestAutoSplitCanonicalFlagsAreRegistered(t *testing.T) {
	t.Parallel()

	for _, name := range []string{"autoSplitCooldown", "autoSplitThreshold", "autoSplitMaxPerCycle", "autoSplitCrossGroup"} {
		require.NotNil(t, flag.CommandLine.Lookup(name), name)
	}
	for _, name := range []string{"autoSplitSplitCooldown", "autoSplitThresholdOpsMin", "autoSplitMaxSplitsPerCycle"} {
		require.Nil(t, flag.CommandLine.Lookup(name), name)
	}
}

func TestCrossGroupAutoSplitRequiresMultipleGroups(t *testing.T) {
	flags := defaultServerFlags()
	*flags.autoSplit.Enabled = true
	*flags.autoSplit.CrossGroup = true
	_, err := setupDistributionWatcherAndAutoSplit(context.Background(), nil, nil, nil, nil, nil, nil,
		nil, nil, flags.keyViz, flags.autoSplit, []uint64{1})
	require.ErrorContains(t, err, "at least two configured Raft groups")
}

func TestCrossGroupAutoSplitRequiresLocalMigrationReadiness(t *testing.T) {
	flags := defaultServerFlags()
	*flags.autoSplit.Enabled = true
	*flags.autoSplit.CrossGroup = true
	ready := nodeauth.Flags{SplitMigrationPeerAuthEnforced: true}
	t.Setenv(adapter.MigrationImportOpcodeEnv, "")
	t.Setenv(adapter.MigrationPromoteOpcodeEnv, "1")
	t.Setenv(adapter.MigrationCleanupOpcodeEnv, "1")

	_, err := setupDistributionWatcherAndAutoSplit(t.Context(), nil, ready.SplitMigrationLocalReadinessGate,
		nil, nil, nil, nil, nil, nil, flags.keyViz, flags.autoSplit, []uint64{1, 2})
	require.ErrorContains(t, err, "--autoSplitCrossGroup requires local split migration readiness")
	require.ErrorContains(t, err, adapter.MigrationImportOpcodeEnv)

	t.Setenv(adapter.MigrationImportOpcodeEnv, "1")
	ready.SplitMigrationPeerAuthEnforced = false
	_, err = setupDistributionWatcherAndAutoSplit(t.Context(), nil, ready.SplitMigrationLocalReadinessGate,
		nil, nil, nil, nil, nil, nil, flags.keyViz, flags.autoSplit, []uint64{1, 2})
	require.ErrorContains(t, err, "migration is paused during peer auth rollout")

	_, err = setupDistributionWatcherAndAutoSplit(t.Context(), nil, nil,
		nil, nil, nil, nil, nil, nil, flags.keyViz, flags.autoSplit, []uint64{1, 2})
	require.ErrorIs(t, err, adapter.ErrMigrationReadinessGateMissing)
}

func TestSetupDistributionWatcherAndAutoSplitReturnsNilRuntimeWhenDisabled(t *testing.T) {
	flags := defaultServerFlags()
	*flags.autoSplit.Enabled = false

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	catalog := distribution.NewCatalogStore(store.NewMVCCStore())
	_, err := catalog.Save(ctx, 0, []distribution.RouteDescriptor{{
		RouteID: 1,
		Start:   []byte(""),
		End:     nil,
		GroupID: 1,
		State:   distribution.RouteStateActive,
	}})
	require.NoError(t, err)

	var eg errgroup.Group
	runtime, err := setupDistributionWatcherAndAutoSplit(ctx, &eg, nil, catalog, distribution.NewEngine(), nil, nil, nil, nil, flags.keyViz, flags.autoSplit, nil)
	require.NoError(t, err)
	require.Nil(t, runtime)

	cancel()
	require.NoError(t, eg.Wait())
}

func TestAutoSplitStartupWithAndWithoutMigrationReadiness(t *testing.T) {
	flags := defaultServerFlags()
	*flags.autoSplit.Enabled = true
	ready := nodeauth.Flags{SplitMigrationPeerAuthEnforced: true}
	t.Setenv(adapter.MigrationImportOpcodeEnv, "")
	t.Setenv(adapter.MigrationPromoteOpcodeEnv, "1")
	t.Setenv(adapter.MigrationCleanupOpcodeEnv, "1")

	for _, tc := range []struct {
		name       string
		crossGroup bool
		importOn   bool
	}{
		{name: "same group without migration"},
		{name: "cross group with migration", crossGroup: true, importOn: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			*flags.autoSplit.CrossGroup = tc.crossGroup
			if tc.importOn {
				t.Setenv(adapter.MigrationImportOpcodeEnv, "1")
			}
			ctx, cancel := context.WithCancel(t.Context())
			catalog := distribution.NewCatalogStore(store.NewMVCCStore())
			_, err := catalog.Save(ctx, 0, []distribution.RouteDescriptor{{
				RouteID: 1, GroupID: 1, State: distribution.RouteStateActive,
			}})
			require.NoError(t, err)
			var eg errgroup.Group
			runtime, err := setupDistributionWatcherAndAutoSplit(ctx, &eg, ready.SplitMigrationLocalReadinessGate,
				catalog, distribution.NewEngine(), nil, nil, nil, nil, flags.keyViz, flags.autoSplit, []uint64{1, 2})
			require.NoError(t, err)
			require.NotNil(t, runtime)
			cancel()
			require.NoError(t, eg.Wait())
		})
	}
}
