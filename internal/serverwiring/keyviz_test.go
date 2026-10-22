package serverwiring

import (
	"context"
	"flag"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/keyviz"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// parseWiringFlags registers both shared flag groups on a fresh flag set and
// parses args, so tests never touch the process-wide flags.
func parseWiringFlags(t *testing.T, args ...string) (*KeyVizFlags, *AutoSplitFlags) {
	t.Helper()
	fs := flag.NewFlagSet("serverwiring-test", flag.ContinueOnError)
	k := RegisterKeyVizFlags(fs)
	a := RegisterAutoSplitFlags(fs)
	require.NoError(t, fs.Parse(args))
	k.RecordExplicit(fs)
	return k, a
}

// BuildSampler returns nil unless --keyvizEnabled or --autoSplit is set, and
// forwards every sampler option.
func TestBuildSamplerHonoursFlags(t *testing.T) {
	t.Parallel()

	k, a := parseWiringFlags(t)
	require.Nil(t, BuildSampler(k, a))

	k, a = parseWiringFlags(t, "-keyvizEnabled", "-keyvizStep=250ms", "-keyvizMaxTrackedRoutes=5",
		"-keyvizMaxMemberRoutesPerSlot=7", "-keyvizHistoryColumns=16")
	s := BuildSampler(k, a)
	require.NotNil(t, s)
	require.Equal(t, 250*time.Millisecond, s.Step())
	require.Equal(t, 16, s.HistoryColumns())

	k, a = parseWiringFlags(t, "-autoSplit")
	require.NotNil(t, BuildSampler(k, a), "--autoSplit needs the sampler even without --keyvizEnabled")
}

// --keyvizLabelsEnabled gives each adapter its own rows; without it every
// observation lands in the legacy route row.
func TestBuildSamplerHonoursLabelsFlag(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		args      []string
		wantLabel keyviz.Label
	}{
		{args: []string{"-keyvizEnabled"}, wantLabel: keyviz.LabelLegacy},
		{args: []string{"-keyvizEnabled", "-keyvizLabelsEnabled"}, wantLabel: keyviz.LabelRedis},
	} {
		k, a := parseWiringFlags(t, tc.args...)
		s := BuildSampler(k, a)
		require.NotNil(t, s)
		require.True(t, s.RegisterRoute(1, []byte("a"), []byte("z"), 1))
		s.Observe(1, []byte("k"), keyviz.OpWrite, 1, keyviz.LabelRedis)
		s.Flush()

		var labels []keyviz.Label
		for _, column := range s.Snapshot(time.Time{}, time.Time{}) {
			for _, row := range column.Rows {
				if row.Writes > 0 {
					labels = append(labels, row.Label)
				}
			}
		}
		require.Equal(t, []keyviz.Label{tc.wantLabel}, labels, "args %v", tc.args)
	}
}

func TestEffectiveKeyBucketsPerRouteAutoSplitImpliedDefault(t *testing.T) {
	t.Parallel()

	require.Equal(t, 16, effectiveKeyBucketsPerRoute(true, false, keyviz.DefaultKeyBucketsPerRoute, 16))
	require.Equal(t, 1, effectiveKeyBucketsPerRoute(true, true, keyviz.DefaultKeyBucketsPerRoute, 16))
	require.Equal(t, 8, effectiveKeyBucketsPerRoute(true, false, 8, 16))
	require.Equal(t, 1, effectiveKeyBucketsPerRoute(false, false, keyviz.DefaultKeyBucketsPerRoute, 16))
}

// SeedRoutes registers each route the engine reports, so the first
// Observe(routeID, ...) calls find a slot.
func TestSeedRoutesCopiesEngineCatalogue(t *testing.T) {
	t.Parallel()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte("a"), []byte("m"), 1)

	s := keyviz.NewMemSampler(keyviz.MemSamplerOptions{Step: time.Second, HistoryColumns: 4})
	SeedRoutes(s, engine)

	for _, r := range engine.Stats() {
		s.Observe(r.RouteID, make([]byte, 1), keyviz.OpRead, 1, keyviz.LabelLegacy)
	}
	s.Flush()
	cols := s.Snapshot(time.Time{}, time.Time{})
	require.Len(t, cols, 1)
	require.Len(t, cols[0].Rows, 1)
	require.Equal(t, []byte("a"), cols[0].Rows[0].Start)
}

func TestSeedRoutesNoOpOnNilSampler(t *testing.T) {
	t.Parallel()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte("a"), nil, 1)
	require.NotPanics(t, func() {
		SeedRoutes(nil, engine)
	})
}

// When ctx ends the flusher returns, and its final Flush harvests the
// in-progress step.
func TestStartFlusherReturnsAfterCancel(t *testing.T) {
	t.Parallel()
	s := keyviz.NewMemSampler(keyviz.MemSamplerOptions{Step: time.Millisecond, HistoryColumns: 4})
	require.True(t, s.RegisterRoute(1, []byte("a"), []byte("b"), 0))
	s.Observe(1, make([]byte, 0), keyviz.OpRead, 0, keyviz.LabelLegacy)

	ctx, cancel := context.WithCancel(context.Background())
	eg, _ := errgroup.WithContext(ctx)
	StartFlusher(ctx, eg, s)
	cancel()
	require.NoError(t, eg.Wait())
	saw := false
	for _, c := range s.Snapshot(time.Time{}, time.Time{}) {
		for _, r := range c.Rows {
			if r.RouteID == 1 && r.Reads > 0 {
				saw = true
			}
		}
	}
	require.True(t, saw, "post-cancel Flush did not harvest pending Observe")
}
