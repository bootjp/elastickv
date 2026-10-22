package serverwiring

import (
	"context"
	"flag"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/keyviz"
	"golang.org/x/sync/errgroup"
)

// KeyVizFlags is the --keyviz* flag group that configures the in-memory key
// visualizer sampler. The sampler is off unless --keyvizEnabled or
// --autoSplit is set; the other flags are no-ops while it is off.
type KeyVizFlags struct {
	Enabled                *bool
	Step                   *time.Duration
	MaxTrackedRoutes       *int
	MaxMemberRoutesPerSlot *int
	HistoryColumns         *int
	KeyBucketsPerRoute     *int
	LabelsEnabled          *bool

	// Hot-key drill-down (design 2026_05_28_implemented_keyviz_hot_key_topk).
	// When enabled the sampler retains actual hot key bytes and exposes
	// them through the admin /keyviz/hotkeys drill-down.
	HotKeysEnabled    *bool
	HotKeysPerRoute   *int
	HotKeysSampleRate *int
	HotKeysQueueSize  *int
	HotKeysMaxKeyLen  *int

	// keyBucketsPerRouteExplicit records whether --keyvizKeyBucketsPerRoute
	// was given on the command line; see RecordExplicit.
	keyBucketsPerRouteExplicit bool
}

// RegisterKeyVizFlags registers the --keyviz* sampler flags on fs.
func RegisterKeyVizFlags(fs *flag.FlagSet) *KeyVizFlags {
	return &KeyVizFlags{
		Enabled:                fs.Bool("keyvizEnabled", false, "Enable the in-memory key visualizer sampler that feeds AdminServer.GetKeyVizMatrix"),
		Step:                   fs.Duration("keyvizStep", keyviz.DefaultStep, "Flush interval / matrix-column resolution for the keyviz sampler"),
		MaxTrackedRoutes:       fs.Int("keyvizMaxTrackedRoutes", keyviz.DefaultMaxTrackedRoutes, "Maximum routes tracked individually before excess routes coarsen into virtual buckets"),
		MaxMemberRoutesPerSlot: fs.Int("keyvizMaxMemberRoutesPerSlot", keyviz.DefaultMaxMemberRoutesPerSlot, "Maximum members listed on a virtual bucket; excess routes still drive the bucket counters"),
		HistoryColumns:         fs.Int("keyvizHistoryColumns", keyviz.DefaultHistoryColumns, "Maximum matrix columns retained in the keyviz ring buffer (each column = one Step)"),
		KeyBucketsPerRoute:     fs.Int("keyvizKeyBucketsPerRoute", keyviz.DefaultKeyBucketsPerRoute, "Order-preserving sub-range buckets per individual route for the hot-key heatmap; 1 disables sub-bucketing (route-granular, today's behaviour). Raising K trades route breadth for intra-route depth at a fixed payload size: each route emits up to K sub-rows that compete for the same row budget, so fewer distinct routes fit in one response. Capped at 256; memory is ~K*32 bytes/route, so K_max ~= memBudget/(32*keyvizMaxTrackedRoutes)"),
		LabelsEnabled:          fs.Bool("keyvizLabelsEnabled", false, "Enable per-adapter KeyViz row labels. Default false keeps legacy route-only rows during rolling upgrades"),
		HotKeysEnabled:         fs.Bool("keyvizHotKeysEnabled", false, "Enable per-route Top-K hot-key drill-down (retains actual key bytes; admin auth + keyviz flag both required)"),
		HotKeysPerRoute:        fs.Int("keyvizHotKeysPerRoute", keyviz.DefaultHotKeysPerRoute, "Space-Saving sketch capacity m per route (default 64, cap 256). Larger m tightens the error bound N_total/m at the cost of memory"),
		HotKeysSampleRate:      fs.Int("keyvizHotKeysSampleRate", keyviz.DefaultHotKeysSampleRate, "Hot-keys hot-path sample rate R: 1-in-R observes are enqueued (default 16, cap 1024). Higher R reduces hot-path cost; raises Chernoff miss probability over the sampled stream"),
		HotKeysQueueSize:       fs.Int("keyvizHotKeysQueueSize", keyviz.DefaultHotKeysQueueSize, "Bounded channel size between Observe and the hot-keys aggregator. Default 8192, cap 65536. Drops past this are counted (dropped_samples -> degraded)"),
		HotKeysMaxKeyLen:       fs.Int("keyvizHotKeysMaxKeyLen", keyviz.DefaultHotKeysMaxKeyLen, "Maximum key length sampled into the hot-keys sketch (default 1024 B, cap 4096). Longer keys bump skipped_long_keys -> degraded, never truncated"),
	}
}

// RecordExplicit notes which of these flags fs saw on the command line. Call
// it after fs.Parse: --autoSplit raises the per-route key buckets only when
// the operator did not choose them.
func (k *KeyVizFlags) RecordExplicit(fs *flag.FlagSet) {
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "keyvizKeyBucketsPerRoute" {
			k.keyBucketsPerRouteExplicit = true
		}
	})
}

// BuildSampler builds the in-memory KeyViz sampler, or returns nil when
// neither --keyvizEnabled nor --autoSplit is set. The coordinators and the
// admin server treat a nil sampler as "KeyViz disabled", so this is the one
// decision point.
func BuildSampler(k *KeyVizFlags, a *AutoSplitFlags) *keyviz.MemSampler {
	if !*k.Enabled && !*a.Enabled {
		return nil
	}
	return keyviz.NewMemSampler(keyviz.MemSamplerOptions{
		Step:                   *k.Step,
		HistoryColumns:         *k.HistoryColumns,
		MaxTrackedRoutes:       *k.MaxTrackedRoutes,
		MaxMemberRoutesPerSlot: *k.MaxMemberRoutesPerSlot,
		KeyBucketsPerRoute:     k.keyBucketsPerRoute(a),
		KeyVizLabelsEnabled:    *k.LabelsEnabled,
		HotKeysEnabled:         *k.HotKeysEnabled,
		HotKeysPerRoute:        *k.HotKeysPerRoute,
		HotKeysSampleRate:      *k.HotKeysSampleRate,
		HotKeysQueueSize:       *k.HotKeysQueueSize,
		HotKeysMaxKeyLen:       *k.HotKeysMaxKeyLen,
	})
}

// keyBucketsPerRoute is --keyvizKeyBucketsPerRoute, or --autoSplitDefaultBuckets
// when --autoSplit needs sub-route resolution and the operator left the key
// buckets at the route-granular default.
func (k *KeyVizFlags) keyBucketsPerRoute(a *AutoSplitFlags) int {
	return effectiveKeyBucketsPerRoute(*a.Enabled, k.keyBucketsPerRouteExplicit, *k.KeyBucketsPerRoute, *a.DefaultBuckets)
}

// autoSplitUsesDefaultBuckets reports whether the sampler takes its per-route
// key buckets from --autoSplitDefaultBuckets.
func (k *KeyVizFlags) autoSplitUsesDefaultBuckets(a *AutoSplitFlags) bool {
	return *a.Enabled && !k.keyBucketsPerRouteExplicit && *k.KeyBucketsPerRoute <= keyviz.DefaultKeyBucketsPerRoute
}

func effectiveKeyBucketsPerRoute(autoEnabled, bucketsExplicit bool, configuredBuckets, defaultBuckets int) int {
	if autoEnabled && !bucketsExplicit && configuredBuckets <= keyviz.DefaultKeyBucketsPerRoute {
		return defaultBuckets
	}
	return configuredBuckets
}

// SeedRoutes copies the engine's current route catalogue into the sampler so
// the first matrix snapshots have non-empty metadata. The auto-split
// reconciler keeps membership current after catalog changes.
func SeedRoutes(s *keyviz.MemSampler, engine *distribution.Engine) {
	if s == nil || engine == nil {
		return
	}
	for _, r := range engine.Stats() {
		s.RegisterRoute(r.RouteID, r.Start, r.End, r.GroupID)
	}
}

// StartFlusher runs the sampler's flusher and hot-key aggregator in eg. The
// flusher harvests the in-progress step with a final Flush once ctx ends, so
// a graceful shutdown keeps the most recent partial column. Nothing starts
// when the sampler is disabled.
func StartFlusher(ctx context.Context, eg *errgroup.Group, s *keyviz.MemSampler) {
	if eg == nil || s == nil {
		return
	}
	eg.Go(func() error {
		keyviz.RunFlusher(ctx, s, s.Step())
		s.Flush()
		return nil
	})
	// RunHotKeysAggregator blocks without work when hot keys are disabled,
	// so starting it unconditionally keeps the wiring uniform.
	eg.Go(func() error {
		keyviz.RunHotKeysAggregator(ctx, s)
		return nil
	})
}
