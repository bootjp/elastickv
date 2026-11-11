package main

import (
	"context"
	"log/slog"
	"strings"

	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

func startTSOModeReloader(ctx context.Context, eg *errgroup.Group, wiring coordinatorTSOWiring) {
	if eg == nil || wiring.runtimeController == nil || strings.TrimSpace(*tsoModeFile) == "" {
		return
	}
	eg.Go(func() error {
		return kv.RunTSOModeFileReload(
			ctx,
			*tsoModeFile,
			*tsoModeReloadInterval,
			wiring.runtimeController,
			slog.Default(),
		)
	})
}

type coordinatorTSOWiring struct {
	serverAllocator   kv.TSOAllocator
	routedAllocator   *kv.LeaderRoutedTSOAllocator
	runtimeController *kv.TSORuntimeController
}

var (
	// ErrTSOActivationNotConfigured is returned when a caller asks to activate a
	// TSO marker on a node that runs no dedicated TSO runtime.
	ErrTSOActivationNotConfigured = errors.New("tso activation requires a dedicated TSO runtime on this node")
	// ErrTSOActivationNotPermitted is returned when the request asks for a stage
	// beyond the one this node is configured for.
	ErrTSOActivationNotPermitted = errors.New("tso activation is beyond this node's configured rollout stage")
)

// authorizeActivation reports whether this node's own rollout configuration
// permits committing the durable cutover or Phase-D marker a caller asked for.
//
// The two request booleans on GetTimestamp commit one-way markers, and the
// Distribution service sits on the shared Raft gRPC server with no bearer-token
// interceptor of its own. Trusting the wire fields alone would let any caller
// that reaches the port drive the group-0 leader through the staged rollout --
// committing Phase D before every member supports it, or committing cutover on
// a node still issuing through the legacy path. The runtime mode is what the
// operator configured through --tsoModeFile, so it is the right authority.
func (w coordinatorTSOWiring) authorizeActivation(cutover, phaseD bool) error {
	if !cutover && !phaseD {
		return nil
	}
	if w.runtimeController == nil {
		return errors.WithStack(ErrTSOActivationNotConfigured)
	}
	mode := w.runtimeController.CurrentMode()
	if phaseD && mode < kv.TSOModePhaseD {
		return errors.Wrapf(ErrTSOActivationNotPermitted, "phase-d activation requires local mode phase-d, have %s", mode)
	}
	if cutover && mode < kv.TSOModeCutover {
		return errors.Wrapf(ErrTSOActivationNotPermitted, "cutover activation requires local mode cutover or later, have %s", mode)
	}
	return nil
}

func (w coordinatorTSOWiring) Close() error {
	if w.routedAllocator == nil {
		return nil
	}
	return errors.Wrap(w.routedAllocator.Close(), "close TSO leader routing")
}

func (w coordinatorTSOWiring) CloseWithLog() {
	if err := w.Close(); err != nil {
		slog.Warn("failed to close TSO routing connections", slog.Any("err", err))
	}
}

func configureCoordinatorTSO(
	coordinate *kv.ShardedCoordinator,
	shardGroups map[uint64]*kv.ShardGroup,
	floorProviders ...kv.TSOCutoverFloorProvider,
) (coordinatorTSOWiring, error) {
	var floorProvider kv.TSOCutoverFloorProvider
	if len(floorProviders) > 0 {
		floorProvider = floorProviders[0]
	}
	return configureCoordinatorTSOWithObserver(coordinate, shardGroups, floorProvider, nil)
}

func configureCoordinatorTSOWithObserver(
	coordinate *kv.ShardedCoordinator,
	shardGroups map[uint64]*kv.ShardGroup,
	floorProvider kv.TSOCutoverFloorProvider,
	observer kv.TSOObserver,
	networks ...*bootstrapNetwork,
) (coordinatorTSOWiring, error) {
	var wiring coordinatorTSOWiring
	if coordinate == nil {
		return wiring, errors.Wrap(kv.ErrTSOCoordinatorNil, "configure tso allocator")
	}
	mode, err := configuredTSOMode()
	if err != nil {
		return wiring, err
	}

	tsoGroup, dedicated := shardGroups[dedicatedTSORaftGroupID]
	if !dedicated {
		return configureLegacyCoordinatorTSO(coordinate, mode)
	}
	// Let the state machine publish cutover / phase-D from its own apply and
	// restore paths. Otherwise a replica that applies a marker but serves no
	// allocations keeps reporting the pre-cutover values, and under the
	// backward-compatible startup flags no mode-reload loop ever corrects it.
	if tsoGroup != nil && tsoGroup.TSOState != nil && observer != nil {
		tsoGroup.TSOState.SetDurableStateObserver(observer)
	}
	return configureDedicatedCoordinatorTSO(coordinate, tsoGroup, floorProvider, mode, observer, networks...)
}

func configuredTSOMode() (kv.TSOMode, error) {
	if err := validateTSOModeFlags(); err != nil {
		return kv.TSOModeLegacy, err
	}
	if strings.TrimSpace(*tsoModeFile) != "" {
		mode, err := kv.ReadTSOModeFile(*tsoModeFile)
		return mode, errors.Wrap(err, "load initial tso runtime mode")
	}
	return tsoModeFromLegacyFlags(), nil
}

func validateTSOModeFlags() error {
	if err := validateLegacyTSOModeFlags(); err != nil {
		return err
	}
	if strings.TrimSpace(*tsoModeFile) == "" {
		return nil
	}
	if *tsoEnabled || *tsoShadowEnabled || *tsoPhaseDEnabled {
		return errors.New("--tsoModeFile cannot be combined with tsoEnabled, tsoShadowEnabled, or tsoPhaseDEnabled")
	}
	if *tsoModeReloadInterval <= 0 {
		return errors.New("--tsoModeReloadInterval must be positive")
	}
	return nil
}

func validateLegacyTSOModeFlags() error {
	if *tsoEnabled && *tsoShadowEnabled {
		return errors.New("--tsoEnabled and --tsoShadowEnabled are mutually exclusive")
	}
	if *tsoPhaseDEnabled && !*tsoEnabled {
		return errors.New("--tsoPhaseDEnabled requires --tsoEnabled")
	}
	return nil
}

func tsoModeFromLegacyFlags() kv.TSOMode {
	switch {
	case *tsoPhaseDEnabled:
		return kv.TSOModePhaseD
	case *tsoEnabled:
		return kv.TSOModeCutover
	case *tsoShadowEnabled:
		return kv.TSOModeShadow
	default:
		return kv.TSOModeLegacy
	}
}

func configureLegacyCoordinatorTSO(coordinate *kv.ShardedCoordinator, mode kv.TSOMode) (coordinatorTSOWiring, error) {
	var wiring coordinatorTSOWiring
	if strings.TrimSpace(*tsoModeFile) != "" {
		return wiring, errors.Wrap(kv.ErrTSOGroupRequired, "configure tso mode reload")
	}
	if mode == kv.TSOModePhaseD {
		return wiring, errors.Wrap(kv.ErrTSOGroupRequired, "configure tso phase D")
	}
	if mode == kv.TSOModeShadow {
		return wiring, errors.Wrap(kv.ErrTSOGroupRequired, "configure tso shadow allocator")
	}
	if mode == kv.TSOModeLegacy {
		return wiring, nil
	}
	// Preserve the pre-group-0 bridge for deployments that enable TSO
	// batching before adding the dedicated group.
	local, err := kv.NewLocalTSOAllocator(coordinate)
	if err != nil {
		return wiring, errors.Wrap(err, "configure local tso bridge")
	}
	batch, err := kv.NewBatchAllocator(local, *tsoBatchSize)
	if err != nil {
		return wiring, errors.Wrap(err, "configure local tso bridge batch")
	}
	coordinate.WithTSOAllocator(batch)
	return wiring, nil
}

func configureDedicatedCoordinatorTSO(
	coordinate *kv.ShardedCoordinator,
	tsoGroup *kv.ShardGroup,
	floorProvider kv.TSOCutoverFloorProvider,
	mode kv.TSOMode,
	observer kv.TSOObserver,
	networks ...*bootstrapNetwork,
) (coordinatorTSOWiring, error) {
	var wiring coordinatorTSOWiring
	opts := []kv.RaftTSOAllocatorOption{kv.WithTSOCutoverFloorProvider(floorProvider)}
	if gate, ok := floorProvider.(kv.TSOPhaseDActivationGate); ok {
		opts = append(opts, kv.WithTSOPhaseDActivationGate(gate))
	}
	local, err := kv.NewRaftTSOAllocator(
		tsoGroup,
		coordinate.Clock(),
		opts...,
	)
	if err != nil {
		return wiring, errors.Wrap(err, "configure dedicated tso allocator")
	}
	wiring.serverAllocator = local
	// The cutover state is wired unconditionally so a later Phase-D transition
	// is visible, but the timestamp group is NOT pinned yet. The candidate is
	// recorded alongside it so that a Phase-D marker applied after startup --
	// from a --tsoModeFile reload or from another node -- pins group 0 without
	// the process having to mutate the coordinator at runtime.
	coordinate.WithTSOCutoverState(tsoGroup.TSOState)
	coordinate.WithTimestampGroupCandidate(dedicatedTSORaftGroupID)
	if !dedicatedTSORuntimeRequired(tsoGroup.TSOState, mode) {
		return wiring, nil
	}
	if dedicatedTSOTimestampGroupRequired(tsoGroup.TSOState, mode) {
		// Pinned only once the dedicated allocator is actually in use. In
		// legacy warm-up, persistence timestamps still come from the data
		// groups' local HLC path, and timestampGroupConfigured narrows lease
		// renewal (timestampLeaseRenewalGroupIDs), leadership
		// (IsTimestampLeader), and recovery (hlcLeaseRecoveryTargets) to
		// group 0 alone. Pinning before this point stopped renewing the data
		// groups, so a group-0 quorum loss expired their ceilings and failed
		// legacy writes with ErrCeilingExpired even though those groups were
		// healthy -- contrary to the no-path-change warm-up contract in
		// docs/design/2026_04_16_implemented_centralized_tso.md.
		coordinate.WithTimestampGroup(dedicatedTSORaftGroupID)
	}

	routedOpts := dedicatedTSORoutingOptions(coordinate.Clock(), observer)
	routedOpts = append(routedOpts, bootstrapTSORoutingOptions(networks)...)
	routed, err := kv.NewLeaderRoutedTSOAllocator(local, tsoGroup.Engine, routedOpts...)
	if err != nil {
		return wiring, errors.Wrap(err, "configure tso leader routing")
	}
	wiring.routedAllocator = routed
	controller, err := kv.NewTSORuntimeController(kv.TSORuntimeControllerConfig{
		Clock:       coordinate.Clock(),
		Routed:      routed,
		State:       tsoGroup.TSOState,
		BatchSize:   *tsoBatchSize,
		InitialMode: mode,
		Logger:      slog.Default(),
		Observer:    observer,
	})
	if err != nil {
		_ = wiring.Close()
		return coordinatorTSOWiring{}, errors.Wrap(err, "configure tso runtime controller")
	}
	wiring.runtimeController = controller
	coordinate.WithTSOAllocator(controller.Allocator())
	return wiring, nil
}

func dedicatedTSORuntimeRequired(state *kv.TSOStateMachine, mode kv.TSOMode) bool {
	return strings.TrimSpace(*tsoModeFile) != "" || dedicatedTSOTimestampGroupRequired(state, mode)
}

func dedicatedTSOTimestampGroupRequired(state *kv.TSOStateMachine, mode kv.TSOMode) bool {
	return mode != kv.TSOModeLegacy || (state != nil && state.CutoverActive())
}

func dedicatedTSORoutingOptions(clock *kv.HLC, observer kv.TSOObserver) []kv.LeaderRoutedTSOAllocatorOption {
	opts := []kv.LeaderRoutedTSOAllocatorOption{kv.WithTSORoutedClock(clock)}
	if observer != nil {
		opts = append(opts, kv.WithTSOObserver(observer))
	}
	return opts
}

type hlcLeaseRenewalBlocker interface {
	SetHLCLeaseRenewalBlocker(func() bool)
}

type hlcLeaseRenewalRunner interface {
	RunHLCLeaseRenewal(context.Context)
}

func installHLCLeaseRenewalBlocker(coordinate kv.Coordinator, blocked func() bool) {
	if coordinate == nil || blocked == nil {
		return
	}
	if installer, ok := coordinate.(hlcLeaseRenewalBlocker); ok {
		installer.SetHLCLeaseRenewalBlocker(blocked)
	}
}

func startHLCLeaseRenewal(ctx context.Context, eg *errgroup.Group, coordinate kv.Coordinator) {
	if eg == nil || coordinate == nil {
		return
	}
	runner, ok := coordinate.(hlcLeaseRenewalRunner)
	if !ok {
		return
	}
	eg.Go(func() error {
		runner.RunHLCLeaseRenewal(ctx)
		return nil
	})
}
