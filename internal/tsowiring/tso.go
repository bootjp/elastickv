package tsowiring

import (
	"context"
	"flag"
	"log/slog"
	"strings"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

const (
	DefaultBatchSize      = 256
	DefaultReloadInterval = 5 * time.Second
)

// Flags selects the timestamp-oracle rollout stage, either through the
// legacy booleans or through a runtime-reloadable mode file.
type Flags struct {
	Enabled            bool
	ShadowEnabled      bool
	PhaseDEnabled      bool
	BatchSize          int
	ModeFile           string
	ModeReloadInterval time.Duration
}

func (f *Flags) Register(fs *flag.FlagSet) {
	fs.BoolVar(&f.Enabled, "tsoEnabled", false, "Commit the one-way cutover marker and issue coordinator-owned persistence timestamps through the dedicated TSO leader when group 0 is configured")
	fs.BoolVar(&f.ShadowEnabled, "tsoShadowEnabled", false, "Serialize legacy HLC issuance through the dedicated TSO; fail closed on TSO errors and switch to TSO values after cutover")
	fs.BoolVar(&f.PhaseDEnabled, "tsoPhaseDEnabled", false, "Close the centralized-TSO compatibility window after every group-0 member understands the M7 marker")
	fs.IntVar(&f.BatchSize, "tsoBatchSize", DefaultBatchSize, "Timestamp batch size used by TSO cutover and shadow validation")
	fs.StringVar(&f.ModeFile, "tsoModeFile", "", "Path to a runtime-reloadable TSO mode file containing legacy, shadow, cutover, or phase-d")
	fs.DurationVar(&f.ModeReloadInterval, "tsoModeReloadInterval", DefaultReloadInterval, "Polling interval for tsoModeFile")
}

func StartModeReloader(ctx context.Context, eg *errgroup.Group, f Flags, wiring Wiring) {
	if eg == nil || wiring.RuntimeController == nil || strings.TrimSpace(f.ModeFile) == "" {
		return
	}
	eg.Go(func() error {
		return kv.RunTSOModeFileReload(
			ctx,
			f.ModeFile,
			f.ModeReloadInterval,
			wiring.RuntimeController,
			slog.Default(),
		)
	})
}

type Wiring struct {
	ServerAllocator   kv.TSOAllocator
	RoutedAllocator   *kv.LeaderRoutedTSOAllocator
	RuntimeController *kv.TSORuntimeController
}

var (
	// ErrTSOActivationNotConfigured is returned when a caller asks to activate a
	// TSO marker on a node that runs no dedicated TSO runtime.
	ErrTSOActivationNotConfigured = errors.New("tso activation requires a dedicated TSO runtime on this node")
	// ErrTSOActivationNotPermitted is returned when the request asks for a stage
	// beyond the one this node is configured for.
	ErrTSOActivationNotPermitted = errors.New("tso activation is beyond this node's configured rollout stage")
)

// AuthorizeActivation reports whether this node's own rollout configuration
// permits committing the durable cutover or Phase-D marker a caller asked for.
//
// The two request booleans on GetTimestamp commit one-way markers, and the
// Distribution service sits on the shared Raft gRPC server with no bearer-token
// interceptor of its own. Trusting the wire fields alone would let any caller
// that reaches the port drive the group-0 leader through the staged rollout --
// committing Phase D before every member supports it, or committing cutover on
// a node still issuing through the legacy path. The runtime mode is what the
// operator configured through --tsoModeFile, so it is the right authority.
func (w Wiring) AuthorizeActivation(cutover, phaseD bool) error {
	if !cutover && !phaseD {
		return nil
	}
	if w.RuntimeController == nil {
		return errors.WithStack(ErrTSOActivationNotConfigured)
	}
	mode := w.RuntimeController.CurrentMode()
	if phaseD && mode < kv.TSOModePhaseD {
		return errors.Wrapf(ErrTSOActivationNotPermitted, "phase-d activation requires local mode phase-d, have %s", mode)
	}
	if cutover && mode < kv.TSOModeCutover {
		return errors.Wrapf(ErrTSOActivationNotPermitted, "cutover activation requires local mode cutover or later, have %s", mode)
	}
	return nil
}

func (w Wiring) Close() error {
	if w.RoutedAllocator == nil {
		return nil
	}
	return errors.Wrap(w.RoutedAllocator.Close(), "close TSO leader routing")
}

func (w Wiring) CloseWithLog() {
	if err := w.Close(); err != nil {
		slog.Warn("failed to close TSO routing connections", slog.Any("err", err))
	}
}

func configuredTSOMode(f Flags) (kv.TSOMode, error) {
	if err := validateTSOModeFlags(f); err != nil {
		return kv.TSOModeLegacy, err
	}
	if strings.TrimSpace(f.ModeFile) != "" {
		mode, err := kv.ReadTSOModeFile(f.ModeFile)
		return mode, errors.Wrap(err, "load initial tso runtime mode")
	}
	return tsoModeFromLegacyFlags(f), nil
}

func validateTSOModeFlags(f Flags) error {
	if err := validateLegacyTSOModeFlags(f); err != nil {
		return err
	}
	if strings.TrimSpace(f.ModeFile) == "" {
		return nil
	}
	if f.Enabled || f.ShadowEnabled || f.PhaseDEnabled {
		return errors.New("--tsoModeFile cannot be combined with tsoEnabled, tsoShadowEnabled, or tsoPhaseDEnabled")
	}
	if f.ModeReloadInterval <= 0 {
		return errors.New("--tsoModeReloadInterval must be positive")
	}
	return nil
}

func validateLegacyTSOModeFlags(f Flags) error {
	if f.Enabled && f.ShadowEnabled {
		return errors.New("--tsoEnabled and --tsoShadowEnabled are mutually exclusive")
	}
	if f.PhaseDEnabled && !f.Enabled {
		return errors.New("--tsoPhaseDEnabled requires --tsoEnabled")
	}
	return nil
}

func tsoModeFromLegacyFlags(f Flags) kv.TSOMode {
	switch {
	case f.PhaseDEnabled:
		return kv.TSOModePhaseD
	case f.Enabled:
		return kv.TSOModeCutover
	case f.ShadowEnabled:
		return kv.TSOModeShadow
	default:
		return kv.TSOModeLegacy
	}
}

func configureLegacyCoordinatorTSO(f Flags, coordinate *kv.ShardedCoordinator, mode kv.TSOMode) (Wiring, error) {
	var wiring Wiring
	if strings.TrimSpace(f.ModeFile) != "" {
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
	batch, err := kv.NewBatchAllocator(local, f.BatchSize)
	if err != nil {
		return wiring, errors.Wrap(err, "configure local tso bridge batch")
	}
	coordinate.WithTSOAllocator(batch)
	return wiring, nil
}

func configureDedicatedCoordinatorTSO(
	f Flags,
	coordinate *kv.ShardedCoordinator,
	tsoGroupID uint64,
	tsoGroup *kv.ShardGroup,
	floorProvider kv.TSOCutoverFloorProvider,
	mode kv.TSOMode,
	observer kv.TSOObserver,
	routing ...kv.LeaderRoutedTSOAllocatorOption,
) (Wiring, error) {
	var wiring Wiring
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
	wiring.ServerAllocator = local
	// The cutover state is wired unconditionally so a later Phase-D transition
	// is visible, but the timestamp group is NOT pinned yet. The candidate is
	// recorded alongside it so that a Phase-D marker applied after startup --
	// from a --tsoModeFile reload or from another node -- pins group 0 without
	// the process having to mutate the coordinator at runtime.
	coordinate.WithTSOCutoverState(tsoGroup.TSOState)
	coordinate.WithTimestampGroupCandidate(tsoGroupID)
	if !dedicatedTSORuntimeRequired(f, tsoGroup.TSOState, mode) {
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
		coordinate.WithTimestampGroup(tsoGroupID)
	}

	routedOpts := dedicatedTSORoutingOptions(coordinate.Clock(), observer)
	routedOpts = append(routedOpts, routing...)
	routed, err := kv.NewLeaderRoutedTSOAllocator(local, tsoGroup.Engine, routedOpts...)
	if err != nil {
		return wiring, errors.Wrap(err, "configure tso leader routing")
	}
	wiring.RoutedAllocator = routed
	controller, err := kv.NewTSORuntimeController(kv.TSORuntimeControllerConfig{
		Clock:       coordinate.Clock(),
		Routed:      routed,
		State:       tsoGroup.TSOState,
		BatchSize:   f.BatchSize,
		InitialMode: mode,
		Logger:      slog.Default(),
		Observer:    observer,
	})
	if err != nil {
		_ = wiring.Close()
		return Wiring{}, errors.Wrap(err, "configure tso runtime controller")
	}
	wiring.RuntimeController = controller
	coordinate.WithTSOAllocator(controller.Allocator())
	return wiring, nil
}

func dedicatedTSORuntimeRequired(f Flags, state *kv.TSOStateMachine, mode kv.TSOMode) bool {
	return strings.TrimSpace(f.ModeFile) != "" || dedicatedTSOTimestampGroupRequired(state, mode)
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

func InstallHLCLeaseRenewalBlocker(coordinate kv.Coordinator, blocked func() bool) {
	if coordinate == nil || blocked == nil {
		return
	}
	if installer, ok := coordinate.(hlcLeaseRenewalBlocker); ok {
		installer.SetHLCLeaseRenewalBlocker(blocked)
	}
}

func StartHLCLeaseRenewal(ctx context.Context, eg *errgroup.Group, coordinate kv.Coordinator) {
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

// Configure wires the coordinator's timestamp allocator for the configured
// rollout stage. dedicated reports whether this node hosts the dedicated TSO
// group tsoGroupID (tsoGroup); routing adds leader-routing options such as the
// protected-bootstrap dial options.
func Configure(
	f Flags,
	coordinate *kv.ShardedCoordinator,
	tsoGroupID uint64,
	tsoGroup *kv.ShardGroup,
	dedicated bool,
	floorProvider kv.TSOCutoverFloorProvider,
	observer kv.TSOObserver,
	routing ...kv.LeaderRoutedTSOAllocatorOption,
) (Wiring, error) {
	var wiring Wiring
	if coordinate == nil {
		return wiring, errors.Wrap(kv.ErrTSOCoordinatorNil, "configure tso allocator")
	}
	mode, err := configuredTSOMode(f)
	if err != nil {
		return wiring, err
	}

	if !dedicated {
		return configureLegacyCoordinatorTSO(f, coordinate, mode)
	}
	// Let the state machine publish cutover / phase-D from its own apply and
	// restore paths. Otherwise a replica that applies a marker but serves no
	// allocations keeps reporting the pre-cutover values, and under the
	// backward-compatible startup flags no mode-reload loop ever corrects it.
	if tsoGroup != nil && tsoGroup.TSOState != nil && observer != nil {
		tsoGroup.TSOState.SetDurableStateObserver(observer)
	}
	return configureDedicatedCoordinatorTSO(f, coordinate, tsoGroupID, tsoGroup, floorProvider, mode, observer, routing...)
}
