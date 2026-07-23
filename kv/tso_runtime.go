package kv

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
)

var (
	ErrInvalidTSOMode          = errors.New("tso: invalid runtime mode")
	ErrTSOModeRollback         = errors.New("tso: runtime mode rollback is prohibited")
	ErrUnsafeTSOModeTransition = errors.New("tso: runtime mode transition skips a required phase")
)

// TSOMode is the process-local timestamp issuance mode. Its ordering is part
// of the migration contract: runtime reload may only move to the next value.
type TSOMode uint32

const (
	TSOModeLegacy TSOMode = iota
	TSOModeShadow
	TSOModeCutover
	TSOModePhaseD
)

func ParseTSOMode(raw string) (TSOMode, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "legacy":
		return TSOModeLegacy, nil
	case "shadow":
		return TSOModeShadow, nil
	case "cutover":
		return TSOModeCutover, nil
	case "phase-d", "phase_d", "phased":
		return TSOModePhaseD, nil
	default:
		return TSOModeLegacy, errors.Wrapf(ErrInvalidTSOMode, "%q", strings.TrimSpace(raw))
	}
}

func (m TSOMode) String() string {
	switch m {
	case TSOModeLegacy:
		return "legacy"
	case TSOModeShadow:
		return "shadow"
	case TSOModeCutover:
		return "cutover"
	case TSOModePhaseD:
		return "phase-d"
	default:
		return "invalid"
	}
}

func validTSOMode(mode TSOMode) bool {
	return mode <= TSOModePhaseD
}

// TSOObserver is the bounded-cardinality operational surface for production
// allocation latency, shadow divergence, durable state, and mode reloads.
type TSOObserver interface {
	ObserveTSORequest(operation, path, outcome string, duration time.Duration)
	ObserveTSOShadowComparison(result string, divergence uint64)
	ObserveTSOMode(mode string)
	ObserveTSOModeReload(result string)
	ObserveTSODurableState(cutoverActive, phaseDActive bool)
}

type timestampAllocatorSlot struct {
	allocator TimestampAllocator
}

// DynamicTimestampAllocator atomically publishes an optional allocator. A nil
// current allocator deliberately means "use the coordinator's legacy HLC
// path"; callers must resolve it through TimestampAllocatorThrough rather than
// calling Next directly.
type DynamicTimestampAllocator struct {
	current          atomic.Pointer[timestampAllocatorSlot]
	durableState     TSORuntimeState
	durableAllocator TimestampAllocator
	routed           *LeaderRoutedTSOAllocator
}

func NewDynamicTimestampAllocator(initial TimestampAllocator) *DynamicTimestampAllocator {
	d := &DynamicTimestampAllocator{}
	d.store(initial)
	return d
}

func (d *DynamicTimestampAllocator) store(allocator TimestampAllocator) {
	if d == nil {
		return
	}
	if allocator == nil {
		d.current.Store(nil)
		return
	}
	d.current.Store(&timestampAllocatorSlot{allocator: allocator})
}

func (d *DynamicTimestampAllocator) currentTimestampAllocator() TimestampAllocator {
	if d == nil {
		return nil
	}
	if allocator := d.durableTimestampAllocator(); allocator != nil {
		return allocator
	}
	slot := d.current.Load()
	if slot == nil {
		return nil
	}
	return slot.allocator
}

// configureDurableOverride installs immutable references before the dynamic
// allocator is published. An applied one-way marker must override a stale mode
// file immediately rather than waiting for the next reload poll.
func (d *DynamicTimestampAllocator) configureDurableOverride(
	state TSORuntimeState,
	allocator TimestampAllocator,
	routed *LeaderRoutedTSOAllocator,
) {
	d.durableState = state
	d.durableAllocator = allocator
	d.routed = routed
}

func (d *DynamicTimestampAllocator) durableTimestampAllocator() TimestampAllocator {
	if d.durableState == nil || d.durableAllocator == nil {
		return nil
	}
	phaseD := d.durableState.PhaseDActive()
	if !phaseD && !d.durableState.CutoverActive() {
		return nil
	}
	d.routed.promoteActivation(true, phaseD)
	return d.durableAllocator
}

func (d *DynamicTimestampAllocator) Next(ctx context.Context) (uint64, error) {
	allocator := d.currentTimestampAllocator()
	if allocator == nil {
		return 0, errors.WithStack(ErrTSOAllocatorRequired)
	}
	timestamp, err := allocator.Next(ctx)
	return timestamp, errors.Wrap(err, "dynamic tso next")
}

func (d *DynamicTimestampAllocator) NextAfter(ctx context.Context, min uint64) (uint64, error) {
	allocator := d.currentTimestampAllocator()
	if allocator == nil {
		return 0, errors.WithStack(ErrTSOAllocatorRequired)
	}
	if after, ok := allocator.(TimestampAfterAllocator); ok {
		timestamp, err := after.NextAfter(ctx, min)
		return timestamp, errors.Wrap(err, "dynamic tso next after")
	}
	return nextTimestampAfterFromAllocator(ctx, allocator, min, "dynamic tso next after")
}

func (d *DynamicTimestampAllocator) ValidateDurableTimestamp(ctx context.Context, timestamp uint64) error {
	allocator := d.currentTimestampAllocator()
	if allocator == nil {
		return errors.WithStack(ErrTSOAllocatorRequired)
	}
	validator, ok := allocator.(DurableTimestampValidator)
	if !ok {
		return errors.WithStack(ErrTSOProtocolUnsupported)
	}
	return errors.WithStack(validator.ValidateDurableTimestamp(ctx, timestamp))
}

func (d *DynamicTimestampAllocator) PhaseDActive() bool {
	state, ok := d.currentTimestampAllocator().(TSOPhaseDState)
	return ok && state.PhaseDActive()
}

func (d *DynamicTimestampAllocator) PhaseDRequired() bool {
	state, ok := d.currentTimestampAllocator().(TSOPhaseDState)
	return ok && state.PhaseDRequired()
}

func (d *DynamicTimestampAllocator) Invalidate() {
	invalidateTimestampWindow(d.currentTimestampAllocator())
}

// TSORuntimeState is the consensus-owned one-way migration state.
type TSORuntimeState interface {
	CutoverActive() bool
	PhaseDActive() bool
}

type TSORuntimeControllerConfig struct {
	Clock       *HLC
	Routed      *LeaderRoutedTSOAllocator
	State       TSORuntimeState
	BatchSize   int
	InitialMode TSOMode
	Logger      *slog.Logger
	Observer    TSOObserver
}

// TSORuntimeController owns the process-local mode while treating group-0's
// durable markers as authoritative. InitialMode may start at any phase for a
// restart, but ApplyMode requires adjacent, one-way runtime transitions.
type TSORuntimeController struct {
	dynamic  *DynamicTimestampAllocator
	shadow   *ShadowTimestampAllocator
	batch    *BatchAllocator
	routed   *LeaderRoutedTSOAllocator
	state    TSORuntimeState
	observer TSOObserver

	mu   sync.Mutex
	mode atomic.Uint32
}

func NewTSORuntimeController(cfg TSORuntimeControllerConfig) (*TSORuntimeController, error) {
	if cfg.Clock == nil {
		return nil, errors.WithStack(ErrTSOClockNil)
	}
	if cfg.Routed == nil {
		return nil, errors.WithStack(ErrTSOAllocatorRequired)
	}
	if cfg.State == nil {
		return nil, errors.WithStack(ErrTSOStateRequired)
	}
	if !validTSOMode(cfg.InitialMode) {
		return nil, errors.Wrapf(ErrInvalidTSOMode, "%d", cfg.InitialMode)
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	shadow, err := NewShadowTimestampAllocator(
		cfg.Clock,
		cfg.Routed,
		cfg.Logger,
		WithTSOShadowCutoverState(cfg.State),
		WithTSOShadowObserver(cfg.Observer),
	)
	if err != nil {
		return nil, errors.Wrap(err, "configure runtime tso shadow allocator")
	}
	batch, err := NewBatchAllocator(cfg.Routed, cfg.BatchSize)
	if err != nil {
		return nil, errors.Wrap(err, "configure runtime tso batch allocator")
	}
	dynamic := NewDynamicTimestampAllocator(nil)
	dynamic.configureDurableOverride(cfg.State, batch, cfg.Routed)
	controller := &TSORuntimeController{
		dynamic:  dynamic,
		shadow:   shadow,
		batch:    batch,
		routed:   cfg.Routed,
		state:    cfg.State,
		observer: cfg.Observer,
	}
	controller.installMode(controller.effectiveMode(cfg.InitialMode))
	return controller, nil
}

func (c *TSORuntimeController) Allocator() *DynamicTimestampAllocator {
	if c == nil {
		return nil
	}
	return c.dynamic
}

func (c *TSORuntimeController) CurrentMode() TSOMode {
	if c == nil {
		return TSOModeLegacy
	}
	return TSOMode(c.mode.Load())
}

func (c *TSORuntimeController) EffectiveMode(requested TSOMode) TSOMode {
	if c == nil {
		return requested
	}
	return c.effectiveMode(requested)
}

func (c *TSORuntimeController) effectiveMode(requested TSOMode) TSOMode {
	if c.state != nil && c.state.PhaseDActive() {
		return TSOModePhaseD
	}
	if c.state != nil && c.state.CutoverActive() && requested < TSOModeCutover {
		return TSOModeCutover
	}
	return requested
}

func (c *TSORuntimeController) ApplyMode(requested TSOMode) error {
	if c == nil {
		return errors.WithStack(ErrTSOAllocatorRequired)
	}
	if !validTSOMode(requested) {
		c.observeReload("rejected")
		return errors.Wrapf(ErrInvalidTSOMode, "%d", requested)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	target := c.effectiveMode(requested)
	durableFloor := c.effectiveMode(TSOModeLegacy)
	current := c.CurrentMode()
	if target < current {
		c.observeReload("rejected")
		return errors.Wrapf(ErrTSOModeRollback, "%s -> %s", current, target)
	}
	if target == current {
		c.observeDurableState()
		return nil
	}
	if target > current+1 && target > durableFloor+1 {
		c.observeReload("rejected")
		return errors.Wrapf(ErrUnsafeTSOModeTransition, "%s -> %s", current, target)
	}
	c.installMode(target)
	c.observeReload("applied")
	return nil
}

func (c *TSORuntimeController) installMode(mode TSOMode) {
	switch mode {
	case TSOModeLegacy:
		c.routed.setActivation(false, false)
		c.dynamic.store(nil)
	case TSOModeShadow:
		c.routed.setActivation(false, false)
		c.dynamic.store(c.shadow)
	case TSOModeCutover:
		c.routed.setActivation(true, false)
		c.batch.Invalidate()
		c.dynamic.store(c.batch)
	case TSOModePhaseD:
		c.routed.setActivation(true, true)
		c.batch.Invalidate()
		c.dynamic.store(c.batch)
	}
	c.mode.Store(uint32(mode))
	if c.observer != nil {
		c.observer.ObserveTSOMode(mode.String())
	}
	c.observeDurableState()
}

func (c *TSORuntimeController) observeReload(result string) {
	if c != nil && c.observer != nil {
		c.observer.ObserveTSOModeReload(result)
	}
}

func (c *TSORuntimeController) observeDurableState() {
	if c != nil && c.observer != nil && c.state != nil {
		c.observer.ObserveTSODurableState(c.state.CutoverActive(), c.state.PhaseDActive())
	}
}

func ReadTSOModeFile(path string) (TSOMode, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return TSOModeLegacy, errors.Wrap(err, "read tso mode file")
	}
	mode, err := ParseTSOMode(string(raw))
	return mode, errors.Wrap(err, "parse tso mode file")
}

// RunTSOModeFileReload polls an atomically replaceable plain-text mode file.
// Invalid reads or transitions leave the current allocator untouched.
func RunTSOModeFileReload(
	ctx context.Context,
	path string,
	interval time.Duration,
	controller *TSORuntimeController,
	logger *slog.Logger,
) error {
	if controller == nil {
		return errors.WithStack(ErrTSOAllocatorRequired)
	}
	if strings.TrimSpace(path) == "" {
		return nil
	}
	if interval <= 0 {
		return errors.New("tso mode reload interval must be positive")
	}
	if logger == nil {
		logger = slog.Default()
	}
	ctx = nonNilTSOContext(ctx)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	lastError := ""
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			err := reloadTSOModeFile(path, controller)
			lastError = reportTSOModeReloadError(ctx, path, lastError, err, controller, logger)
		}
	}
}

func reloadTSOModeFile(path string, controller *TSORuntimeController) error {
	mode, err := ReadTSOModeFile(path)
	if err != nil {
		return err
	}
	return errors.WithStack(controller.ApplyMode(mode))
}

func reportTSOModeReloadError(
	ctx context.Context,
	path string,
	lastError string,
	err error,
	controller *TSORuntimeController,
	logger *slog.Logger,
) string {
	if err == nil {
		return ""
	}
	if !errors.Is(err, ErrTSOModeRollback) && !errors.Is(err, ErrUnsafeTSOModeTransition) {
		controller.observeReload(classifyTSOModeReloadError(err))
	}
	if lastError == err.Error() {
		return lastError
	}
	logger.ErrorContext(ctx, "tso mode reload rejected",
		slog.String("path", path),
		slog.String("current_mode", controller.CurrentMode().String()),
		slog.Any("err", err),
	)
	return err.Error()
}

func classifyTSOModeReloadError(err error) string {
	switch {
	case errors.Is(err, ErrInvalidTSOMode):
		return "parse_error"
	case errors.Is(err, ErrTSOModeRollback), errors.Is(err, ErrUnsafeTSOModeTransition):
		return "rejected"
	default:
		return "read_error"
	}
}
