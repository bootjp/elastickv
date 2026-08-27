package kv

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"sync/atomic"

	"github.com/bootjp/elastickv/internal/encryption/fsmwire"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
)

var _ raftengine.StateMachine = (*TSOStateMachine)(nil)
var _ raftengine.Snapshot = (*tsoFSMSnapshot)(nil)
var _ raftengine.VolatileEntryClassifier = (*TSOStateMachine)(nil)

var (
	ErrTSOStateMachineInvalidEntry      = errors.New("tso fsm: invalid entry")
	ErrTSOLegacyEncryptionEntryRejected = errors.New("tso fsm: legacy encryption entry rejected")
)

const (
	// tsoAllocationFloorEnvelope starts in kvFSM's fail-closed encryption
	// range so old data-group FSMs halt on a misrouted entry. The remaining
	// magic and version bytes keep it distinct from every encryption opcode.
	tsoAllocationFloorEnvelope = "\x07TSOF\x01"
	// tsoCutoverEnvelope uses the same legacy fail-closed prefix but a distinct
	// magic, so an encryption entry cannot become a one-way TSO state change.
	tsoCutoverEnvelope = "\x07TSOC\x01"
	// tsoPhaseDEnvelope retains the rolling-upgrade halt prefix and gives the
	// irreversible Phase-D marker its own exact wire identity.
	tsoPhaseDEnvelope = "\x07TSOD\x01"
	tsoSnapshotV1Len  = hlcLeasePayloadLen
	tsoSnapshotV2Len  = hlcLeasePayloadLen * 2
	tsoSnapshotV3Len  = tsoSnapshotV2Len + 1
	tsoSnapshotV4Len  = tsoSnapshotV3Len + 1 + hlcLeasePayloadLen

	maxHLCPhysicalMillis = int64((uint64(1) << hlcPhysicalBits) - 1)
)

// TSOStateMachine is the minimal state machine for the dedicated timestamp
// group. It accepts HLC lease-renewal entries plus explicit allocation-floor
// entries. The HLC is only a volatile mirror; snapshots are sourced from the
// TSO FSM's own applied state so unrelated shard-group lease renewals cannot
// advance group-0 state outside the group-0 log.
type TSOStateMachine struct {
	hlc             *HLC
	ceilingMs       atomic.Int64
	allocationFloor atomic.Uint64
	cutoverActive   atomic.Bool
	phaseDActive    atomic.Bool
	phaseDFloor     atomic.Uint64
	// observer publishes durable marker state from the apply and restore
	// paths. Without it the gauges only move on a successful reservation or a
	// runtime mode transition, so a replica that applies a marker but serves no
	// allocations -- and, under the backward-compatible startup flags, runs no
	// mode-reload loop -- keeps reporting cutover=0 / phase_d=0 indefinitely.
	observer atomic.Pointer[tsoDurableStateObserverSlot]
}

// TSODurableStateObserver is the narrow slice of TSOObserver the state machine
// needs. Keeping it separate lets the FSM publish without depending on the
// allocation-latency surface.
type TSODurableStateObserver interface {
	ObserveTSODurableState(cutoverActive, phaseDActive bool)
}

type tsoDurableStateObserverSlot struct {
	observer TSODurableStateObserver
}

// TSOStateMachineOption configures optional state-machine wiring.
type TSOStateMachineOption func(*TSOStateMachine)

// WithTSODurableStateObserver publishes cutover / phase-D gauges whenever the
// markers are applied or restored.
func WithTSODurableStateObserver(observer TSODurableStateObserver) TSOStateMachineOption {
	return func(f *TSOStateMachine) {
		if f == nil || observer == nil {
			return
		}
		f.observer.Store(&tsoDurableStateObserverSlot{observer: observer})
	}
}

// SetDurableStateObserver installs the observer after construction. Wiring
// happens where the metrics registry is in scope rather than at the group
// builder, which several test harnesses construct without one.
func (f *TSOStateMachine) SetDurableStateObserver(observer TSODurableStateObserver) {
	if f == nil || observer == nil {
		return
	}
	f.observer.Store(&tsoDurableStateObserverSlot{observer: observer})
	// Publish immediately: the markers may already have been applied or
	// restored before the observer existed, which is precisely the stale-gauge
	// case this addresses.
	f.observeDurableState()
}

// observeDurableState publishes the current marker state. It is called on every
// marker apply rather than only on transitions: the gauges are idempotent, the
// markers are rare, and replaying an already-set marker after restart is
// exactly when the gauge needs re-publishing.
func (f *TSOStateMachine) observeDurableState() {
	if f == nil {
		return
	}
	slot := f.observer.Load()
	if slot == nil || slot.observer == nil {
		return
	}
	slot.observer.ObserveTSODurableState(f.cutoverActive.Load(), f.phaseDActive.Load())
}

// NewTSOStateMachine constructs the dedicated TSO FSM over the shared HLC.
func NewTSOStateMachine(hlc *HLC, opts ...TSOStateMachineOption) *TSOStateMachine {
	f := &TSOStateMachine{hlc: hlc}
	for _, opt := range opts {
		opt(f)
	}
	return f
}

func (f *TSOStateMachine) Apply(data []byte) any {
	if len(data) == 0 {
		return haltErr(errors.Wrap(ErrTSOStateMachineInvalidEntry, "empty entry"))
	}
	switch {
	case data[0] == raftEncodeHLCLease:
		return f.applyLeaseEntry(data)
	case bytes.HasPrefix(data, []byte(tsoAllocationFloorEnvelope)):
		return f.applyAllocationFloorEntry(data)
	case bytes.Equal(data, []byte(tsoCutoverEnvelope)):
		return f.applyCutoverEntry(data)
	case bytes.HasPrefix(data, []byte(tsoPhaseDEnvelope)):
		return f.applyPhaseDEntry(data)
	case data[0] >= fsmwire.OpEncryptionMin && data[0] <= fsmwire.OpEncryptionMax:
		return rejectLegacyTSOEncryptionEntry(data)
	default:
		return haltErr(errors.Wrapf(ErrTSOStateMachineInvalidEntry, "unexpected tag 0x%02x", data[0]))
	}
}

// rejectLegacyTSOEncryptionEntry lets an upgraded group 0 advance past a
// control entry committed while it still ran kvFSM. New group-0 listeners do
// not expose encryption mutators, so these entries can only be historical.
// Validate the old wire shape before returning an ordinary apply response:
// malformed control bytes still halt, while a valid obsolete entry is
// deterministically rejected without mutating TSO state or halting replay.
func rejectLegacyTSOEncryptionEntry(data []byte) any {
	var err error
	switch data[0] {
	case fsmwire.OpRegistration:
		_, err = fsmwire.DecodeRegistration(data[1:])
	case fsmwire.OpBootstrap:
		_, err = fsmwire.DecodeBootstrap(data[1:])
	case fsmwire.OpRotation:
		_, err = fsmwire.DecodeRotation(data[1:])
	default:
		return haltErr(errors.Wrapf(ErrTSOStateMachineInvalidEntry,
			"reserved encryption entry tag 0x%02x", data[0]))
	}
	if err != nil {
		return haltErr(errors.Wrapf(ErrTSOStateMachineInvalidEntry,
			"malformed legacy encryption entry tag 0x%02x: %v", data[0], err))
	}
	return errors.Wrapf(ErrTSOLegacyEncryptionEntryRejected,
		"tag 0x%02x is not part of dedicated TSO state", data[0])
}

func (f *TSOStateMachine) applyLeaseEntry(data []byte) any {
	if len(data) != hlcLeaseEntryLen {
		return haltErr(errors.Wrapf(ErrTSOStateMachineInvalidEntry, "expected HLC lease entry length %d, got %d", hlcLeaseEntryLen, len(data)))
	}
	ceilingMs, err := decodeTSOCeiling(binary.BigEndian.Uint64(data[1:]), "HLC lease")
	if err != nil {
		return haltErr(err)
	}
	if ceilingMs <= 0 {
		return haltErr(errors.Wrapf(ErrTSOStateMachineInvalidEntry, "non-positive HLC lease ceiling %d", ceilingMs))
	}
	if f != nil {
		f.applyLeaseCeiling(ceilingMs)
	}
	return nil
}

func (f *TSOStateMachine) applyAllocationFloorEntry(data []byte) any {
	expectedLen := len(tsoAllocationFloorEnvelope) + hlcLeasePayloadLen
	if len(data) != expectedLen {
		return haltErr(errors.Wrapf(ErrTSOStateMachineInvalidEntry, "expected TSO allocation floor entry length %d, got %d", expectedLen, len(data)))
	}
	floor := binary.BigEndian.Uint64(data[len(tsoAllocationFloorEnvelope):])
	if floor == 0 {
		return haltErr(errors.Wrap(ErrTSOStateMachineInvalidEntry, "zero TSO allocation floor"))
	}
	if f != nil {
		f.applyAllocationFloor(floor)
	}
	return nil
}

func (f *TSOStateMachine) applyCutoverEntry(data []byte) any {
	if !bytes.Equal(data, []byte(tsoCutoverEnvelope)) {
		return haltErr(errors.Wrapf(ErrTSOStateMachineInvalidEntry,
			"invalid TSO cutover envelope length %d", len(data)))
	}
	if f != nil {
		f.cutoverActive.Store(true)
		f.observeDurableState()
	}
	return nil
}

func (f *TSOStateMachine) applyPhaseDEntry(data []byte) any {
	expectedLen := len(tsoPhaseDEnvelope) + hlcLeasePayloadLen
	if len(data) != expectedLen {
		return haltErr(errors.Wrapf(ErrTSOStateMachineInvalidEntry,
			"expected TSO phase-D entry length %d, got %d", expectedLen, len(data)))
	}
	if f == nil {
		return nil
	}
	if !f.cutoverActive.Load() {
		return haltErr(errors.Wrap(ErrTSOStateMachineInvalidEntry,
			"TSO phase-D marker requires the durable cutover marker"))
	}
	floor := binary.BigEndian.Uint64(data[len(tsoPhaseDEnvelope):])
	if f.phaseDActive.Load() && f.phaseDFloor.Load() != floor {
		return haltErr(errors.Wrapf(ErrTSOStateMachineInvalidEntry,
			"TSO phase-D floor changed from %d to %d", f.phaseDFloor.Load(), floor))
	}
	if !f.phaseDActive.Load() && floor < f.allocationFloor.Load() {
		return haltErr(errors.Wrapf(ErrTSOStateMachineInvalidEntry,
			"TSO phase-D floor %d is below allocation floor %d", floor, f.allocationFloor.Load()))
	}
	f.phaseDFloor.Store(floor)
	f.phaseDActive.Store(true)
	f.observeDurableState()
	return nil
}

// AllocationFloor returns the highest timestamp window end applied by the
// dedicated TSO group. It is consensus-owned state, unlike HLC.Current().
func (f *TSOStateMachine) AllocationFloor() uint64 {
	if f == nil {
		return 0
	}
	return f.allocationFloor.Load()
}

// CutoverActive reports whether production issuance has durably crossed the
// one-way migration marker. The marker cannot be cleared without a separate
// cluster-wide rollback protocol.
func (f *TSOStateMachine) CutoverActive() bool {
	return f != nil && f.cutoverActive.Load()
}

// PhaseDActive reports whether the compatibility window has been durably
// closed. Once active, data-shard HLC renewal and caller-supplied cross-shard
// timestamps may no longer use legacy issuance semantics.
func (f *TSOStateMachine) PhaseDActive() bool {
	return f != nil && f.phaseDActive.Load()
}

// PhaseDFloor is the highest allocation floor that existed when Phase D was
// activated. Only timestamps reserved strictly above it are valid M7 durable
// read/start allocations.
func (f *TSOStateMachine) PhaseDFloor() uint64 {
	if f == nil {
		return 0
	}
	return f.phaseDFloor.Load()
}

func (f *TSOStateMachine) Snapshot() (raftengine.Snapshot, error) {
	var ceilingMs int64
	var allocationFloor uint64
	var cutoverActive bool
	var phaseDActive bool
	var phaseDFloor uint64
	if f != nil {
		ceilingMs = f.ceilingMs.Load()
		allocationFloor = f.allocationFloor.Load()
		cutoverActive = f.cutoverActive.Load()
		phaseDActive = f.phaseDActive.Load()
		phaseDFloor = f.phaseDFloor.Load()
	}
	return &tsoFSMSnapshot{
		ceilingMs:       ceilingMs,
		allocationFloor: allocationFloor,
		cutoverActive:   cutoverActive,
		phaseDActive:    phaseDActive,
		phaseDFloor:     phaseDFloor,
	}, nil
}

func (f *TSOStateMachine) Restore(r io.Reader) error {
	if r == nil {
		return errors.New("tso fsm snapshot: reader is nil")
	}
	br := tsoSnapshotReader(r)
	if legacy, err := restoreLegacyKVFSMSnapshot(f, br); legacy || err != nil {
		return err
	}
	ceilingMs, allocationFloor, cutoverActive, phaseDActive, phaseDFloor, err := readTSOSnapshotState(br)
	if err != nil {
		return err
	}
	if f != nil {
		f.restoreSnapshotState(ceilingMs, allocationFloor, cutoverActive, phaseDActive, phaseDFloor)
	}
	return nil
}

func tsoSnapshotReader(r io.Reader) *bufio.Reader {
	if br, ok := r.(*bufio.Reader); ok {
		return br
	}
	return bufio.NewReader(r)
}

func readTSOSnapshotState(br *bufio.Reader) (int64, uint64, bool, bool, uint64, error) {
	payload, err := io.ReadAll(io.LimitReader(br, tsoSnapshotV4Len+1))
	if err != nil {
		return 0, 0, false, false, 0, errors.Wrap(err, "restore tso fsm snapshot")
	}
	ceilingMs, allocationFloor, cutoverActive, phaseDActive, phaseDFloor, legacySnapshot, err := decodeTSOSnapshotPayload(payload)
	if err != nil {
		return 0, 0, false, false, 0, err
	}
	if ceilingMs < 0 {
		return 0, 0, false, false, 0, errors.Wrapf(ErrTSOStateMachineInvalidEntry, "tso fsm snapshot: negative ceiling %d", ceilingMs)
	}
	if legacySnapshot && ceilingMs > 0 {
		allocationFloor = tsoLeaseAllocationFloor(ceilingMs)
	}
	return ceilingMs, allocationFloor, cutoverActive, phaseDActive, phaseDFloor, nil
}

func decodeTSOSnapshotPayload(payload []byte) (int64, uint64, bool, bool, uint64, bool, error) {
	var ceilingMs int64
	var allocationFloor uint64
	var cutoverActive bool
	var phaseDActive bool
	var phaseDFloor uint64
	var legacySnapshot bool
	switch len(payload) {
	case tsoSnapshotV1Len:
		legacySnapshot = true
		var err error
		ceilingMs, err = decodeTSOCeiling(binary.BigEndian.Uint64(payload[:hlcLeasePayloadLen]), "legacy snapshot")
		if err != nil {
			return 0, 0, false, false, 0, false, err
		}
	case tsoSnapshotV2Len:
		var err error
		ceilingMs, err = decodeTSOCeiling(binary.BigEndian.Uint64(payload[:hlcLeasePayloadLen]), "snapshot")
		if err != nil {
			return 0, 0, false, false, 0, false, err
		}
		allocationFloor = binary.BigEndian.Uint64(payload[hlcLeasePayloadLen:])
	case tsoSnapshotV3Len:
		var err error
		ceilingMs, err = decodeTSOCeiling(binary.BigEndian.Uint64(payload[:hlcLeasePayloadLen]), "snapshot")
		if err != nil {
			return 0, 0, false, false, 0, false, err
		}
		allocationFloor = binary.BigEndian.Uint64(payload[hlcLeasePayloadLen:tsoSnapshotV2Len])
		cutoverActive, err = decodeTSOCutoverByte(payload[tsoSnapshotV2Len])
		if err != nil {
			return 0, 0, false, false, 0, false, err
		}
	case tsoSnapshotV4Len:
		return decodeTSOSnapshotV4(payload)
	default:
		return 0, 0, false, false, 0, false, errors.Wrapf(ErrTSOStateMachineInvalidEntry,
			"tso fsm snapshot: expected %d, %d, %d, or %d bytes, got %d",
			tsoSnapshotV1Len, tsoSnapshotV2Len, tsoSnapshotV3Len, tsoSnapshotV4Len, len(payload))
	}
	return ceilingMs, allocationFloor, cutoverActive, phaseDActive, phaseDFloor, legacySnapshot, nil
}

func decodeTSOSnapshotV4(payload []byte) (int64, uint64, bool, bool, uint64, bool, error) {
	ceilingMs, err := decodeTSOCeiling(binary.BigEndian.Uint64(payload[:hlcLeasePayloadLen]), "snapshot")
	if err != nil {
		return 0, 0, false, false, 0, false, err
	}
	allocationFloor := binary.BigEndian.Uint64(payload[hlcLeasePayloadLen:tsoSnapshotV2Len])
	cutoverActive, err := decodeTSOCutoverByte(payload[tsoSnapshotV2Len])
	if err != nil {
		return 0, 0, false, false, 0, false, err
	}
	phaseDActive, err := decodeTSOBooleanByte("phase-D", payload[tsoSnapshotV3Len])
	if err != nil {
		return 0, 0, false, false, 0, false, err
	}
	phaseDFloor := binary.BigEndian.Uint64(payload[tsoSnapshotV3Len+1:])
	if phaseDActive && !cutoverActive {
		return 0, 0, false, false, 0, false, errors.Wrap(ErrTSOStateMachineInvalidEntry,
			"tso fsm snapshot: phase-D active without cutover")
	}
	if !phaseDActive && phaseDFloor != 0 {
		return 0, 0, false, false, 0, false, errors.Wrapf(ErrTSOStateMachineInvalidEntry,
			"tso fsm snapshot: inactive phase-D has floor %d", phaseDFloor)
	}
	return ceilingMs, allocationFloor, cutoverActive, phaseDActive, phaseDFloor, false, nil
}

func decodeTSOCeiling(raw uint64, field string) (int64, error) {
	if raw > uint64(maxHLCPhysicalMillis) {
		return 0, errors.Wrapf(ErrTSOStateMachineInvalidEntry,
			"tso fsm: %s physical ceiling %d exceeds max %d", field, raw, maxHLCPhysicalMillis)
	}
	return int64(raw), nil // #nosec G115 -- raw is bounded by maxHLCPhysicalMillis above.
}

func decodeTSOCutoverByte(value byte) (bool, error) {
	return decodeTSOBooleanByte("cutover", value)
}

func decodeTSOBooleanByte(name string, value byte) (bool, error) {
	switch value {
	case 0:
		return false, nil
	case 1:
		return true, nil
	default:
		return false, errors.Wrapf(ErrTSOStateMachineInvalidEntry,
			"tso fsm snapshot: invalid %s byte %d", name, value)
	}
}

// restoreLegacyKVFSMSnapshot migrates snapshots produced while reserved group
// 0 still used kvFSM as a compatibility bridge. Only the HLC header is TSO
// state; the empty MVCC payload is drained so the raft engine can verify the
// complete snapshot CRC. Non-kvFSM snapshots are left untouched in br.
func restoreLegacyKVFSMSnapshot(f *TSOStateMachine, br *bufio.Reader) (bool, error) {
	legacy, err := hasLegacyKVFSMSnapshotHeader(br)
	if err != nil {
		return false, err
	}
	if !legacy {
		return restoreHeaderlessLegacyKVFSMSnapshot(br)
	}
	ceiling, _, err := ReadSnapshotHeader(br)
	if err != nil {
		return true, errors.Wrap(err, "tso fsm snapshot: read legacy kv fsm header")
	}
	if _, err := io.Copy(io.Discard, br); err != nil {
		return true, errors.Wrap(err, "tso fsm snapshot: drain legacy kv fsm payload")
	}
	ceilingMs, err := decodeTSOCeiling(ceiling, "legacy kv fsm snapshot")
	if err != nil {
		return true, err
	}
	if f == nil || ceilingMs == 0 {
		return true, nil
	}
	f.restoreSnapshotState(ceilingMs, tsoLeaseAllocationFloor(ceilingMs), false, false, 0)
	return true, nil
}

func restoreHeaderlessLegacyKVFSMSnapshot(br *bufio.Reader) (bool, error) {
	headerless, err := hasHeaderlessLegacyKVFSMSnapshotPayload(br)
	if err != nil || !headerless {
		return headerless, err
	}
	if _, err := io.Copy(io.Discard, br); err != nil {
		return true, errors.Wrap(err, "tso fsm snapshot: drain headerless legacy kv fsm payload")
	}
	return true, nil
}

func hasLegacyKVFSMSnapshotHeader(br *bufio.Reader) (bool, error) {
	peeked, err := br.Peek(len(hlcSnapshotMagic))
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return false, nil
		}
		return false, errors.Wrap(err, "tso fsm snapshot: peek legacy header")
	}
	switch {
	case isV1Magic(peeked):
		return true, nil
	case isV2Magic(peeked):
		return true, nil
	case isUnknownEKVTHLC(peeked):
		return true, nil
	case isLegacyKVFSMStoreSnapshot(peeked):
		return true, nil
	default:
		return false, nil
	}
}

func hasHeaderlessLegacyKVFSMSnapshotPayload(br *bufio.Reader) (bool, error) {
	peeked, err := br.Peek(len(hlcSnapshotMagic))
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return false, errors.Wrap(err, "tso fsm snapshot: peek headerless legacy payload")
	}
	return isLegacyKVFSMStoreSnapshot(peeked), nil
}

func isLegacyKVFSMStoreSnapshot(peeked []byte) bool {
	for _, magic := range legacyKVFSMStoreSnapshotMagics() {
		if bytes.Equal(peeked, magic) {
			return true
		}
	}
	return false
}

func legacyKVFSMStoreSnapshotMagics() [][]byte {
	return [][]byte{
		[]byte("EKVMVCC2"),
		[]byte("EKVPBBL1"),
		[]byte("EKVSSTI1"),
	}
}

func (f *TSOStateMachine) IsVolatileOnlyPayload(payload []byte) bool {
	if bytes.Equal(payload, []byte(tsoCutoverEnvelope)) {
		return true
	}
	return len(payload) == hlcLeaseEntryLen && payload[0] == raftEncodeHLCLease ||
		len(payload) == len(tsoAllocationFloorEnvelope)+hlcLeasePayloadLen &&
			bytes.HasPrefix(payload, []byte(tsoAllocationFloorEnvelope)) ||
		len(payload) == len(tsoPhaseDEnvelope)+hlcLeasePayloadLen &&
			bytes.HasPrefix(payload, []byte(tsoPhaseDEnvelope))
}

func (f *TSOStateMachine) applyLeaseCeiling(ceilingMs int64) {
	if f == nil || ceilingMs <= 0 {
		return
	}
	storeMaxInt64(&f.ceilingMs, ceilingMs)
	if f.hlc != nil {
		f.hlc.SetPhysicalCeiling(f.ceilingMs.Load())
	}
}

func (f *TSOStateMachine) applyAllocationFloor(floor uint64) {
	if f == nil || floor == 0 {
		return
	}
	storeMaxUint64(&f.allocationFloor, floor)
	if f.hlc != nil {
		f.hlc.Observe(f.allocationFloor.Load())
	}
}

func (f *TSOStateMachine) restoreSnapshotState(
	ceilingMs int64,
	allocationFloor uint64,
	cutoverActive bool,
	phaseDActive bool,
	phaseDFloor uint64,
) {
	if f == nil {
		return
	}
	if ceilingMs > 0 {
		storeMaxInt64(&f.ceilingMs, ceilingMs)
	}
	if allocationFloor > 0 {
		storeMaxUint64(&f.allocationFloor, allocationFloor)
	}
	if cutoverActive {
		f.cutoverActive.Store(true)
	}
	if phaseDActive {
		// The Phase-D floor is immutable once active: applyPhaseDMarker halts
		// apply on a marker that changes it. A snapshot carrying a lower floor
		// is therefore older than what this replica already applied, and
		// regressing to it would reclassify every timestamp in between as
		// post-Phase-D -- ValidateDurableTimestamp would start accepting values
		// it had been rejecting. Take the higher, the way the ceiling and the
		// allocation floor above already do.
		storeMaxUint64(&f.phaseDFloor, phaseDFloor)
		f.phaseDActive.Store(true)
	}
	if f.hlc != nil {
		if currentCeiling := f.ceilingMs.Load(); currentCeiling > 0 {
			f.hlc.SetPhysicalCeiling(currentCeiling)
		}
		if currentFloor := f.allocationFloor.Load(); currentFloor > 0 {
			f.hlc.Observe(currentFloor)
		}
	}
	// A replica that joins by snapshot never replays the marker entries, so the
	// gauges have to be published here too.
	f.observeDurableState()
}

func storeMaxInt64(value *atomic.Int64, candidate int64) {
	for {
		current := value.Load()
		if candidate <= current {
			return
		}
		if value.CompareAndSwap(current, candidate) {
			return
		}
	}
}

func storeMaxUint64(value *atomic.Uint64, candidate uint64) {
	for {
		current := value.Load()
		if candidate <= current {
			return
		}
		if value.CompareAndSwap(current, candidate) {
			return
		}
	}
}

func tsoLeaseAllocationFloor(ceilingMs int64) uint64 {
	return (nonNegativeUint64(ceilingMs) << hlcLogicalBits) | hlcLogicalMask
}

func marshalTSOAllocationFloor(floor uint64) []byte {
	out := make([]byte, len(tsoAllocationFloorEnvelope)+hlcLeasePayloadLen)
	copy(out, tsoAllocationFloorEnvelope)
	binary.BigEndian.PutUint64(out[len(tsoAllocationFloorEnvelope):], floor)
	return out
}

func marshalTSOCutover() []byte {
	return []byte(tsoCutoverEnvelope)
}

func marshalTSOPhaseD(floor uint64) []byte {
	out := make([]byte, len(tsoPhaseDEnvelope)+hlcLeasePayloadLen)
	copy(out, tsoPhaseDEnvelope)
	binary.BigEndian.PutUint64(out[len(tsoPhaseDEnvelope):], floor)
	return out
}

type tsoFSMSnapshot struct {
	ceilingMs       int64
	allocationFloor uint64
	cutoverActive   bool
	phaseDActive    bool
	phaseDFloor     uint64
}

func (s *tsoFSMSnapshot) WriteTo(w io.Writer) (int64, error) {
	if w == nil {
		return 0, errors.New("tso fsm snapshot: writer is nil")
	}
	var ceilingMs int64
	var allocationFloor uint64
	var cutoverActive bool
	var phaseDActive bool
	var phaseDFloor uint64
	if s != nil {
		ceilingMs = s.ceilingMs
		allocationFloor = s.allocationFloor
		cutoverActive = s.cutoverActive
		phaseDActive = s.phaseDActive
		phaseDFloor = s.phaseDFloor
	}
	snapshotLen := tsoSnapshotV3Len
	if phaseDActive {
		snapshotLen = tsoSnapshotV4Len
	}
	buf := make([]byte, snapshotLen)
	if err := encodeTSOCeiling(buf, ceilingMs); err != nil {
		return 0, err
	}
	binary.BigEndian.PutUint64(buf[hlcLeasePayloadLen:tsoSnapshotV2Len], allocationFloor)
	if cutoverActive {
		buf[tsoSnapshotV2Len] = 1
	}
	if phaseDActive {
		buf[tsoSnapshotV3Len] = 1
		binary.BigEndian.PutUint64(buf[tsoSnapshotV3Len+1:], phaseDFloor)
	}
	n, err := w.Write(buf)
	if err != nil {
		return int64(n), errors.Wrap(err, "write tso fsm snapshot")
	}
	if n != len(buf) {
		return int64(n), errors.WithStack(io.ErrShortWrite)
	}
	return int64(n), nil
}

func encodeTSOCeiling(dst []byte, ceilingMs int64) error {
	if len(dst) < hlcLeasePayloadLen {
		return errors.Wrapf(ErrTSOStateMachineInvalidEntry,
			"tso fsm snapshot: ceiling buffer too short: %d", len(dst))
	}
	if ceilingMs < 0 || ceilingMs > maxHLCPhysicalMillis {
		return errors.Wrapf(ErrTSOStateMachineInvalidEntry,
			"tso fsm snapshot: invalid ceiling %d", ceilingMs)
	}
	binary.BigEndian.PutUint64(dst, uint64(ceilingMs)) // #nosec G115 -- ceilingMs is validated non-negative above.
	return nil
}

func (s *tsoFSMSnapshot) Close() error {
	return nil
}
