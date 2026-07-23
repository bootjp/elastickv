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
}

func NewTSOStateMachine(hlc *HLC) *TSOStateMachine {
	return &TSOStateMachine{hlc: hlc}
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
	ceilingMs := int64(binary.BigEndian.Uint64(data[1:])) //nolint:gosec // value is a Unix ms timestamp encoded as uint64.
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
		ceilingMs = int64(binary.BigEndian.Uint64(payload[:hlcLeasePayloadLen])) //nolint:gosec // legacy snapshot value.
	case tsoSnapshotV2Len:
		ceilingMs = int64(binary.BigEndian.Uint64(payload[:hlcLeasePayloadLen])) //nolint:gosec // snapshot value.
		allocationFloor = binary.BigEndian.Uint64(payload[hlcLeasePayloadLen:])
	case tsoSnapshotV3Len:
		ceilingMs = int64(binary.BigEndian.Uint64(payload[:hlcLeasePayloadLen])) //nolint:gosec // snapshot value.
		allocationFloor = binary.BigEndian.Uint64(payload[hlcLeasePayloadLen:tsoSnapshotV2Len])
		var err error
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
	ceilingMs := int64(binary.BigEndian.Uint64(payload[:hlcLeasePayloadLen])) //nolint:gosec // snapshot value.
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
	if err != nil || !legacy {
		return legacy, err
	}
	ceiling, _, err := ReadSnapshotHeader(br)
	if err != nil {
		return true, errors.Wrap(err, "tso fsm snapshot: read legacy kv fsm header")
	}
	if _, err := io.Copy(io.Discard, br); err != nil {
		return true, errors.Wrap(err, "tso fsm snapshot: drain legacy kv fsm payload")
	}
	ceilingMs := int64(ceiling) //nolint:gosec // validated below before use.
	if ceilingMs < 0 {
		return true, errors.Wrapf(ErrTSOStateMachineInvalidEntry, "tso fsm snapshot: negative legacy ceiling %d", ceilingMs)
	}
	if f == nil || ceilingMs == 0 {
		return true, nil
	}
	f.restoreSnapshotState(ceilingMs, tsoLeaseAllocationFloor(ceilingMs), false, false, 0)
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
	default:
		return false, nil
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
		f.phaseDFloor.Store(phaseDFloor)
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
	binary.BigEndian.PutUint64(buf, uint64(ceilingMs)) //nolint:gosec // ceilingMs is a Unix ms timestamp.
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

func (s *tsoFSMSnapshot) Close() error {
	return nil
}
