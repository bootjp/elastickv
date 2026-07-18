package kv

import (
	"bufio"
	"encoding/binary"
	"io"
	"sync/atomic"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
)

var _ raftengine.StateMachine = (*TSOStateMachine)(nil)
var _ raftengine.VolatileEntryClassifier = (*TSOStateMachine)(nil)

var ErrTSOStateMachineInvalidEntry = errors.New("tso fsm: invalid entry")

const (
	// raftEncodeTSOAllocationFloor is TSO-FSM-local. It intentionally uses a
	// byte in kvFSM's fail-closed encryption-reserved range so misrouted data
	// group entries cannot fall through to proto3 decoding.
	raftEncodeTSOAllocationFloor byte = 0x07
	tsoSnapshotV1Len                  = hlcLeasePayloadLen
	tsoSnapshotV2Len                  = hlcLeasePayloadLen * 2
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
}

func NewTSOStateMachine(hlc *HLC) *TSOStateMachine {
	return &TSOStateMachine{hlc: hlc}
}

func (f *TSOStateMachine) Apply(data []byte) any {
	if len(data) == 0 {
		return haltErr(errors.Wrap(ErrTSOStateMachineInvalidEntry, "empty entry"))
	}
	switch data[0] {
	case raftEncodeHLCLease:
		return f.applyLeaseEntry(data)
	case raftEncodeTSOAllocationFloor:
		return f.applyAllocationFloorEntry(data)
	default:
		return haltErr(errors.Wrapf(ErrTSOStateMachineInvalidEntry, "unexpected tag 0x%02x", data[0]))
	}
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
	if len(data) != hlcLeaseEntryLen {
		return haltErr(errors.Wrapf(ErrTSOStateMachineInvalidEntry, "expected TSO allocation floor entry length %d, got %d", hlcLeaseEntryLen, len(data)))
	}
	floor := binary.BigEndian.Uint64(data[1:])
	if floor == 0 {
		return haltErr(errors.Wrap(ErrTSOStateMachineInvalidEntry, "zero TSO allocation floor"))
	}
	if f != nil {
		f.applyAllocationFloor(floor)
	}
	return nil
}

func (f *TSOStateMachine) Snapshot() (raftengine.Snapshot, error) {
	var ceilingMs int64
	var allocationFloor uint64
	if f != nil {
		ceilingMs = f.ceilingMs.Load()
		allocationFloor = f.allocationFloor.Load()
	}
	return &tsoFSMSnapshot{ceilingMs: ceilingMs, allocationFloor: allocationFloor}, nil
}

func (f *TSOStateMachine) Restore(r io.Reader) error {
	if r == nil {
		return errors.New("tso fsm snapshot: reader is nil")
	}
	br := tsoSnapshotReader(r)
	if legacy, err := restoreLegacyKVFSMSnapshot(f, br); legacy || err != nil {
		return err
	}
	ceilingMs, allocationFloor, err := readTSOSnapshotState(br)
	if err != nil {
		return err
	}
	if f != nil {
		f.restoreSnapshotState(ceilingMs, allocationFloor)
	}
	return nil
}

func tsoSnapshotReader(r io.Reader) *bufio.Reader {
	if br, ok := r.(*bufio.Reader); ok {
		return br
	}
	return bufio.NewReader(r)
}

func readTSOSnapshotState(br *bufio.Reader) (int64, uint64, error) {
	payload, err := io.ReadAll(io.LimitReader(br, tsoSnapshotV2Len+1))
	if err != nil {
		return 0, 0, errors.Wrap(err, "restore tso fsm snapshot")
	}
	var ceilingMs int64
	var allocationFloor uint64
	var legacySnapshot bool
	switch len(payload) {
	case tsoSnapshotV1Len:
		legacySnapshot = true
		ceilingMs = int64(binary.BigEndian.Uint64(payload[:hlcLeasePayloadLen])) //nolint:gosec // legacy snapshot value.
	case tsoSnapshotV2Len:
		ceilingMs = int64(binary.BigEndian.Uint64(payload[:hlcLeasePayloadLen])) //nolint:gosec // snapshot value.
		allocationFloor = binary.BigEndian.Uint64(payload[hlcLeasePayloadLen:])
	default:
		return 0, 0, errors.Wrapf(ErrTSOStateMachineInvalidEntry, "tso fsm snapshot: expected %d or %d bytes, got %d", tsoSnapshotV1Len, tsoSnapshotV2Len, len(payload))
	}
	if ceilingMs < 0 {
		return 0, 0, errors.Wrapf(ErrTSOStateMachineInvalidEntry, "tso fsm snapshot: negative ceiling %d", ceilingMs)
	}
	if legacySnapshot && ceilingMs > 0 {
		allocationFloor = tsoLeaseAllocationFloor(ceilingMs)
	}
	return ceilingMs, allocationFloor, nil
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
	f.restoreSnapshotState(ceilingMs, tsoLeaseAllocationFloor(ceilingMs))
	return true, nil
}

func hasLegacyKVFSMSnapshotHeader(br *bufio.Reader) (bool, error) {
	peeked, err := br.Peek(len(hlcSnapshotMagic))
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
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
	return len(payload) == hlcLeaseEntryLen &&
		(payload[0] == raftEncodeHLCLease || payload[0] == raftEncodeTSOAllocationFloor)
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

func (f *TSOStateMachine) restoreSnapshotState(ceilingMs int64, allocationFloor uint64) {
	if f == nil {
		return
	}
	if ceilingMs > 0 {
		storeMaxInt64(&f.ceilingMs, ceilingMs)
	}
	if allocationFloor > 0 {
		storeMaxUint64(&f.allocationFloor, allocationFloor)
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
	out := make([]byte, hlcLeaseEntryLen)
	out[0] = raftEncodeTSOAllocationFloor
	binary.BigEndian.PutUint64(out[1:], floor)
	return out
}

type tsoFSMSnapshot struct {
	ceilingMs       int64
	allocationFloor uint64
}

func (s *tsoFSMSnapshot) WriteTo(w io.Writer) (int64, error) {
	if w == nil {
		return 0, errors.New("tso fsm snapshot: writer is nil")
	}
	var ceilingMs int64
	var allocationFloor uint64
	if s != nil {
		ceilingMs = s.ceilingMs
		allocationFloor = s.allocationFloor
	}
	var buf [tsoSnapshotV2Len]byte
	binary.BigEndian.PutUint64(buf[:], uint64(ceilingMs)) //nolint:gosec // ceilingMs is a Unix ms timestamp.
	binary.BigEndian.PutUint64(buf[hlcLeasePayloadLen:], allocationFloor)
	n, err := w.Write(buf[:])
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
