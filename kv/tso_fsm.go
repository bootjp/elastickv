package kv

import (
	"encoding/binary"
	"io"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
)

var _ raftengine.StateMachine = (*TSOStateMachine)(nil)
var _ raftengine.VolatileEntryClassifier = (*TSOStateMachine)(nil)

var ErrTSOStateMachineInvalidEntry = errors.New("tso fsm: invalid entry")

// TSOStateMachine is the minimal state machine for the dedicated timestamp
// group. It accepts only HLC lease-renewal entries and snapshots the last
// applied physical ceiling as an 8-byte big-endian value.
type TSOStateMachine struct {
	hlc *HLC
}

func NewTSOStateMachine(hlc *HLC) *TSOStateMachine {
	return &TSOStateMachine{hlc: hlc}
}

func (f *TSOStateMachine) Apply(data []byte) any {
	if len(data) != hlcLeaseEntryLen {
		return errors.Wrapf(ErrTSOStateMachineInvalidEntry, "expected %d bytes, got %d", hlcLeaseEntryLen, len(data))
	}
	if data[0] != raftEncodeHLCLease {
		return errors.Wrapf(ErrTSOStateMachineInvalidEntry, "unexpected tag 0x%02x", data[0])
	}
	ceilingMs := int64(binary.BigEndian.Uint64(data[1:])) //nolint:gosec // value is a Unix ms timestamp encoded as uint64.
	if f != nil && f.hlc != nil && ceilingMs > 0 {
		f.hlc.SetPhysicalCeiling(ceilingMs)
	}
	return nil
}

func (f *TSOStateMachine) Snapshot() (raftengine.Snapshot, error) {
	var hlc *HLC
	if f != nil {
		hlc = f.hlc
	}
	return &tsoFSMSnapshot{ceilingMs: hlcCeilingFromHLC(hlc)}, nil
}

func (f *TSOStateMachine) Restore(r io.Reader) error {
	if r == nil {
		return errors.New("tso fsm snapshot: reader is nil")
	}
	var buf [hlcLeasePayloadLen]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return errors.Wrap(err, "restore tso fsm snapshot")
	}
	ceilingMs := int64(binary.BigEndian.Uint64(buf[:])) //nolint:gosec // value is a Unix ms timestamp encoded as uint64.
	if f != nil && f.hlc != nil && ceilingMs > 0 {
		f.hlc.SetPhysicalCeiling(ceilingMs)
	}
	return nil
}

func (f *TSOStateMachine) IsVolatileOnlyPayload(payload []byte) bool {
	return len(payload) == hlcLeaseEntryLen && payload[0] == raftEncodeHLCLease
}

type tsoFSMSnapshot struct {
	ceilingMs int64
}

func (s *tsoFSMSnapshot) WriteTo(w io.Writer) (int64, error) {
	if w == nil {
		return 0, errors.New("tso fsm snapshot: writer is nil")
	}
	var ceilingMs int64
	if s != nil {
		ceilingMs = s.ceilingMs
	}
	var buf [hlcLeasePayloadLen]byte
	binary.BigEndian.PutUint64(buf[:], uint64(ceilingMs)) //nolint:gosec // ceilingMs is a Unix ms timestamp.
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
