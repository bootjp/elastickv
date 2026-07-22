package kv

import (
	"encoding/binary"
	"io"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
)

const tsoSnapshotLen = 8

var _ raftengine.StateMachine = (*TSOStateMachine)(nil)
var _ raftengine.Snapshot = (*tsoSnapshot)(nil)
var _ raftengine.VolatileEntryClassifier = (*TSOStateMachine)(nil)

// TSOStateMachine is the minimal FSM for the dedicated timestamp-oracle Raft
// group. It tracks only the Raft-agreed HLC physical ceiling; no KV state,
// route catalog, or sidecar state is attached to group 0.
type TSOStateMachine struct {
	hlc *HLC
}

// NewTSOStateMachine constructs the dedicated TSO FSM over the shared HLC.
func NewTSOStateMachine(hlc *HLC) *TSOStateMachine {
	return &TSOStateMachine{hlc: hlc}
}

func (f *TSOStateMachine) Apply(data []byte) any {
	if len(data) == 0 || data[0] != raftEncodeHLCLease {
		return nil
	}
	return f.applyHLCLease(data[1:])
}

func (f *TSOStateMachine) applyHLCLease(data []byte) any {
	if len(data) != hlcLeasePayloadLen {
		return errors.Newf("tso fsm: hlc lease: expected %d bytes, got %d", hlcLeasePayloadLen, len(data)) //nolint:wrapcheck // creating new error, nothing to wrap
	}
	ceilingMs := int64(binary.BigEndian.Uint64(data)) //nolint:gosec // value is a Unix ms timestamp encoded as uint64; fits in int64 for valid deployments.
	if f.hlc != nil && ceilingMs > 0 {
		f.hlc.SetPhysicalCeiling(ceilingMs)
	}
	return nil
}

func (f *TSOStateMachine) Snapshot() (raftengine.Snapshot, error) {
	return &tsoSnapshot{ceilingMs: hlcCeilingFromHLC(f.hlc)}, nil
}

func (f *TSOStateMachine) Restore(r io.Reader) error {
	var buf [tsoSnapshotLen]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return errors.Wrap(err, "tso fsm: restore snapshot")
	}
	var extra [1]byte
	n, err := r.Read(extra[:])
	if err != nil && !errors.Is(err, io.EOF) {
		return errors.Wrap(err, "tso fsm: restore snapshot")
	}
	if n != 0 {
		return errors.New("tso fsm: restore snapshot: trailing bytes") //nolint:wrapcheck // creating new error, nothing to wrap
	}
	ceilingMs := int64(binary.BigEndian.Uint64(buf[:])) //nolint:gosec // value was written from an int64 Unix ms ceiling.
	if f.hlc != nil && ceilingMs > 0 {
		f.hlc.SetPhysicalCeiling(ceilingMs)
	}
	return nil
}

// IsVolatileOnlyPayload classifies HLC lease entries for the cold-start replay
// gate. Re-applying them is monotonic and reconstructs the in-memory ceiling.
func (f *TSOStateMachine) IsVolatileOnlyPayload(payload []byte) bool {
	return len(payload) > 0 && payload[0] == raftEncodeHLCLease
}

type tsoSnapshot struct {
	ceilingMs int64
}

func (s *tsoSnapshot) WriteTo(w io.Writer) (int64, error) {
	var buf [tsoSnapshotLen]byte
	binary.BigEndian.PutUint64(buf[:], uint64(s.ceilingMs)) //nolint:gosec // ceilingMs is a Unix ms timestamp encoded as uint64.
	n, err := w.Write(buf[:])
	if err != nil {
		return int64(n), errors.WithStack(err)
	}
	if n != tsoSnapshotLen {
		return int64(n), io.ErrShortWrite
	}
	return tsoSnapshotLen, nil
}

func (s *tsoSnapshot) Close() error {
	return nil
}
